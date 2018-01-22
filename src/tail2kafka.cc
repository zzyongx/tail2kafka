#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <map>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#include "logger.h"
#include "sys.h"
#include "runstatus.h"
#include "luactx.h"
#include "cnfctx.h"
#include "inotifyctx.h"
#include "common.h"

LOGGER_INIT();

pid_t spawn(CnfCtx *ctx, CnfCtx *octx);
int runForeGround(CnfCtx *ctx);

int main(int argc, char *argv[])
{
  if (argc != 2) {
    fprintf(stderr, "%s confdir\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *dir = argv[1];
  pid_t pid = -1;
  char errbuf[MAX_ERR_LEN];

  CnfCtx *cnf = CnfCtx::loadCnf(dir, errbuf);
  if (!cnf) {
    fprintf(stderr, "%d:%s load cnf error %s\n", errno, errno ? strerror(errno) : "", errbuf);
    return EXIT_FAILURE;
  }

  Logger::create(cnf->logdir() + "/tail2kafka.log", Logger::DAY, true);

  bool daemonOff = getenv("DAEMON_OFF");

  if (!daemonOff) {
    if (getenv("TAIL2KAFKA_NOSTDIO")) {
      daemon(1, 0);
    } else {
      daemon(1, 1);
    }
  }

  RunStatus *runStatus = RunStatus::create();
  cnf->setRunStatus(runStatus);

  if (daemonOff) return runForeGround(cnf);

  if (!sys::initSingleton(cnf->getPidFile(), errbuf)) {
    log_fatal(0, "init singleton %s", errbuf);
    return EXIT_FAILURE;
  }

  sys::SignalHelper signalHelper(errbuf);

  int signos[] = { SIGTERM, SIGHUP, SIGCHLD };
  RunStatus::Want wants[] = { RunStatus::STOP, RunStatus::RELOAD, RunStatus::START2 };
  if (!signalHelper.signal(runStatus, 3, signos, wants)) {
    log_fatal(errno, "install signal %s", errbuf);
    return EXIT_FAILURE;
  }
  if (!signalHelper.block(SIGCHLD, SIGTERM, SIGHUP, -1)) {
    log_fatal(errno, "block signal %s", errbuf);
    return EXIT_FAILURE;
  }

  int rc = EXIT_SUCCESS;
  while (runStatus->get() != RunStatus::STOP) {
    log_info(0, "runstatus %s", runStatus->status());

    if (runStatus->get() == RunStatus::START2) {
      pid_t opid;
      int status = 0;
      if ((opid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (opid != pid) runStatus->set(RunStatus::WAIT);
        if (WIFEXITED(status)) {
          log_fatal(0, "children %d exit status=%d", (int) opid, WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
          log_fatal(0, "children %d killed by signal %d", (int) opid, WTERMSIG(status));
        }
        sys::nanosleep(500);
      } else {
        runStatus->set(RunStatus::WAIT);
      }
    }

    if (runStatus->get() == RunStatus::START1 || runStatus->get() == RunStatus::START2) {
      pid = spawn(cnf, 0);
      if (pid == -1) {
        log_fatal(errno, "spawn failed, exit");
        rc = EXIT_FAILURE;
        break;
      }
    } else if (runStatus->get() == RunStatus::RELOAD) {
      CnfCtx *ncnf = CnfCtx::loadCnf(dir, errbuf);
      if (ncnf) {
        ncnf->setRunStatus(runStatus);

        pid_t npid = spawn(ncnf, cnf);
        if (npid != -1) {
          log_info(0, "reload cnf");
          kill(pid, SIGTERM);
          cnf = ncnf;
          pid = npid;
        } else {
          delete ncnf;
        }
      } else {
        log_fatal(0, "reload cnf error %s", errbuf);
      }
    }

    runStatus->set(RunStatus::WAIT);
    signalHelper.suspend(-1);
  }

  if (pid != -1) kill(pid, SIGTERM);
  log_info(0, "tail2kafka exit");

  delete cnf;
  return rc;
}

void *routine(void *data)
{
  CnfCtx *cnf = (CnfCtx *) data;
  KafkaCtx *kafka = cnf->getKafka();
  RunStatus *runStatus = cnf->getRunStatus();

  OneTaskReq req;

  int timeout = 0;
  while (runStatus->get() == RunStatus::WAIT) {
    ssize_t nn = read(cnf->accept, &req, sizeof(OneTaskReq));
    if (nn == -1) {
      if (errno != EINTR) break;
      else continue;
    } else if (nn == 0) {
      break;
    }

    assert(nn == sizeof(OneTaskReq));

    if (!req.records) break;  // terminate task
    if (kafka->produce(req.ctx, req.records)) {
      timeout = 0;
    } else if (timeout < 1600) {
      timeout = timeout == 0 ? 100 : timeout * 2;
    }
    delete req.records;

    kafka->poll(timeout);
  }

  runStatus->set(RunStatus::STOP);
  log_info(0, "routine exit");
  return NULL;
}

inline void terminateRoutine(CnfCtx *ctx)
{
  OneTaskReq req = {0, 0};
  write(ctx->server, &req, sizeof(req));
}

void run(InotifyCtx *inotify, CnfCtx *cnf)
{
  cnf->getRunStatus()->set(RunStatus::WAIT);

  sys::SignalHelper signalHelper(0);
  signalHelper.setmask(-1);

  // must call in subprocess
  if (!cnf->initFileReader()) {
    log_fatal(0, "init filereader error %s", cnf->errbuf());
    exit(EXIT_FAILURE);
  }

  if (!cnf->getFileOff()->reinit()) {
    log_fatal(0, "reinit fileoff error %s", cnf->errbuf());
    exit(EXIT_FAILURE);
  }

  /* initKafka startup librdkafka thread */
  if (!cnf->initKafka()) {
    log_fatal(0, "init kafka error %s", cnf->errbuf());
    exit(EXIT_FAILURE);
  }

  pthread_t tid;
  pthread_create(&tid, NULL, routine, cnf);
  inotify->loop();
  terminateRoutine(cnf);
  pthread_join(tid, NULL);
}


int runForeGround(CnfCtx *cnf)
{
  InotifyCtx inotify(cnf);
  if (!inotify.init()) return -1;

  if (!cnf->initFileOff()) return 0;

  run(&inotify, cnf);

  delete cnf;
  return EXIT_SUCCESS;
}

pid_t spawn(CnfCtx *cnf, CnfCtx *ocnf)
{
  InotifyCtx inotify(cnf);
  if (!inotify.init()) return -1;

  if (!cnf->initFileOff()) return 0;

  /* unload old cnf before fork */
  if (ocnf) delete ocnf;

  int pid = fork();
  if (pid == 0) {
    run(&inotify, cnf);

    delete cnf;
    exit(EXIT_SUCCESS);
  }

  return pid;
}
