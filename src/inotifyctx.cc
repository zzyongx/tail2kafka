#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <string>
#include <vector>
#include <errno.h>
#include <sys/inotify.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <poll.h>

#include "logger.h"
#include "sys.h"
#include "cnfctx.h"
#include "luactx.h"
#include "filereader.h"
#include "inotifyctx.h"

#define MAX_ERR_LEN 512

/* watch IN_DELETE_SELF does not work
 * luactx hold fd to the deleted file, the file will never be real deleted
 * so DELETE will be inotified
 */
static const uint32_t WATCH_EVENT = IN_MODIFY | IN_MOVE_SELF;
static const size_t ONE_EVENT_SIZE = sizeof(struct inotify_event) + NAME_MAX;

InotifyCtx::~InotifyCtx()
{
  if (wfd_ > 0) close(wfd_);
}

bool InotifyCtx::init()
{
  // inotify_init1 Linux 2.6.27
  wfd_ = inotify_init();
  if (wfd_ == -1) {
    snprintf(cnf_->errbuf(), MAX_ERR_LEN, "inotify_init error: %s", strerror(errno));
    return false;
  }

  int nb = 1;
  ioctl(wfd_, FIONBIO, &nb);

  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin(); ite != cnf_->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = *ite;
    const std::string &file = ctx->file();

    int wd = inotify_add_watch(wfd_, file.c_str(), WATCH_EVENT);
    if (wd == -1) {
      snprintf(cnf_->errbuf(), MAX_ERR_LEN, "%s add watch error %d:%s",
               file.c_str(), errno, strerror(errno));
      return false;
    }
    fdToCtx_.insert(std::make_pair(wd, ctx));
    log_info(0, "add watch %s @%d", file.c_str(), wd);
  }
  return true;
}

void InotifyCtx::flowControl(RunStatus *runStatus)
{
  int i = 0;
  while (runStatus->get() == RunStatus::WAIT) {
    bool kafkaBlock = cnf_->getKafkaBlock();
    size_t qsize = cnf_->stats()->queueSize();

    if (qsize > 1000) {
      kafkaBlock = true;
      cnf_->setKafkaBlock(true);
    }

    if (kafkaBlock || i == 0) {
      if (i % 500 == 0) {
        cnf_->logStats();
        log_info(0, "queue size %ld, kafka(es) status %s",
                 qsize, kafkaBlock ? "block" : "ok");
      }
    } else {
      break;
    }

    ++i;
    sys::millisleep(10);
    cnf_->fasttime(true, TIMEUNIT_SECONDS);
  }
}

bool InotifyCtx::tryReWatch()
{
  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin();
       ite != cnf_->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = *ite;

    if (ctx->getFileReader()->reinit()) {
      int wd = inotify_add_watch(wfd_, ctx->file().c_str(), WATCH_EVENT);
      if (wd == -1) {
        log_fatal(errno, "rewatch %s error", ctx->file().c_str());
      } else {
        fdToCtx_.insert(std::make_pair(wd, ctx));

        log_info(0, "rewatch %s @%d", ctx->file().c_str(), wd);
        ctx->getFileReader()->tail2kafka();
      }
    }
  }
  return true;
}

/* file was moved */
void InotifyCtx::tryRmWatch(LuaCtx *ctx, int wd)
{
  log_info(0, "tag remove %d %s", wd, ctx->file().c_str());
  ctx->getFileReader()->tagRotate(FILE_MOVED);
}

/* unlink or truncate */
void InotifyCtx::tryRmWatch()
{
  for (std::map<int, LuaCtx *>::iterator ite = fdToCtx_.begin(); ite != fdToCtx_.end(); ) {
    LuaCtx *ctx = ite->second;

    if (ctx->getFileReader()->remove()) {
      log_info(0, "remove watch %s @%d", ctx->file().c_str(), ite->first);

      inotify_rm_watch(wfd_, ite->first);
      fdToCtx_.erase(ite++);
    } else {
      ++ite;
    }
  }
}

void InotifyCtx::loop()
{
  RunStatus *runStatus = cnf_->getRunStatus();

  const size_t eventBufferSize = cnf_->getLuaCtxSize() * ONE_EVENT_SIZE * 5;
  char *eventBuffer = (char *) malloc(eventBufferSize);

  struct pollfd fds[] = {
    {wfd_, POLLIN, 0 }
  };

  long savedTime = cnf_->fasttime(true, TIMEUNIT_MILLI);
  while (runStatus->get() == RunStatus::WAIT) {
    int nfd = poll(fds, 1, cnf_->getTailLimit() ? 1 : 500);
    cnf_->fasttime(true, TIMEUNIT_SECONDS);
    cnf_->setTailLimit(false);

    if (nfd == -1) {
      if (errno != EINTR) return;
    } else if (nfd == 0) {
      globalCheck();
    } else {
      ssize_t nn = read(wfd_, eventBuffer, eventBufferSize);
      assert(nn > 0);

      char *p = eventBuffer;
      while (p < eventBuffer + nn) {
        /* IN_IGNORED when watch was removed */
        struct inotify_event *event = (struct inotify_event *) p;
        if (event->mask & IN_MODIFY) {
          LuaCtx *ctx = getLuaCtx(event->wd);
          if (ctx) {
            log_debug(0, "inotify %s was modified", ctx->file().c_str());
            ctx->getFileReader()->tail2kafka();
          } else {
            log_fatal(0, "@%d could not found ctx", event->wd);
          }
        }
        if (event->mask & IN_MOVE_SELF) {
          LuaCtx *ctx = getLuaCtx(event->wd);
          if (ctx) {
            log_info(0, "inotify %s was moved", ctx->file().c_str());
            tryRmWatch(ctx, event->wd);
          } else {
            log_fatal(0, "@%d could not found ctx", event->wd);
          }
        }
        p += sizeof(struct inotify_event) + event->len;
      }
    }

    if (cnf_->fasttime() != savedTime) {
      globalCheck();
      savedTime = cnf_->fasttime();
    }

    tryRmWatch();
    tryReWatch();

    if (cnf_->getPollLimit()) sys::millisleep(cnf_->getPollLimit());
    flowControl(runStatus);
  }

  runStatus->set(RunStatus::STOP);
}

void InotifyCtx::globalCheck()
{
  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin(); ite != cnf_->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = *ite;
    while (ctx) {
      ctx->getFileReader()->checkCache();
      ctx = ctx->next();
    }
  }
}
