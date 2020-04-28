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
#include "kafkactx.h"

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

bool InotifyCtx::addWatch(LuaCtx *ctx, bool strict)
{
  const std::string &file = ctx->file();

  int fd = open(file.c_str(), O_RDONLY);
  if (fd == -1) {
    if (strict) {
      snprintf(cnf_->errbuf(), MAX_ERR_LEN, "%s open error %d:%s",
               file.c_str(), errno, strerror(errno));
    } else {
      log_fatal(errno, "rewatch open %s error", file.c_str());
      ctx->holdFd(-1);
    }
    return false;
  }

  int wd = inotify_add_watch(wfd_, file.c_str(), WATCH_EVENT);
  if (wd == -1) {
    close(fd);

    if (strict) {
      snprintf(cnf_->errbuf(), MAX_ERR_LEN, "%s add watch error %d:%s",
               file.c_str(), errno, strerror(errno));
    } else {
      log_fatal(errno, "rewatch add %s error", file.c_str());
      ctx->holdFd(-1);
    }
    return false;
  }

  log_info(0, "%s %s @%d", strict ? "watch" : "rewatch", file.c_str(), wd);

  ctx->holdFd(fd);
  fdToCtx_.insert(std::make_pair(wd, ctx));
  return true;
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

  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin();
       ite != cnf_->getLuaCtxs().end(); ++ite) {
    if (!addWatch(*ite, true)) return false;
  }
  return true;
}

void InotifyCtx::flowControl(RunStatus *runStatus)
{
  while (runStatus->get() == RunStatus::WAIT) {
    bool block = cnf_->stats()->queueSize() > MAX_FILE_QUEUE_SIZE;
    cnf_->logStats();

    cnf_->flowControl(block);
    if (!block) break;

    KafkaCtx *kafka = cnf_->getKafka();
    if (kafka) kafka->poll(10);
    else sys::millisleep(10);

    cnf_->fasttime(true, TIMEUNIT_SECONDS);
  }
}

/* file was moved */
void InotifyCtx::tagRotate(LuaCtx *ctx, int wd)
{
  char buffer[64];
  snprintf(buffer, 64, "/proc/self/fd/%d", ctx->holdFd());

  char path[2048];
  ssize_t n;
  if ((n = readlink(buffer, path, 2048)) == -1) {
    log_fatal(errno, "readlink error");
  } else {
    path[n] = '\0';
    log_info(0, "tag remove %d %s", wd, ctx->file().c_str());
    ctx->getFileReader()->tagRotate(FILE_MOVED, path);
  }
}

/* unlink or truncate */
void InotifyCtx::tryReWatch()
{
  std::vector<int> wds;

  for (std::map<int, LuaCtx *>::iterator ite = fdToCtx_.begin(); ite != fdToCtx_.end(); ++ite) {
    LuaCtx *ctx = ite->second;

    if (ctx->getFileReader()->remove()) {
      wds.push_back(ite->first);
    }
  }

  for (std::vector<int>::iterator ite = wds.begin(); ite != wds.end(); ++ite) {
    inotify_rm_watch(wfd_, *ite);

    std::map<int, LuaCtx *>::iterator pos = fdToCtx_.find(*ite);
    LuaCtx *ctx = pos->second;

    fdToCtx_.erase(pos);
    close(ctx->holdFd());

    addWatch(ctx, false);
    ctx->getFileReader()->tail2kafka();
  }

  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin();
       ite != cnf_->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = *ite;

    if (ctx->holdFd() == -1) {
      addWatch(ctx, false);
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

  bool rotate = false;
  long savedTime = cnf_->fasttime(true, TIMEUNIT_MILLI);
  long rewatchTime = savedTime;

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
            tagRotate(ctx, event->wd);
            rotate = true;
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

    if (cnf_->fasttime() > rewatchTime + 5 || rotate) {
      tryReWatch();
      rewatchTime = cnf_->fasttime();
      rotate = false;
    }

    if (cnf_->getPollLimit()) sys::millisleep(cnf_->getPollLimit());
    flowControl(runStatus);
  }

  runStatus->set(RunStatus::STOP);
}

void InotifyCtx::globalCheck()
{
  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin();
       ite != cnf_->getLuaCtxs().end(); ++ite) {

    LuaCtx *ctx = *ite;
    ctx->getFileReader()->checkCache();
    ctx->getFileReader()->tail2kafka();
  }
}
