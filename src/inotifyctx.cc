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
#include "runstatus.h"
#include "cnfctx.h"
#include "luactx.h"
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
    int wd = inotify_add_watch(wfd_, ctx->file().c_str(), WATCH_EVENT);
    if (wd == -1) {
      snprintf(cnf_->errbuf(), MAX_ERR_LEN, "%s add watch error %d:%s", ctx->file().c_str(), errno, strerror(errno));
      return false;
    }
    fdToCtx_.insert(std::make_pair(wd, ctx));
  }
  return true;
}

bool InotifyCtx::tryReWatch()
{
  for (std::vector<LuaCtx *>::iterator ite = cnf_->getLuaCtxs().begin(); ite != cnf_->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = *ite;

    if (ctx->getFileReader()->reinit()) {
      log_info(0, "rewatch %s", ctx->file().c_str());

      int wd = inotify_add_watch(wfd_, ctx->file().c_str(), WATCH_EVENT);
      fdToCtx_.insert(std::make_pair(wd, ctx));

      ctx->getFileReader()->tail2kafka(FileReader::NIL, 0);
    }
  }
  return true;
}

/* file was moved */
void InotifyCtx::tryRmWatch(LuaCtx *ctx, int wd)
{
  log_info(0, "tag remove %d %s", wd, ctx->file().c_str());
  ctx->getFileReader()->tagRemove();
}

/* unlink or truncate */
void InotifyCtx::tryRmWatch()
{
  for (std::map<int, LuaCtx *>::iterator ite = fdToCtx_.begin(); ite != fdToCtx_.end(); ) {
    LuaCtx *ctx = ite->second;

    if (ctx->getFileReader()->remove()) {
      log_info(0, "remove watch %s", ctx->file().c_str());

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

  long savedSn = time(0);
  while (runStatus->get() == RunStatus::WAIT) {
    int nfd = poll(fds, 1, 500);
    long sn = time(0);

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
          log_debug(0, "inotify %s was modified", ctx->file().c_str());
          ctx->getFileReader()->tail2kafka(FileReader::NIL, 0);
        }
        if (event->mask & IN_MOVE_SELF) {
          LuaCtx *ctx = getLuaCtx(event->wd);
          log_info(0, "inotify %s was moved", ctx->file().c_str());
          tryRmWatch(ctx, event->wd);

        }
        p += sizeof(struct inotify_event) + event->len;
      }
    }

    if (sn != savedSn) {
      globalCheck();
      savedSn = sn;
    }

    tryRmWatch();
    tryReWatch();
    if (cnf_->getPollLimit()) sys::nanosleep(cnf_->getPollLimit());
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
