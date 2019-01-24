#include <cstring>
#include <errno.h>
#include <sys/ioctl.h>

#include "logger.h"
#include "common.h"
#include "cnfctx.h"
#include "luactx.h"
#include "esctx.h"

#define MAX_EPOLL_EVENT 1024

static void *eventLoopRoutine(void *data)
{
  EsCtx *esCtx = (EsCtx *) data;
  esCtx->eventLoop();
  return 0;
}

static int curlSocketCallback(CURL *curl, curl_socket_t fd, int what, void *data, void *)
{
  EsCtx *esCtx = (EsCtx *) data;
  esCtx->socketCallback(curl, fd, what);
  return 0;
}

struct EventCtx {
  EventCtx() : input(0), pos(0), inEvent(false) {}

  struct curl_slist *headers;
  const std::string *input;
  size_t pos;
  std::string output;

  bool inEvent;
  int fd;
};

bool EsCtx::newCurl()
{
  CURL *curl = curl_easy_init();
  if (!curl) {
    snprintf(cnf_->errbuf(), MAX_ERR_LEN, "curl_easy_init error");
    return false;
  }
  curls_.push_back(curl);

  struct curl_slist *list = 0;
  list = curl_slist_append(list, "Content-Type: application/json; charset=utf-8");
  list = curl_slist_append(list, "Expect: ");
  curlHeaders_.push_back(list);
  return true;
}

bool EsCtx::init(CnfCtx *cnf, char *errbuf)
{
  cnf_ = cnf;
  splitn(cnf->getEsNodes(), -1, &nodes_, -1, ',');

  for (size_t i = 0; i < cnf->getEsMaxConns(); ++i) {
    if (!newCurl()) return false;
  }

  multi_ = curl_multi_init();
  if (!multi_) {
    snprintf(errbuf, MAX_ERR_LEN, "curl_multi_init error");
    return false;
  }

  curl_multi_setopt(multi_, CURLMOPT_SOCKETFUNCTION, curlSocketCallback);
  curl_multi_setopt(multi_, CURLMOPT_SOCKETDATA, this);

  epfd_ = epoll_create(1024);
  if (epfd_ == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "epoll_create error: %d:%s", errno, strerror(errno));
    return false;
  }

  int pipeFd[2];
  if (pipe(pipeFd) == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "pipe error: %d:%s", errno, strerror(errno));
    return false;
  }

  int nb = 1;
  ioctl(pipeRead_, FIONBIO, &nb);

  pipeRead_ = pipeFd[0];
  pipeWrite_ = pipeFd[1];

  struct epoll_event ev = {EPOLLIN, &pipeRead_};
  if (epoll_ctl(epfd_, EPOLL_CTL_ADD, pipeRead_, &ev) == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "epoll_ctl pipe read error: %d:%s", errno, strerror(errno));
    return false;
  }

  events_ = new struct epoll_event[1024];

  int rc = pthread_create(&tid_, 0, eventLoopRoutine, this);
  if (rc != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "pthread_create error: %d:%s", rc, strerror(rc));
    return false;
  }

  cnf_ = cnf;
  running_ = true;
  return true;
}

EsCtx::~EsCtx()
{
  if (running_) {
    running_ = false;
    pthread_join(tid_, 0);
  }

  if (epfd_ >= 0) close(epfd_);
  if (pipeRead_ >= 0) close(pipeRead_);
  if (pipeWrite_ >= 0) close(pipeWrite_);

  if (multi_) curl_multi_cleanup(multi_);
  if (events_) delete []events_;

  for (std::vector<CURL *>::iterator ite = curls_.begin(); ite != curls_.end(); ++ite) {
    curl_easy_cleanup(*ite);
  }
}

bool EsCtx::produce(LuaCtx *, std::vector<FileRecord *> *records)
{
  if (!running_) return false;

  for (std::vector<FileRecord *>::iterator ite = records->begin(), end = records->end();
       ite != end; ++ite) {

    uintptr_t ptr = (uintptr_t) *ite;
    ssize_t nn = write(pipeWrite_, &ptr, sizeof(FileRecord *));
    if (nn == -1) {
      if (errno != EINTR) {
        log_fatal(errno, "esctx produce error");
        return false;
      }
    }
  }
  return true;
}

void EsCtx::socketCallback(CURL *curl, curl_socket_t fd, int what)
{
  if (what == CURL_POLL_REMOVE) {
    if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, 0) != 0) {
      log_fatal(errno, "epoll_ctl_del error");
    }
  } else {
    EventCtx *ctx;
    curl_easy_getinfo(curl, CURLINFO_PRIVATE, &ctx);

    uint32_t event = 0;
    if (what & CURL_POLL_IN) event |= EPOLLIN;
    if (what & CURL_POLL_OUT) event |= EPOLLOUT;

    struct epoll_event ev = {event, curl};
    if (ctx->inEvent) {
      if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
        log_fatal(errno, "epoll_ctl_mod error");
      }
    } else {
      ctx->fd = fd;
      ctx->inEvent = true;
      if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        log_fatal(errno, "epoll_ctl_add error");
      }

      CURLMcode rc = curl_multi_assign(multi_, fd, 0);
      if (rc != CURLM_OK) {
        log_fatal(0, "epoll_multi_assign error: %s", curl_multi_strerror(rc));
      }
    }
  }
}

void EsCtx::consumeFileRecords()
{
  uintptr_t ptr;
  ssize_t nn = read(pipeRead_, &ptr, sizeof(FileRecord *));
  if (nn == -1) {
    log_fatal(errno, "esctx consume error");
    return;
  }
  assert(nn == sizeof(FileRecord*));
  addToMulti((FileRecord *)ptr);
}


static size_t curlPutData(void *ptr, size_t size, size_t nmemb, void *data)
{
  EventCtx *ctx = (EventCtx *) data;
  size_t min = std::min(size * nmemb, ctx->input->size() - ctx->pos);
  memcpy(ptr, ctx->input->c_str() + ctx->pos, min);
  ctx->pos += min;
  return min;
}

static size_t curlRetData(void *ptr, size_t size, size_t nmemb, void *data)
{
  EventCtx *ctx = (EventCtx *) data;
  ctx->output.append((char *) ptr, size * nmemb);
  return size * nmemb;
}

void EsCtx::addToMulti(FileRecord *record)
{
  if (curls_.empty()) {
    newCurl();
    cnf_->setKafkaBlock(true);
    log_info(0, "too much data for es, set block");
  }

  CURL *curl = curls_.back();
  struct curl_slist *headers = curlHeaders_.back();
  assert(curl);
  curls_.pop_back();
  curlHeaders_.pop_back();

  std::string node = nodes_[random() % nodes_.size()];
  std::string url = "http://" + node + "/" + *record->esIndex + "/_doc";

  log_debug(0, "POST %s DATA %s", url.c_str(), record->data->c_str());

  EventCtx *ctx = new EventCtx;
  ctx->headers = headers;
  ctx->input = record->data;

  curl_easy_reset(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
#ifdef CURLOPT_TCP_KEEPALIVE
  curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
#endif
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (curl_off_t) record->data->size());
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, curlPutData);
  curl_easy_setopt(curl, CURLOPT_READDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlRetData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_PRIVATE, ctx);

  CURLMcode rc = curl_multi_add_handle(multi_, curl);
  if (rc != CURLM_OK) {
    log_fatal(0, "curl_multi_add_handle error: %s", curl_multi_strerror(rc));
  }
}

void EsCtx::eventCallback(CURL *curl, uint32_t event)
{
  int what = (event & EPOLLIN ? CURL_POLL_IN : 0) |
    (event & EPOLLOUT ? CURL_POLL_OUT : 0);

  CURLMcode rc;
  int alive;

  if (curl) {
    EventCtx *ctx;
    curl_easy_getinfo(curl, CURLINFO_PRIVATE, &ctx);

    rc = curl_multi_socket_action(multi_, ctx->fd, what, &alive);
  } else {
    rc = curl_multi_socket_action(multi_, CURL_SOCKET_TIMEOUT, 0, &alive);
  }

  if (rc != CURLM_OK) {
    log_fatal(0, "curl_multi_socket_action error: %s", curl_multi_strerror(rc));
  }

  CURLMsg *msg;
  while ((msg = curl_multi_info_read(multi_, &alive))) {
    if (msg->msg == CURLMSG_DONE) {
      CURL *handler = msg->easy_handle;

      EventCtx *ctx;
      curl_easy_getinfo(handler, CURLINFO_PRIVATE, &ctx);

      long code = -1;
      curl_easy_getinfo(handler, CURLINFO_RESPONSE_CODE, &code);
      if (code != 201) {
        char *url;
        curl_easy_getinfo(handler, CURLINFO_EFFECTIVE_URL, &url);
        log_fatal(0, "alive %d POST %s %s ret status %ld body %s", alive, url, ctx->input->c_str(),
                  code, ctx->output.c_str());
      }

      curl_multi_remove_handle(multi_, handler);
      curls_.push_back(handler);
      curlHeaders_.push_back(ctx->headers);
      cnf_->setKafkaBlock(false);
      delete ctx;
    }
  }
}

void EsCtx::eventLoop()
{
  while (running_) {
    int nfd = epoll_wait(epfd_, events_, MAX_EPOLL_EVENT, 100);
    if (nfd > 0) {
      for (int i = 0; i < nfd; ++i) {
        if (events_[i].data.ptr == &pipeRead_) {
          consumeFileRecords();
        } else {
          eventCallback((CURL *) events_[i].data.ptr, events_[i].events);
        }
      }
    } else if (nfd == 0) {
      eventCallback(0, 0);
    } else {
      if (errno == EINTR) {
        log_fatal(errno, "epoll_wait error");
      } else {
        log_fatal(errno, "epoll_wait error, exit");
        running_ = false;
        break;
      }
    }
  }
}
