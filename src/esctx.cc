#ifdef ENABLE_TAIL2ES
#include <cstring>
#include <errno.h>
#include <sys/ioctl.h>

#include "logger.h"
#include "common.h"
#include "cnfctx.h"
#include "luactx.h"
#include "filereader.h"
#include "esctx.h"

#define MAX_EPOLL_EVENT 1024
static pthread_mutex_t CURL_MANAGER_MUTEX = PTHREAD_MUTEX_INITIALIZER;

static void *eventLoopRoutine(void *data)
{
  EsSender *sender = (EsSender *) data;
  sender->eventLoop();
  return 0;
}

static int curlSocketCallback(CURL *curl, curl_socket_t fd, int what, void *data, void *)
{
  EsSender *sender = (EsSender *) data;
  sender->socketCallback(curl, fd, what);
  return 0;
}

struct EventCtx {
  EventCtx() : record(0), pos(0), inEvent(false) {}

  struct curl_slist *headers;
  FileRecord *record;
  size_t pos;
  std::string output;

  bool inEvent;
  int fd;
};

bool CurlManager::newCurl(CURL **curl, struct curl_slist **list, char *errbuf)
{
  *curl = curl_easy_init();
  if (!*curl) {
    if (errbuf) snprintf(errbuf, MAX_ERR_LEN, "curl_easy_init error");
    return false;
  }

  *list = 0;
  *list = curl_slist_append(*list, "Content-Type: application/json; charset=utf-8");
  *list = curl_slist_append(*list, "Expect: ");

  return true;
}

bool CurlManager::init(size_t capacity, char errbuf[])
{
  capacity_ = capacity;
  mutex_ = &CURL_MANAGER_MUTEX;

  for (size_t i = 0; i < capacity_; ++i) {
    CURL *curl;
    struct curl_slist *list = 0;
    if (!newCurl(&curl, &list, errbuf)) return false;

    curls_.push_back(curl);
    curlHeaders_.push_back(list);
  }
  return true;
}

void CurlManager::get(CURL **curl, struct curl_slist **header)
{
  pthread_mutex_lock(mutex_);
  ++active_;
  if (curls_.empty()) {
    pthread_mutex_unlock(mutex_);

    newCurl(curl, header);
  } else {
    *curl = curls_.back();
    *header = curlHeaders_.back();
    curls_.pop_back();
    curlHeaders_.pop_back();

    pthread_mutex_unlock(mutex_);
  }
}

void CurlManager::release(CURL *curl, struct curl_slist *header)
{
  pthread_mutex_lock(mutex_);
  --active_;
  if (curls_.size() < capacity_) {
    curls_.push_back(curl);
    curlHeaders_.push_back(header);
    pthread_mutex_unlock(mutex_);
  } else {
    pthread_mutex_unlock(mutex_);

    curl_easy_cleanup(curl);
    curl_slist_free_all(header);
  }
}

CurlManager::~CurlManager()
{
  for (std::vector<CURL *>::iterator ite = curls_.begin(); ite != curls_.end(); ++ite) {
    curl_easy_cleanup(*ite);
  }
  for (std::vector<curl_slist *>::iterator ite = curlHeaders_.begin();
       ite != curlHeaders_.end(); ++ite) {
    curl_slist_free_all(*ite);
  }
}

bool EsSender::init(CnfCtx *cnf, CurlManager *curlManager, char *errbuf)
{
  splitn(cnf->getEsNodes(), -1, &nodes_, -1, ',');
  userpass_ = cnf->getEsUserPass();

  cnf_ = cnf;
  curlManager_ = curlManager;

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
  running_ = true;
  return true;
}

EsSender::~EsSender()
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
}

bool EsSender::produce(FileRecord *record)
{
  uintptr_t ptr = (uintptr_t) record;
  ssize_t nn = write(pipeWrite_, &ptr, sizeof(FileRecord *));
  if (nn == -1) {
    if (errno != EINTR) {
      log_fatal(errno, "esctx produce error");
      return false;
    }
  }
  return true;
}

void EsSender::socketCallback(CURL *curl, curl_socket_t fd, int what)
{
  if (what == CURL_POLL_REMOVE) {
    if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, 0) != 0 && errno != EBADF) { // fd may be closed
      log_fatal(errno, "epoll_ctl_del %d error", fd);
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
        log_fatal(errno, "epoll_ctl_mod %d error", fd);
      }
    } else {
      ctx->fd = fd;
      ctx->inEvent = true;
      if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        log_fatal(errno, "epoll_ctl_add %d error", fd);
      }

      CURLMcode rc = curl_multi_assign(multi_, fd, 0);
      if (rc != CURLM_OK) {
        log_fatal(0, "epoll_multi_assign error: %s", curl_multi_strerror(rc));
      }
    }
  }
}

void EsSender::consume()
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
  size_t min = std::min(size * nmemb, ctx->record->data->size() - ctx->pos);
  memcpy(ptr, ctx->record->data->c_str() + ctx->pos, min);
  ctx->pos += min;
  return min;
}

static size_t curlRetData(void *ptr, size_t size, size_t nmemb, void *data)
{
  EventCtx *ctx = (EventCtx *) data;
  ctx->output.append((char *) ptr, size * nmemb);
  return size * nmemb;
}

void EsSender::addToMulti(FileRecord *record)
{
  CURL *curl;
  struct curl_slist *headers;
  curlManager_->get(&curl, &headers);
  assert(curl);

  std::string node = nodes_[random() % nodes_.size()];
  std::string url = "http://" + node + "/" + *record->esIndex + "/_doc";

  log_debug(0, "POST %s DATA %s", url.c_str(), record->data->c_str());

  EventCtx *ctx = new EventCtx;
  ctx->headers = headers;
  ctx->record = record;

  curl_easy_reset(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  if (!userpass_.empty()) {
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, userpass_.c_str());
  }
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

void EsSender::checkMultiInfo()
{
  int alive;
  CURLMsg *msg;

  while ((msg = curl_multi_info_read(multi_, &alive))) {
    if (msg->msg == CURLMSG_DONE) {
      CURL *handler = msg->easy_handle;
      cnf_->stats()->logSendInc();

      EventCtx *ctx;
      curl_easy_getinfo(handler, CURLINFO_PRIVATE, &ctx);

      long code = -1;
      curl_easy_getinfo(handler, CURLINFO_RESPONSE_CODE, &code);
      if (code != 201) {
        char *url;
        curl_easy_getinfo(handler, CURLINFO_EFFECTIVE_URL, &url);
        log_fatal(0, "alive %d POST %s %s ret status %ld body %s", alive, url,
                  ctx->record->data->c_str(), code, ctx->output.c_str());
        if (code != 400 && code != 429) cnf_->stats()->logErrorInc();
      }

      if (ctx->record->off != (off_t) -1 && ctx->record->inode > 0) {
        ctx->record->ctx->getFileReader()->updateFileOffRecord(ctx->record);
      }
      FileRecord::destroy(ctx->record);

      curl_multi_remove_handle(multi_, handler);
      curlManager_->release(handler, ctx->headers);
      delete ctx;
    }
  }
}

void EsSender::eventCallback(CURL *curl, uint32_t event)
{
  int what = (event & EPOLLIN ? CURL_POLL_IN : 0) |
    (event & EPOLLOUT ? CURL_POLL_OUT : 0);

  CURLMcode rc;
  int alive;

  EventCtx *ctx;
  curl_easy_getinfo(curl, CURLINFO_PRIVATE, &ctx);
  rc = curl_multi_socket_action(multi_, ctx->fd, what, &alive);

  if (rc != CURLM_OK) {
    log_fatal(0, "curl_multi_socket_action error: %s", curl_multi_strerror(rc));
  }

  checkMultiInfo();
}

void EsSender::timerCallback()
{
  CURLMcode rc;
  int alive;

  rc = curl_multi_socket_action(multi_, CURL_SOCKET_TIMEOUT, 0, &alive);
  if (rc != CURLM_OK) {
    log_fatal(0, "curl_multi_socket_action error: %s", curl_multi_strerror(rc));
  }

  checkMultiInfo();
}

void EsSender::eventLoop()
{
  while (running_) {
    int nfd = epoll_wait(epfd_, events_, MAX_EPOLL_EVENT, 100);
    if (nfd > 0) {
      for (int i = 0; i < nfd; ++i) {
        if (events_[i].data.ptr == &pipeRead_) {
          consume();
        } else {
          eventCallback((CURL *) events_[i].data.ptr, events_[i].events);
        }
      }
    } else if (nfd == 0) {
      timerCallback();
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

bool EsCtx::init(CnfCtx *cnf, char *errbuf)
{
  cnf_ = cnf;
  if (!curlManager_.init(cnf->getEsMaxConns(), errbuf)) return false;

  lastSenderIndex_ = 0;
  for (size_t i = 0; i < cnf->getEsMaxConns() / 300; ++i) {
    EsSender *sender = new EsSender;
    if (!sender->init(cnf, &curlManager_, cnf->errbuf())) {
      delete sender;
      return false;
    }
    esSenders_.push_back(sender);
  }

  cnf_ = cnf;
  running_ = true;
  return true;
}

EsCtx::~EsCtx()
{
  running_ = false;
  for (std::vector<EsSender *>::iterator ite = esSenders_.begin(); ite != esSenders_.end(); ++ite) {
    delete *ite;
  }
}

void EsCtx::flowControl()
{
  int i = 0;
  int overload;
  while ((overload = curlManager_.overload()) > 10) {
    if (i % 500 == 0) {
      log_info(0, "too much data for es #%d, wait %ds, set block, stop produce", overload, i / 100);
      cnf_->setKafkaBlock(true);
    }
    i++;
    sys::millisleep(10);
  }
  if (i > 0) {
    log_info(0, "es #%d, restart produce", overload);
    cnf_->setKafkaBlock(false);
  }
}

bool EsCtx::produce(std::vector<FileRecord *> *records)
{
  if (!running_) return false;

  cnf_->stats()->logRecvInc(records->size());

  for (std::vector<FileRecord *>::iterator ite = records->begin(), end = records->end();
       ite != end; ++ite) {
    if ((*ite)->off == (off_t) -1) {
      FileRecord::destroy(*ite);
      continue;
    }

    flowControl();

    if (!esSenders_[lastSenderIndex_]->produce(*ite)) {
      return false;
    }
    if (++lastSenderIndex_ >= esSenders_.size()) lastSenderIndex_ = 0;
  }
  return true;
}

#endif
