#include <cstring>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>

#include "logger.h"
#include "util.h"
#include "sys.h"
#include "common.h"
#include "cnfctx.h"
#include "luactx.h"
#include "filereader.h"
#include "esctx.h"

#define ES_CREATE_INDEX_HEADER_TPL                    \
  "POST /%s/_doc HTTP/1.1\r\n"                        \
  "Host: %s\r\n"                                      \
  "Accept: */*\r\n"                                   \
  "Connection: keep-alive\r\n"                        \
  "Content-Type: application/json; charset=utf-8\r\n" \
  "Content-Length: %d\r\n"                            \
  "\r\n"

void EsUrl::reinit(FileRecord *record, bool move)
{
  assert(record);

  if (move) {
    int next = (idx_ + move) % nodes_.size();
    log_error(0, "switch es node from %s to %s",
              nodes_[idx_].c_str(), nodes_[next].c_str());
    idx_ = next;
    node_ = nodes_[idx_];

    ++timeoutRetry_;
  } else {
    timeoutRetry_ = 0;
  }

  body_ = record->data->c_str();
  nbody_ = record->data->size();

  std::string docIndex = *(record->esIndex);
  // docIndex = "debug";

  nheader_ = snprintf(header_, MAX_HTTP_HEADER_LEN, ES_CREATE_INDEX_HEADER_TPL,
                      docIndex.c_str(), node_.c_str(), nbody_);

  url_ = "http://" + node_ + "/" + docIndex + "/_doc";

  log_debug(0, "POST %s DATA %s", url_.c_str(), body_);

  offset_ = 0;

  respWant_ = STATUS_LINE;
  resp_ = header_;

  wantLen_ = -1;
  chunkLen_ = -1;

  respCode_ = 0;
  respBody_.clear();

  record_ = record;

  if (status_ == IDLE) {
    log_debug(0, "%p reuse connect %s #%d", this, node_.c_str(), fd_);
    status_ = WRITING;
    keepalive_++;
  } else {
    keepalive_ = 0;
  }

  activeTime_ = time(0);
}

void EsUrl::destroy(int pfd)
{
  log_debug(0, "%p disconnect %s #%d", this, node_.c_str(), fd_);
  epoll_ctl(pfd, EPOLL_CTL_DEL, fd_, 0);

  close(fd_);
  fd_ = -1;
  status_ = UNINIT;
}

int EsUrl::initIOV(struct iovec *iov)
{
  if (offset_ < nheader_) {
    iov[0].iov_base = header_ + offset_;
    iov[0].iov_len = nheader_ - offset_;
    iov[1].iov_base = (void *) body_;
    iov[1].iov_len = nbody_;
    return 2;
  } else if (offset_ < nheader_ + nbody_) {
    iov[0].iov_len = nheader_ + nbody_ - offset_;
    iov[0].iov_base = (void *) (body_ + offset_ - nheader_);
    return 1;
  } else {
    return 0;
  }
}

bool EsUrl::doConnect(int pfd, char *errbuf)
{
  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ == -1) {
    snprintf(errbuf, 1024, "socket() error: %s", strerror(errno));
    return false;
  }

  int val = 1;
  ioctl(fd_, FIONBIO, &val);

  if (setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1) {
    snprintf(errbuf, 1024, "setsockopt(SOL_SOCKET, SO_KEEPALIVE) error: %s",
             strerror(errno));
    return false;
  }

  if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    log_error(errno, "setsockopt(IPPROTO_TCP, TCP_KEEPIDLE) error");
  }

  if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    log_error(errno, "setsockopt(IPPROTO_TCP, TCP_KEEPINTVL) error");
  }

  val = 3;
  if (setsockopt(fd_, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    log_error(errno, "setsockopt(IPPROTO_TCP, TCP_KEEPCNT) error");
  }

  struct addrinfo hints, *infos;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  std::string node, service;
  size_t pos = node_.find(":");
  if (pos != std::string::npos) {
    node = node_.substr(0, pos);
    service = node_.substr(pos+1);
  } else {
    node = node_;
    service = "9200";
  }

  int rc = getaddrinfo(node.c_str(), service.c_str(), &hints, &infos);
  if (rc != 0) {
    snprintf(errbuf, 1024, "getaddrinfo %s error: %s", node_.c_str(), gai_strerror(rc));
    return false;
  }

  for (struct addrinfo *p = infos; p != NULL; p = p->ai_next) {
    rc = connect(fd_, p->ai_addr, p->ai_addrlen);
    if (rc == -1) {
      if (errno == EINPROGRESS) {
        status_ = ESTABLISHING;
        break;
      }
    } else if (rc == 0) {
      status_ = READING;
      break;
    }
  }

  if (status_ == ESTABLISHING || status_ == READING) {
    log_debug(0, "%p connect %s #%d", this, node_.c_str(), fd_);

    uint32_t e;
    const char *estr;
    if (status_ == READING) {
      e = EPOLLOUT;
      estr = "EPOLLOUT";
    } else {
      e = EPOLLIN | EPOLLOUT;
      estr = "EPOLLIN|EPOLLOUT";
    }

    struct epoll_event event = {e, this};
    if (epoll_ctl(pfd, EPOLL_CTL_ADD, fd_, &event) != 0) {
      snprintf(errbuf, 1024, "epoll_ctl_add(%d, %s) error: %s",
               fd_, estr, strerror(errno));
      status_ = UNINIT;
    }
  } else {
    snprintf(errbuf, 1024, "connect %s error: %s", node_.c_str(), strerror(errno));
  }

  freeaddrinfo(infos);
  return status_ == ESTABLISHING || status_ == READING;
}

bool EsUrl::doConnectFinish(int /*pfd*/, char *errbuf)
{
  int err = 0;
  socklen_t errlen = sizeof(err);
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &errlen) != 0) {
    err = errno;
  }

  if (err) snprintf(errbuf, 1024, "connect %s error: %s", node_.c_str(), strerror(err));
  return !err;
}

bool EsUrl::doRequest(int pfd, char *errbuf)
{
  struct iovec iov[2];
  while (true) {
    size_t niov = initIOV(iov);
    if (niov == 0) {
      offset_ = 0;
      status_ = READING;
      break;
    }

    ssize_t nn = writev(fd_, iov, niov);
    if (nn == -1) {
      if (errno == EAGAIN) {
        status_ = WRITING;
      } else {
        snprintf(errbuf, 1024, "writev error: %s", strerror(errno));
        return false;
      }
    } else {
      offset_ += nn;
    }
  }

  if (status_ == READING) {
    log_debug(0, "wait response %p epoll_ctl_mod(#%d, EPOLLIN)", this, fd_);

    struct epoll_event event = {EPOLLIN, this};
    if (epoll_ctl(pfd, EPOLL_CTL_MOD, fd_, &event) != 0) {
      snprintf(errbuf, 1024, "epoll_ctl_mod(#%d, EPOLLIN) error: %s",
               fd_, strerror(errno));
      return false;
    }
  }

  return true;
}

bool EsUrl::doResponse(int /*pfd*/, char *errbuf)
{
  while (true) {
    ssize_t nn = recv(fd_, header_ + offset_, MAX_HTTP_HEADER_LEN - offset_, 0);
    if (nn == -1) {
      if (errno == EAGAIN) {
        if (initHttpResponse(header_ + offset_)) {
          status_ = IDLE;
        }
        break;
      } else {
        snprintf(errbuf, 1024, "recv error: %s", strerror(errno));
        return false;
      }
    } else if (nn == 0) {
      if (initHttpResponse(header_ + offset_)) {
        status_ = IDLE;
        break;
      } else {
        errbuf[0] = '\0';
        return false;
      }
    } else {
      offset_ += nn;
    }
  }

  if (status_ == IDLE) {
    assert(record_);
    record_->ctx->cnf()->stats()->logSendInc();

    if (respCode_ != 201) {
      log_fatal(0, "INDEX ret status %d body %s, POST %s %s ",
                respCode_, respBody_.c_str(), url_.c_str(), body_);
      if (respCode_ != 400 && respCode_ != 429) {
        record_->ctx->cnf()->stats()->logErrorInc();
      }
    }

    if (record_->off != (off_t) -1 && record_->inode > 0) {
      record_->ctx->getFileReader()->updateFileOffRecord(record_);
    }

    FileRecord::destroy(record_);
    record_ = 0;
  }

  return true;
}

void EsUrl::initHttpResponseStatusLine(const char *eof)
{
  int field = 0;
  const char *start = resp_;

  for (char *p = resp_; p+1 != eof && respWant_ == STATUS_LINE; ++p) {
    if (*p == ' ') {
      ++field;

      if (field == 1) {
        start = p + 1;
      } else if (field == 2) {
        *p = '\0';
        respCode_ = util::toInt(start);
      }
    } else if (*p == '\r' && *(p+1) == '\n') {
      respWant_ = HEADER;
      resp_ = p + 2;
    }
  }
}

void EsUrl::initHttpResponseHeader(const char *eof)
{
  HttpRespWant want = HEADER_NAME;
  const char *key = resp_, *value = 0;

  for (char *p = resp_; p+1 != eof; ++p) {
    if (want == HEADER_NAME) {
      if (*p == ':') {
        *p = '\0';
        want = HEADER_VALUE;
        value = p + 1;
      } else if (*p == '\r' && *(p+1) == '\n') {
        if (wantLen_ >= 0) {
          respWant_ = BODY;
        } else {
          respWant_ = BODY_CHUNK_LEN;
          wantLen_ = 0;
        }
        resp_ = p + 2;
        break;
      }
    } else if (want == HEADER_VALUE) {
      if (*p == '\r' && *(p+1) == '\n') {
        if (strcasecmp(key, "content-length") == 0) {
          *p = '\0';
          wantLen_ = util::toInt(util::trim(value).c_str());
        }
        want = HEADER_NAME;
        key = p+2;
      }
    }
  }
}

void EsUrl::initHttpResponseBody(const char *eof)
{
  while (respWant_ != RESP_EOF) {
    if (respWant_ == BODY_CHUNK_LEN) {
      for (char *p = resp_; p+1 != eof; ++p) {
        if (*p == '\r' && *(p+1) == '\n') {
          *p = '\0';
          util::hexToInt(resp_, &chunkLen_);
          wantLen_ += chunkLen_;
          resp_ = p + 2;
          respWant_ = BODY_CHUNK_CONTENT;
          break;
        }
      }
    }

    if (respWant_ == BODY_CHUNK_CONTENT) {
      const char *p = resp_;
      int left = wantLen_ - respBody_.size();
      int min = eof - p >= left ? left : eof - p;
      if (min) {
        respBody_.append(p, min);
        p += min;
      }

      if (p + 1 < eof && *p == '\r' && *(p + 1) == '\n') {
        if (chunkLen_ == 0) {
          respWant_ = RESP_EOF;
        } else {
          respWant_ = BODY_CHUNK_LEN;
        }
      } else {
        resp_ = header_;
        offset_ = eof - p;
        memmove(resp_, p, offset_);
      }
    }
  }
}

bool EsUrl::initHttpResponse(const char *eof)
{
  if (respWant_ == RESP_EOF) return true;

  if (respWant_ == STATUS_LINE) initHttpResponseStatusLine(eof);
  if (respWant_ == HEADER) initHttpResponseHeader(eof);

  if (respWant_ == BODY) {
    if (eof - resp_ > 0) respBody_.append(resp_, eof - resp_);
    resp_ = header_;
    offset_ = 0;
    if (respBody_.size() == (size_t) wantLen_) respWant_ = RESP_EOF;
  } else if (respWant_ == BODY_CHUNK_LEN || respWant_ == BODY_CHUNK_CONTENT) {
    initHttpResponseBody(eof);
  }

  return respWant_ == RESP_EOF;
}

bool EsUrl::onError(int pfd, const char *error)
{
  assert(record_ && !pool_);

  bool move = false;
  if (error && error[0]) {
    log_fatal(0, "%p #%d POST %s %s INTERNAL ERROR @%d: %s, load %d, keepalive %d",
              this, fd_, url_.c_str(), body_, timeoutRetry_, error,
              urlManager_->load(), keepalive_);
    record_->ctx->cnf()->stats()->logErrorInc();
    move = true;
  }

  destroy(pfd);
  reinit(record_, move);

  if (move) return true;  // wait timeout retry
  else return onEvent(pfd);  // wait right now
}

bool EsUrl::onTimeout(int pfd, time_t now)
{
  if (status_ == IDLE || now - activeTime_ < 30) return true;

  assert(record_);
  if (status_ == UNINIT) return onEvent(pfd);
  else return onError(pfd, "timeout");
}

bool EsUrl::onEvent(int pfd)
{
  log_debug(0, "%p #%d status %s", this, fd_, eventStatusToString(status_));

  if (status_ == IDLE) {
    destroy(pfd);
    urlManager_->release(this);
    return false;
  }

  assert(record_ && !pool_);
  char errbuf[1024] = "OK";

  bool rc = true;
  if (status_ == WRITING) {
    rc = doRequest(pfd, errbuf);
  } else if (status_ == READING) {
    rc = doResponse(pfd, errbuf);
  } else if (status_ == UNINIT) {
    rc = doConnect(pfd, errbuf);
    if (rc && status_ == READING) onEvent(pfd);
  } else if (status_ == ESTABLISHING) {
    rc = doConnectFinish(pfd, errbuf);
    if (rc) {
      status_ = WRITING;
      return onEvent(pfd);
    }
  }

  activeTime_ = time(0);
  if (!rc) onError(pfd, errbuf);

  if (status_ == IDLE) return !urlManager_->release(this);
  else return true;
}

EsUrl *EsUrlManager::get(bool *pool) {
  util::atomic_inc(&active_);

  EsUrl *url;
  if (urls_.empty()) {
    url = new EsUrl(nodes_, random() % nodes_.size(), this);
    holder_.push_back(url);

    if (pool) *pool = false;
    log_info(0, "new get url %p, load %ld", url, active_);
  } else {
    url = urls_.back();
    urls_.pop_back();

    if (pool) *pool = true;
    log_debug(0, "pool get url %p, load %ld", url, active_);
  }

  url->pool(false);
  return url;
}

bool EsUrlManager::release(EsUrl *url) {
  if (url->pool(true)) return false;

  util::atomic_dec(&active_);

  if (urls_.size() < capacity_ * 2) {
    log_debug(0, "pool release url %p, load %ld", url, active_);

    urls_.push_back(url);
    return false;
  } else {
    log_info(0, "destroy release url %p, load %ld", url, active_);

    holder_.remove(url);
    delete url;
    return true;
  }
}

#define MAX_EPOLL_EVENT 1024

static void *eventLoopRoutine(void *data)
{
  EsSender *sender = (EsSender *) data;
  sender->eventLoop();
  return 0;
}

bool EsSender::init(CnfCtx *cnf, size_t capacity)
{
  cnf_ = cnf;

  userpass_ = cnf->getEsUserPass();
  capacity_ = capacity;
  urlManager_ = new EsUrlManager(cnf->getEsNodes(), capacity);

  epfd_ = epoll_create(MAX_EPOLL_EVENT);
  if (epfd_ == -1) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "epoll_create error: %d:%s",
             errno, strerror(errno));
    return false;
  }

  int pipeFd[2];
  if (pipe(pipeFd) == -1) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "pipe error: %d:%s", errno, strerror(errno));
    return false;
  }

  pipeRead_ = pipeFd[0];
  pipeWrite_ = pipeFd[1];

  int nb = 1;
  ioctl(pipeRead_, FIONBIO, &nb);

  events_ = new struct epoll_event[1024];
  int rc = pthread_create(&tid_, 0, eventLoopRoutine, this);
  if (rc != 0) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "pthread_create error: %d:%s",
             rc, strerror(rc));
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
  if (urlManager_) delete urlManager_;

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

size_t EsSender::consume(int pfd, bool once)
{
  size_t c = 0;
  if (urlManager_->load() >= capacity_) return c;

  bool pool = true;
  do {
    uintptr_t ptr;
    ssize_t nn = read(pipeRead_, &ptr, sizeof(FileRecord *));
    if (nn == -1) {
      if (errno != EAGAIN) log_fatal(errno, "esctx consume error");
      return c;
    }

    assert(nn == sizeof(FileRecord*));

    ++c;
    EsUrl *url = urlManager_->get(&pool);
    if (!url->idle()) urls_.push_back(url);

    url->reinit((FileRecord *)ptr);
    if (!url->onEvent(pfd)) {
      urls_.remove(url);
    }
  } while (urlManager_->load() < capacity_ && !once && pool);

  return c;
}

bool EsSender::flowControl(bool block, size_t cn)
{
  bool rc;
  if (urlManager_->load() >= capacity_ * 3/2) {
    if (!block) epoll_ctl(epfd_, EPOLL_CTL_DEL, pipeRead_, 0);
    rc = true;
  } else {
    if (block) {
      struct epoll_event ev = {EPOLLIN, &pipeRead_};
      epoll_ctl(epfd_, EPOLL_CTL_ADD, pipeRead_, &ev);
    }
    rc = false;
  }

  if (rc != block) {
    log_info(0, "flow control load: %d, cn: %lu, status %s -> %s",
             urlManager_->load(), cn,
             block ? "block" : "ok", rc ? "block" : "ok");
  }
  return rc;
}

void EsSender::eventLoop()
{
  bool block = true;
  size_t cn = 0;

  while (running_) {
    block = flowControl(block, cn);

    time_t now = time(0);
    int nfd = epoll_wait(epfd_, events_, MAX_EPOLL_EVENT, 1000);
    if (nfd > 0) {
      bool pipeReadOk = false;
      for (int i = 0; i < nfd; ++i) {
        if (events_[i].data.ptr == &pipeRead_) {
          pipeReadOk = true;
        } else {
          EsUrl *url = (EsUrl *) events_[i].data.ptr;
          if (!url->onEvent(epfd_)) {
            urls_.remove(url);
          } else if (block || cn == 0) {
            cn = consume(epfd_, true);
          }
        }
      }
      if (pipeReadOk) cn = consume(epfd_, false);
    } else if (nfd == 0) {
      for (std::list<EsUrl*>::iterator ite = urls_.begin(); ite != urls_.end();) {
        EsUrl *url = *ite;
        if (!url->onTimeout(epfd_, now)) {
          urls_.erase(ite++);
        } else {
          ++ite;
        }
      }
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

bool EsCtx::init(CnfCtx *cnf)
{
  cnf_ = cnf;

  size_t maxc = cnf->getEsMaxConns();

  size_t nthread = (maxc % 500 == 0) ? maxc / 500 : maxc / 500 + 1;
  if (nthread == 0) nthread = 1;

  lastSenderIndex_ = 0;
  for (size_t i = 0; i < nthread; ++i) {
    EsSender *sender = new EsSender;
    if (!sender->init(cnf, maxc/nthread)) {
      delete sender;
      return false;
    }
    esSenders_.push_back(sender);
  }

  running_ = true;
  return true;
}

EsCtx::~EsCtx()
{
  running_ = false;
  for (std::vector<EsSender *>::iterator ite = esSenders_.begin();
       ite != esSenders_.end(); ++ite) {
    delete *ite;
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

    if (!esSenders_[lastSenderIndex_]->produce(*ite)) {
      return false;
    }
    if (++lastSenderIndex_ >= esSenders_.size()) lastSenderIndex_ = 0;
  }
  return true;
}
