#ifndef _ESCTX_H_
#define _ESCTX_H_

#include <string>
#include <vector>
#include <list>
#include <pthread.h>


#include "gnuatomic.h"
#include "filerecord.h"
class CnfCtx;

#define MAX_HTTP_HEADER_LEN 8192

enum EventStatus {
  UNINIT, ESTABLISHING, WRITING, READING, IDLE
};

inline const char *eventStatusToString(EventStatus status)
{
  switch (status) {
  case UNINIT: return "uninit";
  case ESTABLISHING: return "establishing";
  case WRITING: return "writing";
  case READING: return "reading";
  case IDLE: return "idle";
  default: return "unknow";
  }
}

enum HttpRespWant {
  STATUS_LINE, HEADER, HEADER_NAME, HEADER_VALUE,
  BODY, BODY_CHUNK_LEN, BODY_CHUNK_CONTENT, RESP_EOF,
};

class EsUrlManager;

class EsUrl {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsUrl(const std::vector<std::string> &nodes, int idx, EsUrlManager *mgr)
    : pool_(true), status_(UNINIT), fd_(-1), urlManager_(mgr), keepalive_(0),
      idx_(idx), nodes_(nodes), node_(nodes_[idx_]), record_(0) {}

  ~EsUrl() {
    if (fd_ > 0) close(fd_);
  }

  bool idle() const {
    return status_ == IDLE;
  }

  void reinit(FileRecord *record, bool move = false);
  bool onEvent(int pfd);
  bool onTimeout(int pfd, time_t now);
  bool onError(int pfd, const char *error);

  bool pool(bool p) {
    assert(record_ == 0 && (status_ == IDLE || status_ == UNINIT));

    bool r = pool_;
    pool_ = p;
    return r;
  }

private:
  void initHttpResponseStatusLine(const char *eof);
  void initHttpResponseHeader(const char *eof);
  void initHttpResponseBody(const char *eof);
  bool initHttpResponse(const char *eof);

  int initIOV(struct iovec *iov);

  bool doConnect(int pfd, char *errbuf);
  bool doConnectFinish(int pfd, char *errbuf);
  bool doRequest(int pfd, char *errbuf);
  bool doResponse(int pfd, char *errbuf);

  void destroy(int pfd);

private:
  bool pool_;
  EventStatus status_;
  int fd_;
  time_t activeTime_;
  size_t timeoutRetry_;
  EsUrlManager *urlManager_;
  int keepalive_;

  int idx_;
  std::vector<std::string> nodes_;
  std::string node_;

  FileRecord *record_;

  std::string url_;
  char header_[MAX_HTTP_HEADER_LEN];
  int nheader_;
  const char *body_;
  int nbody_;
  int offset_;

  HttpRespWant respWant_;
  int respCode_;
  int wantLen_;
  int chunkLen_;
  char *resp_;
  std::string respBody_;
};

class EsUrlManager {
public:
  EsUrlManager(const std::vector<std::string> &nodes, int capacity)
    : active_(0), capacity_(capacity), nodes_(nodes) {

    for (size_t i = 0; i < capacity_; ++i) {
      EsUrl *url = new EsUrl(nodes, i % nodes.size(), this);
      urls_.push_back(url);
      holder_.push_back(url);
    }
    nodes_ = nodes;
  }

  ~EsUrlManager() {
    for (std::list<EsUrl *>::iterator ite = holder_.begin();
         ite != holder_.end(); ++ite) {
      delete *ite;
    }
  }

  EsUrl *get();
  bool release(EsUrl *url);

  size_t load() const {
    size_t *ptr = const_cast<size_t*>(&active_);
    return util::atomic_get(ptr);
  }

private:
  size_t active_;
  size_t capacity_;
  std::vector<std::string> nodes_;

  std::vector<EsUrl *> urls_;
  std::list<EsUrl *> holder_;
};

class EsSender {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsSender()
    : epfd_(-1), pipeRead_(-1), pipeWrite_(-1), events_(0),
      urlManager_(0), running_(false) {}

  ~EsSender();

  bool init(CnfCtx *cnf, size_t capacity);
  void eventLoop();
  bool produce(FileRecord *record);

private:
  void consume(int pfd, bool once);
  bool flowControl(bool block);

private:
  CnfCtx *cnf_;

  std::vector<std::string> nodes_;
  std::string userpass_;

  int epfd_;

  int pipeRead_;
  int pipeWrite_;

  struct epoll_event *events_;
  std::list<EsUrl*> urls_;

  size_t capacity_;
  EsUrlManager *urlManager_;

  volatile bool running_;
  pthread_t tid_;
};

class EsCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  ~EsCtx();
  bool init(CnfCtx *cnf);
  bool produce(std::vector<FileRecord *> *datas);

private:
  CnfCtx *cnf_;

  size_t lastSenderIndex_;
  std::vector<EsSender *> esSenders_;

  volatile bool running_;
};

#endif
