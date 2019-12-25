#ifndef _ESCTX_H_
#define _ESCTX_H_

#include <string>
#include <vector>
#include <list>
#include <pthread.h>
#include <sys/epoll.h>

#include "filerecord.h"
class CnfCtx;

#define MAX_HTTP_HEADER_LEN 8192

enum EventStatus {
  UNINIT, ESTABLISHING, WRITING, READING, IDLE
};

enum HttpRespWant {
  STATUS_LINE, HEADER, HEADER_NAME, HEADER_VALUE,
  BODY, BODY_CHUNK_LEN, BODY_CHUNK_CONTENT, RESP_EOF,
};

class EsUrlManager;

class EsUrl {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsUrl(const std::vector<std::string> &nodes, int idx)
    : status_(UNINIT), fd_(-1), idx_(idx), nodes_(nodes), record_(0) {
    node_ = nodes_[idx_];
  }

  ~EsUrl() {
    if (fd_ > 0) close(fd_);
  }

  void reinit(FileRecord *record, int move = 0);
  void onEvent(int pfd);
  void onTimeout(int pfd, time_t now);
  void onError(const char *error);

  bool idle() const {
    return status_ == UNINIT || status_ == IDLE;
  }

  bool keepalive() const {
    return status_ != UNINIT;
  }

private:
  void initHttpResponseStatusLine(const char *eof);
  void initHttpResponseHeader(const char *eof);
  void initHttpResponseBody(const char *eof);
  bool initHttpResponse(const char *eof);

  int initIOV(struct iovec *iov);

  bool doConnect(int pfd, char *errbuf);
  bool doRequest(int pfd, char *errbuf);
  bool doResponse(int pfd, char *errbuf);

  void destroy() {
    close(fd_);
    fd_ = 1;
    status_ = UNINIT;
  }

private:
  EventStatus status_;
  int fd_;
  time_t activeTime_;
  size_t timeoutRetry_;

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
  size_t wantLen_;
  size_t chunkLen_;
  char *resp_;
  std::string respBody_;
};

class EsSender {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsSender() : epfd_(-1), pipeRead_(-1), pipeWrite_(-1), events_(0), running_(false) {}
  ~EsSender();

  bool init(CnfCtx *cnf, EsUrlManager *urlManager, char *errbuf);
  void eventLoop();
  bool produce(FileRecord *record);

private:
  void consume(int pfd);

private:
  CnfCtx *cnf_;
  EsUrlManager *urlManager_;

  std::vector<std::string> nodes_;
  std::string userpass_;

  int epfd_;

  int pipeRead_;
  int pipeWrite_;

  struct epoll_event *events_;
  std::list<EsUrl*> urls_;

  volatile bool running_;
  pthread_t tid_;
};

class EsCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsCtx() : urlManager_(0) {}

  ~EsCtx();
  bool init(CnfCtx *cnf);
  bool produce(std::vector<FileRecord *> *datas);

private:
  void flowControl();

private:
  CnfCtx *cnf_;
  EsUrlManager *urlManager_;

  size_t lastSenderIndex_;
  std::vector<EsSender *> esSenders_;

  volatile bool running_;
};

#endif
