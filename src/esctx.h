#ifdef ENABLE_TAIL2ES
#ifndef _ESCTX_H_
#define _ESCTX_H_

#include <string>
#include <vector>
#include <pthread.h>
#include <sys/epoll.h>
#include <curl/curl.h>

#include "filerecord.h"
class CnfCtx;
class LuaCtx;

class EsCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsCtx() : epfd_(-1), pipeRead_(-1), pipeWrite_(-1), multi_(0), events_(0), running_(false) {}

  ~EsCtx();
  bool init(CnfCtx *cnf, char *errbuf);
  bool produce(std::vector<FileRecord *> *datas);

  void eventLoop();
  void socketCallback(CURL *curl, curl_socket_t fd, int what);

private:
  bool newCurl();
  void eventCallback(CURL *curl, uint32_t event);

  void consumeFileRecords();
  void addToMulti(FileRecord *record);

private:
  CnfCtx *cnf_;

  std::vector<std::string> nodes_;
  std::string userpass_;
  std::vector<CURL *> curls_;
  std::vector<struct curl_slist *> curlHeaders_;

  int epfd_;

  int pipeRead_;
  int pipeWrite_;

  CURLM *multi_;
  struct epoll_event *events_;

  volatile bool running_;
  pthread_t tid_;
};

#endif
#endif
