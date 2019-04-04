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

class CurlManager {
public:
  CurlManager() : active_(0), capacity_(0) {}
  ~CurlManager();

  bool init(size_t capacity, char errbuf[]);
  void get(CURL **curl, struct curl_slist **list);
  void release(CURL *curl, struct curl_slist *list);

  int overload() {
    int ret;
    pthread_mutex_lock(mutex_);
    ret = active_ > capacity_ ? active_ - capacity_ : 0 ;
    pthread_mutex_unlock(mutex_);
    return ret;
  }

private:
  static bool newCurl(CURL **curl, struct curl_slist **list, char *errbuf = 0);

private:
  pthread_mutex_t *mutex_;

  size_t active_;
  size_t capacity_;
  std::vector<CURL *> curls_;
  std::vector<struct curl_slist *> curlHeaders_;
};

class EsSender {
  template<class T> friend class UNITTEST_HELPER;
public:
  EsSender() : epfd_(-1), pipeRead_(-1), pipeWrite_(-1), multi_(0), events_(0), running_(false) {}
  ~EsSender();

  bool init(CnfCtx *cnf, CurlManager *curlManager, char *errbuf);
  void eventLoop();
  void checkMultiInfo();
  void socketCallback(CURL *curl, curl_socket_t fd, int what);
  bool produce(FileRecord *record);

private:
  void eventCallback(CURL *curl, uint32_t event);
  void timerCallback();

  void consume();
  void addToMulti(FileRecord *record);

private:
  CnfCtx *cnf_;
  CurlManager *curlManager_;

  std::vector<std::string> nodes_;
  std::string userpass_;

  int epfd_;

  int pipeRead_;
  int pipeWrite_;

  CURLM *multi_;
  struct epoll_event *events_;

  volatile bool running_;
  pthread_t tid_;
};

class EsCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  ~EsCtx();
  bool init(CnfCtx *cnf, char *errbuf);
  bool produce(std::vector<FileRecord *> *datas);

private:
  void flowControl();

private:
  CnfCtx *cnf_;

  CurlManager curlManager_;

  size_t lastSenderIndex_;
  std::vector<EsSender *> esSenders_;

  volatile bool running_;
};

#endif
#endif
