#ifndef _INOTIFY_CTX_H_
#define _INOTIFY_CTX_H_

#include <map>

class LuaCtx;
class CnfCtx;

class InotifyCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  InotifyCtx(CnfCtx *cnf) : cnf_(cnf), wfd_(-1) {}
  ~InotifyCtx();

  bool init();
  void loop();
  bool tryReWatch();
  void tryRmWatch(LuaCtx *ctx, int wd);
  void tryRmWatch();
  void globalCheck();

  LuaCtx *getLuaCtx(int wd) {
    std::map<int, LuaCtx *>::iterator pos = fdToCtx_.find(wd);
    return pos != fdToCtx_.end() ? pos->second : 0;
  }

private:
  CnfCtx *cnf_;

  int wfd_;
  std::map<int, LuaCtx *> fdToCtx_;
};

#endif
