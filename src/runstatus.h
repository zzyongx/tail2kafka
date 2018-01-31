#ifndef _RUN_STATUS_H_
#define _RUN_STATUS_H_

#include <cassert>

class RunStatus {
public:
  enum Want {WAIT, START1, START2, RELOAD, STOP, IGNORE};
  static RunStatus *create() {
    RunStatus *runStatus = new RunStatus;
    runStatus->want_ = START1;
    return runStatus;
  }

  Want get() const {
    return want_;
  }

  const char *status() const {
    switch (want_) {
    case WAIT:   return "wait";
    case START1: return "start1";
    case START2: return "start2";
    case RELOAD: return "reload";
    case STOP:   return "stop";
    default: assert(0);
    }
  }

  void set(Want want) {
    want_ = want;
  }

private:
  RunStatus() {}
  Want want_;
};

#endif
