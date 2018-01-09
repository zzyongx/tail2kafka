#ifndef _SYS_H_
#define _SYS_H_

#include <vector>
#include <string>
#include <time.h>
#include "runstatus.h"

namespace sys {

inline void nanosleep(int ms)
{
  struct timespec spec = {0, ms * 1000 * 1000};
  nanosleep(&spec, 0);
}

inline std::string timeFormat(time_t time, const char *format)
{
  struct tm ltm;
  localtime_r(&time, &ltm);

  char buffer[64];
  int n = strftime(buffer, 64, format, &ltm);
  return std::string(buffer, n);
}

class SignalHelper {
public:
  static RunStatus       *runStatusPtr;
  static size_t           signoCount;
  static int             *signosPtr;
  static RunStatus::Want *wantsPtr;

  SignalHelper(char *errbuf) : errbuf_(errbuf) {}

  bool signal(RunStatus *runStatus, int num, int *signos, RunStatus::Want *wants);

 // last argument must less than 0
  bool block(int signo, ...);
  int suspend(int signo, ...);
  bool setmask(int signo, ...);

private:
  char *errbuf_;
};

bool endsWith(const char *haystack, const char *needle);
bool readdir(const char *dir, const char *suffix, std::vector<std::string> *files, char *errbuf);
bool isdir(const char *dir, char *errbuf);

bool initSingleton(const char *pidfile, char *errbuf);

} // sys
#endif
