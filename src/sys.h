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

inline std::string timeFormat(time_t time, const char *format, int len = -1)
{
  struct tm ltm;
  localtime_r(&time, &ltm);

  if (len > 0) {
    char *buffer = new char[len + 64];
    int n = strftime(buffer, len + 64, format, &ltm);
    std::string s(buffer, n);
    delete[] buffer;
    return s;
  } else {
    char buffer[64];
    int n = strftime(buffer, 64, format, &ltm);
    return std::string(buffer, n);
  }
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

bool file2vector(const char *file, std::vector<std::string> *files, size_t start = 0, size_t end = -1);

bool initSingleton(const char *pidfile, char *errbuf);

} // sys
#endif
