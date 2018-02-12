#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "sys.h"

#define MAX_ERR_LEN    512

namespace sys {
RunStatus       *SignalHelper::runStatusPtr = 0;
int             *SignalHelper::signosPtr = 0;
RunStatus::Want *SignalHelper::wantsPtr = 0;
size_t           SignalHelper::signoCount = 0;

void sigHandle(int signo)
{
  for (size_t i = 0; i < SignalHelper::signoCount; ++i) {
    if (signo == SignalHelper::signosPtr[i]) {
      SignalHelper::runStatusPtr->set(SignalHelper::wantsPtr[i]);
    }
  }
}

bool SignalHelper::signal(RunStatus *runStatus, int count, int *signos, RunStatus::Want *wants)
{
  if (signosPtr) delete[] signosPtr;
  if (wantsPtr)  delete[] wantsPtr;

  signosPtr = new int[count];
  wantsPtr  = new RunStatus::Want[count];
  signoCount = count;
  runStatusPtr = runStatus;

  struct sigaction sa;
  for (int i = 0; i < count; ++i) {
    memset(&sa, 0x00, sizeof(sa));
    if (wants[i] == RunStatus::IGNORE) {
      sa.sa_handler = SIG_IGN;
    } else {
      sa.sa_handler = sigHandle;
    }
    sigemptyset(&sa.sa_mask);
    if (sigaction(signos[i], &sa, NULL) == -1) {
      if (errbuf_) snprintf(errbuf_, MAX_ERR_LEN, "sigaction error %d:%s", errno, strerror(errno));
      return false;
    }
    signosPtr[i] = signos[i];
    wantsPtr[i]  = wants[i];
  }
  return true;
}

#define SIGSET_INIT(set) do {        \
  sigemptyset(&set);                 \
  va_list valist;                    \
  va_start(valist, signo);           \
  while (signo > 0) {                \
    sigaddset(&set, signo);          \
    signo = va_arg(valist, int);     \
  }                                  \
  va_end(valist);                   \
} while (0)

bool SignalHelper::block(int signo, ...)
{
  sigset_t set;
  SIGSET_INIT(set);

  if (sigprocmask(SIG_BLOCK, &set, NULL) == -1) {
    if (errbuf_) snprintf(errbuf_, MAX_ERR_LEN, "sigprocmask block failed, %s", strerror(errno));
    return false;
  }
  return true;
}

int SignalHelper::suspend(int signo, ...)
{
  sigset_t set;
  SIGSET_INIT(set);
  return sigsuspend(&set);
}


bool SignalHelper::setmask(int signo, ...)
{
  sigset_t set;
  SIGSET_INIT(set);

  if (sigprocmask(SIG_SETMASK, &set, NULL) == -1) {
    if (errbuf_) snprintf(errbuf_, MAX_ERR_LEN, "sigprocmask set failed, %s", strerror(errno));
    return false;
  }
  return true;
}

/* pidfile may stale, this's not a perfect method */
bool initSingleton(const char *pidfile, char *errbuf)
{
  int fd = open(pidfile, O_CREAT | O_WRONLY, 0644);
  if (lockf(fd, F_TLOCK, 0) == 0) {
    ftruncate(fd, 0);
    char buffer[32];
    int len = snprintf(buffer, 32, "%d", getpid());
    write(fd, buffer, len);
    return true;
  } else {
    if (errbuf) snprintf(errbuf, MAX_ERR_LEN, "lock %s failed", pidfile);
    return false;
  }
}

bool endsWith(const char *haystack, const char *needle)
{
  size_t haystackLen = strlen(haystack);
  size_t needleLen = strlen(needle);

  size_t i;
  for (i = 0; i < haystackLen && i < needleLen; ++i) {
    if (haystack[haystackLen-1-i] != needle[needleLen-1-i]) return false;
  }
  return i == needleLen && i <= haystackLen;
}

bool readdir(const char *dir, const char *suffix, std::vector<std::string> *files, char *errbuf)
{
  DIR *dh = opendir(dir);
  if (!dh) {
    if (errbuf) snprintf(errbuf, MAX_ERR_LEN, "could not opendir %s", dir);
    return false;
  }

  static const size_t N = 1024;
  char fullpath[N];

  struct dirent *ent;
  while ((ent = readdir(dh))) {
    if (suffix && !endsWith(ent->d_name, suffix)) continue;
    snprintf(fullpath, N, "%s/%s", dir, ent->d_name);
    files->push_back(fullpath);
  }

  closedir(dh);
  return true;
}

bool isdir(const char *dir, char *errbuf)
{
  struct stat st;
  if (stat(dir, &st) != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "stat %s error %s", dir, strerror(errno));
    return false;
  }

  if (S_ISDIR(st.st_mode)) {
    return true;
  } else {
    snprintf(errbuf, MAX_ERR_LEN, "%s is not directory", dir);
    return false;
  }
}

bool file2vector(const char *file, std::vector<std::string> *lines, size_t start, size_t size)
{
  FILE *fp = fopen(file, "r");
  if (!fp) return false;

  char buffer[8192];
  size_t line = 0;

  while (fgets(buffer, 8192, fp)) {
    if (line >= start && line - start < size) {
      size_t len = strlen(buffer);
      if (buffer[len-1] == '\n') {
        lines->push_back(std::string(buffer, len-1));
        line++;
      } else {
        fclose(fp);
        return false;
      }
    }
  }

  fclose(fp);
  return true;
}

} // sys
