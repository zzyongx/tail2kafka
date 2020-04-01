#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <glob.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>

#define N100M 100 * 1024 * 1024
#define LOGGER_INIT() Logger *Logger::defLogger = 0;
#define LOGGER_ONCE(once) do {if (Logger::defLogger) Logger::defLogger->setOnce((once)); } while(0)

static const size_t ERR_STR = 4095;
static pthread_mutex_t LOGGER_MUTEX = PTHREAD_MUTEX_INITIALIZER;

static const int   DEBUG_INT = 2;
static const int   INFO_INT  = 3;
static const int   ERROR_INT = 4;
static const int   FATAL_INT = 5;

static const char *DEBUG_PTR = "DEBUG";
static const char *INFO_PTR  = "INFO";
static const char *ERROR_PTR = "ERROR";
static const char *FATAL_PTR = "FATAL";

class Logger {
public:
  enum Level { DEBUG, INFO, ERROR, FATAL };
  enum Rotate { DAY, HOUR, SIZE, NIL };

  static Logger *defLogger;

  static Logger *create(const std::string &file, Rotate rotate, bool def = false,
                        time_t *nowPtr = 0, int nfile = 0) {
    if (def && defLogger) return defLogger;

    std::auto_ptr<Logger> logger(new Logger(file, rotate, nowPtr, nfile));
    if (logger->init()) {
      Logger *ptr = logger.release();
      if (def) defLogger = ptr;
      return ptr;
    } else {
      return 0;
    }
  }

  void setLevel(Level level) {
    if (level == DEBUG) level_ = DEBUG_INT;
    else if (level == INFO) level_ = INFO_INT;
    else if (level == ERROR) level_ = ERROR_INT;
    else if (level == FATAL) level_ = FATAL_INT;
    else assert(0);
  }

  bool setOnce(bool once) {
    once_ = once;
    if (once_ && handle_ > 0) {
      close(handle_);
      handle_ = -1;
    } else if (!once && handle_ < 0) {
      return init();
    }
    return true;
  }

  void reOpen(bool reopen) { reopen_ = reopen; }

  bool bindStdout() {
    bindStdout_ = true;
    return dup2(handle_, STDOUT_FILENO) != -1;
  }

  bool bindStderr() {
    bindStderr_ = true;
    return dup2(handle_, STDERR_FILENO) != -1;
  }

  bool debug(const char *file, int line, int eno, const char *fmt, ...) {
    if (level_ > DEBUG_INT) return true;

    va_list ap;
    va_start(ap, fmt);
    bool rc = log(DEBUG_INT, DEBUG_PTR, file, line, eno, fmt, ap);
    va_end(ap);
    return rc;
  }

  bool info(const char *file, int line, int eno, const char *fmt, ...) {
    if (level_ > INFO_INT) return true;

    va_list ap;
    va_start(ap, fmt);
    bool rc = log(INFO_INT, INFO_PTR, file, line, eno, fmt, ap);
    va_end(ap);
    return rc;
  }

  bool error(const char *file, int line, int eno, const char *fmt, ...) {
    if (level_ > ERROR_INT) return true;

    va_list ap;
    va_start(ap, fmt);
    bool rc = log(ERROR_INT, ERROR_PTR, file, line, eno, fmt, ap);
    va_end(ap);
    return rc;
  }

  bool fatal(const char *file, int line, int eno, const char *fmt, ...) {
    if (level_ > FATAL_INT) return true;

    va_list ap;
    va_start(ap, fmt);
    bool rc = log(FATAL_INT, FATAL_PTR, file, line, eno, fmt, ap);
    va_end(ap);
    return rc;
  }

  bool print(const char *ptr, int len, bool autonl) {
    if (autonl) {
      struct iovec iovs[2] = {{(void *) ptr, static_cast<size_t>(len)}, {(void*) "\n", 1}};
      return writev(handle_, iovs, 2) != -1;
    } else {
      return write(handle_, ptr, len) != -1;
    }
  }

private:
  Logger(const std::string &file, Rotate rotate, time_t *nowPtr, int nfile)
    : limit_(N100M), once_(false), rotate_(rotate), reopen_(false), nowPtr_(nowPtr),
      bindStdout_(false), bindStderr_(false), handle_(-1), file_(file) {
#if _DEBUG_
    setLevel(DEBUG);
#else
    setLevel(INFO);
#endif

    if (nfile == 0) {
      if (rotate_ == SIZE) {
        nfile_ = 12;
      } else if (rotate_ == HOUR) {
        nfile_ = 48;
      } else if (rotate_ ==  DAY) {
        nfile_ = 7;
      }
    } else if (nfile > 0) {
      nfile_ = nfile;
    }

    if (rotate_ == SIZE && nfile < 0) {
      nfile_ = 3;
    }
  }

  bool init() {
    struct tm ltm;
    time_t now = nowPtr_ ? *nowPtr_ : time(0);
    localtime_r(&now, &ltm);

    // timezone off
    gmtOff_ = ltm.tm_gmtoff;
    size_ = 0;
    return openFile(now, &ltm);
  }

  bool canRotate(time_t now) {
    if (rotate_ == NIL) {
      return reopen_;
    } else if (rotate_ == SIZE) {
      return size_ >= limit_;
    } else if (once_) {
      return true;
    }

    time_t fileTime = __sync_fetch_and_add(&fileTime_, 0);
    now      += gmtOff_;
    fileTime += gmtOff_;

    if (rotate_ == DAY) return now / 86400 != fileTime / 86400;
    else if (rotate_ == HOUR) return now / 3600 != fileTime / 3600;
    else assert(0);
  }

  void rotateByTime() {
    std::string pattern = file_ + "_*";
    glob_t g;
    if (nfile_ > 0 && glob(pattern.c_str(), 0, 0, &g) == 0) {
      if (int(g.gl_pathc) > nfile_) {
        std::vector<std::string> files;
        for (size_t i = 0; i < g.gl_pathc; ++i) {
          files.push_back(g.gl_pathv[i]);
        }
        std::sort(files.begin(), files.end());
        for (int i = 0; i < int(g.gl_pathc) - nfile_; ++i) {
          unlink(files[i].c_str());
        }
      }
      globfree(&g);
    }
  }

  void rotateBySize() {
    char buffer[32];
    sprintf(buffer, ".%d", nfile_);
    std::string nfile = file_ + buffer;

    for (int i = nfile_-1; i > 0; --i) {
      sprintf(buffer, ".%d", i);
      std::string rfile = file_ + buffer;

      if (access(rfile.c_str(), F_OK) == 0) rename(rfile.c_str(), nfile.c_str());
      nfile = rfile;
    }
    rename(file_.c_str(), nfile.c_str());
  }

  bool openFile(time_t now, struct tm *tm) {
    std::string file(file_);
    if (rotate_ == DAY || rotate_ == HOUR) {
      char buffer[32];
      size_t n = 0;
      if (rotate_ == DAY) n = strftime(buffer, 32, "_%Y-%m-%d", tm);
      else n = strftime(buffer, 32, "_%Y-%m-%d_%H", tm);

      rotateByTime();
      file.append(buffer, n);
    } else if (rotate_ == SIZE) {
      if (size_ == 0) {
        struct stat st;
        if (stat(file.c_str(), &st) == 0) size_ = st.st_size;
      }
      if (size_ >= limit_) rotateBySize();
    }

    int fd = open(file.c_str(), O_WRONLY | O_APPEND | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd != -1) {
      if (handle_ > 0) {
        close(handle_);
        if (bindStdout_) dup2(handle_, STDOUT_FILENO);
        if (bindStderr_) dup2(handle_, STDERR_FILENO);
      }

      handle_ = fd;

      struct stat st;
      fstat(fd, &st);
      size_ = st.st_size;

      fileTime_ = now;
      reopen_ = false;
      return true;
    } else {
      return false;
    }
  }

  bool safeOpenFile(time_t now, struct tm *tm) {
    bool rc = true;
    pthread_mutex_lock(&LOGGER_MUTEX);
    if (canRotate(now)) rc = openFile(now, tm);
    pthread_mutex_unlock(&LOGGER_MUTEX);
    return rc;
  }

  static int microseconds() {
    struct timeval now;
    gettimeofday(&now, 0);
    return now.tv_usec;
  }

  bool log(int level, const char *levelPtr, const char *file, int line, int eno, const char *fmt, va_list ap) {
    struct tm ltm;
    time_t now = nowPtr_ ? *nowPtr_ : time(0);
    localtime_r(&now, &ltm);

    if (canRotate(now)) safeOpenFile(now, &ltm);

    /* one more for '\n' */
    char errstr[ERR_STR + 1];
    size_t n = 0;

    int micros = level == DEBUG_INT ? microseconds() : 0;
    n = strftime(errstr, ERR_STR, "%Y-%m-%d %H:%M:%S ", &ltm);
    n += snprintf(errstr + n, ERR_STR - n, "[%s] #%d #%s@%d \"%d:%s\" ",
                  levelPtr, micros, file, line, eno, eno ? strerror(eno) : "");

    n += vsnprintf(errstr + n, ERR_STR - n, fmt, ap);

    if (n >= ERR_STR) n = ERR_STR;
    errstr[n++] = '\n';

    size_ += n;
    bool rc = write(handle_, errstr, n) != -1;

    if (once_) {
      close(handle_);
      handle_ = -1;
    }
    return rc;
  }

private:
  size_t size_;
  size_t limit_;
  bool once_;

  uint8_t level_;
  Rotate  rotate_;
  int     nfile_;
  bool    reopen_;
  time_t *nowPtr_;

  bool bindStdout_;
  bool bindStderr_;

  int handle_;
  std::string file_;

  long   gmtOff_;
  time_t fileTime_;
};

#define LOG_STDOUT(level, eno, fmt, args...) do {        \
  time_t now__ = time(0);                                \
  struct tm ltm__;                                       \
  localtime_r(&now__, &ltm__);                           \
  char timestr[64];                                      \
  strftime(timestr, 64, "[%Y-%m-%d %H:%M:%S]", &ltm__);  \
  printf("%s [%s] #%s@%d \"%d:%s\" "fmt"\n",             \
         timestr, level, __FILE__, __LINE__,             \
         eno, eno ? strerror(eno) : "", ##args);         \
} while (0)

#define log_fatal(eno, fmt, args...) do {                                                \
  if (Logger::defLogger) Logger::defLogger->fatal(__FILE__, __LINE__, eno, fmt, ##args); \
  else LOG_STDOUT("FATAL", eno, fmt, ##args);                                            \
} while (0)

#define log_error(eno, fmt, args...) do {                                                \
  if (Logger::defLogger) Logger::defLogger->error(__FILE__, __LINE__, eno, fmt, ##args); \
  else LOG_STDOUT("ERROR", eno, fmt, ##args);                                            \
} while (0)

#define log_info(eno, fmt, args...)  do {                                                \
  if (Logger::defLogger) Logger::defLogger->info(__FILE__, __LINE__, eno, fmt, ##args);  \
  else LOG_STDOUT("INFO",  eno, fmt, ##args);                                            \
} while (0)

# if _DEBUG_
#define log_debug(eno, fmt, args...) do {                                                \
  if (Logger::defLogger) Logger::defLogger->debug(__FILE__, __LINE__, eno, fmt, ##args); \
  else LOG_STDOUT("DEBUG", eno, fmt, ##args);                                            \
} while (0)
# else
#define log_debug(eno, fmt, args...) do {                                                \
  if (Logger::defLogger) Logger::defLogger->debug(__FILE__, __LINE__, eno, fmt, ##args); \
} while (0)
# endif

#define log_opaque(ptr, len, autonl) do {                                                \
  if (Logger::defLogger) Logger::defLogger->print(ptr, len, autonl);                     \
  else printf("%.*s%s", (int) len, ptr, autonl ? "\n" : "");                             \
} while (0)

#endif
