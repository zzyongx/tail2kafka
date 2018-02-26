#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <cassert>
#include <string>
#include <memory>
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>

#define LOGGER_INIT() Logger *Logger::defLogger = 0;

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
  enum Rotate { DAY, HOUR, NIL };

  static Logger *defLogger;

  static Logger *create(const std::string &file, Rotate rotate, bool def = false, time_t *nowPtr = 0) {
    if (def && defLogger) return defLogger;

    std::auto_ptr<Logger> logger(new Logger(file, rotate, nowPtr));
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
  Logger(const std::string &file, Rotate rotate, time_t *nowPtr)
    : rotate_(rotate), reopen_(false), nowPtr_(nowPtr),
      bindStdout_(false), bindStderr_(false), handle_(-1), file_(file) {
#if _DEBUG_
    setLevel(DEBUG);
#else
    setLevel(INFO);
#endif
  }

  bool init() {
    struct tm ltm;
    time_t now = nowPtr_ ? *nowPtr_ : time(0);
    localtime_r(&now, &ltm);

    // timezone off
    gmtOff_ = ltm.tm_gmtoff;
    return openFile(now, &ltm);
  }

  bool canRotate(time_t now) {
    if (rotate_ == NIL) return reopen_;

    time_t fileTime = __sync_fetch_and_add(&fileTime_, 0);
    now      += gmtOff_;
    fileTime += gmtOff_;

    if (rotate_ == DAY) return now / 86400 != fileTime / 86400;
    else if (rotate_ == HOUR) return now / 3600 != fileTime / 3600;
    else assert(0);
  }

  bool openFile(time_t now, struct tm *tm) {
    std::string file(file_);
    if (rotate_ != NIL) {
      char buffer[32];
      size_t n = 0;
      if (rotate_ == DAY) n = strftime(buffer, 32, "_%Y-%m-%d", tm);
      else if (rotate_ == HOUR) n = strftime(buffer, 32, "_%Y-%m-%d_%H", tm);
      else assert(0);
      file.append(buffer, n);
    }

    int fd = open(file.c_str(), O_WRONLY | O_APPEND | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd != -1) {
      int tmp = handle_;
      handle_ = fd;
      close(tmp);
      if (bindStdout_) dup2(handle_, STDOUT_FILENO);
      if (bindStderr_) dup2(handle_, STDERR_FILENO);

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

    return (write(handle_, errstr, n) != -1);
  }

private:
  uint8_t level_;
  Rotate  rotate_;
  bool    reopen_;
  time_t *nowPtr_;

  bool bindStdout_;
  bool bindStderr_;

  int handle_;
  std::string file_;

  long   gmtOff_;
  time_t fileTime_;
};

// extern Logger *Logger::defLogger = 0;

#ifdef NO_LOGGER

# define LOG_IMPL(level, eno, fmt, args...) do {       \
  time_t now = time(0);                                \
  struct tm ltm;                                       \
  localtime_r(&now, &ltm);                             \
  char timestr[64];                                    \
  strftime(timestr, 64, "[%Y-%m-%d %H:%M:%S]", &ltm);  \
  printf("%s [%s] #%s@%d \"%d:%s\" "fmt"\n",           \
         timestr, level, __FILE__, __LINE__,           \
         eno, eno ? strerror(eno) : "", ##args);       \
} while (0)

# define log_fatal(eno, fmt, args...) LOG_IMPL("FATAL", eno, fmt, ##args)
# define log_error(eno, fmt, args...) LOG_IMPL("ERROR", eno, fmt, ##args)
# define log_info(eno, fmt, args...)  LOG_IMPL("INFO",  eno, fmt, ##args)
# define log_debug(eno, fmt, args...) LOG_IMPL("DEBUG", eno, fmt, ##args)
# define log_opaque(ptr, len, autonl) printf("%.*s%s", (int) len, ptr, autonl ? "\n" : "")

#else

# define log_fatal(eno, fmt, args...) Logger::defLogger->fatal(__FILE__, __LINE__, eno, fmt, ##args)
# define log_error(eno, fmt, args...) Logger::defLogger->error(__FILE__, __LINE__, eno, fmt, ##args)
# define log_info(eno, fmt, args...)  Logger::defLogger->info(__FILE__, __LINE__, eno, fmt, ##args)
# define log_debug(eno, fmt, args...) Logger::defLogger->debug(__FILE__, __LINE__, eno, fmt, ##args)
# define log_opaque(ptr, len, autonl) Logger::defLogger->print(ptr, len, autonl);

#endif

#endif
