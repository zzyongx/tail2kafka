#ifndef _METRICS_H_
#define _METRICS_H_

#include <string>
#include <vector>
#include <curl/curl.h>
#include "taskqueue.h"

namespace util {

class Metrics {
public:
  static bool create(const char *pingbackUrl, char *errbuf);
  static void pingback(const char *event, const char *fmt, ...);

private:
  Metrics() {}
  static void destroy();
  static Metrics *metrics_;

private:
  std::string pingbackUrl_;
  TaskQueue   tq_;

  CURL         *curl_;
};

} // namespace util

#endif
