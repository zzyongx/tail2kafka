#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdarg.h>

#include "logger.h"
#include "taskqueue.h"
#include "metrics.h"
using namespace util;

class PingbackTask : public TaskQueue::Task {
public:
  PingbackTask(CURL *curl, const char *url)
    : TaskQueue::Task(3), curl_(curl), url_(url)  {
  }
  bool doIt();
  ~PingbackTask();

private:
  CURL *curl_;
  const char *url_;
};

// black hole
static size_t curlWriteCallback(void *, size_t size, size_t nmemb, void *)
{
  return size * nmemb;
}


bool PingbackTask::doIt()
{
  curl_easy_reset(curl_);
  curl_easy_setopt(curl_, CURLOPT_URL, url_);
  curl_easy_setopt(curl_, CURLOPT_NOPROGRESS, 1L);
#ifdef CURLOPT_TCP_KEEPALIVE
  curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
#endif
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, curlWriteCallback);

  CURLcode rc = curl_easy_perform(curl_);
  if (rc != CURLE_OK) {
    log_error(0, "pingback %s error %s", url_, curl_easy_strerror(rc));
    return false;
  }

  long status = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &status);
  log_info(0, "pingback %s status %ld", url_, status);

  return true;
}

PingbackTask::~PingbackTask() {
  delete []url_;
}

Metrics *Metrics::metrics_ = 0;

bool Metrics::create(const char *pingbackUrl, char *errbuf)
{
  if (metrics_ != 0) return true;

  std::auto_ptr<Metrics> metrics(new Metrics());
  bool rc = false;
  do {
    metrics->curl_ = 0;
    if (pingbackUrl) {
      metrics->pingbackUrl_ = pingbackUrl;
      metrics->curl_ = curl_easy_init();
      if (!metrics->curl_) break;
    }

    if (!metrics->tq_.start(errbuf)) break;
    rc = true;
  } while (0);

  metrics_ = metrics.release();
  if (!rc) destroy();

  atexit(Metrics::destroy);
  return true;
}

void Metrics::destroy()
{
  if (metrics_ == 0) return;

  if (metrics_->curl_) curl_easy_cleanup(metrics_->curl_);
  metrics_->tq_.stop(true);

  delete metrics_;
  metrics_ = 0;
}

#define URLN 8192
void Metrics::pingback(const char *event, const char *fmt, ...)
{
  if (metrics_ == 0 || metrics_->curl_ == 0) return;

  char *url = new char[URLN];
  int n = snprintf(url, URLN, "%s?event=%s&", metrics_->pingbackUrl_.c_str(), event);

  va_list ap;
  va_start(ap, fmt);
  n += vsnprintf(url + n, URLN - n, fmt, ap);
  va_end(ap);

  metrics_->tq_.submit(new PingbackTask(metrics_->curl_, url));
}
