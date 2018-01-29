#define _LARGEFILE64_SOURCE

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <string>
#include <map>
#include <errno.h>
#include <signal.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>
#include "sys.h"
#include "runstatus.h"
#include "logger.h"
#include "transform.h"

LOGGER_INIT();

class Offset {
public:
  Offset() : fd_(-1), offset_(RD_KAFKA_OFFSET_END) {}
  ~Offset() { if (fd_ != -1) close(fd_); }

  bool init(const char *dir, const char *topic, int partition);
  uint64_t get() const { return offset_; }
  void update(uint64_t offset) { pwrite(fd_, &offset, sizeof(offset), 0); }

private:
  int      fd_;
  uint64_t offset_;
};

struct FdCache {
  int fd;
  std::string file;
  std::vector<struct iovec> iovs;

  size_t rkmSize;
  rd_kafka_message_t **rkms;

  FdCache() : fd(-1), rkmSize(0), rkms(0) {}

  ~FdCache() {
    assert(rkmSize == 0);
    if (fd != -1) close(fd);
    if (rkms) delete[] rkms;
  }

  void clear() {
    for (size_t i = 0; i < rkmSize; ++i) rd_kafka_message_destroy(rkms[i]);
    rkmSize = 0;
    iovs.clear();
  }
};

class KafkaConsumer {
public:
  static KafkaConsumer *create(const char *wdir, const char *brokers, const char *topic, int partition);

  ~KafkaConsumer() {
    if (rkqu_) rd_kafka_queue_destroy(rkqu_);
    if (rkt_)  rd_kafka_topic_destroy(rkt_);
    if (rk_)   rd_kafka_destroy(rk_);
  }

  bool loop(RunStatus *runStatus, Transform *transform);

private:
  KafkaConsumer() : rk_(0), rkt_(0), rkqu_(0) {}

  bool addToCache(rd_kafka_message_t *rkm, std::string *host, std::string *file);
  bool flushCache(bool force, uint64_t off, bool eof, const std::string &host, std::string *ofile);
  bool write(rd_kafka_message_t *rkm, uint64_t off);

private:
  const char *wdir_;
  const char *topic_;
  int         partition_;

  rd_kafka_t       *rk_;
  rd_kafka_topic_t *rkt_;
  rd_kafka_queue_t *rkqu_;

  Offset offset_;
  std::map<std::string, FdCache> fdCache_;
};

static bool initSingleton(const char *datadir, const char *topic, int partition);

int main(int argc, char *argv[])
{
  if (argc < 5) {
    fprintf(stderr, "%s kafka-broker topic partition datadir [notify] [informat:lua:outformat:interval:delay]\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *brokers   = argv[1];
  const char *topic     = argv[2];
  int         partition = atoi(argv[3]);
  const char *datadir   = argv[4];

  const char *notify = argc >= 5 ? argv[5] : 0;
  const char *output = argc >= 6 ? argv[6] : "raw::raw";

  char buffer[1024];
  snprintf(buffer, 1024, "%s/%s.%d.log", datadir, topic, partition);
  Logger::create(buffer, Logger::DAY, true);

  snprintf(buffer, 1024, "%s/%s", datadir, topic);
  mkdir(buffer, 0755);

  Transform *transform = Transform::create(datadir, topic, partition, notify, output, buffer);
  if (transform == 0) {
    fprintf(stderr, "create transform error %s\n", buffer);
    //  return EXIT_FAILURE;
  }

  RunStatus *runStatus = RunStatus::create();
  sys::SignalHelper signalHelper(buffer);

  int signos[] = { SIGTERM, SIGINT };
  RunStatus::Want wants[] = { RunStatus::STOP, RunStatus::STOP };
  if (!signalHelper.signal(runStatus, 2, signos, wants)) {
    log_fatal(errno, "install signal %s", buffer);
    return EXIT_FAILURE;
  }

  if (!initSingleton(datadir, topic, partition)) return EXIT_FAILURE;

  KafkaConsumer *ctx = KafkaConsumer::create(datadir, brokers, topic, partition);
  if (!ctx) return EXIT_FAILURE;

  bool rc = ctx->loop(runStatus, transform);
  delete ctx;

  log_info(0, "exit");
  return rc ? EXIT_SUCCESS : EXIT_FAILURE;
}

bool Offset::init(const char *wdir, const char *topic, int partition)
{
  char path[512];
  snprintf(path, 512, "%s/%s.%d.offset", wdir, topic, partition);

  bool rc = true;
  struct stat st;
  if (stat(path, &st) == 0) {
    fd_ = open(path, O_RDWR, 0644);
    ssize_t nn = pread(fd_, &offset_, sizeof(offset_), 0);
    if (nn == 0) {
      log_error(0, "%s:%d empty offset file, use default", topic, partition);
    } else if (nn < 0) {
      log_fatal(errno, "%s:%d pread()", topic, partition);
      rc = false;
    }
  } else if (errno == ENOENT) {
    fd_ = open(path, O_CREAT | O_WRONLY, 0644);
    if (fd_ == -1) {
      log_fatal(errno, "%s:%d open %s error", topic, partition, path);
      rc = false;
    } else {
      log_error(0, "%s:%d first create, use default", topic, partition);
    }
  } else {
    log_fatal(errno, "%s:%d stat %s error", topic, partition, path);
    rc = false;
  }

  return rc;
}

KafkaConsumer *KafkaConsumer::create(const char *wdir, const char *brokers, const char *topic, int partition)
{
  std::auto_ptr<KafkaConsumer> ctx(new KafkaConsumer);

  char errstr[512];

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "broker.version.fallback", "0.8.2.1", 0, 0);

  ctx->rk_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (rd_kafka_brokers_add(ctx->rk_, brokers) == 0) {
    log_fatal(0, "invalid brokers %s", brokers);
    return 0;
  }

  ctx->rkt_  = rd_kafka_topic_new(ctx->rk_, topic, 0);
  ctx->rkqu_ = rd_kafka_queue_new(ctx->rk_);

  if (!ctx->offset_.init(wdir, topic, partition)) return 0;
  if (rd_kafka_consume_start_queue(ctx->rkt_, partition, ctx->offset_.get(), ctx->rkqu_) == -1) {
    log_fatal(0, "%s:%d failed to start consuming: %s", topic, partition, rd_kafka_err2name(rd_kafka_last_error()));
    return 0;
  }

  ctx->wdir_      = wdir;
  ctx->topic_     = topic;
  ctx->partition_ = partition;

  return ctx.release();
}

bool KafkaConsumer::addToCache(rd_kafka_message_t *rkm, std::string *host, std::string *file)
{
  bool eof = false;
  char flag = ((char *) rkm->payload) [0];
  if (flag == '*' || flag == '#') {
    char *sp = (char *) memchr(rkm->payload, ' ', rkm->len);
    assert(sp);

    *sp = '\0';
    host->assign((char *) rkm->payload + 1);
    *sp = ' ';

    if (flag == '*') {
      FdCache &fdCache = fdCache_[*host];

      if (!fdCache.rkms) fdCache.rkms = new rd_kafka_message_t*[IOV_MAX];

      struct iovec iov = { sp+1, rkm->len - (sp+1 - (char *) rkm->payload) };
      fdCache.iovs.push_back(iov);
      fdCache.rkms[fdCache.rkmSize++] = rkm;
    } else {
      log_info(0, "META %ld %.*s", rkm->offset, (int) rkm->len, (char *) rkm->payload);

      std::string s((char *)rkm->payload, rkm->len);
      if (s.find("End") != std::string::npos) {
        const char *at = (char *) memchr(rkm->payload, '@', rkm->len);
        if (at) {
          eof = true;
          at++;
          const char *end = (char *) rkm->payload + rkm->len;
          while (*at != ' ' && at < end) file->append(1, *at++);
        }
      }
      rd_kafka_message_destroy(rkm);
    }
  }
  return eof;
}

bool KafkaConsumer::flushCache(bool force, uint64_t off, bool eof, const std::string &host, std::string *ofile)
{
  bool flush = false;
  for (std::map<std::string, FdCache>::iterator ite = fdCache_.begin(); ite != fdCache_.end(); ++ite) {
    FdCache &fdCache = ite->second;
    if (!(force || fdCache.rkmSize == IOV_MAX || (eof && host == ite->first))) continue;

    flush = true;
    if (fdCache.fd < 0) {
      char path[1024];
      snprintf(path, 1024, "%s/%s/%s", wdir_, topic_, ite->first.c_str());
      int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
      if (fd == -1) {
        log_fatal(errno, "open %s error", path);
        exit(EXIT_FAILURE);
      }
      fdCache.fd = fd;
      fdCache.file = path;
    }

    ssize_t wantn = 0;
    for (size_t i = 0; i < fdCache.iovs.size(); ++i) wantn += fdCache.iovs[i].iov_len;

    if (wantn > 0) {
      ssize_t n = writev(fdCache.fd, &(fdCache.iovs[0]), fdCache.iovs.size());
      if (n != wantn) {
        log_fatal(errno, "%s:%d %s writev error", topic_, partition_, ite->first.c_str());
        exit(EXIT_FAILURE);
      }
    }

    if (ite->first == host && ofile) ofile->assign(ite->second.file);
    fdCache.clear();
  }

  if (flush) offset_.update(off);
  return true;
}

bool KafkaConsumer::write(rd_kafka_message_t *rkm, uint64_t off)
{
  bool rc = true;
  std::string host, ofile, nfile;

  bool eof = addToCache(rkm, &host, &nfile);
  rc = flushCache(false, off, eof, host, &ofile);

  if (eof) {
    fdCache_.erase(host);

    size_t slash = nfile.rfind('/');
    if (slash == std::string::npos) nfile = ofile + "_" + nfile;
    else nfile = ofile + "_" + nfile.substr(slash+1);

    if (rename(ofile.c_str(), nfile.c_str()) == -1) {
      log_fatal(errno, "%s:%d rename %s to %s error", topic_, partition_, ofile.c_str(), nfile.c_str());
      exit(EXIT_FAILURE);
    } else {
      log_info(0, "%s:%d rename %s to %s", topic_, partition_, ofile.c_str(), nfile.c_str());
    }
  }

  return rc;
}

bool KafkaConsumer::loop(RunStatus *runStatus, Transform *)
{
  uint64_t off = -1;
  while (runStatus->get() != RunStatus::STOP) {
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume_queue(rkqu_, 1000);
    if (!rkm) continue;  // timeout

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
      log_error(0, "consume %s:%d error %s\n", topic_, partition_, rd_kafka_message_errstr(rkm));
      continue;
    }

    off = rkm->offset;
    log_debug(0, "data @%ld %.*s\n", off, (int) rkm->len, (char *) rkm->payload);

    write(rkm, off);
  }

  if (off != (uint64_t)-1) flushCache(true, off, false, "", 0);
  return true;
}

static char LOCK_FILE[1024] = {0};
static void deleteLockFile()
{
  if (LOCK_FILE[0] != '\0') unlink(LOCK_FILE);
}

/* pidfile may stale, this's not a perfect method */

bool initSingleton(const char *datadir, const char *topic, int partition)
{
  if (datadir[0] == '-') return true;

  snprintf(LOCK_FILE, 1024, "%s/%s.%d.lock", datadir, topic, partition);
  if (sys::initSingleton(LOCK_FILE, 0)) {
    atexit(deleteLockFile);
    return true;
  } else {
    return false;
  }
}
