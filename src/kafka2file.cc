#define _LARGEFILE64_SOURCE

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <memory>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>
#include "sys.h"
#include "runstatus.h"
#include "logger.h"
#include "uint64offset.h"
#include "cmdnotify.h"
#include "transform.h"

LOGGER_INIT();

class KafkaConsumer {
public:
  static KafkaConsumer *create(const char *wdir, const char *brokers, const char *topic, int partition, bool defaultStart);

  ~KafkaConsumer() {
    if (rkqu_) rd_kafka_queue_destroy(rkqu_);
    if (rkt_)  rd_kafka_topic_destroy(rkt_);
    if (rk_)   rd_kafka_destroy(rk_);
  }

  bool loop(RunStatus *runStatus, Transform *transform);

private:
  KafkaConsumer(uint64_t defaultOffset) : rk_(0), rkt_(0), rkqu_(0), offset_(defaultOffset) {}

private:
  const char *wdir_;
  const char *topic_;
  int         partition_;

  rd_kafka_t       *rk_;
  rd_kafka_topic_t *rkt_;
  rd_kafka_queue_t *rkqu_;

  Offset offset_;
};

static bool initSingleton(const char *datadir, const char *topic, int partition);

int main(int argc, char *argv[])
{
  if (argc < 6) {
    fprintf(stderr, "%s kafka-broker topic partition (offset-begining|offset-end) datadir "
            "[notify] [informat:lua:outformat:interval:delay]\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *brokers   = argv[1];
  const char *topic     = argv[2];
  int         partition = atoi(argv[3]);
  const char *offsetstr = argv[4];
  const char *datadir   = argv[5];

  const char *notify = argc > 6 ? argv[6] : 0;
  const char *output = argc > 7 ? argv[7] : "raw::raw";

  bool defaultStart;
  if (strcmp(offsetstr, "offset-begining") == 0) {
    defaultStart = true;
  } else if (strcmp(offsetstr, "offset-end") == 0) {
    defaultStart = false;
  } else {
    fprintf(stderr, "unknow default offset, use offset-begining or offset-end");
    return EXIT_FAILURE;
  }

  char buffer[1024];
  snprintf(buffer, 1024, "%s/%s.%d.log", datadir, topic, partition);
  Logger::create(buffer, Logger::DAY, true);

  snprintf(buffer, 1024, "%s/%s", datadir, topic);
  mkdir(buffer, 0755);

  CmdNotify cmdNotify(notify, datadir, topic, partition);

  std::auto_ptr<Transform> transform(Transform::create(datadir, topic, partition, &cmdNotify, output, buffer));
  if (transform.get() == 0) {
    fprintf(stderr, "create transform error %s\n", buffer);
    return EXIT_FAILURE;
  }

  RunStatus *runStatus = RunStatus::create();
  sys::SignalHelper signalHelper(buffer);

  int signos[] = { SIGTERM, SIGINT, SIGCHLD };
  RunStatus::Want wants[] = { RunStatus::STOP, RunStatus::STOP, RunStatus::IGNORE };
  if (!signalHelper.signal(runStatus, sizeof(signos)/sizeof(signos[0]), signos, wants)) {
    log_fatal(errno, "install signal %s", buffer);
    return EXIT_FAILURE;
  }

  if (!initSingleton(datadir, topic, partition)) return EXIT_FAILURE;

  std::auto_ptr<KafkaConsumer> ctx(KafkaConsumer::create(datadir, brokers, topic, partition, defaultStart));
  if (!ctx.get()) return EXIT_FAILURE;

  bool rc = ctx->loop(runStatus, transform.get());

  log_info(0, "exit");
  return rc ? EXIT_SUCCESS : EXIT_FAILURE;
}

static void log_cb(const rd_kafka_t *, int level, const char *fac, const char *buf)
{
  log_info(0, "kafka error level %d fac %s buf %s", level, fac, buf);
}

KafkaConsumer *KafkaConsumer::create(const char *wdir, const char *brokers, const char *topic, int partition, bool defaultStart)
{
  uint64_t defaultOffset = defaultStart ? RD_KAFKA_OFFSET_BEGINNING : RD_KAFKA_OFFSET_END;
  std::auto_ptr<KafkaConsumer> ctx(new KafkaConsumer(defaultOffset));

  char errstr[512];

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "broker.version.fallback", "0.8.2.1", 0, 0);
  rd_kafka_conf_set(conf, "enable.auto.commit", "false", 0, 0);

  rd_kafka_conf_set_log_cb(conf, log_cb);

  ctx->rk_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (rd_kafka_brokers_add(ctx->rk_, brokers) == 0) {
    log_fatal(0, "invalid brokers %s", brokers);
    return 0;
  }

  ctx->rkt_  = rd_kafka_topic_new(ctx->rk_, topic, 0);
  ctx->rkqu_ = rd_kafka_queue_new(ctx->rk_);

  char path[1024];
  snprintf(path, 1024, "%s/%s.%d.offset", wdir, topic, partition);
  if (!ctx->offset_.init(path, errstr)) {
    log_fatal(0, "%s:%d init offset error, %s", topic, partition, errstr);
    return 0;
  }

  log_info(0, "%s:%d set offset at %ld", topic, partition, ctx->offset_.get());
  if (rd_kafka_consume_start_queue(ctx->rkt_, partition, ctx->offset_.get(), ctx->rkqu_) == -1) {
    log_fatal(0, "%s:%d failed to start consuming: %s", topic, partition, rd_kafka_err2name(rd_kafka_last_error()));
    return 0;
  }

  ctx->wdir_      = wdir;
  ctx->topic_     = topic;
  ctx->partition_ = partition;

  return ctx.release();
}

bool KafkaConsumer::loop(RunStatus *runStatus, Transform *transform)
{
  uint64_t startOff = offset_.get();
  uint64_t off = RD_KAFKA_OFFSET_END;

  while (runStatus->get() != RunStatus::STOP) {
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume_queue(rkqu_, 1000);
    if (!rkm) {    // timeout
      if (transform->timeout(&off) != Transform::IGNORE) { assert(off != (uint64_t) RD_KAFKA_OFFSET_END); offset_.update(off); }
      log_info(0, "consume %s:%d timeout", topic_, partition_);
      continue;
    }

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
      log_error(0, "consume %s:%d error %s", topic_, partition_, rd_kafka_message_errstr(rkm));
      continue;
    }

    if (startOff == (uint64_t) rkm->offset) {
      log_info(0, "%s:%d same offset message %lu %.*s", topic_, partition_, startOff, (int) rkm->len, (char *) rkm->payload);
      rd_kafka_message_destroy(rkm);
      continue;
    }

    log_debug(0, "data @%ld %.*s\n", rkm->offset, (int) rkm->len, (char *) rkm->payload);
    if (transform->write(rkm, &off)) offset_.update(off);
  }

  if (transform->timeout(&off) != Transform::IGNORE) { assert(off != (uint64_t) RD_KAFKA_OFFSET_END); offset_.update(off); }
  log_info(0, "%s:%d end offset at %ld", topic_, partition_, offset_.get());

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
