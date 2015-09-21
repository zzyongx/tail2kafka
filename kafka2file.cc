#define _LARGEFILE64_SOURCE

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <string>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>

// g++ -o kafka2file kafka2file.cc librdkafka.a -O2 -Wall -g -lpthread -lrt -lz -ldl

#define MAX_PARTITION 32
#define OFFDIR "/tmp/kafka2file"

#ifdef DEBUG
#define debug printf
#else
void inline debug_(...) {}
#define debug debug_
#endif

struct KafkaCtx {
  int               out;
  int               iid;
  int               idxMap[MAX_PARTITION];
  rd_kafka_t       *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_queue_t *rkqu;
};

bool initKafka(
  const char *brokers, const char *topic, const char *partition, KafkaCtx *ctx);
bool consumeLoop(KafkaCtx *ctx, const char *datadir, const char *topic);
void uninitKafka(KafkaCtx *ctx);
bool initSingleton(const char *datadir, const char *topic, int uuid);

int main(int argc, char *argv[])
{
  if (argc != 5) {
    fprintf(stderr, "%s kafka-broker topic partition datadir\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *brokers   = argv[1];
  const char *topic     = argv[2];
  const char *partition = argv[3];
  const char *datadir   = argv[4];
  KafkaCtx ctx;

  if (!initKafka(brokers, topic, partition, &ctx)) return EXIT_FAILURE;
  if (!initSingleton(datadir, topic, ctx.iid)) return EXIT_FAILURE;
  bool rc = consumeLoop(&ctx, datadir, topic);
  uninitKafka(&ctx);

  return rc ? EXIT_SUCCESS : EXIT_FAILURE;
}

int getKafkaOffset(const char *topic, int partition, uint64_t *offset)
{
  char path[512];
  snprintf(path, 512, "%s/%s.%d", OFFDIR, topic, partition);

  int fd = -1;
  struct stat st;
  if (stat(path, &st) == 0) {
    fd = open(path, O_RDWR, 0644);
    ssize_t nn = pread(fd, offset, sizeof(*offset), 0);
    if (nn == 0) {
      fprintf(stdout, "%s empty offset file, use default\n", topic);
      *offset = RD_KAFKA_OFFSET_END;
    } else if (nn < 0) {
      fprintf(stderr, "%s pread() error %s\n", topic, strerror(errno));
      return fd;
    }
  } else if (errno == ENOENT) {
    fd = open(path, O_CREAT | O_WRONLY, 0644);
    if (fd == -1) {
      fprintf(stderr, "open %s error %s\n", path, strerror(errno));
      return fd;
    }
    *offset = RD_KAFKA_OFFSET_END;
  } else {
    fprintf(stderr, "stat %s error %s\n", path, strerror(errno));
    return fd;
  }

  return fd;
}

bool initKafka(
  const char *brokers, const char *topic, const char *partition, KafkaCtx *ctx)
{
  char errstr[512];
  int offsetfd;
  uint64_t offset;

  ctx->rk   = 0;
  ctx->rkt  = 0;
  ctx->rkqu = 0;
  memset(ctx->idxMap, 0x00, sizeof(ctx->idxMap));
  ctx->out = -1;

  ctx->rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
  if (rd_kafka_brokers_add(ctx->rk, brokers) == 0) {
    fprintf(stderr, "invalid brokers %s\n", brokers);
    return false;
  }
  ctx->rkt  = rd_kafka_topic_new(ctx->rk, topic, 0);
  ctx->rkqu = rd_kafka_queue_new(ctx->rk);
  ctx->iid  = MAX_PARTITION;
  
  int n = 0;
  for (const char *p = partition; /* */; ++p) {
    if (!*p || *p == ',') {
      if (n >= MAX_PARTITION) {
        fprintf(stderr, "partition %d >= %d\n", n, MAX_PARTITION);
        return false;
      }
      
      offsetfd = getKafkaOffset(topic, n, &offset);
      if (offsetfd == -1) return false;
      ctx->idxMap[n] = offsetfd;

      if (n < ctx->iid) ctx->iid = n;
      
      if (rd_kafka_consume_start_queue(ctx->rkt, n, offset, ctx->rkqu) == -1) {
        fprintf(stderr, "%s:%d failed to start consuming: %s\n", topic, n,
                rd_kafka_err2str(rd_kafka_errno2err(errno)));
        return false;
      }
      if (!*p) break;
      n = 0;
    } else {
      n = n * 10 + *p - '0';
    }
  }

  return true;
}

bool outRotate(KafkaCtx *ctx, const char *datadir, const char *topic)
{
  if (datadir[0] == '-') return STDOUT_FILENO;
  
  time_t now = time(0);
  if (ctx->out != -1 && now % 3600 != 0) return true;

  struct tm tm;
  localtime_r(&now, &tm);

  char path[512];
  snprintf(path, 512, "%s/%s.%d.%04d-%02d-%02dT%02d",
           datadir, topic, ctx->iid,
           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour);
  int fd = open(path, O_CREAT | O_WRONLY | O_APPEND | O_LARGEFILE, 0644);
  if (fd == -1) {
    fprintf(stderr, "rotate %s error, %s\n", path, strerror(errno));
    return false;
  }

  if (ctx->out != -1) close(ctx->out);
  ctx->out = fd;
  return true;
}

bool consumeLoop(KafkaCtx *ctx, const char *datadir, const char *topic)
{
  while (true) {
    if (!outRotate(ctx, datadir, topic)) return false;
    
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume_queue(ctx->rkqu, 1000);
    if (!rkm) continue;  // timeout

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
      fprintf(stderr, "%s error %s\n", topic, rd_kafka_message_errstr(rkm));
      continue;
    }

    debug("data %.*s\n", (int) rkm->len, (char *) rkm->payload);

    write(ctx->out, rkm->payload, rkm->len);
    pwrite(ctx->idxMap[rkm->partition], &rkm->offset, sizeof(rkm->offset), 0);
    rd_kafka_message_destroy(rkm);
  }
  return true;
}

/* pidfile may stale, this's not a perfect method */
bool initSingleton(const char *datadir, const char *topic, int uuid)
{
  if (datadir[0] == '-') return true;
  
  char path[256];
  snprintf(path, 256, "%s/%s.%d.lock", datadir, topic, uuid);
  
  int fd = open(path, O_CREAT | O_WRONLY, 0644);
  if (lockf(fd, F_TLOCK, 0) == 0) {
    ftruncate(fd, 0);
    char pid[32];
    snprintf(pid, 32, "%d", getpid());
    write(fd, pid, strlen(pid));
    return true;
  } else {
    fprintf(stderr, "lock %s failed", path);
    return false;
  }
}

void uninitKafka(KafkaCtx *ctx)
{
  if (ctx->rkqu) rd_kafka_queue_destroy(ctx->rkqu);
  if (ctx->rkt) rd_kafka_topic_destroy(ctx->rkt);
  if (ctx->rk) rd_kafka_destroy(ctx->rk);
  if (ctx->out != -1) close(ctx->out);
}
