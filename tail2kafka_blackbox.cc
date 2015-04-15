#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <errno.h>
#include <time.h>
#include <vector>
#include <string>
#include <pthread.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

typedef std::vector<std::string> StringList;
static const char *hostname = "zzyong.paas.user.vm";

void start_producer();
void start_consumer();

int main(int argc, char *argv[])
{
  start_producer();
  sleep(1);
  start_consumer();
  return EXIT_SUCCESS;
}

#define check(r, fmt, arg...)                                           \
  do { if (!(r)) { fprintf(stderr, "%04d %s -> "fmt"\n", __LINE__, #r, ##arg); exit(1); } } while(0)

void sleep_ms(int ms)
{
  struct timespec to = {0, ms * 1000};
  nanosleep(&to, NULL);
}

class BasicProducer {
public:
  BasicProducer()
    : i(0), j(0) {}
  void reset() {
    i = j = 0;
  }
  char *operator()() {
    snprintf(buffer, 256, "basic.%d.%d", i, j);
    if (++i == 100) {
      j++;
      i = 0;
    }
    return buffer;
  }
private:
  int i;
  int j;
  char buffer[256];
};
BasicProducer basicPro;

void *basic_routine(void *)
{
  FILE *fp = fopen("./basic.log", "a");
  assert(fp);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", basicPro());
  }
  fflush(fp);
  sleep_ms(100000);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", basicPro());
    sleep_ms(1000);
  }
  fclose(fp);

  basicPro.reset();
  return NULL;
}

class FilterProducer {
public:
  FilterProducer() : i(0) {}
  void reset() { i = 0; }
  char *operator()(bool filter = false) {
    if (filter) {
      snprintf(buffer, 256, "%s 2015-04-02T12:05:%02d /%d 200 %d",
               hostname, i/2, i%2, i%10);
    } else {
      snprintf(buffer, 256, "filter - - [02/Apr/2015:12:05:%02d +0800] \"/%d\" 200 - - %d",
               i/2, i%2, i%10);
    }
    i++;
    return buffer;
  }
private:
  int i;
  char buffer[256];
};
FilterProducer filterPro;

void *filter_routine(void *)
{
  FILE *fp = fopen("./filter.log", "a");
  assert(fp);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", filterPro());
  }
  
  fflush(fp);
  sleep_ms(1000);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", filterPro());
  }
  fclose(fp);

  filterPro.reset();
  return NULL;
}

class GrepProducer {
public:
  GrepProducer() : i(0) {}
  void reset() { i = 0; }
  char *operator()(bool grep = false) {
    if (grep) {
      snprintf(buffer, 256, "%s [02/Apr/2015:12:05:%02d] \"GET /%d HTTP/1.0\" 200 %d\n",
               hostname, i/2, i%2, i%10);
    } else {
      snprintf(buffer, 256, "grep - - [02/Apr/2015:12:05:%02d] \"GET /%d HTTP/1.0\" 200 - - %d\n",
               i/2, i%2, i%10);
    }
    i++;
    return buffer;
  }
private:
  int i;
  char buffer[256];
};
GrepProducer grepPro;

void *grep_routine(void *)
{
  FILE *fp = fopen("./grep.log", "a");
  assert(fp);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", grepPro());
  }
  
  fflush(fp);
  sleep_ms(1000);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", grepPro());
  }
  fclose(fp);

  grepPro.reset();
  return NULL;
}

class AggregateProducer {
public:
  AggregateProducer() : i(0) {}
  void reset() { i = 0; }
  char *operator()(bool t = false) {
    assert(i < 100);
    if (t) {
      snprintf(buffer, 256, "%s 2015-04-02T12:05:%02d %d reqt<0.1=4 size=920 status_200=4",
               hostname, i/5, i%5);
    } else {
      snprintf(buffer, 256,
               "aggregate - - [02/Apr/2015:12:05:%02d +0800] - - - - \"200\" 230 0.1 - - - - %d\n",
               i/20, i%5);
    }
    i++;
    return buffer;
  }
private:
  int i;
  char buffer[256];
};
AggregateProducer aggregatePro;

void *aggregate_routine(void *)
{
  FILE *fp = fopen("./aggregate.log", "a");
  assert(fp);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", aggregatePro());
  }
  fclose(fp);

  aggregatePro.reset();
  return NULL;
}

class TransformProducer {
public:
  TransformProducer() : i(0) {}
  void reset() { i = 0; }
  char *operator()(bool t = false) {
    if (t) {
      snprintf(buffer, 256, "%s [error] message", hostname);
    } else {
      snprintf(buffer, 256, "[%s] message", (i % 2 == 0 ? "info" : "error"));
    }
    i++;
    return buffer;
  }
private:
  int i;
  char buffer[256];
};
TransformProducer transformPro;

void *transform_routine(void *)
{
  FILE *fp = fopen("./transform.log", "a");
  assert(fp);

  for (int i = 0; i < 99; ++i) {
    fprintf(fp, "%s\n", transformPro());
  }
  fclose(fp);

  transformPro.reset();
  return NULL;
}
    

void start_producer()
{
  int max = 0;
  pthread_t ptids[5];
  pthread_create(&ptids[max++], NULL, basic_routine, NULL);
  pthread_create(&ptids[max++], NULL, filter_routine, NULL);
  pthread_create(&ptids[max++], NULL, grep_routine, NULL);
  pthread_create(&ptids[max++], NULL, aggregate_routine, NULL);
  pthread_create(&ptids[max++], NULL, transform_routine, NULL);
  for (int i = 0; i < max; ++i) {
    pthread_join(ptids[i], NULL);
  }
}

static const char *BROKERS = "127.0.0.1:9092";

typedef void (*ConsumerFun)(rd_kafka_message_t *rkm);
struct ConsumerCtx {
  const char *topic;
  ConsumerFun fun;
};

#define THREAD_SUCCESS ((void *) 0)
#define THREAD_FAILURE ((void *) 1);

void *consumer_routine(void *data)
{
  ConsumerCtx *ctx = (ConsumerCtx *) data;
  char errstr[512];
  
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
  if (!rd_kafka_brokers_add(rk, BROKERS)) {
    fprintf(stderr, "invalid brokers %s\n", BROKERS);
    return THREAD_FAILURE;
  }
   
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, ctx->topic, 0);
  
  if (rd_kafka_consume_start(rkt, 0,
                             RD_KAFKA_OFFSET_BEGINNING) == -1) {
    fprintf(stderr, "%s failed to start consuming: %s\n", ctx->topic,
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    return THREAD_FAILURE;
  }

  void *rc = THREAD_FAILURE;
  while (true) {
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume(rkt, 0, 1000);
    if (!rkm) {
      fprintf(stderr, "%s timeout\n", ctx->topic);
      continue;
    }

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        fprintf(stderr, "%s eof, exit\n", ctx->topic);
        return rc;
      }

      fprintf(stderr, "%s error, %s\n", ctx->topic, rd_kafka_message_errstr(rkm));
      continue;
    }
    ctx->fun(rkm);
    rc = THREAD_SUCCESS;
  }
  return rc;
}

void basic_consumer(rd_kafka_message_t *rkm)
{
  // printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = basicPro();
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void filter_consumer(rd_kafka_message_t *rkm)
{
  // printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = filterPro(true);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void grep_consumer(rd_kafka_message_t *rkm)
{
  // printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = grepPro(true);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void aggregate_consumer(rd_kafka_message_t *rkm)
{
  // printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = aggregatePro(true);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void transform_consumer(rd_kafka_message_t *rkm)
{
  // printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = transformPro(true);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}
  
void start_consumer()
{
  ConsumerCtx ctxs[] = {
    {"basic",     basic_consumer},
    {"filter",    filter_consumer},
    {"grep",      grep_consumer},
    {"aggregate", aggregate_consumer},
    {"transform", transform_consumer},
  };

  int max = sizeof(ctxs)/sizeof(ctxs[0]);
  pthread_t ctids[max];

  for (int i = 0; i < max; ++i) {
    pthread_create(&ctids[i], NULL, consumer_routine, &ctxs[i]);
  }

  for (int i = 0; i < max; ++i) {
    void *rc;
    pthread_join(ctids[i], &rc);
    check(rc == THREAD_SUCCESS, "%s %s", ctxs[i].topic, rc == THREAD_SUCCESS ? "OK" : "ERROR");
  }
}
  
