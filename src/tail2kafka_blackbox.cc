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

#include "sys.h"
#include "unittesthelper.h"

typedef std::vector<std::string> StringList;
static const char *hostname = "zzyong";

#define LOG(f) "logs/"f

void start_producer();
void start_consumer();

int main()
{
  start_producer();
  sleep(1);
  start_consumer();
	printf("OK\n");
  return EXIT_SUCCESS;
}

class BasicProducer {
public:
  BasicProducer()
    : i(0), j(0) {}
  void reset() {
    i = j = 0;
  }
  char *operator()(bool t = false) {
    if (t) {
      snprintf(buffer, 256, "*%s basic.%d.%d\n", hostname, i, j);
    } else {
      snprintf(buffer, 256, "basic.%d.%d\n", i, j);
    }

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
  FILE *fp = fopen(LOG("basic.log"), "a");
  assert(fp);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s", basicPro());
  }
  fflush(fp);
  sys::nanosleep(100000);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s", basicPro());
    sys::nanosleep(1000);
  }
  fclose(fp);

  rename(LOG("basic.log"), LOG("basic.log.old"));

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
  FILE *fp = fopen(LOG("filter.log"), "a");
  assert(fp);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", filterPro());
  }

  fflush(fp);
  sys::nanosleep(1000);

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
  char *operator()(bool t = false) {
    if (t) {
      snprintf(buffer, 256, "%s [02/Apr/2015:12:05:%02d] \"GET /%d HTTP/1.0\" 200 %d",
               hostname, i/2, i%2, i%10);
    } else {
      // test empty line
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
  FILE *fp = fopen(LOG("grep.log"), "a");
  assert(fp);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", grepPro());
  }

  fflush(fp);
  sys::nanosleep(1000);

  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "%s\n", grepPro());
  }
  fclose(fp);

  grepPro.reset();
  return NULL;
}

class AggregateProducer {
public:
  AggregateProducer() : i(0), pkey(false) {}
  void reset() { i = 0; pkey = false; }
  char *operator()(bool t = false) {
    assert(i < 100);
    if (t) {
      if (pkey) {
        snprintf(buffer, 256, "%s 2015-04-02T12:05:%02d yuntu reqt<0.1=20 size=4600 status_200=20",
                 hostname, i/5-1);
        pkey = false;
      } else {
        snprintf(buffer, 256, "%s 2015-04-02T12:05:%02d %d reqt<0.1=4 size=920 status_200=4",
                 hostname, i/5, i%5);
        i++;
        if (i % 5 == 0) pkey = true;
      }

    } else {
      snprintf(buffer, 256,
               "aggregate - - [02/Apr/2015:12:05:%02d +0800] - - - - \"200\" 230 0.1 - - - - %d\n",
               i/20, i%5);
      i++;
    }
    return buffer;
  }
private:
  int i;
  bool pkey;
  char buffer[256];
};
AggregateProducer aggregatePro;

void *aggregate_routine(void *)
{
  FILE *fp = fopen(LOG("aggregate.log"), "a");
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
  FILE *fp = fopen(LOG("transform.log"), "a");
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

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "broker.version.fallback", "0.8.2.1", 0, 0);

  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!rd_kafka_brokers_add(rk, BROKERS)) {
    fprintf(stderr, "invalid brokers %s\n", BROKERS);
    return THREAD_FAILURE;
  }

  rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, ctx->topic, 0);

  if (rd_kafka_consume_start(rkt, 0,
                             RD_KAFKA_OFFSET_BEGINNING) == -1) {
    fprintf(stderr, "%s failed to start consuming: %s\n", ctx->topic,
            rd_kafka_err2name(rd_kafka_last_error()));
    return THREAD_FAILURE;
  }

  void *rc = THREAD_FAILURE;
  while (true) {
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume(rkt, 0, 5000);
    if (!rkm) {
      fprintf(stderr, "%s timeout, exit\n", ctx->topic);
      break;
    }

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        fprintf(stderr, "%s eof, continue\n", ctx->topic);
      } else {
        fprintf(stderr, "%s error, %s\n", ctx->topic, rd_kafka_message_errstr(rkm));
      }
    } else {
      ctx->fun(rkm);
      rc = THREAD_SUCCESS;
    }
  }
  return rc;
}

void basic_consumer(rd_kafka_message_t *rkm)
{
  if (memcmp(rkm->payload, "#", 1) == 0) return;   // skip comment line

  const char *ptr = basicPro(true);
	check(rkm->len == strlen(ptr),
        "basic: length(%.*s) != length(%s)", (int) rkm->len, (char *) rkm->payload, ptr);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "basic: %.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void filter_consumer(rd_kafka_message_t *rkm)
{
  if (memcmp(rkm->payload, "#", 1) == 0) return;   // skip comment line

  const char *ptr = filterPro(true);
	check(rkm->len == strlen(ptr),
        "filter: length(%.*s) != length(%s)", (int) rkm->len, (char *) rkm->payload, ptr);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "filter: %.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void grep_consumer(rd_kafka_message_t *rkm)
{
  if (memcmp(rkm->payload, "#", 1) == 0) return;   // skip comment line

  const char *ptr = grepPro(true);
	check(rkm->len == strlen(ptr),
        "grep: length(%.*s) != length(%s)", (int) rkm->len, (char *) rkm->payload, ptr);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "grep: %.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void aggregate_consumer(rd_kafka_message_t *rkm)
{
  if (memcmp(rkm->payload, "#", 1) == 0) return;   // skip comment line

  const char *ptr = aggregatePro(true);
	check(rkm->len == strlen(ptr),
        "aggregate: length(%.*s) != length(%s)", (int) rkm->len, (char *) rkm->payload, ptr);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "aggregate: %.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void transform_consumer(rd_kafka_message_t *rkm)
{
  if (memcmp(rkm->payload, "#", 1) == 0) return;   // skip comment line

  const char *ptr = transformPro(true);
	check(rkm->len == strlen(ptr),
        "transform: length(%.*s) != length(%s)", (int) rkm->len, (char *) rkm->payload, ptr);
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "transform: %.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
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
