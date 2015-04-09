#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <errno.h>
#include <time.h>
#include <vector>
#include <string>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

typedef std::vector<std::string> StringList;

void start_producer();
void start_consumer();

int main(int argc, char *argv[])
{
  start_producer();
  start_consumer();
  return EXIT_SUCCESS;
}

#define check(r, fmt, arg...) \
  do { if (!(r)) { fprintf(stderr, "%04d %s -> "fmt"\n", __LINE__, #r, ##arg); abort(); } } while(0)

void sleep_ms(int ms)
{
  struct timespec to = {0, ms * 1000};
  nanosleep(&to, NULL);
}

class BasicProducer {
public:
  BasicProducer()
    : i(0), j(0) {
  }
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

void *filter_routine(void *)
{
  FILE *fp = fopen("./filter.log", "a");
  assert(fp);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "filter - - [02/Apr/2015:12:05:%02d] \"/1\" 200 - - %d\n",
            i/2, i % 10);
  }
  
  fflush(fp);
  sleep_ms(1000);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "filter - - [02/Apr/2015:12:06:%02d] \"/2\" 200 - - %d\n",
            i/2, i % 10);
  }
  fclose(fp);

  return NULL;
}

void *grep_routine(void *)
{
  FILE *fp = fopen("./grep.log", "a");
  assert(fp);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "grep - - [02/Apr/2015:12:05:%02d] \"GET /1 HTTP/1.0\" 200 - - %d\n",
            i/2, i % 10);
  }
  
  fflush(fp);
  sleep_ms(1000);
  
  for (int i = 0; i < 100; ++i) {
    fprintf(fp, "grep - - [02/Apr/2015:12:06:%02d] \"GET /2 HTTP/1.0\" 200 - - %d\n",
            i/2, i % 10);
  }
  fclose(fp);

  return NULL;
}

void *aggregate_routine(void *)
{
  FILE *fp = fopen("./aggregate.log", "a");
  assert(fp);

  for (int i = 0; i < 5; ++i) {
    for (int j = 0; j < 20; ++j) {
      fprintf(fp, "aggregate - - [02/Apr/2015:12:05:%02d] - - - - \"200\" 230 0.1 - - - - %d\n",
              i, i);
    }
  }
  fclose(fp);
  
  return NULL;
}

void *transform_routine(void *)
{
  FILE *fp = fopen("./transform.log", "a");
  assert(fp);

  for (int i = 0; i < 99; ++i) {
    fprintf(fp, "[%s] message\n", (i % 2 == 0 ? "info" : "error"));
  }
  fclose(fp);

  return NULL;
}
    

void start_producer()
{
  pthread_t ptids[5];
  
  pthread_create(&ptids[0], NULL, basic_routine, NULL);
  pthread_create(&ptids[1], NULL, filter_routine, NULL);
  pthread_create(&ptids[2], NULL, grep_routine, NULL);
  pthread_create(&ptids[3], NULL, aggregate_routine, NULL);
  pthread_create(&ptids[4], NULL, transform_routine, NULL);

  for (int i = 0; i < 5; ++i) {
    pthread_join(ptids[i], NULL);
  }
}

static const char *BROKERS = "127.0.0.1:9092";

typedef void (*ConsumerFun)(rd_kafka_message_t *rkm);
struct ConsumerCtx {
  const char *topic;
  ConsumerFun fun;
};

void *consumer_routine(void *data)
{
  ConsumerCtx *ctx = (ConsumerCtx *) data;
  char errstr[512];
  
  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
  if (!rd_kafka_brokers_add(rk, BROKERS)) {
    fprintf(stderr, "invalid brokers %s", BROKERS);
    return NULL;
  }
   
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, ctx->topic, 0);
  
  if (rd_kafka_consume_start(rkt, 0,
                             RD_KAFKA_OFFSET_BEGINNING) == -1) {
    fprintf(stderr, "%s failed to start consuming: %s\n", ctx->topic,
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    return NULL;
  }

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
        return NULL;
      }

      fprintf(stderr, "%s error, %s\n", ctx->topic, rd_kafka_message_errstr(rkm));
      continue;
    }
    ctx->fun(rkm);
  }
}

void basic_consumer(rd_kafka_message_t *rkm)
{
  printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
  const char *ptr = basicPro();
  check(memcmp(rkm->payload, ptr, rkm->len) == 0,
        "%.*s != %s", (int) rkm->len, (char *) rkm->payload, ptr);
}

void filter_consumer(rd_kafka_message_t *rkm)
{
  printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);
}

void grep_consumer(rd_kafka_message_t *rkm)
{
  printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);  
}

void aggregate_consumer(rd_kafka_message_t *rkm)
{
  printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);  
}

void transform_consumer(rd_kafka_message_t *rkm)
{
  printf("%.*s\n", (int) rkm->len, (char *) rkm->payload);  
}
  
void start_consumer()
{
  pthread_t ctids[5];

  ConsumerCtx ctxs[] = {
    {"basic",     basic_consumer},
    {"filter",    filter_consumer},
    {"grep",      grep_consumer},
    {"aggregate", aggregate_consumer},
    {"transform", transform_consumer},
  };

  for (int i = 0; i < 5; ++i) {
    pthread_create(&ctids[i], NULL, consumer_routine, &ctxs[i]);
  }

  for (int i = 0; i < 5; ++i) {
    pthread_join(ctids[i], NULL);
  }
}
  
