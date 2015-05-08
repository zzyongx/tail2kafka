#define _XOPEN_SOURCE 500

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <errno.h>
#include <string>
#include <vector>
#include <list>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <cassandra.h>
#include <librdkafka/rdkafka.h>

// -lpthread -lcassandra -lrdkafka
// perl -n -e 'print unpack("Q", $_)'

/* CREATE KEYSPACE de WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
 * CREATE TABLE de.realdata (topic TEXT, id TEXT, timestamp TEXT, datas map<text, text>, PRIMARY KEY(topic, id, timestamp));
 * CREATE TABLE de.cachedata (topic TEXT, id TEXT, timestamp TEXT, datas text, PRIMARY KEY(topic, id, timestamp));
 */

#define THREAD_SUCCESS ((void *) 0)
#define THREAD_FAILURE ((void *) 1)
static const char   SP       = ' ';
static const size_t MAX_QLEN = 1024;
static const size_t MAX_CNP  = 1024;
static const char *OFFDIR    = "/tmp/aggregate2cassandra";

typedef std::list<CassFuture *> CfList;

struct OneTaskReq {
  const char *topic;
  std::string *node;
  std::string *id; 
  std::string *time;
  std::string *data;
  OneTaskReq() : node(0), id(0), time(0), data(0) {}
};

struct ConsumerCtx {
  pthread_t   tid;
  int         server;
  int         offsetfd;
  const char *topic;

  rd_kafka_t       *rk;
  rd_kafka_topic_t *rkt;

  ConsumerCtx() : tid(0), offsetfd(-1), rk(0), rkt(0) {}
};
typedef std::vector<ConsumerCtx> ConsumerList;

struct CassandraCtx {
  pthread_t tid;
  int       accept;
  
  CassCluster        *cluster;
  CassSession        *session;
  CassFuture         *connect;
  const CassPrepared *prepared;
  CassandraCtx() : tid(0), cluster(0), session(0), connect(0) {}
};

bool startAgent(int rfd, const char *cluster, CassandraCtx *ctx);
bool startConsumers(int wfd, const char *brokers, int argc, char *argv[], ConsumerList *consumers);
bool waitFinish(CassandraCtx *ctx, ConsumerList *consumers);

int main(int argc, char *argv[])
{
  if (argc < 4) {
    fprintf(stderr, "%s kafka-brokers cassandra-cluster topic1 topic2 ..\n", argv[0]);
    return EXIT_FAILURE;
  }

  int fd[2];
  if (pipe(fd) != 0) {
    fprintf(stderr, "pipe() %s\n", strerror(errno));
    return EXIT_FAILURE;
  }

  CassandraCtx agent;
  if (!startAgent(fd[0], argv[2], &agent)) return EXIT_FAILURE;

  std::vector<ConsumerCtx> consumers;
  if (!startConsumers(fd[1], argv[1], argc-3, argv+3, &consumers)) return EXIT_FAILURE;

  if (!waitFinish(&agent, &consumers)) return EXIT_FAILURE;
  return EXIT_SUCCESS;
}

bool getKafkaOffset(ConsumerCtx *ctx, uint64_t *offset)
{
  char path[512];
  snprintf(path, 512, "%s/%s", OFFDIR, ctx->topic);

  struct stat st;
  if (stat(path, &st) == 0) {
    ctx->offsetfd = open(path, O_RDWR, 0644);
    ssize_t nn = pread(ctx->offsetfd, offset, sizeof(*offset), 0);
    if (nn == 0) {
      fprintf(stdout, "%s empty offset file, use default\n", ctx->topic);
      *offset = RD_KAFKA_OFFSET_END;
    } else if (nn < 0) {
      fprintf(stderr, "%s pread() error %s\n", ctx->topic, strerror(errno));
      return false;
    }
  } else if (errno == ENOENT) {
    ctx->offsetfd = open(path, O_CREAT | O_WRONLY, 0644);
    if (ctx->offsetfd == -1) {
      fprintf(stderr, "open %s error %s\n", path, strerror(errno));
      return false;
    }
    *offset = RD_KAFKA_OFFSET_END;
  } else {
    fprintf(stderr, "stat %s error %s\n", path, strerror(errno));
    return false;
  }

  return true;
}

inline bool setKafkaOffset(ConsumerCtx *ctx, uint64_t offset)
{
  pwrite(ctx->offsetfd, &offset, sizeof(offset), 0);
  return true;
}

void taskFinish(OneTaskReq *req)
{
  if (req->node) delete req->node;
  if (req->id)   delete req->id;
  if (req->time) delete req->time;
  if (req->data) delete req->data;
}

/* node time id data */
bool sendToStoreAgent(ConsumerCtx *ctx, rd_kafka_message_t *rkm)
{
  OneTaskReq req;
  req.topic = ctx->topic;
  
  char *ptr = (char *) rkm->payload;
  char *end = ptr + rkm->len;
  
  char *pos = (char *) memchr(ptr, SP, end - ptr);
  if (!pos) goto error;
  req.node = new std::string(ptr, pos - ptr);
  ptr = pos + 1;

  pos = (char *) memchr(ptr, SP, end - ptr);
  if (!pos || pos - ptr != sizeof("YYYY-MM-DDTHH:MM:SS")-1) goto error;
  req.time = new std::string(ptr, pos - ptr);
  ptr = pos + 1;

  pos = (char *) memchr(ptr, SP, end - ptr);
  if (!pos) goto error;
  req.id = new std::string(ptr, pos - ptr);
  ptr = pos + 1;

  req.data = new std::string(ptr, end - ptr);

  write(ctx->server, &req, sizeof(req));
  setKafkaOffset(ctx, rkm->offset);
  return true;

  error:
  taskFinish(&req);
  return false;
}
                                                                    
void *consume_routine(void *data)
{
  ConsumerCtx *ctx = (ConsumerCtx *) data;

  void *rc = THREAD_FAILURE;
  while (true) {
    rd_kafka_message_t *rkm;
    rkm = rd_kafka_consume(ctx->rkt, 0, 1000);
    if (!rkm) continue;  // timeout

    if (rkm->err) {
      if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) continue;
      fprintf(stderr, "%s error %s\n", ctx->topic, rd_kafka_message_errstr(rkm));
      continue;
    }

    printf("data %.*s\n", (int) rkm->len, (char *) rkm->payload);

    if (!sendToStoreAgent(ctx, rkm)) {
      fprintf(stderr, "Bad data, skip %.*s\n", (int) rkm->len, (char *) rkm->payload);
    }
  }
  return rc;
}

bool startConsumers(int fd, const char *brokers, int argc, char *argv[], ConsumerList *consumers)
{
  char errstr[512];
  consumers->resize(argc);
  
  for (int i = 0; i < argc; ++i) {
    ConsumerCtx *ctx = &(consumers->at(i));
    ctx->server = fd;
    ctx->topic = argv[i];
    
    uint64_t offset;
    if (!getKafkaOffset(ctx, &offset)) return false;

    ctx->rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, errstr, sizeof(errstr));
    if (rd_kafka_brokers_add(ctx->rk, brokers) == 0) {
      fprintf(stderr, "invalid brokers %s\n", brokers);
      return false;
    }

    ctx->rkt = rd_kafka_topic_new(ctx->rk, ctx->topic, 0);
    if (rd_kafka_consume_start(ctx->rkt, 0, offset) == -1) {
      fprintf(stderr, "%s failed to start consuming: %s\n", ctx->topic,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      return false;
    }
  }

  for (size_t i = 0; i < consumers->size(); ++i) {
    ConsumerCtx *ctx = &(consumers->at(i));
    int rc = pthread_create(&(ctx->tid), NULL, consume_routine, ctx);
    if (rc != 0) {
      fprintf(stderr, "pthread_create error %s\n", strerror(rc));
      return false;
    }
  }

  return true;
}

/* dashboard data's write is very heavy than read
 * advanced aggregate should be done when read
 * use cache and preread to accelerate read
 * so we store the raw data
 * dashboard data is write once, so cache will be persist
 *
 * we have second precision, but when display on dashboard, fetch all seconds is not neccessary.
 * we could read two random seconds in one minute and display it,
 * and load more seconds in one minute if necessary.
 */
void *store_routine(void *data)
{
  CassandraCtx *ctx = (CassandraCtx *) data;
  CfList cfwaits;
  void *rc = THREAD_SUCCESS;

  while (true) {
    if (cfwaits.size() < MAX_CNP) {
      OneTaskReq req;      
      ssize_t nn = read(ctx->accept, &req, sizeof(req));
      if (nn <= 0) {
        if (nn < 0) {
          fprintf(stderr, "read error %s\n", strerror(errno));
          rc = THREAD_FAILURE;
        }
        break;
      }

      assert(nn == sizeof(req));

      CassStatement *statm = cass_prepared_bind(ctx->prepared);
      // cass_statement_set_consistency(statm, CASS_CONSISTENCY_TWO);

      CassCollection *collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 1);
      cass_collection_append_string(collection, cass_string_init(req.node->c_str()));
      cass_collection_append_string(collection, cass_string_init(req.data->c_str()));

      cass_statement_bind_collection(statm, 0, collection);
      cass_statement_bind_string(statm, 1, cass_string_init(req.topic));
      cass_statement_bind_string(statm, 2, cass_string_init(req.id->c_str()));
      cass_statement_bind_string(statm, 3, cass_string_init(req.time->c_str()));

      cass_collection_free(collection);

      cfwaits.push_back(cass_session_execute(ctx->session, statm));
      cass_statement_free(statm);
      taskFinish(&req);
    }
    
    for (CfList::iterator ite = cfwaits.begin(); ite != cfwaits.end();) {
      if (cass_future_ready(*ite)) {
        if (cass_future_error_code(*ite) != CASS_OK) {
          CassString msg = cass_future_error_message(*ite);
          fprintf(stderr, "Bad query %.*s\n", (int) msg.length, msg.data);
        }
        cass_future_free(*ite);
        ite = cfwaits.erase(ite);
      } else {
        ++ite;
      }
    }
  }

  for (CfList::iterator ite = cfwaits.begin(); ite != cfwaits.end(); ++ite) {
    cass_future_wait(*ite);
    if (cass_future_error_code(*ite) != CASS_OK) {
      CassString msg = cass_future_error_message(*ite);
      fprintf(stderr, "Bad query %.*s\n", (int) msg.length, msg.data);
    }
    cass_future_free(*ite);
  }

  return THREAD_SUCCESS;
}

bool startAgent(int rfd, const char *db, CassandraCtx *ctx)
{
  ctx->accept = rfd;
  ctx->cluster = cass_cluster_new();
  ctx->session = cass_session_new();

  cass_cluster_set_contact_points(ctx->cluster, db);
  cass_cluster_set_max_connections_per_host(ctx->cluster, MAX_CNP);
  cass_cluster_set_max_concurrent_creation(ctx->cluster, 100);
  
  ctx->connect = cass_session_connect(ctx->session, ctx->cluster);
  if (cass_future_error_code(ctx->connect) != CASS_OK) {
    CassString msg = cass_future_error_message(ctx->connect);
    fprintf(stderr, "connect %s error %.*s\n", db, (int) msg.length, msg.data);
    return false;
  }

  CassString query = cass_string_init("UPDATE de.realdata SET datas = datas + ?"
                                      "WHERE topic = ? AND id = ? AND timestamp = ?");
  CassFuture *prepareFuture = cass_session_prepare(ctx->session, query);
  if (cass_future_error_code(prepareFuture) != CASS_OK) {
    CassString msg = cass_future_error_message(prepareFuture);
    fprintf(stderr, "prepare %.*s error %.*s\n", (int) query.length, query.data,
            (int) msg.length, msg.data);
    return false;
  }
  ctx->prepared = cass_future_get_prepared(prepareFuture);
  cass_future_free(prepareFuture);

  int rc = pthread_create(&ctx->tid, NULL, store_routine, ctx);
  if (rc != 0) {
    fprintf(stderr, "pthread_create() %s\n", strerror(rc));
    return false;
  }

  return true;
}

bool waitFinish(CassandraCtx *ctx, ConsumerList *consumers)
{
  void *status;
  void *rc = THREAD_SUCCESS;
  
  for (ConsumerList::iterator ite = consumers->begin(); ite != consumers->end(); ++ite) {
    if (ite->tid != 0) {
      pthread_join(ite->tid, &status);
      if (status == THREAD_FAILURE) rc = status;
    }
    if (ite->rkt) rd_kafka_topic_destroy(ite->rkt);
    if (ite->rk) rd_kafka_destroy(ite->rk);
    if (ite->offsetfd != -1) close(ite->offsetfd);
  }

  if (ctx->tid != 0) {
    pthread_join(ctx->tid, &status);
    if (status == THREAD_FAILURE) rc = status;
  }

  if (ctx->connect) {
    CassFuture *closeFuture = cass_session_close(ctx->session);
    cass_future_wait(closeFuture);
    cass_future_free(closeFuture);
    cass_future_free(ctx->connect);
  }
  
  if (ctx->prepared) cass_prepared_free(ctx->prepared);

  if (ctx->session) cass_session_free(ctx->session);
  if (ctx->cluster) cass_cluster_free(ctx->cluster);
  
  return rc == THREAD_SUCCESS ? true : false;
}
