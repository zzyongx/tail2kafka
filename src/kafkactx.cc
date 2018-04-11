#include <cstring>
#include <errno.h>
#include <arpa/inet.h>

#include "logger.h"
#include "cnfctx.h"
#include "luactx.h"
#include "filereader.h"
#include "kafkactx.h"

static int stats_cb(rd_kafka_t *, char * /*json*/, size_t /*json_len*/, void *)
{
//  log_opaque(json, (int) json_len, true);
  return 0;
}

void KafkaCtx::error_cb(rd_kafka_t *, int err, const char *reason, void *opaque)
{
  KafkaCtx *kafka = (KafkaCtx *) opaque;

  if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN ||
      err == RD_KAFKA_RESP_ERR__TRANSPORT) {
    for (size_t i = 0; i < kafka->nrkt_; ++i) util::atomic_dec(kafka->errors_ + i, 1);
  }
  log_error(0, "kafka error level %d reason %s", err, reason);
}

static void log_cb(const rd_kafka_t *, int level, const char *fac, const char *buf)
{
  log_info(0, "kafka error level %d fac %s buf %s", level, fac, buf);
}

static void dr_msg_cb(rd_kafka_t *, const rd_kafka_message_t *rkmsg, void *)
{
  FileRecord *record = (FileRecord *) rkmsg->_private;

  if (record->off != (off_t) -1) {
    LuaCtx *ctx = record->ctx;
    ctx->getFileReader()->updateFileOffRecord(record);
  }

  FileRecord::destroy(record);
}

static int32_t partitioner_cb (
  const rd_kafka_topic_t *, const void *, size_t, int32_t pc, void *opaque, void *)
{
  LuaCtx *ctx = (LuaCtx *) opaque;
  int partition = ctx->getPartition(pc);
  if (partition < 0) return RD_KAFKA_PARTITION_UA;
  else return partition;
}

bool KafkaCtx::initKafka(const char *brokers, const std::map<std::string, std::string> &gcnf, char *errbuf)
{
  char errstr[512];

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  for (std::map<std::string, std::string>::const_iterator ite = gcnf.begin(); ite != gcnf.end(); ++ite) {
    rd_kafka_conf_res_t res;
    res = rd_kafka_conf_set(conf, ite->first.c_str(), ite->second.c_str(), errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
      snprintf(errbuf, MAX_ERR_LEN, "kafka conf %s=%s %s", ite->first.c_str(), ite->second.c_str(), errstr);

      rd_kafka_conf_destroy(conf);
      return false;
    }
  }

  rd_kafka_conf_set_opaque(conf, this);
  rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
  rd_kafka_conf_set_stats_cb(conf, stats_cb);
  rd_kafka_conf_set_error_cb(conf, error_cb);
  rd_kafka_conf_set_log_cb(conf, log_cb);

  /* rd_kafka_t will own conf */
  if (!(rk_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
    snprintf(errbuf, MAX_ERR_LEN, "new kafka produce error %s", errstr);
    return false;
  }

  if (rd_kafka_brokers_add(rk_, brokers) < 1) {
    snprintf(errbuf, MAX_ERR_LEN, "kafka invalid brokers %s", brokers);
    return false;
  }
  return true;
}

rd_kafka_topic_t *KafkaCtx::initKafkaTopic(LuaCtx *ctx, const std::map<std::string, std::string> &tcnf, char *errbuf)
{
  char errstr[512];

  rd_kafka_topic_conf_t *tconf = rd_kafka_topic_conf_new();
  for (std::map<std::string, std::string>::const_iterator ite = tcnf.begin(); ite != tcnf.end(); ++ite) {
    rd_kafka_conf_res_t res;
    res = rd_kafka_topic_conf_set(tconf, ite->first.c_str(), ite->second.c_str(), errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
      snprintf(errbuf, MAX_ERR_LEN, "kafka topic conf %s=%s %s", ite->first.c_str(), ite->second.c_str(), errstr);
      rd_kafka_topic_conf_destroy(tconf);
      return 0;
    }
  }

  rd_kafka_topic_conf_set_opaque(tconf, ctx);
  if (ctx->getPartitioner() == PARTITIONER_RANDOM) {
    rd_kafka_topic_conf_set_partitioner_cb(tconf, rd_kafka_msg_partitioner_random);
  } else {
    rd_kafka_topic_conf_set_partitioner_cb(tconf, partitioner_cb);
  }

  rd_kafka_topic_t *rkt;
  /* rd_kafka_topic_t will own tconf */
  rkt = rd_kafka_topic_new(rk_, ctx->topic().c_str(), tconf);
  if (!rkt) {
    snprintf(errbuf, MAX_ERR_LEN, "kafka_topic_new error");
    return 0;
  }
  return rkt;
}

bool KafkaCtx::init(CnfCtx *cnf, char *errbuf)
{
  if (!initKafka(cnf->getBrokers(), cnf->getKafkaGlobalConf(), errbuf)) return false;

  rkts_ = new rd_kafka_topic_t*[cnf->getLuaCtxSize()];
  errors_ = new int[cnf->getLuaCtxSize()];
  memset(errors_, 0, cnf->getLuaCtxSize());

  for (LuaCtxPtrList::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = (*ite);
    while (ctx) {
      rd_kafka_topic_t *rkt = initKafkaTopic(ctx, cnf->getKafkaTopicConf(), errbuf);
      if (!rkt) return false;

      rkts_[nrkt_] = rkt;
      ctx->setRktId(nrkt_);
      nrkt_++;

      ctx = ctx->next();
    }
  }

  return true;
}

KafkaCtx::~KafkaCtx()
{
  for (size_t i = 0; i < nrkt_; ++i) rd_kafka_topic_destroy(rkts_[i]);
  if (rk_) rd_kafka_destroy(rk_);

  if (rkts_) delete[] rkts_;
  if (errors_) delete[] errors_;
}

bool KafkaCtx::ping(LuaCtx *ctx)
{
  int id = ctx->rktId();
  if (util::atomic_get(errors_ + id) > 0) return true;
  return false;

  /*
  struct rd_kafka_message_t *meta = 0;
  rd_kafka_resp_err_t rc = rd_kafka_metadata(rk_, 0, ctx->rkt, 30);
  if (rc == RD_KAFKA_RESP_ERR_NO_ERROR) {
    util::atomic_set(errors_ + id, 10);
  }

  if (meta) rd_kafka_metadata_destroy(meta);
  */
}

bool KafkaCtx::produce(LuaCtx *ctx, FileRecord *record)
{
  rd_kafka_topic_t *rkt = rkts_[ctx->rktId()];
  record->ctx = ctx;

  int rc;
  int i = 1;
  time_t startTime = ctx->cnf()->fasttime();
  while ((rc = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, 0, (void *) record->data->c_str(),
                                record->data->size(), 0, 0, record)) != 0) {
    rd_kafka_resp_err_t err = rd_kafka_last_error();
    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      if (ctx->cnf()->fasttime() - startTime > KAFKA_ERROR_TIMEOUT + 2) {
        // librdkafka may trap this loop, call exit to restart
        return false;
      }

      ctx->cnf()->setKafkaBlock(true);
      int nevent = rd_kafka_poll(rk_, 100 * i);
      log_error(0, "%s kafka produce error(#%d) %s, poll event %d",
                rd_kafka_topic_name(rkt), i++, rd_kafka_err2str(err), nevent);
    } else {
      log_fatal(0, "%s kafka produce error %s", rd_kafka_topic_name(rkt), rd_kafka_err2str(err));
      FileRecord::destroy(record);
      break;
    }
  }

  ctx->cnf()->setKafkaBlock(false);
  return true;
}

bool KafkaCtx::produce(LuaCtx *ctx, std::vector<FileRecord *> *datas)
{
  assert(!datas->empty());
  rd_kafka_topic_t *rkt = rkts_[ctx->rktId()];

  std::vector<rd_kafka_message_t> rkmsgs;
  rkmsgs.resize(datas->size());

  size_t i = 0;
  for (std::vector<FileRecord *>::iterator ite = datas->begin(), end = datas->end();
       ite != end; ++ite, ++i) {
    FileRecord *record = (*ite);
    record->ctx = ctx;

    rkmsgs[i].payload  = (void *) record->data->c_str();
    rkmsgs[i].len      = record->data->size();
    rkmsgs[i].key      = 0;
    rkmsgs[i].key_len  = 0;
    rkmsgs[i]._private = record;
  }

  int n = rd_kafka_produce_batch(rkt, RD_KAFKA_PARTITION_UA, 0, &rkmsgs[0], rkmsgs.size());
  if (n != (int) rkmsgs.size()) {
    for (std::vector<rd_kafka_message_t>::iterator ite = rkmsgs.begin(), end = rkmsgs.end();
         ite != end; ++ite) {
      if (ite->err) {
        if (!produce(ctx, (FileRecord *) ite->_private)) return false;
      }
    }
  }

  return true;
}
