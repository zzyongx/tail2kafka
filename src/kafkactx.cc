#include <cstring>
#include <errno.h>
#include <arpa/inet.h>

#include "logger.h"
#include "cnfctx.h"
#include "luactx.h"
#include "filereader.h"
#include "kafkactx.h"

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
  rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

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

bool KafkaCtx::initKafkaTopic(LuaCtx *ctx, const std::map<std::string, std::string> &tcnf, char *errbuf)
{
  char errstr[512];

  rd_kafka_topic_conf_t *tconf = rd_kafka_topic_conf_new();
  for (std::map<std::string, std::string>::const_iterator ite = tcnf.begin(); ite != tcnf.end(); ++ite) {
    rd_kafka_conf_res_t res;
    res = rd_kafka_topic_conf_set(tconf, ite->first.c_str(), ite->second.c_str(), errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
      snprintf(errbuf, MAX_ERR_LEN, "kafka topic conf %s=%s %s", ite->first.c_str(), ite->second.c_str(), errstr);
      rd_kafka_topic_conf_destroy(tconf);
      return false;
    }
  }

  rd_kafka_topic_conf_set_opaque(tconf, ctx);
  rd_kafka_topic_conf_set_partitioner_cb(tconf, partitioner_cb);

  rd_kafka_topic_t *rkt;
  /* rd_kafka_topic_t will own tconf */
  rkt = rd_kafka_topic_new(rk_, ctx->topic().c_str(), tconf);
  if (!rkt) {
    snprintf(errbuf, MAX_ERR_LEN, "kafka_topic_new error");
    return false;
  }

  ctx->setRkt(rkt);
  rkts_.push_back(rkt);
  return true;
}

bool KafkaCtx::init(CnfCtx *cnf, char *errbuf)
{
  if (!initKafka(cnf->getBrokers(), cnf->getKafkaGlobalConf(), errbuf)) return false;

  for (LuaCtxPtrList::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = (*ite);
    while (ctx) {
      if (!initKafkaTopic(ctx, cnf->getKafkaTopicConf(), errbuf)) return false;
      ctx = ctx->next();
    }
  }

  return true;
}

KafkaCtx::~KafkaCtx()
{
  for (std::vector<rd_kafka_topic_t *>::iterator ite = rkts_.begin(); ite != rkts_.end(); ++ite) {
    rd_kafka_topic_destroy(*ite);
  }
  if (rk_) rd_kafka_destroy(rk_);
}

void KafkaCtx::produce(LuaCtx *ctx, FileRecord *record)
{
  rd_kafka_topic_t *rkt = ctx->rkt();
  record->ctx = ctx;

  int rc;
  int i = 0;
  while ((rc = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, 0, (void *) record->data->c_str(),
                                record->data->size(), 0, 0, record)) != 0) {
    if (errno == ENOBUFS) {
      log_error(0, "%s kafka produce error(#%d) %s", rd_kafka_topic_name(rkt), ++i, strerror(errno));
      rd_kafka_poll(rk_, i < 1000 ? 100 * i : 1000);
    } else {
      log_fatal(0, "%s kafka produce error %d:%s", rd_kafka_topic_name(rkt), errno, strerror(errno));
      FileRecord::destroy(record);
      break;
    }
  }
}

bool KafkaCtx::produce(LuaCtx *ctx, std::vector<FileRecord *> *datas)
{
  assert(!datas->empty());
  rd_kafka_topic_t *rkt = ctx->rkt();

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

  bool rc = true;

  int n = rd_kafka_produce_batch(rkt, RD_KAFKA_PARTITION_UA, 0, &rkmsgs[0], rkmsgs.size());
  if (n != (int) rkmsgs.size()) {
    for (std::vector<rd_kafka_message_t>::iterator ite = rkmsgs.begin(), end = rkmsgs.end();
         ite != end; ++ite) {
      if (ite->err) {
        log_info(0, "%s kafka produce batch error %s", rd_kafka_topic_name(rkt), rd_kafka_err2str(ite->err));
        rd_kafka_poll(rk_, 100);
        produce(ctx, (FileRecord *) ite->_private);
      }
    }
    rc = false;
  }

  return rc;
}
