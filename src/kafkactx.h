#ifndef _KAFKACTX_H_
#define _KAFKACTX_H_

#include <map>
#include <string>
#include <vector>
#include <librdkafka/rdkafka.h>

#include "filerecord.h"
class CnfCtx;
class LuaCtx;

class KafkaCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  KafkaCtx() : rk_(0), nrkt_(0), rkts_(0), errors_(0) {}
  ~KafkaCtx();
  bool init(CnfCtx *cnf, char *errbuf);
  bool produce(LuaCtx *ctx, std::vector<FileRecord *> *datas);
  void poll(int timeout) { rd_kafka_poll(rk_, timeout); }
  bool ping(LuaCtx *ctx);

private:
  rd_kafka_t        *rk_;
  size_t             nrkt_;
  rd_kafka_topic_t **rkts_;
  int               *errors_;

  static void error_cb(rd_kafka_t *, int, const char *, void *);

  bool initKafka(const char *brokers, const std::map<std::string, std::string> &gcnf, char *errbuf);
  rd_kafka_topic_t *initKafkaTopic(LuaCtx *ctx, const std::map<std::string, std::string> &tcnf, char *errbuf);
  bool produce(LuaCtx *ctx, FileRecord *data);
};

#endif
