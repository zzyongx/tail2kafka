#ifndef _KAFKACTX_H_
#define _KAFKACTX_H_

#include <map>
#include <string>
#include <vector>
#include <librdkafka/rdkafka.h>

class CnfCtx;
class LuaCtx;

class KafkaCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  ~KafkaCtx();
  bool init(CnfCtx *cnf, char *errbuf);
  void produce(size_t idx, std::vector<std::string *> *datas);
  void poll(int timeout) { rd_kafka_poll(rk_, timeout); }

private:
  rd_kafka_t                      *rk_;
  std::vector<rd_kafka_topic_t *> rkts_;

  bool initKafka(const char *brokers, const std::map<std::string, std::string> &gcnf, char *errbuf);
  bool initKafkaTopic(LuaCtx *ctx, const std::map<std::string, std::string> &tcnf, char *errbuf);
  void produce(size_t idx, std::string *data);
};

#endif
