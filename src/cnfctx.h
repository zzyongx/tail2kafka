#ifndef _CNFCTX_H_
#define _CNFCTX_H_

#include "fileoff.h"
#include "luahelper.h"
#include "kafkactx.h"
#include "common.h"

class RunStatus;

class CnfCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  int                    accept;
  int                    server;
  uint64_t               sn;

public:

public:
  static CnfCtx *loadCnf(const char *dir, char *errbuf);

  static CnfCtx *loadFile(const char *file, char *errbuf);
  ~CnfCtx();
  void addLuaCtx(LuaCtx *ctx);

  bool initKafka();
  KafkaCtx *getKafka() { return kafka_; }

  bool initFileOff();
  FileOff *getFileOff() { return fileOff_; }

  bool initFileReader();

  void setRunStatus(RunStatus *runStatus) { runStatus_ = runStatus; }
  RunStatus *getRunStatus() { return runStatus_; }

  const char *getBrokers() const { return brokers_.c_str(); }
  const std::map<std::string, std::string> &getKafkaGlobalConf() const { return kafkaGlobal_; }
  const std::map<std::string, std::string> &getKafkaTopicConf() const { return kafkaTopic_; }

  const char *getPidFile() const {
    return pidfile_.c_str();
  }

  LuaHelper *getLuaHelper() { return helper_; }

  size_t getLuaCtxSize() const { return count; }
  std::vector<LuaCtx *> &getLuaCtxs() { return luaCtxs_; }

  int getPollLimit() const { return pollLimit_; }

  uint32_t addr() const { return addr_; }
  int partition() const { return partition_; }
  const std::string &host() const { return host_; }

  char *errbuf() { return errbuf_; }
  const std::string &libdir() const { return libdir_; }
  const std::string &logdir() const { return logdir_; }

private:
  CnfCtx();

  std::string pidfile_;
  std::string host_;
  uint32_t    addr_;
  int         partition_;
  int         pollLimit_;
  std::string logdir_;
  std::string libdir_;

  size_t                 count;
  std::vector<LuaCtx *>  luaCtxs_;

private:
  std::string                         brokers_;
  std::map<std::string, std::string>  kafkaGlobal_;
  std::map<std::string, std::string>  kafkaTopic_;
  KafkaCtx                           *kafka_;

private:
  char        *errbuf_;
  RunStatus   *runStatus_;

  LuaHelper  *helper_;
  FileOff    *fileOff_;
};

#endif
