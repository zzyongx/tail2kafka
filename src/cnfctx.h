#ifndef _CNFCTX_H_
#define _CNFCTX_H_

#include <string>
#include <vector>
#include <map>
#include <sys/time.h>

#include "gnuatomic.h"
#include "fileoff.h"
#include "luahelper.h"
#include "esctx.h"
#include "kafkactx.h"
#include "common.h"

#define QUEUE_ERROR_TIMEOUT 60
#define MAX_FILE_QUEUE_SIZE 50000

class TailStats {
public:
  TailStats() :
    fileRead_(0), logRead_(0), logWrite_(0),
    logRecv_(0), logSend_(0), logError_(0),
    queueSize_(0) {}

  void fileReadInc(int add = 1) { util::atomic_inc(&fileRead_, add); }
  void logReadInc(int add = 1) { util::atomic_inc(&logRead_, add); }
  void logWriteInc(int add = 1) { util::atomic_inc(&logWrite_, add); }

  void logRecvInc(int add = 1) { util::atomic_inc(&logRecv_, add); }
  void logSendInc(int add = 1) { util::atomic_inc(&logSend_, add); }
  void logErrorInc(int add = 1) { util::atomic_inc(&logError_, add); }

  void queueSizeInc(int add = 1) { util::atomic_inc(&queueSize_, add); }
  void queueSizeDec(int add = 1) { util::atomic_dec(&queueSize_, add); }

  int64_t fileRead() const { return fileRead_; }
  int64_t logRead() const { return logRead_; }
  int64_t logWrite() const { return logWrite_; }

  int64_t logSend() const { return logSend_; }
  int64_t logRecv() const { return logRecv_; }
  int64_t logError() const { return logError_; }

  int64_t queueSize() const { return util::atomic_get((int64_t *) &queueSize_); }

  void get(TailStats *stats) {
    stats->fileRead_ = util::atomic_get(&fileRead_);
    stats->logRead_ = util::atomic_get(&logRead_);
    stats->logWrite_ = util::atomic_get(&logWrite_);

    stats->logRecv_ = util::atomic_get(&logRecv_);
    stats->logSend_ = util::atomic_get(&logSend_);
    stats->logError_ = util::atomic_get(&logError_);

    stats->queueSize_ = util::atomic_get(&queueSize_);
  }

private:
  int64_t fileRead_;
  int64_t logRead_;
  int64_t logWrite_;

  int64_t logRecv_;
  int64_t logSend_;
  int64_t logError_;

  int64_t queueSize_;
};

class RunStatus;

enum TimeUnit {
  TIMEUNIT_MILLI, TIMEUNIT_SECONDS,
};

class CnfCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  int                    accept;
  int                    server;

public:
  static CnfCtx *loadCnf(const char *dir, char *errbuf);
  bool reset();
  bool rectifyHistoryFile();

  static CnfCtx *loadFile(const char *file, char *errbuf);
  ~CnfCtx();
  void addLuaCtx(LuaCtx *ctx);

  bool enableKafka() const { return !brokers_.empty(); }
  bool initKafka();
  KafkaCtx *getKafka() { return kafka_; }

  bool enableEs() const { return !esNodes_.empty(); }

  bool initEs();
  EsCtx *getEs() { return es_; }

  bool initFileOff();
  FileOff *getFileOff() { return fileOff_; }

  bool initFileReader();

  void setRunStatus(RunStatus *runStatus) { runStatus_ = runStatus; }
  RunStatus *getRunStatus() { return runStatus_; }

  TailStats *stats() { return &stats_; }
  void logStats();

  const char *getBrokers() const { return brokers_.c_str(); }
  const std::map<std::string, std::string> &getKafkaGlobalConf() const { return kafkaGlobal_; }
  const std::map<std::string, std::string> &getKafkaTopicConf() const { return kafkaTopic_; }

  std::vector<std::string> getEsNodes() const { return esNodes_; }
  size_t getEsMaxConns() const { return esMaxConns_; }
  const std::string getEsUserPass() const { return esUserPass_; }

  const char *getPidFile() const {
    return pidfile_.c_str();
  }

  LuaHelper *getLuaHelper() { return helper_; }

  size_t getLuaCtxSize() const { return count_; }
  std::vector<LuaCtx *> &getLuaCtxs() { return luaCtxs_; }

  int getPollLimit() const { return pollLimit_; }
  const std::string &pingbackUrl() const { return pingbackUrl_; }

  uint32_t addr() const { return addr_; }
  int partition() const { return partition_; }
  const std::string &host() const { return host_; }

  int64_t fasttime(TimeUnit unit = TIMEUNIT_SECONDS) const {
    if (unit == TIMEUNIT_MILLI) return timeval_.tv_sec * 1000 + timeval_.tv_usec / 1000;
    else return timeval_.tv_sec;
  }

  int64_t fasttime(bool force, TimeUnit unit) {
    if (force) gettimeofday(&timeval_, 0);
    return fasttime(unit);
  }

  char *errbuf() { return errbuf_; }
  const std::string &libdir() const { return libdir_; }
  const std::string &logdir() const { return logdir_; }
  int daemonize() const { return daemonize_; }

  void setTailLimit(bool tailLimit) { tailLimit_ = tailLimit; }
  bool getTailLimit() const { return tailLimit_; }

  void flowControl(bool block) { util::atomic_set(&flowControl_, block ? 1 : 0); }

  bool flowControlOn() const {
    return util::atomic_get((int *) &flowControl_) ||
      stats_.queueSize() > MAX_FILE_QUEUE_SIZE;
  }

private:
  CnfCtx();

  long lastLog_;
  TailStats stats_;

  std::string pidfile_;
  std::string host_;
  uint32_t    addr_;
  int         partition_;
  int         pollLimit_;
  std::string pingbackUrl_;
  std::string logdir_;
  std::string libdir_;
  int daemonize_;

  size_t                 count_;
  std::vector<LuaCtx *>  luaCtxs_;

  std::string                         brokers_;
  std::map<std::string, std::string>  kafkaGlobal_;
  std::map<std::string, std::string>  kafkaTopic_;
  KafkaCtx                           *kafka_;

  std::vector<std::string> esNodes_;
  std::string  esUserPass_;
  int          esMaxConns_;
  EsCtx       *es_;

  struct timeval timeval_;
  char        *errbuf_;
  RunStatus   *runStatus_;

  LuaHelper  *helper_;
  FileOff    *fileOff_;

  bool tailLimit_;
  int flowControl_;
};

#endif
