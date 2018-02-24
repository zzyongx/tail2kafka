#ifndef _TRANSFORM_H_
#define _TRANSFORM_H_

#include <string>
#include <vector>
#include <map>
#include <unistd.h>
#include <sys/types.h>
#include <librdkafka/rdkafka.h>
#include "luahelper.h"
#include "cmdnotify.h"

struct MessageInfo {
  enum InfoType { META, NMSG, MSG };
  static bool extract(const char *payload, size_t len, MessageInfo *info, bool nonl);

  InfoType type;

  std::string host;
  long pos;

  std::string file;
  size_t size;
  std::string md5;

  const char *ptr;
  int len;
};

class Transform {
public:
  enum Format { NGINX, TSV, RAW, ORC, JSON, NIL };
  static Format stringToFormat(const char *s, size_t len);

  enum TimeFormat { TIMELOCAL, ISO8601, TIMEFORMAT_NIL };
  static TimeFormat stringToTimeFormat(const char *s);

  static Transform *create(const char *wdir, const char *topic, int partition,
                           CmdNotify *notify, const char *format, char *errbuf);
  virtual ~Transform();

  static const uint32_t GLOBAL  = 0x0001;
  static const uint32_t LOCAL   = 0x0002;
  static const uint32_t IGNORE  = 0x0004;
  static const uint32_t RKMFREE = 0x0008;

  virtual uint32_t write(rd_kafka_message_t *rkm, uint64_t *offsetPtr) = 0;
  virtual uint32_t timeout(uint64_t *offsetPtr);

protected:
  Transform(const char *wdir, const char *topic, int partition, CmdNotify *notify)
    : wdir_(wdir), topic_(topic), partition_(partition), notify_(notify) {}

  const char *wdir_;
  const char *topic_;
  int         partition_;
  CmdNotify  *notify_;
};

class MirrorTransform : public Transform {
public:
  struct FdCache {
    int                       fd;
    std::vector<struct iovec> iovs;

    long                 pos;
    size_t               rkmSize;
    rd_kafka_message_t **rkms;

    FdCache() : fd(-1), pos(-1), rkmSize(0), rkms(0) {}

    ~FdCache() {
      assert(rkmSize == 0);
      if (fd != -1) close(fd);
      if (rkms) delete[] rkms;
    }

    void clear() {
      for (size_t i = 0; i < rkmSize; ++i) rd_kafka_message_destroy(rkms[i]);
      pos = -1;
      rkmSize = 0;
      iovs.clear();
    }
  };

  MirrorTransform(const char *wdir, const char *topic, int partition, CmdNotify *notify)
    : Transform(wdir, topic, partition, notify) {}
  uint32_t write(rd_kafka_message_t *rkm, uint64_t *offsetPtr);

private:
  void addToCache(rd_kafka_message_t *rkm, const MessageInfo &info);
  bool flushCache(bool eof, const std::string &host);

  std::map<std::string, FdCache> fdCache_;
};

class LuaTransform : public Transform {
public:
  LuaTransform(const char *wdir, const char *topic, int partition, CmdNotify *notify)
    : Transform(wdir, topic, partition, notify), helper_(0), currentTimestamp_(0),
      currentIntervalCnt_(-1), currentIntervalFd_(-1), currentOffset_(-1),
      lastIntervalCnt_(-1), lastIntervalFd_(-1), lastOffset_(-1) {}

  ~LuaTransform();

  bool init(Format inputFormat, Format outputFormat, int interval, int delay, const char *luaFile, char *errbuf);
  uint32_t write(rd_kafka_message_t *rkm, uint64_t *offsetPtr);
  uint32_t timeout(uint64_t *offsetPtr);

private:
  void updateTimestamp(time_t timestamp) {
    if (currentTimestamp_ == -1 || timestamp > currentTimestamp_) currentTimestamp_ = timestamp;
  }

  bool parseFields(const char *ptr, size_t len, std::vector<std::string> *fields, time_t *timestamp);
  bool fieldsToJson(const std::vector<std::string> &fields, const std::string &method, const std::string &path,
                    std::map<std::string, std::string> *query, std::string *json) const;


  void initCurrentFile(long intervalCnt, uint64_t offset);
  bool lastIntervalTimeout() const {
    return lastIntervalFd_ > 0 && currentTimestamp_ > currentIntervalCnt_ * interval_ + delay_;
  }
  uint32_t timeout_(uint64_t *offsetPtr);

  void rotateCurrentToLast();
  void rotateLastToFinish();

  uint32_t rotate(long intervalCnt, uint64_t offset, uint64_t *offsetPtr);

private:
  LuaHelper *helper_;
  Format inputFormat_;

  std::vector<std::string> fields_;
  TimeFormat timestampFormat_;
  size_t timeLocalIndex_;
  int requestIndex_;

  bool deleteRequestField_;
  std::string timeLocalFormat_;

  std::map<std::string, std::string> requestNameMap_;
  std::map<std::string, std::string> requestTypeMap_;

  int interval_;
  int delay_;

  time_t currentTimestamp_;

  long currentIntervalCnt_;
  int currentIntervalFd_;
  std::string currentIntervalFile_;
  uint64_t currentOffset_;

  long lastIntervalCnt_;
  int lastIntervalFd_;
  std::string lastIntervalFile_;
  uint64_t lastOffset_;
};

#endif
