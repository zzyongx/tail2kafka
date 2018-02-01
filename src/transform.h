#ifndef _TRANSFORM_H_
#define _TRANSFORM_H_

#include <string>
#include <vector>
#include <map>
#include <unistd.h>
#include <sys/types.h>
#include <librdkafka/rdkafka.h>
#include "luahelper.h"

class Transform {
public:
  enum Format { NGINX, RAW, ORC, JSON, NIL };
  static Format stringToFormat(const char *s, size_t len);

  static Transform *create(const char *wdir, const char *topic, int partition,
                           const char *notify, const char *format, char *errbuf);
  virtual ~Transform();

  enum Idempotent { GLOBAL, LOCAL, IGNORE };

  virtual Idempotent write(rd_kafka_message_t *rkm, uint64_t *offsetPtr) = 0;
  virtual Idempotent timeout(uint64_t * /*offsetPtr*/);

protected:
  Transform(const char *wdir, const char *topic, int partition, const char *notify)
    : wdir_(wdir), topic_(topic), partition_(partition), notify_(notify) {}

  const char *wdir_;
  const char *topic_;
  int         partition_;
  const char *notify_;
};

class MirrorTransform : public Transform {
public:
  struct FdCache {
    int                       fd;
    std::string               file;
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

  MirrorTransform(const char *wdir, const char *topic, int partition, const char *notify)
    : Transform(wdir, topic, partition, notify) {}
  Idempotent write(rd_kafka_message_t *rkm, uint64_t *offsetPtr);

private:
  bool addToCache(rd_kafka_message_t *rkm, std::string *host, std::string *file);
  bool flushCache(bool force, bool eof, const std::string &host, std::string *ofile);

  std::map<std::string, FdCache> fdCache_;
};

class LuaTransform : public Transform {
public:
  LuaTransform(const char *wdir, const char *topic, int partition, const char *notify)
    : Transform(wdir, topic, partition, notify), helper_(0), currentTimestamp_(0),
      currentIntervalCnt_(-1), currentIntervalFd_(-1), currentOffset_(-1),
      lastIntervalCnt_(-1), lastIntervalFd_(-1), lastOffset_(-1) {}

  ~LuaTransform();

  bool init(Format inputFormat, Format outputFormat, int interval, int delay, const char *luaFile, char *errbuf);
  Idempotent write(rd_kafka_message_t *rkm, uint64_t *offsetPtr);
  Idempotent timeout(uint64_t *offsetPtr);

private:
  bool selectCurrentFile(int intervalCnt, int **fd, std::string **file);
  void openCurrent(int intervalCnt, int *fd, std::string *file);

  bool lastIntervalTimeout() {
    return lastIntervalFd_ > 0 && currentTimestamp_ > currentIntervalCnt_ * interval_ + delay_;
  }
  Idempotent timeout_(uint64_t *offsetPtr);

  void rotateCurrentToLast();
  void rotateLastToFinish();

private:
  LuaHelper *helper_;

  std::vector<std::string> fields_;
  size_t timeLocalIndex_;
  size_t requestIndex_;

  bool deleteRequestField_;
  std::string timeLocalFormat_;

  std::map<std::string, std::string> requestNameMap_;
  std::map<std::string, std::string> requestTypeMap_;

  int interval_;
  int delay_;

  time_t currentTimestamp_;

  int currentIntervalCnt_;
  int currentIntervalFd_;
  std::string currentIntervalFile_;
  uint64_t currentOffset_;

  int lastIntervalCnt_;
  int lastIntervalFd_;
  std::string lastIntervalFile_;
  uint64_t lastOffset_;
};

#endif
