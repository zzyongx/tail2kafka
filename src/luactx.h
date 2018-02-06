#ifndef _LUACTX_H_
#define _LUACTX_H_

#include <string>
#include <deque>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "sys.h"
#include "luafunction.h"
#include "cnfctx.h"

struct rk_kafka_topic_t;
class FileReader;

class LuaCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  static LuaCtx *loadFile(CnfCtx *cnf, const char *file);
  ~LuaCtx();

  bool initFileReader(char *errbuf);
  FileReader *getFileReader() { return fileReader_; }

  void setRkt(rd_kafka_topic_t *rktopic) { rkt_ = rktopic; }
  rd_kafka_topic_t *rkt() { return rkt_; }

  void setNext(LuaCtx* nxt) { next_ = nxt; }
  LuaCtx *next() { return next_; }

  CnfCtx *cnf() { return cnf_; }

  void setIdx(int i) { idx_ = i; }
  int idx() const { return idx_; }

  bool copyRawRequired() const {
#ifdef DISABLE_COPYRAW
    return false;
#else
    return function_->empty() && cnf_->getPollLimit() && rawcopy_;
#endif
  }

  int getPartition(uint32_t pc) const {
    if (partition_ < 0) {
      if (autoparti_) {
        return (ntohl(addr_) & 0xff) % pc;
      } else {
        return cnf_->partition();
      }
    } else {
      return partition_;
    }
  }

  bool withhost() const { return withhost_; }
  bool withtime() const { return withtime_; }
  int timeidx() const { return timeidx_; }
  bool autonl() const { return autonl_; }
  const std::string &pkey() const { return pkey_; }

  const char *getStartPosition() const { return startPosition_.c_str(); }
  const std::string &host() const { return cnf_->host(); }

  int getRotateDelay() const { return rotateDelay_ <= 0 ? cnf_->getRotateDelay() : rotateDelay_; }
  bool fileWithTimeFormat() const { return fileWithTimeFormat_; }

  bool getTimeFormatFile(std::string *timeFormatFile) const {
    if (fileWithTimeFormat_) {
      std::string f = sys::timeFormat(cnf_->fasttime(), file_.c_str(), file_.size());
      if (f != timeFormatFile_) {
        timeFormatFile->assign(f);
        return true;
      }
    }
    return false;
  }

  void setTimeFormatFile(const std::string &timeFormatFile) {
    if (fileWithTimeFormat_) timeFormatFile_ = timeFormatFile;
  }

  const std::string &file() const { return fileWithTimeFormat_ ? timeFormatFile_ : file_; }
  const std::string & datafile() const {
    if (!fqueue_.empty()) return fqueue_.front();
    else return fileWithTimeFormat_ ? timeFormatFile_ : file_;
  }

  bool addHistoryFile(const std::string &historyFile);
  bool removeHistoryFile();

  const std::string &topic() const { return topic_; }
  bool autocreat() const { return autocreat_; }
  LuaFunction *function() const { return function_; }

private:
  LuaCtx();
  bool testFile(const char *luaFile, char *errbuf);

private:
  /* default set to topic_, it must be unique
   * if multi file write to one topic_, manual set fileAlias
   */
  std::string   fileAlias_;

  bool          autocreat_;
  std::string   file_;
  std::string   topic_;

  bool          withhost_;
  bool          withtime_;
  int           timeidx_;
  bool          autonl_;
  int           rotateDelay_;
  std::string   pkey_;

  bool          fileWithTimeFormat_;
  std::string   timeFormatFile_;
  std::deque<std::string> fqueue_;

  uint32_t      addr_;
  bool          autoparti_;
  int           partition_;
  bool          rawcopy_;

  LuaFunction  *function_;
  std::string   startPosition_;
  FileReader   *fileReader_;

  int           idx_;
  LuaCtx       *next_;
  CnfCtx       *cnf_;
  LuaHelper    *helper_;

  rd_kafka_topic_t *rkt_;
};

#endif
