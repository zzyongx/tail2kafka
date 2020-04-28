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

class FileReader;

#define PARTITIONER_RANDOM -100

#define ESDOC_DATAFORMAT_NGINX_JSON 1
#define ESDOC_DATAFORMAT_NGINX_LOG  2
#define ESDOC_DATAFORMAT_JSON       3

class LuaCtx {
  template<class T> friend class UNITTEST_HELPER;
public:
  static LuaCtx *loadFile(CnfCtx *cnf, const char *file);
  ~LuaCtx();

  bool parseEsIndexDoc(const std::string &esIndex, const std::string &esDoc, char errbuf[]);
  void es(std::string *esIndex, bool *esIndexWithTimeFormat, int *esIndexPos,
          int *esDocPos, int *esDocDataFormat) const {
    *esIndex = esIndex_;
    *esIndexWithTimeFormat = esIndexWithTimeFormat_;
    *esIndexPos = esIndexPos_;
    *esDocPos = esDocPos_;
    *esDocDataFormat = esDocDataFormat_;
  }

  bool testFile(const char *luaFile, char *errbuf);
  bool loadHistoryFile();

  bool initFileReader(FileReader *reader, char *errbuf);
  FileReader *getFileReader() { return fileReader_; }

  void setRktId(int id) { rktId_ = id; }
  int rktId() { return rktId_; }

  void setNext(LuaCtx* nxt) { next_ = nxt; }
  LuaCtx *next() { return next_; }

  CnfCtx *cnf() { return cnf_; }

  bool copyRawRequired() const {
#ifdef DISABLE_COPYRAW
    return false;
#else
    return function_->getType() == LuaFunction::KAFKAPLAIN && cnf_->getPollLimit() && rawcopy_;
#endif
  }

  int getPartitioner() const {
    if (partition_ == PARTITIONER_RANDOM) return partition_;
    else return -1;
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
  bool md5sum() const { return md5sum_; }
  const std::string &pkey() const { return pkey_; }

  const char *getStartPosition() const { return startPosition_.c_str(); }
  const std::string &host() const { return cnf_->host(); }

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

  const std::string &file() const {
    return fileWithTimeFormat_ ? timeFormatFile_ : file_;
  }

  const std::string & datafile() const {
    if (!fqueue_.empty()) return fqueue_.front();
    else return fileWithTimeFormat_ ? timeFormatFile_ : file_;
  }

  bool addHistoryFile(const std::string &historyFile);
  bool removeHistoryFile();

  const std::string &topic() const { return topic_; }
  LuaFunction *function() const { return function_; }

  bool autocreat() const { return autocreat_; }
  const char *fileOwner() const { return autocreat_ && !fileOwner_.empty() ? fileOwner_.c_str() : 0; }
  int uid() const { return uid_; }
  int gid() const { return gid_; }

  int holdFd() const { return holdFd_; }
  void holdFd(int fd) { holdFd_ = fd; }

private:
  LuaCtx();

private:
  /* default set to topic_, it must be unique
   * if multi file write to one topic_, manual set fileAlias
   */
  std::string   fileAlias_;

  bool          autocreat_;
  std::string   fileOwner_;
  uid_t         uid_;
  gid_t         gid_;

  std::string   file_;
  std::string   topic_;

  std::string   esIndex_;
  int esIndexWithTimeFormat_;
  int esIndexPos_;
  int esDocPos_;
  int esDocDataFormat_;

  bool          withhost_;
  bool          withtime_;
  int           timeidx_;
  bool          autonl_;
  std::string   pkey_;

  bool          fileWithTimeFormat_;
  std::string   timeFormatFile_;
  std::deque<std::string> fqueue_;

  uint32_t      addr_;
  bool          autoparti_;
  int           partition_;
  bool          rawcopy_;
  bool          md5sum_;

  LuaFunction  *function_;
  std::string   startPosition_;
  FileReader   *fileReader_;

  LuaCtx       *next_;
  CnfCtx       *cnf_;
  LuaHelper    *helper_;

  size_t rktId_;
  int holdFd_;
};

#endif
