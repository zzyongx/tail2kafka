#ifndef _LUACTX_H_
#define _LUACTX_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "filereader.h"
#include "luafunction.h"
#include "cnfctx.h"

class LuaCtx {
  template<class T> friend class UNITTEST_HELPER;
public:

public:
  static LuaCtx *loadFile(CnfCtx *cnf, const char *file);
  ~LuaCtx();

  bool initFileReader(char *errbuf);
  FileReader *getFileReader() { return fileReader_; }

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

  const std::string &file() const { return file_; }
  const std::string &topic() const { return topic_; }

  LuaFunction *function() const { return function_; }

private:
  LuaCtx();
  bool createFileIf(const char *luaFile, char *errbuf) const;

private:
  bool          autocreat_;
  std::string   file_;
  std::string   topic_;

  bool          withhost_;
  bool          withtime_;
  int           timeidx_;
  bool          autonl_;
  std::string   pkey_;

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
};

#endif
