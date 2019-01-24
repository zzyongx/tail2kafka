#ifndef _LUAFUNCTION_H_
#define _LUAFUNCTION_H_

#include <string>
#include <vector>
#include <sys/types.h>

#include "luahelper.h"
#include "luactx.h"
#include "filerecord.h"

class LuaFunction {
  template<class T> friend class UNITTEST_HELPER;
public:
  enum Type { FILTER, GREP, TRANSFORM, AGGREGATE, INDEXDOC, KAFKAPLAIN, ESPLAIN, NIL };

  static LuaFunction *create(LuaCtx *ctx, LuaHelper *helper, Type defType);
  int process(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records);
  int serializeCache(std::vector<FileRecord *> *records);

  Type getType() const { return type_; }
  size_t extraSize() const { return extraSize_; }

private:
  static const char *typeToString(Type type);

  LuaFunction(LuaCtx *ctx) : ctx_(ctx), type_(NIL) {}
  void init(LuaHelper *helper, const std::string &funName, Type type) {
    helper_  = helper;
    funName_ = funName;
    type_    = type;
  }

  int filter(off_t off, const std::vector<std::string> &fields, std::vector<FileRecord *> *records);
  int grep(off_t off, const std::vector<std::string> &fields, std::vector<FileRecord *> *records);
  int transform(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records);
  int aggregate(const std::vector<std::string> &fields, std::vector<FileRecord *> *records);
  int kafkaPlain(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records);

  int indexdoc(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records);
  int esPlain(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records);

private:
  LuaCtx      *ctx_;
  LuaHelper   *helper_;
  std::string funName_;
  Type        type_;
  size_t      extraSize_;

  std::vector<int> filters_;

  std::string                                        lasttime_;
  std::map<std::string, std::map<std::string, int> > aggregateCache_;
};

#endif
