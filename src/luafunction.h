#ifndef _LUAFUNCTION_H_
#define _LUAFUNCTION_H_

#include <string>
#include <vector>
#include <sys/types.h>

#include "luahelper.h"
#include "luactx.h"

class LuaFunction {
  template<class T> friend class UNITTEST_HELPER;
public:
  enum Type { FILTER, GREP, TRANSFORM, AGGREGATE, NIL };

  static LuaFunction *create(LuaCtx *ctx, LuaHelper *helper);
  int process(off_t off, const char *line, size_t nline, std::vector<std::string *> *lines);
  int serializeCache(std::vector<std::string*> *lines);

  bool empty() const { return type_ == NIL; }
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

  int filter(off_t off, const std::vector<std::string> &fields, std::vector<std::string *> *lines);
  int grep(off_t off, const std::vector<std::string> &fields, std::vector<std::string *> *lines);
  int transform(off_t off, const char *line, size_t nline, std::vector<std::string *> *lines);
  int aggregate(const std::vector<std::string> &fields, std::vector<std::string *> *lines);

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
