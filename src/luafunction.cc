#include <memory>

#include "util.h"
#include "luactx.h"
#include "luafunction.h"

#define PADDING_LEN 13

const char *LuaFunction::typeToString(Type type)
{
  switch (type) {
  case FILTER: return "filter";
  case GREP: return "grep";
  case TRANSFORM: return "transform";
  case AGGREGATE: return "aggregate";
  default: {assert(0); return "null";}
  }
}

LuaFunction *LuaFunction::create(LuaCtx *ctx, LuaHelper *helper)
{
  std::auto_ptr<LuaFunction> function(new LuaFunction(ctx));

  if (!helper->getArray("filter", &function->filters_, false)) return 0;
  if (!function->filters_.empty()) {
    function->init(helper, "filter", FILTER);
    return function.release();
  }

  std::string value;
  Type types[] = {GREP, TRANSFORM, AGGREGATE};

  for (size_t i = 0; i < sizeof(types)/sizeof(Type); ++i) {
    std::string fun = typeToString(types[i]);
    if (!helper->getFunction(fun.c_str(), &value, std::string(""))) return 0;
    if (value.empty()) {         // function not exist
      continue;
    } else if (value == fun) {   // found function
      function->init(helper, value, types[i]);
    } else {                     // found function name
      fun = value;
      if (!ctx->cnf()->getLuaHelper()->getFunction(fun.c_str(), &value, "")) return 0;
      if (value == fun) {
        function->init(ctx->cnf()->getLuaHelper(), value, types[i]);
      }
    }
  }

  if (function->type_ == AGGREGATE && ctx->timeidx() < 0) {
    snprintf(ctx->cnf()->errbuf(), MAX_ERR_LEN, "%s aggreagte must have timeidx", helper->file());
    return 0;
  }

  if (ctx->withhost()) {
    if (function->type_ == NIL || function->type_ == FILTER || function->type_ == GREP || function->type_ == TRANSFORM) {
      function->extraSize_ = 1 + ctx->cnf()->host().size() + 1 + PADDING_LEN + 1;  // *host@off
    } else {
      function->extraSize_ = ctx->cnf()->host().size() + 1; // host
    }
  } else {
    function->extraSize_ = 0;
  }

  return function.release();
}

inline std::string *addHost(std::string *ptr, const std::string &host, off_t off, bool space) {
  ptr->append(1, '*').append(host);
  if (off != (off_t) -1) ptr->append(1, '@').append(util::toStr(off, PADDING_LEN));
  if (space) ptr->append(1, ' ');
  return ptr;
}

int LuaFunction::filter(off_t off, const std::vector<std::string> &fields, std::vector<std::string *> *lines)
{
  std::string *result = new std::string;
  if (ctx_->withhost()) result = addHost(result, ctx_->cnf()->host(), off, false);

  for (std::vector<int>::iterator ite = filters_.begin(), end = filters_.end();
       ite != end; ++ite) {
    int idx = absidx(*ite, fields.size());
    if (idx < 0 || (size_t) idx >= fields.size()) continue;

    if (!result->empty()) result->append(1, ' ');
    result->append(fields[idx]);
  }

  lines->push_back(result);
  return 1;
}

int LuaFunction::grep(off_t off, const std::vector<std::string> &fields, std::vector<std::string *> *lines)
{
  if (!helper_->call(funName_.c_str(), fields, 1)) return -1;
  if (helper_->callResultNil()) return 0;

  std::string *result = new std::string;
  if (ctx_->withhost()) result = addHost(result, ctx_->cnf()->host(), off, true);

  if (helper_->callResultListAsString(funName_.c_str(), result)) {
    lines->push_back(result);
    return 1;
  } else {
    delete result;
    return -1;
  }
}

int LuaFunction::transform(off_t off, const char *line, size_t nline, std::vector<std::string *> *lines)
{
  if (!helper_->call(funName_.c_str(), line, nline)) return -1;
  if (helper_->callResultNil()) return 0;

  std::string *result = new std::string;
  if (ctx_->withhost()) result = addHost(result, ctx_->cnf()->host(), off, true);

  if (helper_->callResultString(funName_.c_str(), result)) {
    lines->push_back(result);
    return 1;
  } else {
    delete result;
    return -1;
  }
}

int LuaFunction::serializeCache(std::vector<std::string*> *lines)
{
  if (aggregateCache_.empty()) return 0;

  int n = 0;
  for (std::map<std::string, std::map<std::string, int> >::iterator ite = aggregateCache_.begin();
       ite != aggregateCache_.end(); ++ite) {
    std::string *s = new std::string;
    if (ctx_->withhost()) s->append(ctx_->host()).append(1, ' ');
    if (ctx_->withtime()) s->append(lasttime_).append(1, ' ');

    s->append(ite->first);
    for (std::map<std::string, int>::iterator jte = ite->second.begin(); jte != ite->second.end(); ++jte) {
      s->append(1, ' ').append(jte->first).append(1, '=').append(util::toStr(jte->second));
    }
    lines->push_back(s);
    ++n;
  }
  aggregateCache_.clear();
  return n;
}

int LuaFunction::aggregate(const std::vector<std::string> &fields, std::vector<std::string *> *lines)
{
  int n = 0;
  std::string curtime = fields[absidx(ctx_->timeidx(), fields.size())];
  if (!lasttime_.empty() && curtime != lasttime_) {
    n = serializeCache(lines);
  }
  lasttime_ = curtime;

  if (!helper_->call(funName_.c_str(), fields, 2)) return false;
  if (helper_->callResultNil()) return true;

  std::string pkey;
  std::map<std::string, int> map;
  if (!helper_->callResult(funName_.c_str(), &pkey, &map)) return false;

  for (std::map<std::string, int>::iterator ite = map.begin(), end = map.end(); ite != end; ++ite) {
    aggregateCache_[pkey][ite->first] += ite->second;
    if (!ctx_->pkey().empty()) aggregateCache_[ctx_->pkey()][ite->first] += ite->second;
  }

  return n;
}

int LuaFunction::process(off_t off, const char *line, size_t nline, std::vector<std::string *> *lines)
{
  if (type_ == TRANSFORM) {
    return transform(off, line, nline, lines);
  } else if (type_ == AGGREGATE || type_ == GREP || type_ == FILTER) {
    std::vector<std::string> fields;
    split(line, nline, &fields);

    if (ctx_->timeidx() >= 0) {
      int idx = absidx(ctx_->timeidx(), fields.size());
      if (idx < 0 || (size_t) idx >= fields.size()) return false;
      iso8601(fields[idx], &fields[idx]);
    }

    if (type_ == AGGREGATE) {
      return aggregate(fields, lines);
    } else if (type_ == GREP) {
      return grep(off, fields, lines);
    } else if (type_ == FILTER) {
      return filter(off, fields, lines);
    } else {
      return 0;
    }
  } else {
    std::string *ptr = new std::string;

    if (ctx_->withhost()) addHost(ptr, ctx_->cnf()->host(), off, true);
    ptr->append(line, nline);
    if (ctx_->autonl()) ptr->append(1, '\n');

    lines->push_back(ptr);
    return 1;
  }
}
