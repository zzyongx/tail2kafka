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
  case INDEXDOC: return "indexdoc";
  case ESPLAIN: return "esplain";
  case KAFKAPLAIN: return "kafkaplain";
  default: {assert(0); return "null";}
  }
}

LuaFunction *LuaFunction::create(LuaCtx *ctx, LuaHelper *helper, Type defType)
{
  std::auto_ptr<LuaFunction> function(new LuaFunction(ctx));

  if (!helper->getArray("filter", &function->filters_, false)) return 0;
  if (!function->filters_.empty()) {
    function->init(helper, "filter", FILTER);
    return function.release();
  }

  std::string value;
  Type types[] = {GREP, TRANSFORM, AGGREGATE, INDEXDOC};

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

  if (function->type_ == NIL) function->type_ = defType;
  if (function->type_ == NIL) {
    snprintf(ctx->cnf()->errbuf(), MAX_ERR_LEN, "%s, topic or es_index,es_doc or indexdoc is required",
             helper->file());
    return 0;
  }

  if (function->type_ == AGGREGATE && ctx->timeidx() < 0) {
    snprintf(ctx->cnf()->errbuf(), MAX_ERR_LEN, "%s aggreagte must have timeidx", helper->file());
    return 0;
  }

  if (ctx->withhost()) {
    if (function->type_ == KAFKAPLAIN || function->type_ == FILTER ||
        function->type_ == GREP || function->type_ == TRANSFORM) {
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

int LuaFunction::filter(off_t off, const std::vector<std::string> &fields, std::vector<FileRecord *> *records)
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

  records->push_back(FileRecord::create(0, off, result));
  return 1;
}

int LuaFunction::grep(off_t off, const std::vector<std::string> &fields, std::vector<FileRecord *> *records)
{
  if (!helper_->call(funName_.c_str(), fields, 1)) return -1;
  if (helper_->callResultNil()) return 0;

  std::string *result = new std::string;
  if (ctx_->withhost()) result = addHost(result, ctx_->cnf()->host(), off, true);

  if (helper_->callResultListAsString(funName_.c_str(), result)) {
    records->push_back(FileRecord::create(0, off, result));
    return 1;
  } else {
    delete result;
    return -1;
  }
}

int LuaFunction::transform(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records)
{
  if (!helper_->call(funName_.c_str(), line, nline)) return -1;
  if (helper_->callResultNil()) return 0;

  std::string *result = new std::string;
  if (ctx_->withhost()) result = addHost(result, ctx_->cnf()->host(), off, true);

  if (helper_->callResultString(funName_.c_str(), result)) {
    records->push_back(FileRecord::create(0, off, result));
    return 1;
  } else {
    delete result;
    return -1;
  }
}

int LuaFunction::kafkaPlain(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records)
{
  std::string *ptr = new std::string;

  if (ctx_->withhost()) addHost(ptr, ctx_->cnf()->host(), off, true);
  ptr->append(line, nline);
  if (ctx_->autonl()) ptr->append(1, '\n');

  records->push_back(FileRecord::create(0, off, ptr));
  return 1;
}

int LuaFunction::indexdoc(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records)
{
  if (!helper_->call(funName_.c_str(), line, nline, 2)) return -1;
  if (helper_->callResultNil()) return 0;

  std::string *index = new std::string;
  std::string *doc = new std::string;

  if (helper_->callResultString(funName_.c_str(), index, doc)) {
    records->push_back(FileRecord::create(0, off, index, doc));
    return 1;
  } else {
    delete index;
    delete doc;
    return -1;
  }
}

#define hex2int(hex) ((hex) >= 'A' ? 10 + (hex) - 'A' : (hex) - '0')

// {\x22receiver\x22:\x22bb_up\x22} -> {"receiver":"bb_up"}
void LuaFunction::transformEsDocNginxLog(const std::string &src, std::string *dst)
{
  for (size_t i = 0; i < src.size(); ++i) {
    if (src[i] == '\\' && i+3 < src.size() && src[i+1] == 'x') {
      dst->append(1, hex2int(src[i+2]) * 16 + hex2int(src[i+3]));
      i += 3;
    } else {
      dst->append(1, src[i]);
    }
  }
}

// "{\"receiver\":\"bb_up\"}\n" -> {"receiver":"bb_up"}
void LuaFunction::transformEsDocNginxJson(const std::string &src, std::string *dst)
{
  size_t slen = 1;
  if (src.size() > 3 && src[src.size()-3] == '\\' && src[src.size()-2] == 'n') {
    slen = 3;
  }

  for (size_t i = 1; i+slen < src.size(); ++i) {
    if (src[i] == '\\' && i+1 < src.size() && src[i+1] == '"') {
      dst->append(1, '"');
      i += 1;
    } else {
      dst->append(1, src[i]);
    }
  }
}

int LuaFunction::esPlain(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records)
{
  std::string esIndex;
  bool esIndexWithTimeFormat;
  int esIndexPos, esDocPos, esDocDataFormat;
  ctx_->es(&esIndex, &esIndexWithTimeFormat, &esIndexPos, &esDocPos, &esDocDataFormat);

  if (esIndexWithTimeFormat) {
    struct tm ltm;
    time_t now = ctx_->cnf()->fasttime();
    localtime_r(&now, &ltm);
    char buf[256];
    size_t n = strftime(buf, 256, esIndex.c_str(), &ltm);
    esIndex.assign(buf, n);
  }

  std::string *doc = 0, *index = 0;
  if (esDocPos == 1) {
    index = new std::string(esIndex);
    doc = new std::string(line, nline);
  } else {
    std::vector<std::string> v;
    splitn(line, nline, &v, esDocPos);
    if ((int) v.size() != esDocPos) {
      return -1;
    }

    if (esIndexPos > 0) {
      index = new std::string(v[esIndexPos-1] + esIndex);
    } else {
      index = new std::string(esIndex);
    }
    if (esDocDataFormat == ESDOC_DATAFORMAT_NGINX_LOG) {
      doc = new std::string;
      transformEsDocNginxLog(v[esDocPos-1], doc);
    } else if (esDocDataFormat == ESDOC_DATAFORMAT_NGINX_JSON) {
      doc = new std::string;
      transformEsDocNginxJson(v[esDocPos-1], doc);
    } else {
      doc = new std::string(v[esDocPos-1]);
    }
  }

  records->push_back(FileRecord::create(0, off, index, doc));
  return 0;
}

int LuaFunction::serializeCache(std::vector<FileRecord *> *records)
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
    records->push_back(FileRecord::create(0, -1, s));
    ++n;
  }
  aggregateCache_.clear();
  return n;
}

int LuaFunction::aggregate(const std::vector<std::string> &fields, std::vector<FileRecord *> *records)
{
  int n = 0;
  std::string curtime = fields[absidx(ctx_->timeidx(), fields.size())];
  if (!lasttime_.empty() && curtime != lasttime_) {
    n = serializeCache(records);
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

int LuaFunction::process(off_t off, const char *line, size_t nline, std::vector<FileRecord *> *records)
{
  if (type_ == TRANSFORM) {
    return transform(off, line, nline, records);
  } else if (type_ == INDEXDOC) {
    return indexdoc(off, line, nline, records);
  } else if (type_ == AGGREGATE || type_ == GREP || type_ == FILTER) {
    std::vector<std::string> fields;
    split(line, nline, &fields);

    if (ctx_->timeidx() >= 0) {
      int idx = absidx(ctx_->timeidx(), fields.size());
      if (idx < 0 || (size_t) idx >= fields.size()) return false;
      timeLocalToIso8601(fields[idx], &fields[idx]);
    }

    if (type_ == AGGREGATE) {
      return aggregate(fields, records);
    } else if (type_ == GREP) {
      return grep(off, fields, records);
    } else if (type_ == FILTER) {
      return filter(off, fields, records);
    } else {
      return 0;
    }
  } else if (type_ == KAFKAPLAIN) {
    return kafkaPlain(off, line, nline, records);
  } else if (type_ == ESPLAIN) {
    return esPlain(off, line, nline, records);
  } else {
    return 0;
  }
}
