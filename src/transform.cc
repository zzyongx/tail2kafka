#include <cstdio>
#include <cassert>
#include <string>
#include <algorithm>
#include <unistd.h>
#include <limits.h>
#include <sys/uio.h>
#include <json/json.h>

#include "logger.h"
#include "util.h"
#include "sys.h"
#include "transform.h"

Transform::~Transform() {}
uint32_t Transform::timeout(uint64_t * /*offsetPtr*/) { return IGNORE; }

const uint32_t Transform::GLOBAL;
const uint32_t Transform::LOCAL;
const uint32_t Transform::IGNORE;
const uint32_t Transform::RKMFREE;

enum FormatToken {INFORMAT, LUA, OUTFORMAT, INTERVAL, DELAY};
static FormatToken FORMAT_TOKENS[] = {INFORMAT, LUA, OUTFORMAT, INTERVAL, DELAY};
static size_t MAX_TOKEN_SIZE = sizeof(FORMAT_TOKENS)/sizeof(FORMAT_TOKENS[0]);

Transform::Format Transform::stringToFormat(const char *ptr, size_t len)
{
  if (strncmp(ptr, "nginx", len) == 0) return NGINX;
  else if (strncmp(ptr, "tsv", len) == 0) return TSV;
  else if (strncmp(ptr, "raw", len) == 0) return RAW;
  else if (strncmp(ptr, "orc", len) == 0) return ORC;
  else if (strncmp(ptr, "json", len) == 0) return JSON;
  else return NIL;
}

Transform::TimeFormat Transform::stringToTimeFormat(const char *ptr)
{
  if (strcmp(ptr, "timelocal") == 0) return TIMELOCAL;
  else if (strcmp(ptr, "iso8601") == 0) return ISO8601;
  else return TIMEFORMAT_NIL;
}

Transform *Transform::create(
  const char *wdir, const char *topic, int partition, CmdNotify *notify, const char *format, char *errbuf)
{
  const char *ptr   = format;
  const char *start = ptr;

  FormatToken token      = INFORMAT;
  size_t      tokenIndex = 0;

  Format inputFormat = NIL, outputFormat = NIL;
  std::string  luaFile;
  int interval = -1, delay = 0;

  while (tokenIndex < MAX_TOKEN_SIZE) {
    if (!*ptr || *ptr == ':') {
      switch (token) {
      case INFORMAT: inputFormat = stringToFormat(start, ptr - start); break;
      case LUA: luaFile.assign(start, ptr - start); break;
      case OUTFORMAT: outputFormat = stringToFormat(start, ptr - start); break;
      case INTERVAL: interval = ptr == start ? 0 : util::toInt(start, ptr - start); break;
      case DELAY: delay = ptr == start ? 0 : util::toInt(start, ptr - start); break;
      }
      if (!*ptr) break;

      token = FORMAT_TOKENS[++tokenIndex];
      start = ptr+1;
      if (tokenIndex >= MAX_TOKEN_SIZE) {
        sprintf(errbuf, "too many token %s", format);
        return 0;
      }
    }
    ++ptr;
  }

  if (inputFormat == RAW && luaFile.empty() && outputFormat == RAW) {
    MirrorTransform *transform = new MirrorTransform(wdir, topic, partition, notify);
    return transform;
  } else if (inputFormat == NGINX && access(luaFile.c_str(), R_OK) == 0 &&
             outputFormat == JSON) {
    LuaTransform *transform = new LuaTransform(wdir, topic, partition, notify);
    if (!transform->init(inputFormat, outputFormat, interval, delay, luaFile.c_str(), errbuf)) {
      delete transform;
      return 0;
    } else {
      return transform;
    }
  } else {
    sprintf(errbuf, "unknow format %s", format);
    return 0;
  }
}

bool MessageInfo::extract(const char *payload, size_t len, MessageInfo *info, bool nonl)
{
  char flag = payload[0];
  if (flag == '*') info->type = NMSG;
  else if (flag == '#') info->type = META;
  else info->type = MSG;

  char *spacePos = 0;
  if (info->type == META || info->type == NMSG) {
    spacePos = (char *) memchr(payload, ' ', len);
    if (!spacePos) return false;

    if (info->type == META) {
      info->host.assign(payload+1, spacePos - (payload+1));
    } else {
      char *atPos = (char *) memchr(payload, '@', spacePos - payload);
      if (!atPos) return false;
      info->host.assign(payload+1, atPos - (payload+1));
      info->pos = util::toLong(atPos+1, spacePos - (atPos+1));
    }
  }

  if (info->type == META) {
    Json::Value root;
    Json::Reader reader;

    bool rc = reader.parse(spacePos + 1, payload + len, root);
    if (!rc) return false;

    std::string event = root["event"].asString();
    if (event != "END") return false;

    info->size = root["size"].asUInt64();
    info->file = root["file"].asString();

    Json::Value &val = root["md5"];
    if (!val.isNull()) info->md5  = val.asString();
  } else if (info->type == NMSG) {
    info->ptr = spacePos + 1;
    if (nonl && payload[len-1] == '\n') {
      info->len = payload + len - info->ptr - 1;
    } else {
      info->len = payload + len - info->ptr;
    }
  } else {
    info->ptr = payload;
    if (nonl && payload[len-1] == '\n') {
      info->len = len - 1;
    } else {
      info->len = len;
    }
  }

  return true;
}

void MirrorTransform::addToCache(rd_kafka_message_t *rkm, const MessageInfo &info)
{
  FdCache &fdCache = fdCache_[info.host];

  if (fdCache.pos == info.pos) {
    log_error(0, "%s:%d duplicate %ld message %.*s", topic_, partition_,
              rkm->offset, (int) rkm->len, (char *) rkm->payload);
    rd_kafka_message_destroy(rkm);
  } else if (fdCache.pos > info.pos) {
    log_fatal(0, "%s:%d unorder %ld > %ld %ld message %.*s", topic_, partition_, fdCache.pos, info.pos,
              rkm->offset, (int) rkm->len, (char *) rkm->payload);
  }

  fdCache.pos = info.pos;
  if (!fdCache.rkms) fdCache.rkms = new rd_kafka_message_t*[IOV_MAX];

  struct iovec iov = { (void *) info.ptr, static_cast<size_t>(info.len) };
  fdCache.iovs.push_back(iov);
  fdCache.rkms[fdCache.rkmSize++] = rkm;
}

bool MirrorTransform::flushCache(bool eof, const std::string &host)
{
  bool flush = false;
  for (std::map<std::string, FdCache>::iterator ite = fdCache_.begin(); ite != fdCache_.end(); ++ite) {
    FdCache &fdCache = ite->second;
    if (!(fdCache.rkmSize == IOV_MAX || (eof && host == ite->first))) continue;

    flush = true;
    if (fdCache.fd < 0) {
      char path[1024];
      snprintf(path, 1024, "%s/%s/%s", wdir_, topic_, ite->first.c_str());
      int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
      if (fd == -1) {
        log_fatal(errno, "open %s error", path);
        exit(EXIT_FAILURE);
      }
      fdCache.fd = fd;
    }

    ssize_t wantn = 0;
    for (size_t i = 0; i < fdCache.iovs.size(); ++i) wantn += fdCache.iovs[i].iov_len;

    if (wantn > 0) {
      ssize_t n = writev(fdCache.fd, &(fdCache.iovs[0]), fdCache.iovs.size());
      if (n != wantn) {
        log_fatal(errno, "%s:%d %s writev error", topic_, partition_, ite->first.c_str());
        exit(EXIT_FAILURE);
      }
    }

    fdCache.clear();
  }

  return flush;
}

uint32_t MirrorTransform::write(rd_kafka_message_t *rkm, uint64_t *offsetPtr)
{
  uint64_t offset = rkm->offset;

  MessageInfo info;
  if (!MessageInfo::extract((char *) rkm->payload, rkm->len, &info, false) || info.type == MessageInfo::MSG) {
    log_error(0, "%s:%d unknow message %.*s", topic_, partition_, (int) rkm->len, (char *) rkm->payload);
    return IGNORE | RKMFREE;
  }

  if (info.type == MessageInfo::NMSG) {
    addToCache(rkm, info);
  } else {
    log_info(0, "%s:%d META %ld %.*s", topic_, partition_, rkm->offset, (int) rkm->len, (char *) rkm->payload);
  }

  bool eof = info.type == MessageInfo::META;
  uint32_t ide = flushCache(eof, info.host) ? LOCAL : IGNORE;

  if (eof) {
    fdCache_.erase(info.host);

    char opath[1024];
    snprintf(opath, 2048, "%s/%s/%s", wdir_, topic_, info.host.c_str());

    size_t slash = info.file.rfind('/');
    char npath[1024];
    snprintf(npath, 1024, "%s_%s", opath,
             (slash == std::string::npos) ? info.file.c_str() : info.file.substr(slash+1).c_str());

    if (rename(opath, npath) == -1) {
      log_fatal(errno, "%s:%d rename %s to %s error, exit", topic_, partition_, opath, npath);
      exit(EXIT_FAILURE);
    } else {
      log_info(0, "%s:%d rename %s to %s", topic_, partition_, opath, npath);
      if (notify_) notify_->exec(npath, info.file.c_str(), -1, info.size, info.md5.c_str());
    }
    ide = GLOBAL | RKMFREE;
  }

  if (ide != IGNORE) *offsetPtr = offset;
  return ide;
}

LuaTransform::~LuaTransform()
{
  if (helper_) delete helper_;

  if (currentIntervalFd_ > 0) {
    close(currentIntervalFd_);
    unlink(currentIntervalFile_.c_str());
  }

  if (lastIntervalFd_ > 0) {
    close(lastIntervalFd_);
    unlink(lastIntervalFile_.c_str());
  }
}

bool LuaTransform::init(Format inputFormat, Format outputFormat, int interval, int delay, const char *luaFile, char *errbuf)
{
  assert(inputFormat == NGINX || inputFormat == TSV);
  assert(outputFormat == JSON);

  inputFormat_ = inputFormat;

  if (interval > 3600 || interval < 60) {
    sprintf(errbuf, "use interval %d > 3600 or %d < 60 is meaningless", interval, interval);
    return 0;
  }

  if (delay > interval) {
    sprintf(errbuf, "use delay %d > interval %d is a bad idea", delay, interval);
    return 0;
  }

  interval_ = interval;
  delay_    = delay;

  std::vector<std::string> files;
  char dir[1024];
  sprintf(dir, "%s/%s", wdir_, topic_);
  if (!sys::readdir(dir, ".current", &files, errbuf)) return 0;
  if (!sys::readdir(dir, ".last", &files, errbuf)) return 0;
  if (!files.empty()) {
    sprintf(errbuf, "%s:%d found current/last file in %s", topic_, partition_, dir);
    return 0;
  }

  helper_ = new LuaHelper;
  if (!helper_->dofile(luaFile, errbuf)) return false;

  if (!helper_->getArray("informat", &fields_, true)) return false;

  std::string timestampName;
  if (!helper_->getString("timestamp_name", &timestampName, "time_local")) return false;

  std::vector<std::string>::iterator pos = std::find(fields_.begin(), fields_.end(), timestampName.c_str());
  if (pos == fields_.end()) {
    sprintf(errbuf, "timestamp %s notfound in %s informat", timestampName.c_str(), luaFile);
    return false;
  }
  timeLocalIndex_ = pos - fields_.begin();

  std::string timestampFormat;
  if (!helper_->getString("timestamp_format", &timestampFormat, "timelocal")) return false;
  timestampFormat_ = stringToTimeFormat(timestampFormat.c_str());
  if (timestampFormat_ == TIMEFORMAT_NIL) {
    sprintf(errbuf, "unknow timestamp format %s in %s informat", timestampFormat.c_str(), luaFile);
    return false;
  }

  if (inputFormat == NGINX) {
    pos = std::find(fields_.begin(), fields_.end(), "request");
    if (pos == fields_.end()) {
      sprintf(errbuf, "request notfound in %s informat", luaFile);
      return false;
    }
    requestIndex_ = pos - fields_.begin();
  } else {
    requestIndex_ = -1;
  }

  if (!helper_->getBool("delete_request_field", &deleteRequestField_, true)) return false;
  if (!helper_->getString("time_local_format", &timeLocalFormat_, "iso8601")) return false;

  if (!helper_->getTable("request_map", &requestNameMap_, false)) return false;
  if (!helper_->getTable("request_type", &requestTypeMap_, false)) return false;

  return true;
}

void LuaTransform::initCurrentFile(long intervalCnt, uint64_t offset)
{
  std::string timeSuffix = sys::timeFormat(intervalCnt * interval_, "%Y-%m-%d_%H-%M-%S");

  char path[1024];
  int n = snprintf(path, 1024, "%s/%s/%s.%d_%s.current", wdir_, topic_, topic_, partition_, timeSuffix.c_str());
  currentIntervalFd_ = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (currentIntervalFd_ == -1) {
    log_fatal(errno, "open %s error", path);
    exit(EXIT_FAILURE);
  }

  currentIntervalCnt_ = intervalCnt;
  currentIntervalFile_.assign(path, n);
  currentOffset_ = offset;
}

void LuaTransform::rotateCurrentToLast()
{
  size_t dot = currentIntervalFile_.rfind('.');
  if (dot == std::string::npos || access(currentIntervalFile_.c_str(), F_OK) != 0) {
    log_fatal(errno, "%s:%d current file %s not exists", topic_, partition_, currentIntervalFile_.c_str());
    exit(EXIT_FAILURE);
  }

  std::string path = currentIntervalFile_.substr(0, dot);
  lastIntervalFile_ = path + ".last";
  lastIntervalFd_   = currentIntervalFd_;
  lastIntervalCnt_  = currentIntervalCnt_;
  lastOffset_       = currentOffset_;

  if (rename(currentIntervalFile_.c_str(), lastIntervalFile_.c_str()) == 0) {
    log_info(0, "%s:%d rotate %s to %s", topic_, partition_,
             currentIntervalFile_.c_str(), lastIntervalFile_.c_str());
  } else {
    log_fatal(errno, "%s:%d rename %s to %s error", topic_, partition_,
              currentIntervalFile_.c_str(), lastIntervalFile_.c_str());
    exit(EXIT_FAILURE);
  }
}

void LuaTransform::rotateLastToFinish()
{
  assert(lastIntervalFd_ != -1);

  size_t dot = lastIntervalFile_.rfind('.');
  if (dot != std::string::npos && access(lastIntervalFile_.c_str(), F_OK) == 0) {
    std::string path = lastIntervalFile_.substr(0, dot);
    if (access(path.c_str(), F_OK) == 0) {
      log_fatal(0, "%s:%d finish file %s exists, exit", topic_, partition_, path.c_str());
      exit(EXIT_FAILURE);
    }

    if (rename(lastIntervalFile_.c_str(), path.c_str()) != 0) {
      log_fatal(errno, "%s:%d rename %s to %s error, exit", topic_, partition_,
                lastIntervalFile_.c_str(), path.c_str());
      exit(EXIT_FAILURE);
    } else {
      log_info(0, "%s:%d rotate %s to %s", topic_, partition_,
               lastIntervalFile_.c_str(), path.c_str());
      if (notify_) notify_->exec(path.c_str(), 0, lastIntervalCnt_ * interval_);
    }
  }

  close(lastIntervalFd_);
  lastIntervalFd_ = -1;
  lastIntervalCnt_ = -1;
}

uint32_t LuaTransform::timeout(uint64_t *offsetPtr) {
  uint32_t flags = IGNORE;
  if (lastIntervalTimeout()) flags = timeout_(offsetPtr);
  return flags;
}

// WARN: performance issue, don't merge timeout_ and lastIntervalTimeout to one function
uint32_t LuaTransform::timeout_(uint64_t *offsetPtr)
{
  rotateLastToFinish();

  assert(lastOffset_ != (uint64_t) -1);
  *offsetPtr = lastOffset_;
  lastOffset_ = -1;

  return GLOBAL;
}

uint32_t LuaTransform::rotate(long intervalCnt, uint64_t offset, uint64_t *offsetPtr)
{
  uint32_t flags = IGNORE;

  if (currentIntervalCnt_ == -1) {
    initCurrentFile(intervalCnt, offset);
  } else if (intervalCnt > currentIntervalCnt_) {
    if (lastIntervalTimeout()) flags = timeout_(offsetPtr);

    rotateCurrentToLast();
    initCurrentFile(intervalCnt, offset);
  } else if (intervalCnt == currentIntervalCnt_) {
    currentOffset_ = offset;
  } else if (intervalCnt == lastIntervalCnt_) {
    lastOffset_ = offset;
  }

  if (lastIntervalTimeout()) flags |= timeout_(offsetPtr);
  return flags;
}

Json::Value toJsonValue(const std::string &s, char t)
{
  if (t == 'i') return Json::Value(atoi(s.c_str()));
  else if (t == 'f') return Json::Value(atof(s.c_str()));
  else if (t == 'j') {
    Json::Value root;
    Json::Reader reader;
    if (reader.parse(s, root)) return root;
    else return Json::Value(Json::objectValue);
  }
  else return Json::Value(s);
}

bool LuaTransform::fieldsToJson(
  const std::vector<std::string> &fields, const std::string &method, const std::string &path,
  std::map<std::string, std::string> *query, std::string *json) const
{
  Json::Value root(Json::objectValue);
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields_[i] == "-" || (deleteRequestField_ && (int) i == requestIndex_) ||
        fields_[i][0] == '#') continue;

    std::map<std::string, std::string>::const_iterator pos = requestTypeMap_.find(fields_[i]);
    char type = (pos != requestTypeMap_.end() && !pos->second.empty()) ? pos->second[0] : 's';
    root[fields_[i]] = toJsonValue(fields[i], type);
  }

  std::string qskey;
  for (std::map<std::string, std::string>::const_iterator ite = requestNameMap_.begin();
       ite != requestNameMap_.end(); ++ite) {
    if (ite->second == "__uri__") root[ite->first] = path;
    else if (ite->second == "__method__") root[ite->first] = method;
    else if (ite->second == "__query__") qskey = ite->first;
    else {
      std::map<std::string, std::string>::iterator pos = query->find(ite->second);
      if (pos != query->end()) {
        std::map<std::string, std::string>::const_iterator pos2 = requestTypeMap_.find(ite->first);
        if (pos2 != requestTypeMap_.end() && !pos2->second.empty()) {
          root[ite->first] = toJsonValue(pos->second, pos2->second[0]);
        } else {
          root[ite->first] = pos->second;
        }
        query->erase(pos);
      } else if (!root.isMember(ite->first)) {
        root[ite->first] = Json::nullValue;
      }
    }
  }

  if (!qskey.empty()) {
    Json::Value q(Json::objectValue);
    for (std::map<std::string, std::string>::iterator ite = query->begin(); ite != query->end(); ++ite) {
      q[ite->first] = ite->second;
    }
    root[qskey] = q;
  }

  json->assign(Json::FastWriter().write(root));
  return true;
}

bool LuaTransform::parseFields(const char *ptr, size_t len, std::vector<std::string> *fields, time_t *timestamp)
{
  char delimiter = ' ';
  if (inputFormat_ == TSV) delimiter = '\t';

  split(ptr, len, fields, delimiter);

  if (fields->size() != fields_.size()) {
    log_error(0, "%s:%d invalid field size %.*s", topic_, partition_, static_cast<int>(len), ptr);
    return false;
  }

  if (timestampFormat_ == TIMELOCAL) {
    std::string isoTime;
    if (!timeLocalToIso8601(fields->at(timeLocalIndex_), &isoTime, timestamp)) {
      log_error(0, "%s:%d invalid timestamp %.*s", topic_, partition_, static_cast<int>(len), ptr);
      return false;
    }
    if (timeLocalFormat_ == "iso8601") (*fields)[timeLocalIndex_] = isoTime;
  } else if (timestampFormat_ == ISO8601) {
    if (!parseIso8601(fields->at(timeLocalIndex_), timestamp)) {
      log_error(0, "%s:%d invalid timestamp %.*s", topic_, partition_, static_cast<int>(len), ptr);
      return false;
    }
  } else {
    assert(0);
  }
  return true;
}

uint32_t LuaTransform::write(rd_kafka_message_t *rkm, uint64_t *offsetPtr)
{
  uint64_t offset = rkm->offset;

  MessageInfo info;
  if (!MessageInfo::extract((char *) rkm->payload, rkm->len, &info, true) || info.type == MessageInfo::MSG) {
    log_error(0, "%s:%d unknow message %.*s", topic_, partition_, (int) rkm->len, (char *) rkm->payload);
    return IGNORE | RKMFREE;
  }
  if (info.type == MessageInfo::META) {
    log_info(0, "%s:%d META %lu %.*s", topic_, partition_, offset, (int) rkm->len, (char *) rkm->payload);
    return IGNORE | RKMFREE;
  }

  std::vector<std::string> fields;
  time_t timestamp;
  if (!parseFields(info.ptr, info.len, &fields, &timestamp)) return IGNORE | RKMFREE;

  std::string method, path;
  std::map<std::string, std::string> query;
  if (requestIndex_ >= 0) {
    if (!parseRequest(fields[requestIndex_].c_str(), &method, &path, &query)) {
      log_error(0, "%s:%d invalid request %s", topic_, partition_, fields_[requestIndex_].c_str());
      return IGNORE | RKMFREE;
    }
  }

  updateTimestamp(timestamp);
  long intervalCnt = timestamp / interval_;
  uint32_t flags = rotate(intervalCnt, offset, offsetPtr);

  if (intervalCnt != currentIntervalCnt_ && intervalCnt != lastIntervalCnt_) {
    log_info(0, "%s:%d message delay %.*s at %ld", topic_, partition_, info.len, info.ptr, currentTimestamp_);
    return flags | RKMFREE;
  }

  std::string json;
  fieldsToJson(fields, method, path, &query, &json);

  int fd = (intervalCnt == currentIntervalCnt_) ? currentIntervalFd_ : lastIntervalFd_;
  if (::write(fd, json.c_str(), json.size()) == -1) {
    const std::string &file = (intervalCnt == currentIntervalCnt_) ? currentIntervalFile_ : lastIntervalFile_;
    log_fatal(errno, "%s:%d write %s error", topic_, partition_, file.c_str());
    exit(EXIT_FAILURE);
  }

  return flags | RKMFREE;
}
