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

Transform::Idempotent Transform::timeout(uint64_t * /*offsetPtr*/)
{
  return IGNORE;
}

enum FormatToken {INFORMAT, LUA, OUTFORMAT, INTERVAL, DELAY};
static FormatToken FORMAT_TOKENS[] = {INFORMAT, LUA, OUTFORMAT, INTERVAL, DELAY};
static size_t MAX_TOKEN_SIZE = sizeof(FORMAT_TOKENS)/sizeof(FORMAT_TOKENS[0]);

Transform::Format Transform::stringToFormat(const char *ptr, size_t len)
{
  if (strncmp(ptr, "nginx", len) == 0) return NGINX;
  else if (strncmp(ptr, "raw", len) == 0) return RAW;
  else if (strncmp(ptr, "orc", len) == 0) return ORC;
  else if (strncmp(ptr, "json", len) == 0) return JSON;
  else return NIL;
}

Transform *Transform::create(
  const char *wdir, const char *topic, int partition, const char *notify, const char *format, char *errbuf)
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

static void notify(const char *notifyCmd, const char *wdir, const char *topic, int partition, const char *file, time_t timestamp = -1)
{
  if (!notifyCmd) return;

  pid_t pid = fork();
  if (pid == 0) {
    char log[2048];
    snprintf(log, 2048, "%s/%s.%d.notify.log", wdir, topic, partition);
    int fd = open(log, O_CREAT | O_WRONLY | O_APPEND, 0666);
    if (fd != -1) {
      dup2(fd, STDOUT_FILENO);
      dup2(fd, STDERR_FILENO);
    }

    char topicPtr[128];
    snprintf(topicPtr, 128, "NOTIFY_TOPIC=%s", topic);

    char partitionPtr[128];
    snprintf(partitionPtr, 128, "NOTIFY_PARTITION=%d", partition);

    char filePtr[1024];
    snprintf(filePtr, 1024, "NOTIFY_FILE=%s", file);

    char timestampPtr[64];
    if (timestamp != (time_t) -1) {
      snprintf(timestampPtr, 64, "NOTIFY_TIMESTAMP=%ld", timestamp);
    } else {
      snprintf(timestampPtr, 64, "NOTIFY_TIMESTAMP=");
    }

    char * const argv[] = { (char *) notifyCmd, NULL };
    char * const envp[] = { topicPtr, partitionPtr, filePtr, timestampPtr, NULL };

    execve(notifyCmd, argv, envp);
    exit(0);
  }
}

bool MirrorTransform::addToCache(rd_kafka_message_t *rkm, std::string *host, std::string *file)
{
  bool eof = false;
  char flag = ((char *) rkm->payload) [0];
  if (flag == '*' || flag == '#') {
    char *sp = (char *) memchr(rkm->payload, ' ', rkm->len);
    assert(sp);

    long pos = -1;
    char *at = (char *) memchr(rkm->payload, '@', sp - (char *) rkm->payload -1);
    if (at) {
      host->assign((char *) rkm->payload + 1, at - (char *) rkm->payload -1);
      pos = util::toLong(at+1, sp - (at+1));
    } else {
      host->assign((char *) rkm->payload + 1, sp - (char *) rkm->payload -1);
    }

    *sp = '\0';
    host->assign((char *) rkm->payload + 1);
    *sp = ' ';

    if (flag == '*') {
      assert(pos != -1);
      FdCache &fdCache = fdCache_[*host];

      if (fdCache.pos != -1) {
        if (fdCache.pos == pos) {
          log_error(0, "%s:%d duplicate %ld message %.*s", topic_, partition_,
                    rkm->offset, (int) rkm->len, (char *) rkm->payload);
          rd_kafka_message_destroy(rkm);
          return eof;
        } else if (fdCache.pos > pos) {
          log_fatal(0, "%s:%d unorder %ld > %ld %ld message %.*s", topic_, partition_, fdCache.pos, pos,
                    rkm->offset, (int) rkm->len, (char *) rkm->payload);
        }
      }

      fdCache.pos = pos;
      if (!fdCache.rkms) fdCache.rkms = new rd_kafka_message_t*[IOV_MAX];

      struct iovec iov = { sp+1, rkm->len - (sp+1 - (char *) rkm->payload) };
      fdCache.iovs.push_back(iov);
      fdCache.rkms[fdCache.rkmSize++] = rkm;
    } else {
      log_info(0, "%s:%d META %ld %.*s", topic_, partition_, rkm->offset, (int) rkm->len, (char *) rkm->payload);

      std::string s((char *)rkm->payload, rkm->len);
      if (s.find("End") != std::string::npos) {
        at = (char *) memchr(rkm->payload, '@', rkm->len);
        if (at) {
          eof = true;
          at++;
          const char *end = (char *) rkm->payload + rkm->len;
          while (*at != ' ' && at < end) file->append(1, *at++);
        }
      }
      rd_kafka_message_destroy(rkm);
    }
  }
  return eof;
}

bool MirrorTransform::flushCache(bool force, bool eof, const std::string &host, std::string *ofile)
{
  bool flush = false;
  for (std::map<std::string, FdCache>::iterator ite = fdCache_.begin(); ite != fdCache_.end(); ++ite) {
    FdCache &fdCache = ite->second;
    if (!(force || fdCache.rkmSize == IOV_MAX || (eof && host == ite->first))) continue;

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
      fdCache.file = path;
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

    if (ite->first == host && ofile) ofile->assign(ite->second.file);
    fdCache.clear();
  }

  return flush;
}

Transform::Idempotent MirrorTransform::write(rd_kafka_message_t *rkm, uint64_t *offsetPtr)
{
  uint64_t offset = rkm->offset;
  std::string host, ofile, nfile;

  bool eof = addToCache(rkm, &host, &nfile);
  Idempotent ide = flushCache(false, eof, host, &ofile) ? LOCAL : IGNORE;

  if (eof) {
    assert(ide != IGNORE);

    fdCache_.erase(host);

    size_t slash = nfile.rfind('/');
    if (slash == std::string::npos) nfile = ofile + "_" + nfile;
    else nfile = ofile + "_" + nfile.substr(slash+1);

    if (rename(ofile.c_str(), nfile.c_str()) == -1) {
      log_fatal(errno, "%s:%d rename %s to %s error", topic_, partition_, ofile.c_str(), nfile.c_str());
      exit(EXIT_FAILURE);
    } else {
      log_info(0, "%s:%d rename %s to %s", topic_, partition_, ofile.c_str(), nfile.c_str());
      notify(notify_, wdir_, topic_, partition_, nfile.c_str());
    }
    ide = GLOBAL;
  }

  if (ide != IGNORE) *offsetPtr = offset;
  return ide;
}

LuaTransform::~LuaTransform() {
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
  assert(inputFormat == NGINX);
  assert(outputFormat == JSON);

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

  std::vector<std::string>::iterator pos = std::find(fields_.begin(), fields_.end(), "time_local");
  if (pos == fields_.end()) {
    sprintf(errbuf, "time_local notfound in %s informat", luaFile);
    return false;
  }
  timeLocalIndex_ = pos - fields_.begin();

  pos = std::find(fields_.begin(), fields_.end(), "request");
  if (pos == fields_.end()) {
    sprintf(errbuf, "request notfound in %s informat", luaFile);
    return false;
  }
  requestIndex_ = pos - fields_.begin();

  if (!helper_->getBool("delete_request_field", &deleteRequestField_, true)) return false;
  if (!helper_->getString("time_local_format", &timeLocalFormat_, "iso8601")) return false;

  if (!helper_->getTable("request_map", &requestNameMap_, false)) return false;
  if (!helper_->getTable("request_type", &requestTypeMap_, false)) return false;

  return true;
}

bool LuaTransform::selectCurrentFile(int intervalCnt, int **fdPtr, std::string **filePtr)
{
  if (intervalCnt > currentIntervalCnt_) {
    lastIntervalCnt_ = currentIntervalCnt_;
    currentIntervalCnt_ = intervalCnt;

    if (lastIntervalFd_ > 0) close(lastIntervalFd_);
    lastIntervalFd_= currentIntervalFd_;
    currentIntervalFd_ = -1;

    *fdPtr   = &currentIntervalFd_;
    *filePtr = &currentIntervalFile_;
  } else if (intervalCnt == currentIntervalCnt_) {
    *fdPtr   = &currentIntervalFd_;
    *filePtr = &currentIntervalFile_;
  } else if (intervalCnt == lastIntervalCnt_) {
    if (lastIntervalFd_ == -1) return false;

    *fdPtr   = &lastIntervalFd_;
    *filePtr = &lastIntervalFile_;
  } else {
    return false;
  }
  return true;
}

void LuaTransform::openCurrent(int intervalCnt, int *fdPtr, std::string *filePtr)
{
  assert(intervalCnt == currentIntervalCnt_);
  std::string timeSuffix = sys::timeFormat(intervalCnt * interval_, "%Y-%m-%d_%H-%M-%S");

  char path[1024];
  int n = snprintf(path, 1024, "%s/%s/%s.%d_%s.current", wdir_, topic_, topic_, partition_, timeSuffix.c_str());
  *fdPtr = ::open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (*fdPtr == -1) {
    log_fatal(errno, "open %s error", path);
    exit(EXIT_FAILURE);
  }
  filePtr->assign(path, n);
}

void LuaTransform::rotateCurrentToLast()
{
  size_t dot = currentIntervalFile_.rfind('.');
  if (dot != std::string::npos && access(currentIntervalFile_.c_str(), F_OK) == 0) {
    std::string path = currentIntervalFile_.substr(0, dot);
    lastIntervalFile_ = path + ".last";
    if (rename(currentIntervalFile_.c_str(), lastIntervalFile_.c_str()) == 0) {
      log_info(0, "%s:%d rotate %s to %s", topic_, partition_,
               currentIntervalFile_.c_str(), lastIntervalFile_.c_str());
      lastIntervalFd_ = open(lastIntervalFile_.c_str(), O_WRONLY | O_APPEND, 0644);
      if (lastIntervalFd_ == -1) {
        log_fatal(errno, "%s:%d open %s error", topic_, partition_, lastIntervalFile_.c_str());
        exit(EXIT_FAILURE);
      }
    } else {
      log_fatal(errno, "%s:%d rename %s to %s error", topic_, partition_,
                currentIntervalFile_.c_str(), lastIntervalFile_.c_str());
      exit(EXIT_FAILURE);
    }
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
      notify(notify_, wdir_, topic_, partition_, path.c_str(), lastIntervalCnt_ * interval_);
    }
  }
  if (lastIntervalFd_ > 0) close(lastIntervalFd_);
  lastIntervalFd_ = -1;
}

LuaTransform::Idempotent LuaTransform::timeout(uint64_t *offsetPtr) {
  Idempotent ide = IGNORE;
  if (lastIntervalTimeout()) ide = timeout_(offsetPtr);
  return ide;
}

// WARN: performance issue, don't merge timeout_ and lastIntervalTimeout to one function
Transform::Idempotent LuaTransform::timeout_(uint64_t *offsetPtr)
{
  rotateLastToFinish();

  assert(lastOffset_ != (uint64_t) -1);
  *offsetPtr = lastOffset_;
  lastOffset_ = -1;

  return GLOBAL;
}

Json::Value toJsonValue(const std::string &s, char t)
{
  if (t == 'i') return Json::Value(atoi(s.c_str()));
  else if (t == 'f') return Json::Value(atof(s.c_str()));
  else return Json::Value(s);
}

Transform::Idempotent LuaTransform::write(rd_kafka_message_t *rkm, uint64_t *offsetPtr)
{
  uint64_t offset = rkm->offset;

  const char *ptr = (char *) rkm->payload;
  size_t len = rkm->len;

  if (*ptr == '#') {
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }

  if (*ptr == '*') {
    const char *sp = (char *) memchr(ptr, ' ', len);
    if (sp) {
      len -= sp + 1 - ptr;
      ptr = sp + 1;
    }
  }

  if (ptr[len-1] == '\n') --len;

  std::vector<std::string> fields;
  split(ptr, len, &fields);

  if (fields.size() != fields_.size()) {
    log_error(0, "%s:%d invalid field size %.*s", topic_, partition_, len, ptr);
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }

  time_t      timestamp;
  std::string isoTime;
  if (!iso8601(fields[timeLocalIndex_], &isoTime, &timestamp)) {
    log_error(0, "%s:%d invalid time_local %.*s", topic_, partition_, len, ptr);
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }
  if (timeLocalFormat_ == "iso8601") fields[timeLocalIndex_] = isoTime;

  std::string method, path;
  std::map<std::string, std::string> query;
  if (!parseRequest(fields[requestIndex_].c_str(), &method, &path, &query)) {
    log_error(0, "%s:%d invalid request %s", topic_, partition_, fields_[requestIndex_].c_str());
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }

  if (currentTimestamp_ == -1 || timestamp > currentTimestamp_) currentTimestamp_ = timestamp;

  if (timestamp + delay_ < currentTimestamp_) {
    log_info(0, "%s:%d message delay %.*s", topic_, partition_, len, ptr);
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }

  int intervalCnt = timestamp / interval_;
  if (currentIntervalCnt_ > 0 && intervalCnt > currentIntervalCnt_) {
    rotateCurrentToLast();
    lastOffset_ = currentOffset_;
  }

  int *fd;
  std::string *file;

  if (!selectCurrentFile(intervalCnt, &fd, &file)) {
    log_info(0, "%s:%d message delay %.*s", topic_, partition_, len, ptr);
    rd_kafka_message_destroy(rkm);
    return IGNORE;
  }

  if (intervalCnt == currentIntervalCnt_) currentOffset_ = offset;
  else lastOffset_ = offset;

  if (*fd == -1) openCurrent(intervalCnt, fd, file);

  Json::Value root(Json::objectValue);
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields_[i] == "-" || (deleteRequestField_ && i == requestIndex_) ||
        fields_[i][0] == '#') continue;

    std::map<std::string, std::string>::iterator pos = requestTypeMap_.find(fields_[i]);
    char type = (pos != requestTypeMap_.end() && !pos->second.empty()) ? pos->second[0] : 's';
    root[fields_[i]] = toJsonValue(fields[i], type);
  }

  std::string qskey;
  for (std::map<std::string, std::string>::iterator ite = requestNameMap_.begin();
       ite != requestNameMap_.end(); ++ite) {
    if (ite->second == "__uri__") root[ite->first] = path;
    else if (ite->second == "__method__") root[ite->first] = path;
    else if (ite->second == "__query__") qskey = ite->first;
    else {
      std::map<std::string, std::string>::iterator pos = query.find(ite->second);
      if (pos != query.end()) {
        std::map<std::string, std::string>::iterator pos2 = requestTypeMap_.find(ite->first);
        if (pos2 != requestTypeMap_.end() && !pos2->second.empty()) {
          root[ite->first] = toJsonValue(pos->second, pos2->second[0]);
        } else {
          root[ite->first] = pos->second;
        }
        query.erase(pos);
      } else {
        root[ite->first] = Json::nullValue;
      }
    }
  }

  if (!qskey.empty()) {
    Json::Value q(Json::objectValue);
    for (std::map<std::string, std::string>::iterator ite = query.begin(); ite != query.end(); ++ite) {
      q[ite->first] = ite->second;
    }
    root[qskey] = q;
  }

  std::string json = Json::FastWriter().write(root);
  if (::write(*fd, json.c_str(), json.size()) == -1) {
    log_fatal(errno, "%s:%d write %s error", topic_, partition_, file->c_str());
    exit(EXIT_FAILURE);
  }

  Idempotent ide = IGNORE;
  if (lastIntervalTimeout()) ide = timeout_(offsetPtr);

  rd_kafka_message_destroy(rkm);
  return ide;
}
