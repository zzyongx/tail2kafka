#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "util.h"
#include "gnuatomic.h"
#include "bitshelper.h"
#include "logger.h"
#include "sys.h"
#include "metrics.h"
#include "luactx.h"
#include "filereader.h"

#define NL                  '\n'
#define MAX_LINE_LEN        1024 * 1024         // 1M
#define MAX_TAIL_SIZE       100 * MAX_LINE_LEN  // 100M
#define SEND_QUEUE_SIZE     2000
#define KAFKA_ERROR_TIMEOUT 60

FileReader::StartPosition FileReader::stringToStartPosition(const char *s)
{
  if (strcasecmp(s, "log_start") == 0) return LOG_START;
  else if (strcasecmp(s, "start") == 0) return START;
  else if (strcasecmp(s, "log_end") == 0) return LOG_END;
  else if (strcasecmp(s, "end") == 0) return END;
  else return NIL;
}

FileReader::FileReader(LuaCtx *ctx)
{
  fd_     = -1;
  ctx_    = ctx;
  buffer_ = new char[MAX_LINE_LEN];
  npos_   = 0;
  flags_  = 0;

  size_ = dsize_ = 0;
  line_ = dline_ = 0;

  qsize_ = 0;
  lastQueueFullTime_ = ctx_->cnf()->fasttime(TIMEUNIT_MILLI);

  fileRotateTime_ = ctx->cnf()->fasttime();
  holdFd_ = -1;
}

FileReader::~FileReader()
{
  delete[] buffer_;
  if (fd_ > 0) close(fd_);
  if (holdFd_ > 0) close(holdFd_);
}

// try best to watch the file
bool FileReader::tryOpen(char *errbuf)
{
  const std::string &file = ctx_->datafile();

  for (int i = 0; i < 15; ++i) {
    fd_ = open(file.c_str(), O_RDONLY);
    if (fd_ == -1) sleep(1);
    else break;
  }
  if (fd_ == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "%s open error: %s", file.c_str(), strerror(errno));
    return false;
  } else {
    return true;
  }
}

bool FileReader::init(char *errbuf)
{
  if (!tryOpen(errbuf)) return false;

  struct stat st;
  fstat(fd_, &st);
  inode_ = st.st_ino;

  log_info(0, "open file %s fd %d inode %ld", ctx_->datafile().c_str(), fd_, inode_);

  bits_set(flags_, FILE_WATCHED);
  if (ctx_->datafile() != ctx_->file()) bits_set(flags_, FILE_HISTORY);

  MD5_Init(&md5Ctx_);
  md5_.clear();
  return setStartPosition(st.st_size, errbuf);
}

bool FileReader::checkRewatch()
{
  // rewatch only existing file
  int tmpFd = -1;
  bool rewatch = access(ctx_->file().c_str(), F_OK) == 0;
  if (!rewatch && ctx_->autocreat()) {
    tmpFd = open(ctx_->file().c_str(), O_CREAT | O_RDONLY, 0644);
    if (tmpFd != -1) {
      rewatch = true;
    } else {
      log_fatal(errno, "create file %s error", ctx_->file().c_str());
    }
  }

  // if datafile is not the file, use holdFd to track file name changes
  if (rewatch && bits_test(flags_, FILE_HISTORY)) {
    if (holdFd_ > 0) {
      close(holdFd_);
      log_info(0, "close holdFd %d", holdFd_);
    }

    if (tmpFd != -1) {
      holdFd_ = tmpFd;
      tmpFd = -1;
    } else {
      holdFd_ = open(ctx_->file().c_str(), O_RDONLY, 0644);
    }
    if (holdFd_ == -1) {
      log_fatal(errno, "open holdFd %s error", ctx_->file().c_str());
      rewatch = false;
    } else {
      log_info(0, "open file %s fd %d as holdFd", ctx_->file().c_str(), holdFd_);
    }
  }

  if (tmpFd != -1) close(tmpFd);
  if (rewatch) bits_set(flags_, FILE_WATCHED);
  return rewatch;
}

bool FileReader::reinit()
{
  bool reopen = false;
  bool rewatch = false;
  if (bits_test(flags_, FILE_OPENONLY)) {   // datafile change
    reopen = true;
  } else if (!bits_test(flags_, FILE_WATCHED)) {
    if (fd_ == -1) reopen = true;         // fd > 0, use history fd
    rewatch = true;
  }

  bool tryNext = true;
  while (reopen && tryNext) {
    tryNext = false;
    const std::string &file = ctx_->datafile();
    log_info(0, "reopen %s", file.c_str());

    bool doOpen = false;
    if (!bits_test(flags_, FILE_HISTORY) && holdFd_ > 0) {
      fd_ = holdFd_;
      holdFd_ = -1;
    } else {
      fd_ = open(file.c_str(), (ctx_->autocreat() ? O_CREAT : 0) | O_RDONLY, 0644);
      doOpen = true;
    }

    if (fd_ != -1) {
      struct stat st;
      fstat(fd_, &st);
      size_ = 0;
      line_ = 0;
      npos_ = 0;
      inode_ = st.st_ino;
      bits_clear(flags_, FILE_OPENONLY);

      MD5_Init(&md5Ctx_);
      md5_.clear();

      if (doOpen) log_info(0, "open file %s fd %d inode %ld", file.c_str(), fd_, inode_);
      else log_info(0, "%d %s use holdFd instead of reopen inode %ld", fd_, ctx_->datafile().c_str(), inode_);

      tail2kafka(START, &st, buildFileStartRecord(time(0)));
    } else if (bits_test(flags_, FILE_HISTORY)) {
      if (ctx_->removeHistoryFile()) bits_clear(flags_, FILE_HISTORY);
      log_fatal(errno, "history file %s reinit error, try next %s", file.c_str(), ctx_->datafile().c_str());
      tryNext = true;
    } else {
      log_error(errno, "%s reinit error", file.c_str());
      rewatch = false;
    }
  }

  // as a side effect, poll history file
  if (!reopen && bits_test(flags_, FILE_HISTORY)) {
    tail2kafka();
  }

  if (rewatch) rewatch = checkRewatch();
  return rewatch;
}

bool FileReader::setStartPosition(off_t fileSize, char *errbuf)
{
  StartPosition startPosition = stringToStartPosition(ctx_->getStartPosition());
  if (startPosition == FileReader::LOG_START) {
    size_ = ctx_->cnf()->getFileOff()->getOff(inode_);
    if (size_ == (off_t) -1 || size_ > fileSize) {
      size_ = 0;
      log_error(0, "%s fileoff notfound, set to start", ctx_->file().c_str());
    }
  } else if (startPosition == FileReader::START) {
    size_ = 0;
  } else if (startPosition == FileReader::LOG_END) {
    size_ = ctx_->cnf()->getFileOff()->getOff(inode_);
    if (size_ == (off_t) -1 || size_ > fileSize) {
      log_error(0, "%s fileoff notfound, set to end", ctx_->file().c_str());
      return setStartPositionEnd(fileSize, errbuf);
    }
  } else if (startPosition == FileReader::END) {
    return setStartPositionEnd(fileSize, errbuf);
  }

  lseek(fd_, size_, SEEK_SET);
  return true;
}

void FileReader::initFileOffRecord(FileOffRecord * fileOffRecord)
{
  fileOffRecord_ = fileOffRecord;
  fileOffRecord_->inode = inode_;
  fileOffRecord_->off   = size_;
}

// FileOffRecord should be called in only one thread, but it must not call thread unsafe function
void FileReader::updateFileOffRecord(const FileRecord *record)
{
  util::atomic_dec(&qsize_);

  if (record->inode != fileOffRecord_->inode) {
    // rename file does not change inode
    log_info(0, "%d %s change inode from %ld/%ld to %ld/%ld", fd_, ctx_->topic().c_str(),
      (long) fileOffRecord_->inode, (long) fileOffRecord_->off,
      (long) record->inode, (long) record->off);

    fileOffRecord_->inode = record->inode;
    fileOffRecord_->off   = record->off;

    dline_ = 1;
    dsize_ = record->data->size();
  } else if (record->off > fileOffRecord_->off) {
    fileOffRecord_->off   = record->off;

    util::atomic_inc(&dline_);
    util::atomic_inc(&dsize_, record->data->size() - ctx_->function()->extraSize());
  } else {
    log_fatal(0, "%d %s off change smaller, from %ld/%ld to %ld/%ld", fd_, ctx_->topic().c_str(),
              (long) fileOffRecord_->inode, (long) fileOffRecord_->off,
              (long) record->inode, (long) record->off);
  }
}

static std::string getFileNameFromFd(int fd)
{
  char buffer[64];
  snprintf(buffer, 64, "/proc/self/fd/%d", fd);

  char path[1024];
  ssize_t n;
  if ((n = readlink(buffer, path, 1024)) == -1) {
    log_error(errno, "readlink error");
    return "";
  } else {
    return std::string(path, n);
  }
}

static struct FileInotifyStatusWithDesc {
  uint32_t    flags;
  const char *desc;
} fileInotifyStatusWithDesc[] = {
  { FILE_MOVED,     "FILE_MOVED" },
  { FILE_CREATED,   "FILE_CREATED" },
  { FILE_ICHANGE,   "FILE_ICHANGE" },
  { FILE_TRUNCATED, "FILE_TRUNCATED" },
  { FILE_DELETED,   "FILE_DELETED" },

  { FILE_LOGGED,    "FILE_LOGGED" },
  { FILE_WATCHED,   "FILE_WATCHED" },
  { FILE_OPENONLY,  "FILE_OPENONLY" },
  { FILE_HISTORY,   "FILE_HISTORY" },
  { 0, 0 }
};

inline std::string flagsToString(uint32_t flags)
{
  std::string s;
  for (int i = 0; fileInotifyStatusWithDesc[i].flags != 0; ++i) {
    if (bits_test(flags, fileInotifyStatusWithDesc[i].flags)) {
      if (!s.empty()) s.append(1, ',');
      s.append(fileInotifyStatusWithDesc[i].desc);
    }
  }
  return s;
}

void FileReader::tagRotate(int action, const char *fptr)
{
  bits_set(flags_, action);

  std::string newFileName;
  if (!fptr && bits_test(flags_, FILE_MOVED)) {
    newFileName = getFileNameFromFd(holdFd_ > 0 ? holdFd_ : fd_);
    fptr = newFileName.c_str();
  }

  log_info(0, "%d %s rotate %s to %s", fd_, ctx_->file().c_str(), flagsToString(flags_).c_str(), fptr ? fptr : "");
  util::Metrics::pingback("TAG_ROTATE", "new=%s&old=%s", ctx_->file().c_str(), fptr);

  if (ctx_->cnf()->fasttime() - fileRotateTime_ < KAFKA_ERROR_TIMEOUT ||
      (ctx_->getRotateDelay() > 0 && ctx_->cnf()->fasttime() - fileRotateTime_ < ctx_->getRotateDelay())) {
    log_fatal(0, "%d %s rotate %s too frequent, may lose data", fd_, ctx_->file().c_str(),
              flagsToString(flags_).c_str());
  }
}

void FileReader::checkHistoryRotate(const struct stat *stPtr)
{
  if (bits_test(flags_, FILE_HISTORY) && stPtr->st_size == size_) {
    std::string oldFile = ctx_->datafile();

    if (tail2kafka(END, stPtr, buildFileEndRecord(time(0), size_, oldFile.c_str()))) {
      log_info(0, "%d %s size=%lu sendsize=%lu lines=%lu sendlines=%lu md5=%s historyrotate %s", fd_, oldFile.c_str(),
               size_, dsize_, line_, dline_, md5_.c_str(), oldFile.c_str());
      util::Metrics::pingback("ROTATE", "file=%s&size=%lu&md5=%s", oldFile.c_str(), size_, md5_.c_str());

      bits_set(flags_, FILE_OPENONLY);
      if (ctx_->removeHistoryFile()) bits_clear(flags_, FILE_HISTORY);

      log_info(0, "history file %s finished, close fd %d try next %s", oldFile.c_str(), fd_, ctx_->datafile().c_str());
      close(fd_);
      fd_ = -1;
    }
  }
}

// when mv x to x.old, the process may still write to x.old untill reopen x
// we wait use roateDelay to reduce data lose
bool FileReader::waitRotate()
{
  bool rc = false;
  if (bits_test(flags_, FILE_MOVED) || bits_test(flags_, FILE_CREATED)) {
    if (ctx_->getRotateDelay() > 0 && bits_test(flags_, FILE_LOGGED)) {  // if config rotate delay
      if (ctx_->cnf()->fasttime() - fileRotateTime_ >= ctx_->getRotateDelay()) rc = true;  // rotate delay timeout
    } else if (bits_test(flags_, FILE_MOVED) && access(ctx_->file().c_str(), F_OK) == 0) {  // if file reopened
      rc = true;
    }

    if (!rc && !bits_test(flags_, FILE_LOGGED)) {
      log_info(0, "inotify %s %s, wait rotatedelay timeout or wait reopen", ctx_->file().c_str(),
               flagsToString(flags_).c_str());
    }
  }

  if (!bits_test(flags_, FILE_LOGGED)) fileRotateTime_ = ctx_->cnf()->fasttime();
  bits_set(flags_, FILE_LOGGED);
  return rc;
}


bool FileReader::remove()
{
  struct stat st;
  if (fstat(fd_, &st) != 0) {
    log_error(0, "%d %s fstat error", fd_, ctx_->file().c_str());
    return false;
  }

  // make reinit REOPEN  #flow HISTORY_ROTATE
  checkHistoryRotate(&st);

  if (st.st_nlink == 0) bits_set(flags_, FILE_DELETED);
  else if (st.st_size < size_) bits_set(flags_, FILE_TRUNCATED);
  else if (st.st_ino != inode_) bits_set(flags_, FILE_ICHANGE);

  std::string timeFormatFile;
  if (ctx_->getTimeFormatFile(&timeFormatFile) && access(timeFormatFile.c_str(), F_OK) == 0) {
    tagRotate(FILE_CREATED, timeFormatFile.c_str());
  }

  bool rc = bits_test(flags_, FILE_MOVED) || bits_test(flags_, FILE_CREATED) ||
    bits_test(flags_, FILE_DELETED) || bits_test(flags_, FILE_TRUNCATED) || bits_test(flags_, FILE_ICHANGE);

  if (rc) rc = waitRotate();

  if (rc) {
    std::string rotateFileName;
    if (bits_test(flags_, FILE_MOVED)) rotateFileName = getFileNameFromFd(holdFd_ > 0 ? holdFd_ : fd_);
    else if (bits_test(flags_, FILE_CREATED)) rotateFileName = ctx_->file();

    std::string oldFileName = rotateFileName;
    if (oldFileName.empty()) oldFileName = ctx_->file() + "." + sys::timeFormat(time(0), "%Y-%m-%d_%H:%M:%S");

    rc = tail2kafka(END, &st, buildFileEndRecord(time(0), size_, oldFileName.c_str()));

    bool closeFd = true;
    if (bits_test(flags_, FILE_HISTORY)) rc = false;  // FILE_HISTORY exec flow #HISTORY_ROTATE
    if (!rc) {
      if (bits_test(flags_, FILE_MOVED) || bits_test(flags_, FILE_CREATED)) {
        closeFd = false;

        if (ctx_->cnf()->fasttime() - fileRotateTime_ > KAFKA_ERROR_TIMEOUT &&
            ctx_->addHistoryFile(rotateFileName)) {
          log_fatal(0, "kafka queue full duration %d, kafka may unavaliable, %s turn on history",
                  (int) (ctx_->cnf()->fasttime() - fileRotateTime_), ctx_->topic().c_str());
          util::Metrics::pingback("KAFKA_ERROR", "topic=%s", ctx_->topic().c_str());
          rc = true;
        }
      } else {
        log_info(0, "%d %s treat tail2kafka fail as success when file status is %s", fd_, ctx_->file().c_str(),
                 flagsToString(flags_).c_str());
        rc = true;
      }
    }

    if (rc) {
      if (closeFd) {
        const char *oldFile = rotateFileName.empty() ? "NIL" : rotateFileName.c_str();
        log_info(0, "%d %s size=%lu sendsize=%lu lines=%lu sendlines=%lu md5=%s %s %s, close fd %d", fd_, ctx_->file().c_str(),
                 size_, dsize_, line_, dline_, md5_.c_str(), flagsToString(flags_).c_str(), oldFile, fd_);

        util::Metrics::pingback("ROTATE", "file=%s&size=%lu&md5=%s", oldFile, size_, md5_.c_str());

        // TODO add inode file
        close(fd_);
        fd_ = -1;
        flags_ = 0;
      } else {
        flags_ = FILE_HISTORY;
      }
      ctx_->setTimeFormatFile(timeFormatFile);
    }
  }

  return rc;
}

bool FileReader::setStartPositionEnd(off_t fileSize, char *errbuf)
{
  if (fileSize == 0) {
    size_ = 0;
    return true;
  }

  off_t min = std::min(fileSize, (off_t) MAX_LINE_LEN);
  lseek(fd_, fileSize - min, SEEK_SET);

  if (read(fd_, buffer_, MAX_LINE_LEN) != min) {
    snprintf(errbuf, MAX_ERR_LEN, "read %s less min %s", ctx_->file().c_str(), errno == 0 ? "" : strerror(errno));
    return false;
  }

  char *pos = (char *) memrchr(buffer_, NL, min);
  if (!pos) {
    snprintf(errbuf, MAX_ERR_LEN, "%s line length bigger than %ld", ctx_->file().c_str(), (long) min);
    return false;
  }

  size_ = fileSize - (min - (pos+1 - buffer_));
  return true;
}

bool FileReader::tail2kafka(StartPosition pos, const struct stat *stPtr, std::string *rawData)
{
  std::auto_ptr<std::string> rawDataPtr(rawData);

  if (util::atomic_get(&qsize_) > SEND_QUEUE_SIZE) {
    int queueFullTimeDuration = ctx_->cnf()->fasttime(TIMEUNIT_MILLI) - lastQueueFullTime_;
    if (queueFullTimeDuration > 1500) {    // suppress queue exceed log
      log_info(0, "%d %s queue exceed %d duration %d", fd_, ctx_->datafile().c_str(),
               SEND_QUEUE_SIZE, queueFullTimeDuration);
      lastQueueFullTime_ = ctx_->cnf()->fasttime(TIMEUNIT_MILLI);
    }
    return false;
  }

  struct stat stat;
  if (stPtr == 0) {
    if (fstat(fd_, &stat) != 0) {
      log_fatal(errno, "%d %s fstat error", fd_, ctx_->file().c_str());
      return false;
    }
    stPtr = &stat;
  }

  off_t off = lseek(fd_, 0, SEEK_CUR);  // get last read seek
  if (off == (off_t) -1) {
    log_fatal(errno, "%d %s lseek error", fd_, ctx_->file().c_str());
    return false;
  }

  if (off > stPtr->st_size) {
    bits_set(flags_, FILE_TRUNCATED);
    return true;
  }

  if (pos == START) propagateRawData(rawDataPtr.release());
  else if (size_ == 0) propagateRawData(buildFileStartRecord(time(0)));

  if (pos == NIL) {   // limit tailsize
    size_ = stPtr->st_size - off > MAX_TAIL_SIZE ? off + MAX_TAIL_SIZE : stPtr->st_size;
  } else {
    size_ = stPtr->st_size;
  }

  off_t loff = off - npos_;
  assert(loff >= 0);

  while (off < size_) {
    size_t min = std::min(size_ - off, (off_t) (MAX_LINE_LEN - npos_));
    assert(min > 0);
    ssize_t nn = read(fd_, buffer_ + npos_, min);
    if (nn == -1) {
      log_fatal(errno, "%d %s read error", fd_, ctx_->file().c_str());
      return false;
    } else if (nn == 0) { // file was truncated
      bits_set(flags_, FILE_TRUNCATED);
      break;
    }
    off += nn;

    propagateTailContent(nn);
    propagateProcessLines(inode_, &loff);
  }

  if (pos == END) propagateRawData(rawDataPtr.release());
  return true;
}

void FileReader::propagateTailContent(size_t size)
{
  LuaCtx *ctx = ctx_->next();
  while (ctx) {
    FileReader *f = ctx->getFileReader();
    memcpy(f->buffer_ + f->npos_, buffer_ + npos_, size);
    f->npos_ += size;

    ctx = ctx->next();
  }

  npos_ += size;
}

void FileReader::propagateProcessLines(ino_t inode, off_t *off)
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    ctx->getFileReader()->processLines(inode, off);
    ctx = ctx->next();
    off = 0;   // only first topic have off
  }
}

std::string *FileReader::buildFileStartRecord(time_t now)
{
  std::string dt = sys::timeFormat(now, "%Y-%m-%dT%H:%M:%S");

  char buffer[8192];
  int n = snprintf(buffer, 8192, "#%s {\"time\":\"%s\", \"event\":\"START\"}",
                   ctx_->cnf()->host().c_str(), dt.c_str());
  return new std::string(buffer, n);
}

std::string *FileReader::buildFileEndRecord(time_t now, off_t size, const char *oldFileName)
{
  std::string dt = sys::timeFormat(now, "%Y-%m-%dT%H:%M:%S");

  if (ctx_->md5sum() && md5_.empty()) {
    char md5[33] = "";
    unsigned char digest[16];
    MD5_Final(digest, &md5Ctx_);   // MD5_final can only be called once
    util::binToHex(digest, 16, md5);
    md5_.assign(md5, 32);
  }

  char buffer[8192];
  int n = snprintf(buffer, 8192, "#%s {\"time\":\"%s\", \"event\":\"END\", \"file\":\"%s\", "
                   "\"size\":%lu, \"sendsize\":%lu, \"lines\":%lu, \"sendlines\":%lu, \"md5\":\"%s\"}",
                   ctx_->cnf()->host().c_str(), dt.c_str(), oldFileName,
                   size, dsize_, line_, dline_, md5_.c_str());
  return new std::string(buffer, n);
}

void FileReader::propagateRawData(const std::string *data)
{
  assert(data != 0);

  LuaCtx *ctx = ctx_;
  while (ctx) {
    if (!ctx->withhost()) continue;

    std::vector<FileRecord *> *records = new std::vector<FileRecord *>(1, FileRecord::create(-1, -1, data));
    ctx->getFileReader()->sendLines(-1, records);

    ctx = ctx->next();
  }
}

void FileReader::processLines(ino_t inode, off_t *offPtr)
{
  size_t n = 0;
  char *pos;

  std::vector<FileRecord *> *records = new std::vector<FileRecord *>;
  if (ctx_->copyRawRequired()) {
    if ((pos = (char *) memrchr(buffer_, NL, npos_))) {
      int np = processLine(offPtr ? *offPtr : -1, buffer_, pos - buffer_, records);

      if (offPtr) *offPtr += pos - buffer_ + 1;

      if (np > 0) line_++;
      if (ctx_->md5sum() && pos != buffer_) MD5_Update(&md5Ctx_, buffer_, pos - buffer_ + 1);
      n = (pos+1) - buffer_;
    }
  } else {
    while ((pos = (char *) memchr(buffer_ + n, NL, npos_ - n))) {
      int np = processLine(offPtr ? *offPtr : -1, buffer_ + n, pos - (buffer_ + n), records);

      if (offPtr) *offPtr += pos - (buffer_ + n) + 1;

      if (np > 0) line_++;
      if (ctx_->md5sum() && pos != buffer_ + n) MD5_Update(&md5Ctx_, buffer_ + n, pos - (buffer_ + n) + 1);
      n = (pos+1) - buffer_;
      if (n == npos_) break;
    }
  }

  sendLines(inode, records);

  if (n == 0) {
    if (npos_ == MAX_LINE_LEN) {
      log_error(0, "%s line length exceed, truncate\n", ctx_->file().c_str());
      npos_ = 0;
    }
  } else if (npos_ > n) {
    npos_ -= n;
    memmove(buffer_, buffer_ + n, npos_);
  } else {
    npos_ = 0;
  }
}

/* line without NL */
int FileReader::processLine(off_t off, char *line, size_t nline, std::vector<FileRecord *> *records)
{
  /* ignore empty line */
  if (nline == 0) return 0;

  int n;
  if (line == 0 && nline == (size_t)-1) {
    n = ctx_->function()->serializeCache(records);
  } else {
    n = ctx_->function()->process(off, line, nline, records);
  }
  return n;
}

bool FileReader::checkCache()
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    std::vector<FileRecord *> *records = new std::vector<FileRecord *>;
    ctx->getFileReader()->processLine(-1, 0, -1, records);
    ctx->getFileReader()->sendLines(-1, records);

    ctx = ctx->next();
  }
  return true;
}

bool FileReader::sendLines(ino_t inode, std::vector<FileRecord *> *records)
{
  if (records->empty()) {
    delete records;
    return true;
  } else {
    for (std::vector<FileRecord *>::iterator ite = records->begin(); ite != records->end(); ++ite) {
      (*ite)->inode = inode;
    }

    size_t size = records->size();
    OneTaskReq req = {ctx_, records};

    ssize_t nn = write(ctx_->cnf()->server, &req, sizeof(OneTaskReq));
    if (nn == -1) {
      if (errno != EINTR) {
        log_fatal(errno, "write onetaskrequest error");
        return false;
      }
    }

    util::atomic_inc(&qsize_, size);
    assert(nn != -1 && nn == sizeof(OneTaskReq));
    return true;
  }
}
