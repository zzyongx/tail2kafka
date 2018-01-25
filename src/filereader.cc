#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "logger.h"
#include "sys.h"
#include "luactx.h"
#include "filereader.h"

static const char   NL           = '\n';
static const size_t MAX_LINE_LEN = 1024 * 1024; // 1M

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
  fileRecordsCache_ = 0;
}

FileReader::~FileReader()
{
  delete[] buffer_;
  if (fd_ > 0) close(fd_);
}

// try best to watch the file
bool FileReader::tryOpen(char *errbuf)
{
  for (int i = 0; i < 15; ++i) {
    fd_ = open(file_.c_str(), O_RDONLY);
    if (fd_ == -1) sleep(1);
    else break;
  }
  if (fd_ == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "%s open error: %s", file_.c_str(), strerror(errno));
    return false;
  } else {
    return true;
  }
}

bool FileReader::init(char *errbuf)
{
  file_ = ctx_->file();
  if (!tryOpen(errbuf)) return false;

  struct stat st;
  fstat(fd_, &st);
  inode_ = st.st_ino;

  return setStartPosition(st.st_size, errbuf);
}

bool FileReader::reinit()
{
  if (fd_ != -1) return false;   // init already

  fd_ = open(file_.c_str(), O_RDONLY);
  if (fd_ != -1) {
    struct stat st;
    fstat(fd_, &st);
    size_ = 0;
    line_ = 0;
    npos_ = 0;
    inode_ = st.st_ino;

    tail2kafka(START, &st);
    return true;
  } else {
    log_error(errno, "%s reinit error", file_.c_str());
    return false;
  }
}

bool FileReader::setStartPosition(off_t fileSize, char *errbuf)
{
  StartPosition startPosition = stringToStartPosition(ctx_->getStartPosition());
  if (startPosition == FileReader::LOG_START) {
    size_ = ctx_->cnf()->getFileOff()->getOff(inode_);
    if (size_ == (off_t) -1 || size_ > fileSize) {
      size_ = 0;
      log_error(0, "%s fileoff notfound, set to start", file_.c_str());
    }
  } else if (startPosition == FileReader::START) {
    size_ = 0;
  } else if (startPosition == FileReader::LOG_END) {
    size_ = ctx_->cnf()->getFileOff()->getOff(inode_);
    if (size_ == (off_t) -1 || size_ > fileSize) {
      log_error(0, "%s fileoff notfound, set to end", file_.c_str());
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

void FileReader::updateFileOffRecord(const FileRecord *record)
{
  if (record->inode != fileOffRecord_->inode) {
    fileOffRecord_->inode = record->inode;
    fileOffRecord_->off   = record->off;
    dline_ = 1;
    dsize_ = record->data->size();
    log_info(0, "%s change inode from %ld to %ld", ctx_->file().c_str(),
             (long) fileOffRecord_->inode, (long) record->inode);
  } else if (record->off > fileOffRecord_->off) {
    fileOffRecord_->off   = record->off;
    dline_++;
    dsize_ += record->data->size() - ctx_->function()->extraSize();
  } else {
    log_fatal(0, "%s off change smaller, from %ld to %ld", ctx_->file().c_str(),
              (long) fileOffRecord_->off, (long) record->off);
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

bool FileReader::remove()
{
  struct stat st;
  if (fstat(fd_, &st) != 0) {
    log_error(0, "%d %s fstat error", fd_, ctx_->file().c_str());
    return false;
  }

  if (st.st_nlink == 0) flags_ |= FILE_DELETED;
  else if (st.st_size < size_) flags_ |= FILE_TRUNCATED;

  bool rc = false;
  std::string oldFileName;
  if (flags_ & FILE_MOVED) {
    oldFileName = getFileNameFromFd(fd_);
    log_info(0, "%d %s size(%lu) send(%lu) line(%lu) send(%lu) moved to %s", fd_, ctx_->file().c_str(),
             size_, dsize_, line_, dline_, oldFileName.c_str());
    rc = true;
  } else if (flags_ & FILE_DELETED) {
    log_info(0, "%d %s size(%lu) send(%lu) line(%lu) send(%lu) deleted %s", fd_, ctx_->file().c_str(),
             size_, dsize_, line_, dline_, getFileNameFromFd(fd_).c_str());
    rc = true;
  } else if (flags_ & FILE_TRUNCATED) {
    log_info(0, "%d %s size(%lu) send(%lu) line(%lu) send(%lu) truncated", fd_, ctx_->file().c_str(),
             size_, dsize_, line_, dline_);
    rc = true;
  } else if (st.st_ino != inode_) {
    log_info(0, "%d %s inode changed", fd_, ctx_->file().c_str());
    rc = true;
  }

  if (rc) {
    if (oldFileName.empty()) oldFileName = ctx_->file() + "." + sys::timeFormat(time(0), "%Y-%m-%d_%H:%M:%S");
    tail2kafka(END, &st, oldFileName.c_str());

    close(fd_);
    fd_ = -1;
    flags_ = 0;
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
    snprintf(errbuf, MAX_ERR_LEN, "read %s less min %s", file_.c_str(), errno == 0 ? "" : strerror(errno));
    return false;
  }

  char *pos = (char *) memrchr(buffer_, NL, min);
  if (!pos) {
    snprintf(errbuf, MAX_ERR_LEN, "%s line length bigger than %ld", file_.c_str(), (long) min);
    return false;
  }

  size_ = fileSize - (min - (pos+1 - buffer_));
  return true;
}

bool FileReader::tail2kafka(StartPosition pos, struct stat *stPtr, const char *oldFileName)
{
  struct stat stat;
  if (stPtr == 0) {
    if (fstat(fd_, &stat) != 0) {
      log_error(errno, "%d %s fstat error", fd_, file_.c_str());
      return false;
    }
    stPtr = &stat;
  }

  off_t off = lseek(fd_, 0, SEEK_CUR);  // get last read seek
  if (off == (off_t) -1) {
    log_error(errno, "%d %s lseek error", fd_, file_.c_str());
    return false;
  }

  if (off > stPtr->st_size) {
    flags_ |= FILE_TRUNCATED;
    return true;
  }

  if (pos == START || size_ == 0) cacheFileStartRecord();
  size_ = stPtr->st_size;

  off_t loff = off - npos_;
  assert(loff >= 0);

  while (off < size_) {
    size_t min = std::min(size_ - off, (off_t) (MAX_LINE_LEN - npos_));
    assert(min > 0);
    ssize_t nn = read(fd_, buffer_ + npos_, min);
    if (nn == -1) {
      log_error(errno, "%d %s read error", fd_, file_.c_str());
      return false;
    } else if (nn == 0) { // file was truncated
      flags_ |= FILE_TRUNCATED;
      break;
    }
    off += nn;

    propagateTailContent(nn);
    propagateProcessLines(inode_, &loff);
    propagateSendLines();
  }

  if (pos == END) cacheFileEndRecord(size_, oldFileName);
  propagateSendLines();
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

void FileReader::propagateSendLines()
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    ctx->getFileReader()->sendLines();
    ctx = ctx->next();
  }
}

void FileReader::cacheFileStartRecord()
{
  std::string line;
  line.append(1, '#').append(ctx_->cnf()->host()).append(1, ' ');
  line.append(sys::timeFormat(time(0), "[%Y-%m-%d %H-%M-%S]")).append(1, ' ');
  line.append("Start");

  propagateRawData(line, -1);
}

void FileReader::cacheFileEndRecord(off_t size, const char *oldFileName)
{
  std::string line;
  line.append(1, '#').append(ctx_->cnf()->host()).append(1, ' ');
  if (oldFileName) line.append(1, '@').append(oldFileName).append(1, ' ');
  line.append(sys::timeFormat(time(0), "[%Y-%m-%d %H-%M-%S]")).append(1, ' ');
  line.append("End");

  propagateRawData(line, size);
}

void FileReader::propagateRawData(const std::string &line, off_t size)
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    if (!ctx->withhost()) continue;

    FileReader *fileReader = ctx->getFileReader();

    if (!fileReader->fileRecordsCache_) {
      fileReader->fileRecordsCache_ = new std::vector<FileRecord *>;
    }

    std::string *linePtr = new std::string(line);
    if (size != (off_t) -1) {
      char buffer[128];
      snprintf(buffer, 128, " size(%lu) send(%lu) lines(%lu), send(%lu)", size,
               fileReader->dsize_, fileReader->line_, fileReader->dline_);
      linePtr->append(buffer);
    }
    if (ctx->autonl()) linePtr->append(1, NL);

    fileReader->fileRecordsCache_->push_back(FileRecord::create(-1, -1, linePtr));
    ctx = ctx->next();
  }
}

void FileReader::cacheFileRecord(ino_t inode, off_t off, const std::vector<std::string *> &lines, size_t n)
{
  if (n == 0) return;
  if (!fileRecordsCache_) fileRecordsCache_ = new std::vector<FileRecord *>;

  for (size_t i = lines.size()-n; i < lines.size(); ++i) {
    fileRecordsCache_->push_back(FileRecord::create(inode, off, lines[i]));
  }
}

void FileReader::processLines(ino_t inode, off_t *offPtr)
{
  size_t n = 0;
  char *pos;

  std::vector<std::string *> lines;
  if (ctx_->copyRawRequired()) {
    if ((pos = (char *) memrchr(buffer_, NL, npos_))) {
      int np = processLine(buffer_, pos - buffer_, &lines);

      if (offPtr) *offPtr += pos - buffer_ + 1;
      cacheFileRecord(inode, offPtr ? *offPtr : -1, lines, np);

      if (np > 0) line_++;
      n = (pos+1) - buffer_;
    }
  } else {
    while ((pos = (char *) memchr(buffer_ + n, NL, npos_ - n))) {
      int np = processLine(buffer_ + n, pos - (buffer_ + n), &lines);

      if (offPtr) *offPtr += pos - (buffer_ + n) + 1;
      cacheFileRecord(inode, offPtr ? *offPtr : -1, lines, np);

      if (np > 0) line_++;
      n = (pos+1) - buffer_;
      if (n == npos_) break;
    }
  }

  if (n == 0) {
    if (npos_ == MAX_LINE_LEN) {
      log_error(0, "%s line length exceed, truncate\n", file_.c_str());
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
int FileReader::processLine(char *line, size_t nline, std::vector<std::string *> *lines)
{
  /* ignore empty line */
  if (nline == 0) return 0;

  int n;
  if (line == 0 && nline == (size_t)-1) {
    n = ctx_->function()->serializeCache(lines);
  } else {
    n = ctx_->function()->process(line, nline, lines);
  }
  return n;
}

bool FileReader::checkCache()
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    std::vector<std::string *> lines;
    int n = processLine(0, -1, &lines);
    if (n > 0) {
      cacheFileRecord(-1, -1, lines, n);
      ctx->getFileReader()->sendLines();
    }
    ctx = ctx->next();
  }
  return true;
}

bool FileReader::sendLines()
{
  if (fileRecordsCache_ == 0) {
    return true;
  } else if (fileRecordsCache_->empty()) {
    delete fileRecordsCache_;
    fileRecordsCache_ = 0;
    return true;
  } else {
    OneTaskReq req = {ctx_, fileRecordsCache_};
    fileRecordsCache_ = 0;

    ssize_t nn = write(ctx_->cnf()->server, &req, sizeof(OneTaskReq));
    if (nn == -1) {
      if (errno != EINTR) {
        log_fatal(errno, "write onetaskrequest error");
        return false;
      }
    }

    assert(nn != -1 && nn == sizeof(OneTaskReq));
    return true;
  }
}
