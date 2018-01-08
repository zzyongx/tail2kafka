#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "logger.h"
#include "luactx.h"
#include "filereader.h"

static const char   NL           = '\n';
static const size_t MAX_LINE_LEN = 512 * 1024; // 512K

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
    inode_ = st.st_ino;
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
    if (size_ < 0 || size_ > fileSize) size_ = 0;
  } else if (startPosition == FileReader::START) {
    size_ = 0;
  } else if (startPosition == FileReader::LOG_END) {
    size_ = ctx_->cnf()->getFileOff()->getOff(inode_);
    if (size_ < 0 || size_ > fileSize) return setStartPositionEnd(fileSize, errbuf);
  } else if (startPosition == FileReader::END) {
    return setStartPositionEnd(fileSize, errbuf);
  }

  lseek(fd_, size_, SEEK_SET);
  return true;
}

bool FileReader::remove()
{
  struct stat st;
  if (fstat(fd_, &st) != 0) {
    log_error(0, "%d %s fstat error", fd_, ctx_->file().c_str());
    return false;
  }

  if (st.st_size < size_) flags_ |= FILE_TRUNCATED;

  bool rc = false;
  if (flags_ & FILE_MOVED) {
    log_info(0, "%d %s moved", fd_, ctx_->file().c_str());
    rc = true;
  } else if (flags_ & FILE_TRUNCATED) {
    log_info(0, "%d %s truncated", fd_, ctx_->file().c_str());
    rc = true;
  } else if (st.st_ino != inode_ || st.st_nlink == 0) {
    log_info(0, "%d %s inode changed || st_nlink == 0", fd_, ctx_->file().c_str());
    rc = true;
  }

  if (rc) {
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

bool FileReader::tail2kafka()
{
  struct stat st;
  if (fstat(fd_, &st) != 0) {
    log_error(errno, "%d %s fstat error", fd_, file_.c_str());
    return false;
  }
  size_ = st.st_size;

  off_t off = lseek(fd_, 0, SEEK_CUR);
  if (off == (off_t) -1) {
    log_error(errno, "%d %s lseek error", fd_, file_.c_str());
    return false;
  }

  if (off > size_) {
    flags_ |= FILE_TRUNCATED;
    return true;
  }

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
    propagateProcessLines();
  }
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

void FileReader::propagateProcessLines()
{
  LuaCtx *ctx = ctx_;
  while (ctx) {
    ctx->getFileReader()->processLines();
    ctx = ctx->next();
  }
}

void FileReader::processLines()
{
  size_t n = 0;
  char *pos;

  std::vector<std::string *> *lines = new std::vector<std::string *>;
  if (ctx_->copyRawRequired()) {
    if ((pos = (char *) memrchr(buffer_, NL, npos_))) {
      processLine(buffer_, pos - buffer_, lines);
      n = (pos+1) - buffer_;
    }
  } else {
    while ((pos = (char *) memchr(buffer_ + n, NL, npos_ - n))) {
      processLine(buffer_ + n, pos - (buffer_ + n), lines);
      n = (pos+1) - buffer_;
      if (n == npos_) break;
    }
  }

  sendLines(lines);

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
bool FileReader::processLine(char *line, size_t nline, std::vector<std::string *> *lines)
{
  /* ignore empty line */
  if (nline == 0) return true;

  bool rc;
  if (line == 0 && nline == (size_t)-1) {
    rc = ctx_->function()->serializeCache(lines);
  } else {
    rc = ctx_->function()->process(line, nline, lines);
  }
  return rc;
}

bool FileReader::sendLines(std::vector<std::string *> *lines)
{
  if (lines->empty()) {
    delete lines;
    return true;
  } else {
    OneTaskReq req = {ctx_->idx(), lines};
    req.datas = lines;

    ssize_t nn = write(ctx_->cnf()->server, &req, sizeof(OneTaskReq));
    assert(nn != -1 && nn == sizeof(OneTaskReq));
    return true;
  }
}
