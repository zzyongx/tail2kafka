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
#define MAX_LINE_LEN        8 * 1024 * 1024     // 8M
#define MAX_TAIL_SIZE       50 * MAX_LINE_LEN   // 400M

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

  eof_ = false;

  parent_ = 0;
}

FileReader::~FileReader()
{
  delete[] buffer_;
  if (fd_ > 0) close(fd_);
}

bool FileReader::openFile(struct stat *st, char *errbuf)
{
  assert(parent_ == 0);

  const std::string &file = ctx_->datafile();
  log_info(0, "file current tail %s", file.c_str());

  fd_ = open(file.c_str(), (ctx_->autocreat() ? O_CREAT : 0) | O_RDONLY, 0644);
  if (fd_ == -1) {
    if (errbuf) {
      snprintf(errbuf, 1024, "open file %s error: %d:%s",
               file.c_str(), errno, strerror(errno));
    } else {
      log_fatal(errno, "open file %s error", file.c_str());
    }
    return false;
  }

  if (ctx_->fileOwner() && chown(ctx_->file().c_str(), ctx_->uid(), ctx_->gid()) != 0) {
    if (errbuf) {
      snprintf(errbuf, 1024, "file %s chown(%s) error: %d:%s", file.c_str(),
               ctx_->fileOwner(), errno, strerror(errno));
    } else {
      log_fatal(errno, "file %s chown(%s) error", file.c_str(), ctx_->fileOwner());
    }
    close(fd_);
    fd_ = -1;
    return false;
  }

  fstat(fd_, st);
  inode_ = st->st_ino;

  MD5_Init(&md5Ctx_);
  md5_.clear();

  return true;
}

bool FileReader::init(char *errbuf)
{
  std::string timeFormatFile;
  if (ctx_->getTimeFormatFile(&timeFormatFile)) {
    ctx_->setTimeFormatFile(timeFormatFile);
  }

  ctx_->addHistoryFile(ctx_->datafile());

  struct stat st;
  if (openFile(&st, errbuf)) {
    return setStartPosition(st.st_size, errbuf);
  } else {
    return false;
  }
}

bool FileReader::tryReinit()
{
  assert(parent_ == 0);

  struct stat st;

  if (fd_ != -1) {
    bool reinit = false;
    if (eof_ && ctx_->datafile() != ctx_->file()) {
      fstat(fd_, &st);
      if (st.st_size == size_) {
        reinit = true;
      }
    }

    if (!reinit) return false;

    log_info(0, "%s size=%lu sendsize=%lu lines=%lu sendlines=%lu md5=%s %s", ctx_->file().c_str(),
             size_, dsize_, line_, dline_, md5_.c_str(), ctx_->datafile().c_str());
    util::Metrics::pingback("ROTATE", "file=%s&size=%lu&md5=%s", ctx_->datafile().c_str(), size_, md5_.c_str());

    tail2kafka(END, &st, buildFileEndRecord(time(0), size_, ctx_->datafile().c_str()));

    close(fd_);
    fd_ = -1;
    eof_ = false;
    ctx_->removeHistoryFile();
  }

  if (fd_ == -1 && openFile(&st)) {
    size_ = 0;
    line_ = 0;
    npos_ = 0;

    tail2kafka(START, &st, buildFileStartRecord(time(0)));
    return true;
  }
  return false;
}

bool FileReader::setStartPosition(off_t fileSize, char *errbuf)
{
  assert(parent_ == 0);

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
  assert(parent_ == 0);

  fileOffRecord_ = fileOffRecord;
  fileOffRecord_->inode = inode_;
  fileOffRecord_->off   = size_;
}

// FileOffRecord should be called in only one thread, but it must not call thread unsafe function
void FileReader::updateFileOffRecord(const FileRecord *record)
{
  ctx_->cnf()->stats()->logSendInc();
  ctx_->cnf()->stats()->queueSizeDec();

  if (record->off == (off_t) -1) {
    return;
  }

  assert(parent_ == 0);

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
  } else if (ctx_->getPartitioner() > PARTITIONER_RANDOM) {
    log_fatal(0, "%d %s off change smaller, from %ld/%ld to %ld/%ld", fd_, ctx_->topic().c_str(),
              (long) fileOffRecord_->inode, (long) fileOffRecord_->off,
              (long) record->inode, (long) record->off);
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

void FileReader::tagRotate(int action, const char *newFile)
{
  assert(parent_ == 0);
  if (action != FILE_MOVED && action != FILE_CREATED) return;

  bits_set(flags_, action);
  ctx_->addHistoryFile(newFile);

  log_info(0, "%s rotate to %s", ctx_->file().c_str(), newFile);
  util::Metrics::pingback("TAG_ROTATE", "new=%s&old=%s", newFile, ctx_->file().c_str());
}

bool FileReader::remove()
{
  assert(parent_ == 0);

  if (fd_ == -1) return tryReinit();

  struct stat st;
  if (fstat(fd_, &st) != 0) {
    log_error(0, "%d %s fstat error", fd_, ctx_->file().c_str());
    return false;
  }

  if (st.st_nlink == 0) bits_set(flags_, FILE_DELETED);
  else if (st.st_size < size_) bits_set(flags_, FILE_TRUNCATED);
  else if (st.st_ino != inode_) bits_set(flags_, FILE_ICHANGE);

  std::string timeFormatFile;
  if (ctx_->getTimeFormatFile(&timeFormatFile) && timeFormatFile != ctx_->file()) {
    bits_set(flags_, FILE_CREATED);
    tagRotate(FILE_CREATED, timeFormatFile.c_str());
    ctx_->setTimeFormatFile(timeFormatFile);
  }

  bool rc = bits_test(flags_, FILE_MOVED) || bits_test(flags_, FILE_CREATED) ||
    bits_test(flags_, FILE_DELETED) || bits_test(flags_, FILE_TRUNCATED) || bits_test(flags_, FILE_ICHANGE);

  flags_ = 0;
  if (rc) tryReinit();

  return rc;
}

bool FileReader::setStartPositionEnd(off_t fileSize, char *errbuf)
{
  assert(parent_ == 0);

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
  assert(parent_ == 0);
  if (fd_ == -1 && !tryReinit()) return false;

  std::auto_ptr<std::string> rawDataPtr(rawData);
  if (pos == NIL && ctx_->cnf()->flowControlOn()) return false;

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

  bool fileStart = (pos == START || size_ == 0);

  if (stPtr->st_size - off > MAX_TAIL_SIZE) { // limit tailsize
    log_info(0, "%d %s limit tail, off %ld, size %ld",
             fd_, ctx_->datafile().c_str(), off, stPtr->st_size);

    size_ = off + MAX_TAIL_SIZE;
    ctx_->cnf()->setTailLimit(true);
  } else {
    size_ = stPtr->st_size;
    eof_ = true;
  }

  if (size_ > 0 && fileStart) {  // ignore empty file
    if (pos == START) propagateRawData(rawDataPtr.release());
    else propagateRawData(buildFileStartRecord(time(0)));
  }

  off_t loff = off - npos_;
  assert(loff >= 0);

  while (off < size_) {
    size_t min = std::min(size_ - off, (off_t) (MAX_LINE_LEN - npos_));
    assert(min > 0);
    ssize_t nn = read(fd_, buffer_ + npos_, min);
    if (nn == -1) {
      log_fatal(errno, "%d %s read error", fd_, ctx_->datafile().c_str());
      return false;
    } else if (nn == 0) { // file was truncated
      bits_set(flags_, FILE_TRUNCATED);
      break;
    }
    off += nn;

    propagateTailContent(nn);
    propagateProcessLines(inode_, &loff);

    if (ctx_->cnf()->flowControlOn()) {
      size_ = off;
      eof_ = false;
      break;
    }
  }

  if (pos == END && size_ > 0) {  // ignore empty file
    assert(off == stPtr->st_size);
    propagateRawData(rawDataPtr.release());
  }

  if (pos == NIL && eof_)  return tryReinit();
  else return eof_;
}

void FileReader::propagateTailContent(size_t size)
{
  assert(parent_ == 0);

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
  assert(parent_ == 0);

  LuaCtx *ctx = ctx_;
  while (ctx) {
    ctx->getFileReader()->processLines(inode, off);
    ctx = ctx->next();
    off = 0;   // only first topic have off
  }
}

std::string *FileReader::buildFileStartRecord(time_t now)
{
  assert(parent_ == 0);

  std::string dt = sys::timeFormat(now, "%Y-%m-%dT%H:%M:%S");

  char buffer[8192];
  int n = snprintf(buffer, 8192, "#%s {\"time\":\"%s\", \"event\":\"START\"}",
                   ctx_->cnf()->host().c_str(), dt.c_str());
  return new std::string(buffer, n);
}

std::string *FileReader::buildFileEndRecord(time_t now, off_t size, const char *oldFileName)
{
  assert(parent_ == 0);

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
  assert(data != 0 && parent_ == 0);
  log_info(0, "%d %s META %s", fd_, ctx_->file().c_str(), data->c_str());

  LuaCtx *ctx = ctx_;
  while (ctx) {
    if (ctx->withhost()) {
      std::string *raw = new std::string(*data);
      std::vector<FileRecord *> *records = new std::vector<FileRecord *>(1, FileRecord::create(-1, -1, raw));
      ctx->getFileReader()->sendLines(-1, records);
    }
    ctx = ctx->next();
  }
  delete data;
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
      if (parent_ == 0 && ctx_->md5sum() && pos != buffer_) MD5_Update(&md5Ctx_, buffer_, pos - buffer_ + 1);
      n = (pos+1) - buffer_;
    }
  } else {
    while ((pos = (char *) memchr(buffer_ + n, NL, npos_ - n))) {
      int np = processLine(offPtr ? *offPtr : -1, buffer_ + n, pos - (buffer_ + n), records);

      if (offPtr) *offPtr += pos - (buffer_ + n) + 1;

      if (np > 0) line_++;
      if (parent_ == 0 && ctx_->md5sum() && pos != buffer_ + n) MD5_Update(&md5Ctx_, buffer_ + n, pos - (buffer_ + n) + 1);
      n = (pos+1) - buffer_;
      if (n == npos_) break;
    }
  }

  sendLines(inode, records);

  if (n == 0) {
    if (npos_ == MAX_LINE_LEN) {
      log_error(0, "%s line length exceed, truncate", ctx_->file().c_str());
      npos_ = 0;
    }
  } else if (npos_ > n) {
    npos_ -= n;
    memmove(buffer_, buffer_ + n, npos_);
  } else {
    npos_ = 0;
  }
}

#define TR_NOTPRINT(line, nline) do {     \
  for (size_t i = 0; i < nline; ++i) {    \
    if (!isprint(line[i])) line[i] = 'X'; \
  }                                       \
} while (0)

/* line without NL */
int FileReader::processLine(off_t off, char *line, size_t nline, std::vector<FileRecord *> *records)
{
  /* ignore empty line */
  if (nline == 0) return 0;

  int n;
  if (line == 0 && nline == (size_t)-1) {
    n = ctx_->function()->serializeCache(records);
  } else {
    ctx_->cnf()->stats()->logReadInc();
    n = ctx_->function()->process(off, line, nline, records);
  }
  return n;
}

bool FileReader::checkCache()
{
  assert(parent_ == 0);

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

    for (std::vector<FileRecord *>::iterator ite = records->begin(); ite != records->end(); ++ite) {
      (*ite)->ctx = ctx_;
      log_debug(0, "%.*s", (int) (*ite)->data->size(), (*ite)->data->c_str());
    }

    size_t size = records->size();

    uintptr_t ptr = (uintptr_t) records;
    ssize_t nn = write(ctx_->cnf()->server, &ptr, sizeof(ptr));
    if (nn == -1) {
      if (errno != EINTR) {
        log_fatal(errno, "write onetaskrequest error");
        delete records;
        return false;
      }
    }

    ctx_->cnf()->stats()->logWriteInc(size);
    ctx_->cnf()->stats()->queueSizeInc(size);

    assert(nn != -1 && nn == sizeof(uintptr_t));
    return true;
  }
}
