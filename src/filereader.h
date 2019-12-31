#ifndef _FILE_READER_H_
#define _FILE_READER_H_

#include <string>
#include <vector>
#include <stdint.h>
#include <sys/types.h>
#include <openssl/md5.h>

#include "filerecord.h"
class LuaCtx;
class FileOffRecord;

enum FileInotifyStatus {
  FILE_MOVED     = 0x0001,
  FILE_CREATED   = 0x0002,
  FILE_ICHANGE   = 0x0004,
  FILE_TRUNCATED = 0x0008,
  FILE_DELETED   = 0x0010,

  FILE_LOGGED    = 0x0020,
  FILE_WATCHED   = 0x0040,
  FILE_OPENONLY  = 0x0080,
  FILE_HISTORY   = 0x0100,
};

class FileReader {
  template<class T> friend class UNITTEST_HELPER;
public:
  enum StartPosition { LOG_START, LOG_END, START, END, NIL };
  static StartPosition stringToStartPosition(const char *);

  FileReader(LuaCtx *ctx);
  ~FileReader();

  bool init(char *errbuf);
  bool reinit();

  bool eof() const { return eof_; }

  void tagRotate(int action, const char *oldFile = 0, const char *newFile = 0);
  bool remove();

  bool tail2kafka(StartPosition pos = NIL, const struct stat *stPtr = 0, std::string *rawData = 0);
  bool checkCache();

  void initFileOffRecord(FileOffRecord * fileOffRecord);
  void updateFileOffRecord(const FileRecord *record);

private:
  void propagateTailContent(size_t size);
  void propagateProcessLines(ino_t inode, off_t *off);
  void processLines(ino_t inode, off_t *off);
  int processLine(off_t off, char *line, size_t nline, std::vector<FileRecord *> *records);
  bool sendLines(ino_t inode, std::vector<FileRecord *> *records);

  bool tryOpen(char *errbuf);
  bool setStartPosition(off_t fileSize, char *errbuf);
  bool setStartPositionEnd(off_t fileSize, char *errbuf);

  std::string *buildFileStartRecord(time_t now);
  std::string *buildFileEndRecord(time_t now, off_t size, const char *oldFileName);
  void propagateRawData(const std::string *data);

  bool checkRewatch();
  void checkHistoryRotate(const struct stat *stPtr);
  bool waitRotate();
  bool checkRotate(const struct stat *stPtr, std::string *rotateFileName, bool *closeFd);

private:
  int      fd_;
  off_t    size_;
  ino_t    inode_;
  uint32_t flags_;
  bool eof_;

  time_t   fileRotateTime_;
  int      holdFd_;    // trace moved file when datafile != file

  FileOffRecord *fileOffRecord_;

  size_t line_;
  size_t dline_;  // send line
  off_t  dsize_;  // send size

  MD5_CTX md5Ctx_;
  std::string md5_;

  char         *buffer_;
  size_t        npos_;
  LuaCtx       *ctx_;
};

#endif
