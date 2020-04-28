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
};

class FileReader {
  template<class T> friend class UNITTEST_HELPER;
public:
  enum StartPosition { LOG_START, LOG_END, START, END, NIL };
  static StartPosition stringToStartPosition(const char *);

  FileReader(LuaCtx *ctx);
  ~FileReader();

  void init(FileReader *reader) {
    parent_ = reader;
  }

  bool init(char *errbuf);

  bool eof() const { return eof_; }

  void tagRotate(int action, const char *newFile);
  bool remove();

  bool tail2kafka(StartPosition pos = NIL, const struct stat *stPtr = 0, std::string *rawData = 0);
  bool checkCache();

  void initFileOffRecord(FileOffRecord * fileOffRecord);
  void updateFileOffRecord(const FileRecord *record);

private:
  bool tryReinit();

  void propagateTailContent(size_t size);
  void propagateProcessLines(ino_t inode, off_t *off);
  void processLines(ino_t inode, off_t *off);
  int processLine(off_t off, char *line, size_t nline, std::vector<FileRecord *> *records);
  bool sendLines(ino_t inode, std::vector<FileRecord *> *records);

  bool openFile(struct stat *st, char *errbuf = 0);
  bool setStartPosition(off_t fileSize, char *errbuf);
  bool setStartPositionEnd(off_t fileSize, char *errbuf);

  std::string *buildFileStartRecord(time_t now);
  std::string *buildFileEndRecord(time_t now, off_t size, const char *oldFileName);
  void propagateRawData(const std::string *data);

private:
  int      fd_;
  off_t    size_;
  ino_t    inode_;
  uint32_t flags_;
  bool eof_;

  FileOffRecord *fileOffRecord_;

  size_t line_;
  size_t dline_;  // send line
  off_t  dsize_;  // send size

  MD5_CTX md5Ctx_;
  std::string md5_;

  FileReader *parent_;

  char         *buffer_;
  size_t        npos_;
  LuaCtx       *ctx_;
};

#endif
