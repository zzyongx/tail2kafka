#ifndef _FILE_READER_H_
#define _FILE_READER_H_

#include <string>
#include <vector>
#include <stdint.h>
#include <sys/types.h>

#include "filerecord.h"
class LuaCtx;
class FileOffRecord;

#define FILE_MOVED     0x01
#define FILE_TRUNCATED 0x02
#define FILE_DELETED   0x04
#define FILE_LOGGED    0x08

struct OneTaskReq {
  LuaCtx *ctx;
  std::vector<FileRecord *> *records;
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

  void tagRemove() { flags_ |= FILE_MOVED; }
  bool remove();

  bool tail2kafka(StartPosition pos = NIL, struct stat *stPtr = 0, const char *oldFileName = 0);
  bool checkCache();

  void initFileOffRecord(FileOffRecord * fileOffRecord);
  void updateFileOffRecord(const FileRecord *record);

private:
  void propagateTailContent(size_t size);
  void propagateProcessLines(ino_t inode, off_t *off);
  void processLines(ino_t inode, off_t *off);
  int processLine(off_t off, char *line, size_t nline, std::vector<std::string *> *lines);
  void propagateSendLines();
  bool sendLines();

  bool tryOpen(char *errbuf);
  bool setStartPosition(off_t fileSize, char *errbuf);
  bool setStartPositionEnd(off_t fileSize, char *errbuf);

  void cacheFileStartRecord();
  void cacheFileEndRecord(off_t size, const char *oldFileName);
  void propagateRawData(const std::string &line, off_t size);
  void cacheFileRecord(ino_t inode, off_t off, const std::vector<std::string *> &lines, size_t n);

private:
  int      fd_;
  off_t    size_;
  ino_t    inode_;
  uint32_t flags_;
  time_t   fileRotateTime_;

  FileOffRecord *fileOffRecord_;

  size_t line_;
  size_t dline_;  // send line
  off_t  dsize_;  // send size

  char         *buffer_;
  size_t        npos_;
  std::string   file_;
  LuaCtx       *ctx_;

  std::vector<FileRecord*> *fileRecordsCache_;
};

#endif
