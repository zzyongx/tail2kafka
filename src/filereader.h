#ifndef _FILE_READER_H_
#define _FILE_READER_H_

#include <string>
#include <vector>
#include <stdint.h>
#include <sys/types.h>

#include "filerecord.h"
class LuaCtx;
class FileOffRecord;

struct OneTaskReq {
  LuaCtx *ctx;
  std::vector<FileRecord *> *records;
};

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

  void tagRotate(int action = FILE_MOVED, const char *fptr = 0);
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

  size_t qsize_;  // send queue size

  char         *buffer_;
  size_t        npos_;
  LuaCtx       *ctx_;

  std::vector<FileRecord*> *fileRecordsCache_;
};

#endif
