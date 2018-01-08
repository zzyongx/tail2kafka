#ifndef _FILE_READER_H_
#define _FILE_READER_H_

#include <string>
#include <vector>
#include <stdint.h>
#include <sys/types.h>

#define FILE_MOVED     0x01
#define FILE_TRUNCATED 0x02

class LuaCtx;

struct OneTaskReq {
  int                         idx;
  std::vector<std::string *> *datas;
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

  bool tail2kafka();

  bool checkCache() {
    std::vector<std::string *> *lines = new std::vector<std::string *>;
    processLine(0, -1, lines);
    return sendLines(lines);
  }

  off_t getPos() const { return size_; }
  ino_t getInode() const { return inode_; }

private:
  void propagateTailContent(size_t size);
  void propagateProcessLines();
  void processLines();
  bool processLine(char *line, size_t nline, std::vector<std::string *> *lines);
  bool sendLines(std::vector<std::string *> *lines);

  bool tryOpen(char *errbuf);
  bool setStartPosition(off_t fileSize, char *errbuf);
  bool setStartPositionEnd(off_t fileSize, char *errbuf);

private:
  int    fd_;
  off_t  size_;
  ino_t  inode_;
  int    lines_;
  uint32_t flags_;
  time_t start_;

  char         *buffer_;
  size_t        npos_;
  std::string   file_;
  LuaCtx       *ctx_;
};

#endif
