#ifndef _FILE_OFF_H_
#define _FILE_OFF_H_

#include <map>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

class CnfCtx;

struct FileOffRecord {
  ino_t  inode;
  size_t off;
  FileOffRecord() {}
  FileOffRecord(ino_t inode_, size_t off_) : inode(inode_), off(off_) {}

//  char   file[FileOff::MAX_FILENAME_LENGTH];
};

class FileOff {
  template<class T> friend class UNITTEST_HELPER;
public:
  static const size_t MAX_FILENAME_LENGTH;

  FileOff();
  ~FileOff();


  bool init(CnfCtx *cnf, char *errbuf);
  bool reinit();
  off_t getOff(ino_t inode) const;
  bool setOff(ino_t inode, off_t off);

private:
  bool loadFromFile(char *errbuf);
  void deleteMallocPtr();

private:
  CnfCtx      *cnf_;
  std::string  file_;
  void        *addr_;
  size_t      length_;
  std::map<ino_t, FileOffRecord*> map_;
};

#endif
