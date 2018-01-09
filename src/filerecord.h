#ifndef _FILE_RECORD_H_
#define _FILE_RECORD_H_

#include <string>
#include <sys/types.h>

class LuaCtx;

struct FileRecord {
  LuaCtx        *ctx;
  ino_t          inode;
  off_t          off;

  std::string   *data;

  static FileRecord *create(ino_t inode_, off_t off_, std::string *data_) {
    FileRecord *record = new FileRecord;
    record->inode = inode_;
    record->off   = off_;
    record->data  = data_;
    return record;
  }

  static void destroy(FileRecord *record) {
    delete record->data;
    delete record;
  }
};

#endif
