#ifndef _FILE_RECORD_H_
#define _FILE_RECORD_H_

#include <string>
#include <sys/types.h>

class LuaCtx;

struct FileRecord {
  LuaCtx        *ctx;
  ino_t          inode;
  off_t          off;

  const std::string   *esIndex;
  const std::string   *data;

  static FileRecord *create(ino_t inode_, off_t off_, const std::string *data_) {
    return create(inode_, off_, 0, data_);
  }

  static FileRecord *create(ino_t inode_, off_t off_, const std::string *esIndex_,
                            const std::string *data_) {
    FileRecord *record = new FileRecord;
    record->inode = inode_;
    record->off   = off_;

    record->esIndex = esIndex_;
    record->data    = data_;
    return record;
  }

  static void destroy(FileRecord *record) {
    if (record->esIndex) delete record->esIndex;

    delete record->data;
    delete record;
  }
};

#endif
