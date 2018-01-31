#ifndef _UINT64OFFSET_H_
#define _UINT64OFFSET_H_

#include <cstdio>
#include <cstring>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

class Offset {
public:
  Offset(uint64_t defaultOffset) : fd_(-1), offset_(defaultOffset) {}
  ~Offset() { if (fd_ != -1) close(fd_); }

  bool init(const char *path, char *errbuf) {
    bool rc = true;
    struct stat st;
    if (stat(path, &st) == 0) {
      fd_ = open(path, O_RDWR, 0644);
      ssize_t nn = pread(fd_, &offset_, sizeof(offset_), 0);
      if (nn == 0) {
        sprintf(errbuf, "%s empty offset file, use default %lu", path, offset_);
      } else if (nn < 0) {
        sprintf(errbuf, "%s pread() error %d:%s", path, errno, strerror(errno));
        rc = false;
      }
    } else if (errno == ENOENT) {
      fd_ = open(path, O_CREAT | O_WRONLY, 0644);
      if (fd_ == -1) {
        sprintf(errbuf, "open %s error, %d:%s", path, errno, strerror(errno));
        rc = false;
      } else {
        sprintf(errbuf, "%s first create, use default %lu", path, offset_);
      }
    } else {
      sprintf(errbuf, "%s stat error, %d:%s", path, errno, strerror(errno));
      rc = false;
    }

    return rc;
  }

  uint64_t get() const { return offset_; }

  void update(uint64_t offset) {
    offset_ = offset;
    pwrite(fd_, &offset, sizeof(offset), 0);
  }

private:
  int      fd_;
  uint64_t offset_;
};

#endif
