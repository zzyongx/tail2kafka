#ifndef _CMD_NOTIFY_H_
#define _CMD_NOTIFY_H_

#include <time.h>
#include <stdint.h>

class CmdNotify {
public:
  CmdNotify(const char *cmd, const char *wdir, const char *topic, int partition)
    : cmd_(cmd), wdir_(wdir), topic_(topic), partition_(partition) {}
  bool exec(const char *file, const char *oriFile = 0, time_t timestamp = -1, uint64_t size = -1);

private:
  char * const *buildEnv(const char *file, const char *oriFile, time_t timestamp, uint64_t size);

private:
  const char *cmd_;
  const char *wdir_;
  const char *topic_;
  int partition_;
};

#endif
