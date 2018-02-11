#ifndef _CMD_NOTIFY_H_
#define _CMD_NOTIFY_H_

#include <time.h>

class CmdNotify {
public:
  CmdNotify(const char *cmd, const char *wdir, const char *topic, int partition)
    : cmd_(cmd), wdir_(wdir), topic_(topic), partition_(partition) {}
  bool exec(const char *file, time_t timestamp = -1);

private:
  char * const *buildEnv(const char *file, time_t timestamp);

private:
  const char *cmd_;
  const char *wdir_;
  const char *topic_;
  int partition_;
};

#endif
