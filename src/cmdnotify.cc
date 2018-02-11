#include <cstdio>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "logger.h"
#include "cmdnotify.h"

char * const *CmdNotify::buildEnv(const char *file, time_t timestamp)
{
  int i = 0;
  static char *envp[10];

  char topicPtr[128];
  snprintf(topicPtr, 128, "NOTIFY_TOPIC=%s", topic_);
  envp[i++] = topicPtr;

  char partitionPtr[128];
  snprintf(partitionPtr, 128, "NOTIFY_PARTITION=%d", partition_);
  envp[i++] = partitionPtr;

  char filePtr[1024];
  snprintf(filePtr, 1024, "NOTIFY_FILE=%s", file);
  envp[i++] = filePtr;

  char timestampPtr[64];
  if (timestamp != (time_t) -1) {
    snprintf(timestampPtr, 64, "NOTIFY_TIMESTAMP=%ld", timestamp);
    envp[i++] = timestampPtr;
  }

  envp[i] = 0;
  return envp;
}

bool CmdNotify::exec(const char *file, time_t timestamp)
{
  if (!cmd_) return false;

  pid_t pid = fork();
  if (pid == 0) {
    char log[2048];
    snprintf(log, 2048, "%s/%s.%d.notify.log", wdir_, topic_, partition_);
    int fd = open(log, O_CREAT | O_WRONLY | O_APPEND, 0666);
    if (fd != -1) {
      dup2(fd, STDOUT_FILENO);
      dup2(fd, STDERR_FILENO);
    }

    char * const argv[] = { (char *) cmd_, NULL };
    char * const *envp = buildEnv(file, timestamp);

    if (execve(cmd_, argv, envp) == -1) {
      log_fatal(errno, "exec cmd %s error", cmd_);
    }
    exit(0);
  } else if (pid > 0) {
    return true;
  } else {
    log_fatal(errno, "exec cmd %s fork() error", cmd_);
    return false;
  }
}
