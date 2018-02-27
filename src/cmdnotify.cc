#include <cstdio>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "logger.h"
#include "cmdnotify.h"

#define MAX_ENVP_NUM 511
extern char **environ;

char * const *CmdNotify::buildEnv(const char *file, const char *oriFile, time_t timestamp, uint64_t size, const char *md5)
{
  int i = 0;
  static char *envp[MAX_ENVP_NUM+1];

  static char topicPtr[128];
  snprintf(topicPtr, 128, "NOTIFY_TOPIC=%s", topic_);
  envp[i++] = topicPtr;

  static char partitionPtr[128];
  snprintf(partitionPtr, 128, "NOTIFY_PARTITION=%d", partition_);
  envp[i++] = partitionPtr;

  static char filePtr[1024];
  snprintf(filePtr, 1024, "NOTIFY_FILE=%s", file);
  envp[i++] = filePtr;

  static char oriFilePtr[1024];
  if (oriFile) {
    snprintf(oriFilePtr, 1024, "NOTIFY_ORIFILE=%s", oriFile);
    envp[i++] = oriFilePtr;
  }

  static char timestampPtr[64];
  if (timestamp != (time_t) -1) {
    snprintf(timestampPtr, 64, "NOTIFY_TIMESTAMP=%ld", timestamp);
    envp[i++] = timestampPtr;
  }

  static char sizePtr[64];
  if (size != (uint64_t) -1) {
    snprintf(sizePtr, 64, "NOTIFY_FILESIZE=%lu", size);
    envp[i++] = sizePtr;
  }

  static char md5Ptr[64];
  if (md5) {
    snprintf(md5Ptr, 64, "NOTIFY_FILEMD5=%s", md5);
    envp[i++] = md5Ptr;
  }

  for (int j = 0; i < MAX_ENVP_NUM && environ[j]; ++j) envp[i++] = environ[j];

  envp[i] = 0;
  return envp;
}

bool CmdNotify::exec(const char *file, const char *oriFile, time_t timestamp, uint64_t size, const char *md5)
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
    char * const *envp = buildEnv(file, oriFile, timestamp, size, md5);

    std::string buffer;
    for (int i = 0; envp[i]; ++i) buffer.append(envp[i]).append(1, ' ');
    log_info(0, "exec cmd %s with env %s", cmd_, buffer.c_str());

    if (execve(cmd_, argv, envp) == -1) {
      log_fatal(errno, "exec cmd %s with env %s error", cmd_, buffer.c_str());
    }
    exit(0);
  } else if (pid > 0) {
    return true;
  } else {
    log_fatal(errno, "exec cmd %s fork() error", cmd_);
    return false;
  }
}
