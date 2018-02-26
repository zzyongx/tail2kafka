#ifndef _TASK_QUEUE_H_
#define _TASK_QUEUE_H_

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <queue>
#include <pthread.h>

namespace util {

class TaskQueue {
public:
  class Task {
  protected:
    int retry_;
    int maxRetry_;
  public:
    Task(int maxRetry = 1) : retry_(0), maxRetry_(maxRetry) {}
    /* if return true, finish
     * if return false, try again
     */
    void incRetry() {
      ++retry_;
    }

    bool canRetry() {
      return retry_+1 < maxRetry_;
    }

    virtual bool doIt() = 0;
    virtual ~Task();
  };

  TaskQueue();

  bool submit(Task *task) {
    pthread_mutex_lock(mutex_);
    if (!quit_) tasks_.push(task);
    pthread_mutex_unlock(mutex_);
    pthread_cond_signal(cond_);
    return !quit_;
  }

  typedef std::vector<TaskQueue::Task *> TaskArray;
  bool submit(TaskArray *tasks) {
    for (TaskArray::iterator ite = tasks->begin(); ite != tasks->end(); ++ite) {
      if (!submit(*ite)) return false;
    }
  }

  static void* run(void *ctx);

  bool start(char *errbuf, size_t nthread = 1) {
    bool ret = true;
    for (size_t i = 0; ret && i < nthread; ++i) {
      pthread_t tid;
      int rc = pthread_create(&tid, 0, run, this);
      if (rc != 0) {
        snprintf(errbuf, 1024, "pthread_create error %s", strerror(rc));
        ret = false;
      } else {
        tids_.push_back(tid);
      }
    }

    if (!ret) stop(true);
    else quit_ = false;
    return ret;
  }

  void run();

  void stop(bool force = false) {
    submit(static_cast<Task *>(force ? 0 : (void *) 0x01));

    for (std::vector<pthread_t>::iterator ite = tids_.begin(); ite != tids_.end(); ++ite) {
      pthread_join(*ite, 0);
    }
    tids_.clear();
  }

private:
  bool quit_;
  std::queue<Task *> tasks_;
  std::vector<pthread_t> tids_;

  pthread_mutex_t    *mutex_;
  pthread_cond_t     *cond_;
};

}  // namespace util

#endif
