#include "taskqueue.h"

using namespace util;

static pthread_mutex_t PTHREAD_MUTEX = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  PTHREAD_COND  = PTHREAD_COND_INITIALIZER;

TaskQueue::Task::~Task() {}

TaskQueue::TaskQueue()
  : quit_(true), mutex_(&PTHREAD_MUTEX), cond_(&PTHREAD_COND) {}

void *TaskQueue::run(void *ctx)
{
  TaskQueue *tq = (TaskQueue *) ctx;
  tq->run();
  return 0;
}

void TaskQueue::run()
{
  while (true) {
    pthread_mutex_lock(mutex_);
    if (!quit_ && tasks_.empty()) pthread_cond_wait(cond_, mutex_);

    Task *task = tasks_.front();
    tasks_.pop();
    pthread_mutex_unlock(mutex_);

    if (task == (Task *) 0) {
      quit_ = true;
      break;
    } else if (task == (Task *) -1) {
      quit_ = true;
      continue;
    }

    if (task->doIt()) {
      delete task;
    } else {
      if (task->canRetry()) {
        task->incRetry();
        submit(task);
      } else {
        delete task;
      }
    }
  }
}
