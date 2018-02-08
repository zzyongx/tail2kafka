#include "taskqueue.h"

using namespace util;

TaskQueue::Task::~Task() {}

void *TaskQueue::run(void *ctx)
{
  TaskQueue *tq = (TaskQueue *) ctx;
  tq->run();
  return 0;
}

void TaskQueue::run()
{
  while (true) {
    pthread_mutex_lock(&mutex_);
    if (!quit_ && tasks_.empty()) pthread_cond_wait(&cond_, &mutex_);

    Task *task = tasks_.front();
    tasks_.pop();
    pthread_mutex_unlock(&mutex_);

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
