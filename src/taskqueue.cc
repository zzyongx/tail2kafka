#include "taskqueue.h"

using namespace util;

TaskQueue::Task::~Task() {}

TaskQueue::TaskQueue(const std::string &name)
  : name_(name), quit_(true)
{
  pthread_mutex_init(&mutex_, 0);
  pthread_cond_init(&cond_, 0);
}

TaskQueue::~TaskQueue()
{
  pthread_mutex_destroy(&mutex_);
  pthread_cond_destroy(&cond_);
}

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
