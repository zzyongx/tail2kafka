#include <cstdio>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "logger.h"
#include "unittesthelper.h"
#include "runstatus.h"
#include "sys.h"
#include "util.h"
#include "luactx.h"
#include "cnfctx.h"
#include "filereader.h"
#include "inotifyctx.h"

#define PADDING_LEN 13

LOGGER_INIT();

static CnfCtx *cnf = 0;

#define LUACNF_SIZE 6
#define ETCDIR "blackboxtest/tail2kafka"
#define LUA(f) "blackboxtest/tailkafka/"f
#define LOG(f) "logs/"f

DEFINE(split)
{
  std::vector<std::string> list;

  const char *s1 = "hello \"1 [] 2\"[world] [] [\"\"]  bj";
  split(s1, strlen(s1), &list);
  check(list.size() == 6, "%d", (int) list.size());
  assert(list[0] == "hello");
  assert(list[1] == "1 [] 2");
  assert(list[2] == "world");
  assert(list[3] == "");
  check(list[4] == "\"\"", "%s", list[4].c_str());
  assert(list[5] == "bj");
}

DEFINE(iso8601)
{
  std::string iso;
  bool rc;

  rc = iso8601("28/Feb/2015:12:30:23", &iso);
  check(rc, "%s", "28/Feb/2015:12:30:23");
  check(iso == "2015-02-28T12:30:23", "%s", iso.c_str());

  rc = iso8601("28/Feb:12:30:23", &iso);
  check(rc == false, "%s", "28/Feb:12:30:23");

  rc = iso8601("28/Feb/2015:12:30", &iso);
  check(rc, "%s", "28/Feb/2015:12:30");
  check(iso == "2015-02-28T12:30:00", "%s", iso.c_str());
}

static LuaCtx *getLuaCtx(const char *topic)
{
  for (std::vector<LuaCtx *>::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = (*ite);
    while (ctx) {
      if (ctx->topic() == topic) return ctx;
      ctx = ctx->next();
    }
  }
  return 0;
}

DEFINE(loadCnf)
{
  static char errbuf[MAX_ERR_LEN];

  cnf = CnfCtx::loadCnf(ETCDIR, errbuf);
  check(cnf, "loadCnf %s", errbuf);

  check(cnf->host() == "zzyong", "cnf host %s", cnf->host().c_str());
  check(cnf->partition() == 0, "cnf partition %d", cnf->partition());
  check(cnf->getPollLimit() == 50, "cnf polllimit %d", cnf->getPollLimit());

  check(cnf->getKafkaGlobalConf().count("client.id"), "kafkaGlobalConf client.id notfound");
  check(cnf->getKafkaGlobalConf().find("client.id")->second == "tail2kafka", "kafkaGlobalConf client.id = %s", PTRS(cnf->getKafkaGlobalConf().find("client.id")->second));

  check(cnf->getKafkaTopicConf().count("request.required.acks"), "kafkaTopicConf request.required.acks notfound");
  check(cnf->getKafkaTopicConf().find("request.required.acks")->second == "1", "kafkaTopicConf request.required.acks = %s", PTRS(cnf->getKafkaTopicConf().find("request.required.acks")->second));

  check(cnf->rotateDelay_ == 10, "rotatedelay %d", cnf->rotateDelay_);
  check(cnf->pingbackUrl_ == "http://pingbackdst/pingback/tail2kafka", "pingbackUrl %s", cnf->pingbackUrl_.c_str());

  check(cnf->getLuaCtxSize() == LUACNF_SIZE, "%d", (int) cnf->getLuaCtxSize());
  for (std::vector<LuaCtx *>::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = (*ite);
    while (ctx) {
      check(ctx->cnf() == cnf, "%s", "luactx cnf error");
      ctx = ctx->next();
    }
  }
}

DEFINE(loadLuaCtx)
{
  LuaCtx *ctx;
  LuaFunction *function;

  ctx = getLuaCtx("basic");
  check(ctx, "%s", "basic not found");

  check(ctx->file() == LOG("basic.log"), "%s", PTRS(ctx->file()));
  check(ctx->topic() == "basic", "%s", PTRS(ctx->topic()));
  check(ctx->autoparti_, "%s", BTOS(ctx->autoparti_));
  check(ctx->partition_ == -1, "%d", ctx->partition_);
  check(ctx->autonl(), "%s", BTOS(ctx->autonl()));
  check(!ctx->rawcopy_, "%s", BTOS(ctx->rawcopy_));
  check(!ctx->fileWithTimeFormat_, "fileWithTimeFormat_ %s", BTOS(ctx->fileWithTimeFormat_));
  check(strcmp(ctx->getStartPosition(), "LOG_START") == 0, "%s", ctx->getStartPosition());
  check(access(ctx->file().c_str(), F_OK) == 0, "file %s autocreat but notfound", PTRS(ctx->file()));

  ctx = getLuaCtx("basic2");
  check(ctx, "%s", "basic2 notfound");

  check(ctx->autocreat_, "autocreat %s", BTOS(ctx->autocreat_));
  check(ctx->fileWithTimeFormat_, "fileWithTimeFormat_ %s", BTOS(ctx->fileWithTimeFormat_));
  std::string file = sys::timeFormat(time(0), LOG("basic.%Y-%m-%d_%H-%M.log"));
  check(access(file.c_str(), F_OK) == 0, "file %s autocreat but %s notfound", PTRS(ctx->file()), PTRS(file));

  ctx = getLuaCtx("filter");
  check(ctx, "%s", "filter not found");
  check(ctx->timeidx() == 4, "%d", ctx->timeidx());

  function = ctx->function();
  check(function->filters_.size() == 4, "filters size %d", (int) function->filters_.size());
  check(function->filters_[0] == 4, "filters #[%d]", function->filters_[0]);
  check(function->filters_[1] == 5, "filters #[%d]", function->filters_[1]);
  check(function->filters_[2] == 6, "filters #[%d]", function->filters_[2]);
  check(function->filters_[3] == -1, "filters #[%d]", function->filters_[3]);
  check(function->type_ == LuaFunction::FILTER, "function type %s, expect filter", LuaFunction::typeToString(function->type_));

  ctx = getLuaCtx("aggregate");
  check(ctx, "%s", "aggregate not found");
  check(ctx->withhost() == true, "%s", BTOS(ctx->withhost()));
  check(ctx->withtime() == true, "%s", BTOS(ctx->withtime()));
  check(ctx->timeidx() == 4, "%d", ctx->timeidx());

  function = ctx->function();
  check(function->type_ == LuaFunction::AGGREGATE, "function type %s, expect aggregate", LuaFunction::typeToString(function->type_));

  ctx = getLuaCtx("transform");
  check(ctx, "%s", "transform not found");

  function = ctx->function();
  check(function->type_ == LuaFunction::TRANSFORM, "function type %s, expect transform", LuaFunction::typeToString(function->type_));
}

DEFINE(filter)
{
  std::vector<std::string *> datas;
  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "GET / HTTP/1.0",
    "200", "-", "-", "95555"};

  LuaFunction *function = getLuaCtx("filter")->function();
  function->filter(0, std::vector<std::string>(fields1, fields1+9), &datas);
  check(datas.size() == 1, "datas size %d", (int) datas.size());
  check(*datas[0] == "*" + cnf->host() + "@" + std::string(PADDING_LEN, '0') + " 2015-04-02T12:05:05 GET / HTTP/1.0 200 95555", "%s", PTRS(*datas[0]));
  delete datas[0];
}

DEFINE(grep)
{
  std::vector<std::string *> datas;
  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "GET / HTTP/1.0",
    "200", "-", "-", "95555"};

  LuaFunction *function = getLuaCtx("grep")->function();
  function->grep(0, std::vector<std::string>(fields1, fields1+9), &datas);
  check(datas.size() == 1, "data size %d", (int) datas.size());
  check(*datas[0] == "*" + cnf->host() + "@" + std::string(PADDING_LEN, '0') + " [2015-04-02T12:05:05] \"GET / HTTP/1.0\" 200 95555", "%s", PTRS(*datas[0]));
  delete datas[0];
}

DEFINE(transform)
{
  std::vector<std::string *> datas;

  LuaCtx *ctx = getLuaCtx("transform");
  LuaFunction *function = ctx->function();

  function->transform(0, "[error] this", sizeof("[error] this")-1, &datas);
  check(datas.size() == 1, "data size %d", (int) datas.size());
  check(*datas[0] == "*" + cnf->host() + "@" + std::string(PADDING_LEN, '0') + " [error] this", "'%s'", PTRS(*datas[0]));
  delete datas[0]; datas.clear();

  ctx->withhost_ = false;
  function->transform(0, "[error] this", sizeof("[error] this")-1, &datas);
  check(datas.size() == 1, "data size %d", (int) datas.size());
  check("[error] this", "'%s'", PTRS(*datas[0]));
  delete datas[0]; datas.clear();

  function->transform(0, "[debug] that", sizeof("[debug] that")-1, &datas);
  check(datas.empty(), "data size %d", (int) datas.size());
}

DEFINE(aggregate)
{
  std::vector<std::string *> datas;

  LuaCtx *ctx = getLuaCtx("aggregate");
  LuaFunction *function = ctx->function();

  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:04", "-",
    "-", "-", "-", "200", "230",
    "0.1", "-", "-", "-", "-",
    "10086"};
  function->aggregate(std::vector<std::string>(fields1, fields1 + 16), &datas);
  check(datas.empty(), "%d", (int) datas.size());

  const char *fields2[] = {
    "-", "-", "-", "2015-04-02T12:05:04", "-",
    "-", "-", "-", "200", "270",
    "0.2", "-", "-", "-", "-",
    "10086"};
  function->aggregate(std::vector<std::string>(fields2, fields2 + 16), &datas);
  check(datas.empty(), "%d", (int) datas.size());

  const char *fields3[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "-",
    "-", "-", "-", "404", "250",
    "0.2", "-", "-", "-", "-",
    "95555"};
  function->aggregate(std::vector<std::string>(fields3, fields3 + 16), &datas);
  check(datas.size() == 2, "%d", (int) datas.size());

  const char *msg = "2015-04-02T12:05:04 10086 reqt<0.1=1 reqt<0.3=1 size=500 status_200=2";
  check(*datas[0] == cnf->host() + " " + msg, "%s", PTRS(*datas[0]));

  msg = "2015-04-02T12:05:04 yuntu reqt<0.1=1 reqt<0.3=1 size=500 status_200=2";
  check(*datas[1] == cnf->host() + " " + msg, "%s", PTRS(*datas[1]));
  delete datas[0]; delete datas[1];

  function->serializeCache(&datas);
  check(function->aggregateCache_.empty(), "cache size %d", (int) function->aggregateCache_.size());
}

DEFINE(initKafka)
{
  check(cnf->initKafka(), "%s", cnf->errbuf());

  check(cnf->kafka_->rk_, "rk_ == 0");
  check(cnf->kafka_->rkts_.size() == cnf->getLuaCtxSize(), "rkts size %d", (int) cnf->getLuaCtxSize());
}

DEFINE(initFileOff)
{
  check(cnf->initFileOff(), "%s", cnf->errbuf());
  check(cnf->fileOff_->file_ == cnf->libdir() + "/fileoff", "%s", PTRS(cnf->fileOff_->file_));
}

DEFINE(reinitFileOff)
{
  check(cnf->getFileOff()->reinit(), "%s", cnf->errbuf());
  check(cnf->fileOff_->length_ == cnf->getLuaCtxSize() * sizeof(FileOffRecord), "%d", (int) cnf->fileOff_->length_);

  LuaCtx *ctx = getLuaCtx("basic");
  ino_t inode = ctx->fileReader_->fileOffRecord_->inode;
  off_t off = ctx->fileReader_->fileOffRecord_->off;
  ctx->fileReader_->fileOffRecord_->off += 100;

  check(cnf->getFileOff()->loadFromFile(cnf->errbuf()), "fileoff load");
  check(cnf->getFileOff()->map_[inode] == off+100, "mmap not work");

  ctx->fileReader_->fileOffRecord_->off = off;
}

DEFINE(initFileReader)
{
  const char *c = "12\n456\n7890";
  int fd = open(LOG("basic.log"), O_WRONLY);
  write(fd, c, strlen(c));

  struct stat st;
  fstat(fd, &st);

  LuaCtx *ctx = getLuaCtx("basic");
  ctx->startPosition_ = "LOG_END";

  check(ctx->initFileReader(cnf->errbuf()), "%s", cnf->errbuf());
  check(ctx->fileReader_->file_ == LOG("basic.log"), "%s", ctx->fileReader_->file_.c_str());
  check(ctx->fileReader_->buffer_, "buffer init ok");
  check(ctx->fileReader_->npos_ == 0, "%d", (int) ctx->fileReader_->npos_);
  check(ctx->fileReader_->size_ == 7, "%d", (int) ctx->fileReader_->size_);
  SAFE_DELETE(ctx->fileReader_);

  cnf->fileOff_->map_.insert(std::make_pair(st.st_ino, 3));

  check(ctx->initFileReader(cnf->errbuf()), "%s", cnf->errbuf());
  check(ctx->fileReader_->npos_ == 0, "%d", (int) ctx->fileReader_->npos_);
  check(ctx->fileReader_->size_ == 3, "%d", (int) ctx->fileReader_->size_);
  SAFE_DELETE(ctx->fileReader_);

  cnf->fileOff_->map_[st.st_ino] = 7;
  ctx->startPosition_ = "LOG_START";
  check(ctx->initFileReader(cnf->errbuf()), "%s", cnf->errbuf());
  check(ctx->fileReader_->size_ == 7, "%d", (int) ctx->fileReader_->size_);
  SAFE_DELETE(ctx->fileReader_);

  cnf->fileOff_->map_.clear();

  ctx->startPosition_ = "START";
  check(ctx->initFileReader(cnf->errbuf()), "%s", cnf->errbuf());
  check(ctx->fileReader_->size_ == 0, "%d", (int) ctx->fileReader_->size_);
  SAFE_DELETE(ctx->fileReader_);

  ctx->startPosition_ = "LOG_END";
  ftruncate(fd, 0);

  check(cnf->initFileReader(), "%s", cnf->errbuf());
  check(ctx->fileReader_->size_ == 0, "empty file seek %d", (int) ctx->fileReader_->size_);

  const char *topics[] = {"basic", "basic2", "filter", "grep", "transform", "aggregate"};
  for (int i = 0; i < 5; ++i) {
    ctx = getLuaCtx(topics[i]);
    check(ctx->getFileReader()->fd_ >= 0, "%s fd_ %d", topics[i], ctx->getFileReader()->fd_);
  }
}

void *watchLoop(void *data)
{
  InotifyCtx *inotify = (InotifyCtx *) data;
  inotify->loop();
  return 0;
}

DEFINE(watchLoop)
{
  RunStatus *runStatus = RunStatus::create();
  runStatus->set(RunStatus::WAIT);
  cnf->setRunStatus(runStatus);

  LuaCtx *ctx = getLuaCtx("basic");
  ctx->withhost_ = true;

  InotifyCtx inotify(cnf);
  check(inotify.init(), "%s", cnf->errbuf());

  pthread_t tid;
  pthread_create(&tid, NULL, watchLoop, &inotify);

  check(ctx->getFileReader()->size_ == 0, "%d", (int) ctx->getFileReader()->size_);

  const char *s1 = "456";
  int fd = open(LOG("basic.log"), O_WRONLY);
  write(fd, s1, strlen(s1));

  cnf->pollLimit_ = 0;
  ctx->rawcopy_ = false;
  check(ctx->autonl(), "%s", BTOS(ctx->autonl()));

  const char *s2 = "\n\n789\n";   // test empty line
  write(fd, s2, strlen(s2));
  close(fd);

  time_t renameStartTime = cnf->fasttime(true);
  rename(LOG("basic.log"), LOG("basic.log.old"));

  OneTaskReq req;
  read(cnf->accept, &req, sizeof(OneTaskReq));

  // ignore memory leak
  std::vector<FileRecord *> *records = req.records;
  const std::string *ptr;

  check(records->size() == 3, "%d", (int) records->size());

  ptr = records->at(0)->data;
  check(ptr->substr(ptr->size() - 6) == "Start\n", "%s", PTRS(*ptr));
  ptr = records->at(1)->data;
  check(*ptr == "*" + cnf->host() + "@" + std::string(PADDING_LEN, '0') + " 456\n", "%s", PTRS(*ptr));
  ptr = records->at(2)->data;
  check(*ptr == "*" + cnf->host() + "@" + util::toStr(sizeof("456\n"), PADDING_LEN) + " 789\n", "%s", PTRS(*ptr));

  read(cnf->accept, &req, sizeof(OneTaskReq));
  records = req.records;

  check(records->size() == 1, "%d", (int) records->size());
  ptr = records->at(0)->data;
  check(ptr->find("End") != std::string::npos, "%s", PTRS(*ptr));

  sleep(1);
  for (std::map<int, LuaCtx*>::iterator ite = inotify.fdToCtx_.begin(); ite != inotify.fdToCtx_.end(); ++ite) {
    check(ite->second != ctx, "%s should be remove from inotify", PTRS(ctx->file()));
  }

  time_t renameEndTime = cnf->fasttime(true);
  check(renameEndTime - renameStartTime > cnf->rotateDelay_, "%d", (int) (renameEndTime - renameStartTime));

  cnf->pollLimit_ = 300;
  ctx->rawcopy_ = true;

  fd = open(LOG("basic.log"), O_CREAT | O_WRONLY, 0644);
  assert(fd != -1);
  write(fd, "abcd\nefg\n", sizeof("abcd\nefg\n")-1);
  close(fd);

  read(cnf->accept, &req, sizeof(OneTaskReq));
  records = req.records;

  check(records->size() == 2, "%d", (int) records->size());
  ptr = records->at(0)->data;
  check(ptr->substr(ptr->size() - 6) == "Start\n", "%s", PTRS(*ptr));
  ptr = records->at(1)->data;
  check(*ptr == "*" + cnf->host() + "@" + std::string(PADDING_LEN, '0') + " abcd\nefg\n", "%s", PTRS(*ptr));

  runStatus->set(RunStatus::STOP);
  pthread_join(tid, 0);
}

static const char *files[] = {
  LOG("basic.log"),
  LOG("filter.log"),
  LOG("aggregate.log"),
  LOG("grep.log"),
  LOG("transform.log"),
  0
};

DEFINE(prepare)
{
  mkdir(LOG(""), 0755);
  unlink("/var/lib/tail2kafka/fileoff");

  for (int i = 0; files[i]; ++i) {
    int fd = creat(files[i], 0644);
    if (fd != -1) close(fd);
  }
}

DEFINE(clean)
{
  for (int i = 0; files[i]; ++i) {
    unlink(files[i]);
  }
  unlink("./basic.log.old");
}

int main()
{
  DO(prepare);

  TEST(split);
  TEST(iso8601);

  TEST(loadCnf);
  TEST(loadLuaCtx);
  TEST(filter);
  TEST(grep);
  TEST(transform);
  TEST(aggregate);

  TEST(initKafka);
  TEST(initFileOff);
  TEST(initFileReader);
  TEST(reinitFileOff);
  TEST(watchLoop);

  DO(clean);

  if (cnf) delete cnf;
  return 0;
}
