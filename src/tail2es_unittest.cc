#include <cstdio>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "logger.h"
#include "unittesthelper.h"
#include "sys.h"
#include "luactx.h"
#include "cnfctx.h"

LOGGER_INIT();

static CnfCtx *cnf = 0;

#define LUACNF_SIZE 2
#define ETCDIR "blackboxtest/tail2es"
#define LOG(f) "logs/"f

static LuaCtx *getLuaCtx(const char *file)
{
  for (std::vector<LuaCtx *>::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    LuaCtx *ctx = (*ite);
    while (ctx) {
      if (ctx->file() == file) return ctx;
      ctx = ctx->next();
    }
  }
  return 0;
}

static std::string fileGetContent(const char *file)
{
  FILE *fp = fopen(file, "r");
  check(fp, "read file %s error: %s", file, strerror(errno));

  std::string content;
  char buf[512];
  while (fgets(buf, 512, fp)) {
    content.append(buf);
  }
  fclose(fp);

  return content;
}

DEFINE(transformEsDocNginxLog)
{
  std::string log = fileGetContent(ETCDIR"/nginx_log.log");
  std::string *doc = new std::string;
  LuaFunction::transformEsDocNginxLog(log, doc);
  std::string expectDoc = "{\"y\":\"\\ufffd\\ufffd\"}";
  check(*doc == expectDoc, "got %s, expect %s", PTRS(*doc), PTRS(expectDoc));
  delete doc;
}

DEFINE(transformEsDocNginxJson)
{
  std::string log = fileGetContent(ETCDIR"/nginx_json.log");
  std::string *doc = new std::string;
  LuaFunction::transformEsDocNginxJson(log, doc);
  std::string expectDoc = "{\"receiver\":\"bb_up\"}";
  check(*doc == expectDoc, "got %s, expect %s", PTRS(*doc), PTRS(expectDoc));
  delete doc;
}

DEFINE(loadCnf)
{
  static char errbuf[MAX_ERR_LEN];

  cnf = CnfCtx::loadCnf(ETCDIR, errbuf);
  check(cnf, "loadCnf %s", errbuf);

  cnf->fasttime(true, TIMEUNIT_MILLI);
  check(cnf->getLuaCtxSize() == LUACNF_SIZE, "%d", (int) cnf->getLuaCtxSize());
}

DEFINE(loadLuaCtx)
{
  LuaCtx *ctx;
  LuaFunction *function;

  ctx = getLuaCtx(LOG("basic.log"));
  check(ctx, "%s", "basic not found");

  std::string esIndex;
  bool esIndexWithTimeFormat;
  int esIndexPos, esDocPos, esDocDataFormat;
  ctx->es(&esIndex, &esIndexWithTimeFormat, &esIndexPos, &esDocPos, &esDocDataFormat);

  check(esIndex == "_%F", "%s", PTRS(esIndex));
  check(esIndexWithTimeFormat, "%s", BTOS(esIndexWithTimeFormat));
  check(esIndexPos == 1, "%d", esIndexPos);
  check(esDocPos == 3, "%d", esDocPos);
  check(esDocDataFormat == ESDOC_DATAFORMAT_NGINX_LOG, "%d", esDocDataFormat);

  function = ctx->function();
  check(function->type_ == LuaFunction::ESPLAIN, "function type %s, expect esplain", LuaFunction::typeToString(function->type_));

  ctx = getLuaCtx(LOG("indexdoc.log"));
  check(ctx, "%s", "indexdoc not found");

  function = ctx->function();
  check(function->type_ == LuaFunction::INDEXDOC, "function type %s, expect indexdoc", LuaFunction::typeToString(function->type_));
}

#define CONTENT_LENGTH_HEADER                         \
  "HTTP/1.1 400 Bad Request\r\n"                      \
  "content-type: application/json; charset=UTF-8\r\n" \
  "content-length: 175\r\n"

#define CONTENT_BODY_PART1 \
"{\"_index\":\"indexdoc\",\"_type\":\"_doc\",\"_id\":\"lmBMN28B-3LtigQc8FLf\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}"

#define CONTENT_BODY_PART2 ",\"_seq_no\":0,\"_primary_term\":1}"

DEFINE(httpProtocol_1)
{
  std::vector<std::string> v;
  v.push_back("127.0.0.1:9200");
  EsUrl url(v, 0);
  url.respWant_ = STATUS_LINE;
  url.resp_ = url.header_;

  strcpy(url.header_, CONTENT_LENGTH_HEADER);
  url.offset_ = sizeof(CONTENT_LENGTH_HEADER)-1;

  url.initHttpResponse(url.header_ + url.offset_);
  check(url.respWant_ == HEADER, "parse part header error");
  check(url.respCode_ == 400, "http status error %d", url.respCode_);

  strcpy(url.header_, CONTENT_LENGTH_HEADER "\r\n");
  url.offset_ = sizeof(CONTENT_LENGTH_HEADER)-1 + 2;

  url.initHttpResponse(url.header_ + url.offset_);
  check(url.respWant_ == BODY, "parse want %d", url.respWant_);
  check(url.wantLen_ == 175, "content length error %d", int(url.wantLen_));

  strcpy(url.header_, CONTENT_BODY_PART1 CONTENT_BODY_PART2);
  url.offset_ = strlen(url.header_);

  url.initHttpResponse(url.header_ + url.offset_);
  check(url.respWant_ == RESP_EOF, "parse want %d", url.respWant_);
  check(url.respBody_ == CONTENT_BODY_PART1 CONTENT_BODY_PART2, "content error");
}

DEFINE(basic)
{
  std::vector<FileRecord *> datas;

  LuaCtx *ctx = getLuaCtx(LOG("basic.log"));
  LuaFunction *function = ctx->function_;

  time_t now = time(0);
  struct tm ltm;
  localtime_r(&now, &ltm);

  char index[64];
  strftime(index, 64, "basic_%F", &ltm);

  const char *s1 = "basic IP {\x22x\x22: 1}";
  const char *json = "{\"x\": 1}";
  function->process(0, s1, strlen(s1), &datas);
  check(datas.size() == 1, "datas size %d", (int) datas.size());
  check(*datas[0]->esIndex == index, "expect %s, got %s", index, PTRS(*datas[0]->esIndex));
  check(*datas[0]->data == "{\"x\": 1}", "expect %s, got %s", json, PTRS(*datas[0]->data));
}

DEFINE(indexdoc)
{
  std::vector<FileRecord *> datas;

  LuaCtx *ctx = getLuaCtx(LOG("indexdoc.log"));
  LuaFunction *function = ctx->function_;

  const char *s1 = "{\"x\": 1}";
  check(function->process(0, s1, strlen(s1), &datas) > 0, "indexdoc error %s", cnf->errbuf());
  check(datas.size() == 1, "data size %d", (int) datas.size());
  check(*datas[0]->esIndex == "indexdoc", "expect indexdoc, got %s", PTRS(*datas[0]->esIndex));
  check(*datas[0]->data == s1, "expect %s, got %s", s1, PTRS(*datas[0]->data));
}

DEFINE(initEs)
{
  check(cnf->initEs(), "%s", cnf->errbuf());

  EsCtx *es = cnf->getEs();
  for (std::vector<EsSender *>::iterator ite = es->esSenders_.begin();
       ite != es->esSenders_.end(); ++ite) {
    check((*ite)->epfd_ > 0, "init epoll error");
    check((*ite)->running_, "es is not running");
  }
}

DEFINE(esProduce)
{
  LuaCtx *ctx = getLuaCtx(LOG("basic.log"));

  char json[64];
  long x = 0;
  std::vector<FileRecord *> datas;
  for (int i = 0; i < 1000; ++i) {
    x = random();
    snprintf(json, 64, "{\x22x\x22: %ld, \x22timestamp\x22: %ld}", x, cnf->fasttime(true, TIMEUNIT_MILLI));

    std::string *index = new std::string("indexdoc");
    std::string *data = new std::string(json);
    FileRecord *record = FileRecord::create(0, 0, index, data);
    record->ctx = ctx;
    datas.assign(1, record);

    cnf->getEs()->produce(&datas);
    if (cnf->getKafkaBlock()) {
      printf("too much data, sleep 1\n");
      sleep(1);
    } else {
      sys::millisleep(1);
    }
  }

  bool esOk = false;
  char cmd[256];
  for (int i = 0; i < 5 * 1000; ++i) {
    snprintf(cmd, 256, "curl -Ss http://127.0.0.1:9200/indexdoc/_doc/_search?sort=timestamp:desc | grep -q %ld", x);
    int status;
    BASH(cmd, status);
    if (status == 0) {
      esOk = true;
      break;
    } else {
      printf("wait tail2es #%d\n", i);
      sys::millisleep(1);
    }
  }
  check(esOk, "expect %s in es, got nothing, use command: %s", json, cmd);
}

static const char *files[] = {
  LOG("basic.log"),
  LOG("indexdoc.log"),
  0
};

DEFINE(prepare)
{
  mkdir(LOG(""), 0755);

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
}

TEST_RUN(tail2es)
{
  DO(prepare);

  TEST(transformEsDocNginxLog);
  TEST(transformEsDocNginxJson);

  TEST(loadCnf);
  TEST(loadLuaCtx);
  TEST(basic);
  TEST(indexdoc);

  TEST(httpProtocol_1);

  TEST(initEs);
  TEST(esProduce);

  DO(clean);
  if (cnf) delete cnf;
}

int main() {
  UNITTEST_RUN(tail2es);
  printf("OK\n");
  return 0;
}
