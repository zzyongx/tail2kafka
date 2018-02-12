#include <string>
#include <map>

#include "sys.h"
#include "unittesthelper.h"
#include "logger.h"
#include "common.h"
#include "transform.h"

LOGGER_INIT();
UNITTEST_INIT();

#define WDIR       "kafka2filedir"
#define TOPIC      "nginx"
#define TOPICDIR   WDIR"/"TOPIC
#define PARTITION  "0"
#define LUAFILE(f) "blackboxtest/kafka2file/"f
#define LUALOGFILE(t, p, f) TOPICDIR"/"t"."p"_"f

static char errbuf[1024];

DEFINE(parseRequest)
{
  const char *request = "GET /pingback/tail2kafka?event=UPGRADE_ERROR&product=test.tail2kafka&error=upgrade%20config%20from%20to%200.0.2,%20reload%20failed HTTP/1.1";
  std::string method, path;
  std::map<std::string, std::string> query;
  bool rc = parseRequest(request, &method, &path, &query);

  check(rc, "parse %s error", request);
  check(method == "GET", "method %s", PTRS(method));
  check(path == "/pingback/tail2kafka", "path %s", PTRS(path));
  check(query["event"] == "UPGRADE_ERROR", "query['event'] %s", PTRS(query["event"]));
  check(query["product"] == "test.tail2kafka", "query['product'] %s", PTRS(query["product"]));
  check(query["error"] == "upgrade config from to 0.0.2, reload failed", "query['error'] %s", PTRS(query["error"]));
}

inline rd_kafka_message_t *initKafkaMessage(rd_kafka_message_t *rkm, const char *payload, uint64_t offset)
{
  rkm->payload = (void *) payload;
  rkm->len     = strlen(payload);
  rkm->offset  = offset;
  return rkm;
}

#define NGX_REQUEST "\"GET /pingback/tail2kafka?event=RELOAD HTTP/1.1\""
#define NGX_MSG_10_25_01 "[12/Feb/2018:10:25:01 +0800] " NGX_REQUEST
#define NGX_MSG_10_25_02 "[12/Feb/2018:10:25:02 +0800] " NGX_REQUEST
#define NGX_MSG_10_26_01 "[12/Feb/2018:10:26:01 +0800] " NGX_REQUEST
#define NGX_MSG_10_25_58 "[12/Feb/2018:10:25:58 +0800] " NGX_REQUEST
#define NGX_MSG_10_26_30 "[12/Feb/2018:10:26:30 +0800] " NGX_REQUEST
#define NGX_MSG_10_25_59 "[12/Feb/2018:10:25:59 +0800] " NGX_REQUEST
#define NGX_MSG_10_28_01 "[12/Feb/2018:10:28:01 +0800] " NGX_REQUEST
#define NGX_MSG_10_28_02 "[12/Feb/2018:10:28:02 +0800] " NGX_REQUEST
#define NGX_MSG_10_29_11 "[12/Feb/2018:10:29:11 +0800] " NGX_REQUEST

DEFINE(luaTransformLogRotate)
{
  LuaTransform *luaTransform = new LuaTransform(WDIR, TOPIC, atoi(PARTITION), 0);
  bool rc = luaTransform->init(Transform::NGINX, Transform::JSON, 60, 10, LUAFILE("test_rotate.lua"), errbuf);
  checkx(rc, "luaTransform.init error %s", errbuf);

  const char *msgs[] = {
    NGX_MSG_10_25_01, NGX_MSG_10_25_02,
    NGX_MSG_10_26_01, NGX_MSG_10_25_58, NGX_MSG_10_26_30, NGX_MSG_10_25_59,
    NGX_MSG_10_28_01, NGX_MSG_10_28_02,
    NGX_MSG_10_29_11, 0};

  bool *withTimeout = ENV_GET("WITH_TIMEOUT", bool *);

  uint64_t offset;
  rd_kafka_message_t rkm;
  for (int i = 0; msgs[i]; ++i) {
    printf("%s\n", msgs[i]);
    uint32_t flags = luaTransform->write(initKafkaMessage(&rkm, msgs[i], i), &offset);
    checkx(flags & Transform::RKMFREE, "luaFunction.write should return rkmfree");

    if (*withTimeout) {
      flags = luaTransform->timeout(&offset);
      checkx(!(flags & Transform::RKMFREE), "luaFunction.timeout should not return rkmfree");
    }
  }
  delete luaTransform;

  const char *f_10_25_00 = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-25-00");
  checkx(access(f_10_25_00, F_OK) == 0, "logfile 2018-02-12_10-25-00 notfound");

  std::vector<std::string> lines;
  sys::file2vector(f_10_25_00, &lines);
  checkx(lines.size() == 3, "file size error, %s, %d", f_10_25_00, (int) lines.size());
  checkx(lines[0] == "{\"time_local\":\"2018-02-12T10:25:01\"}", "line 0 error, %s", PTRS(lines[0]));
  checkx(lines[1] == "{\"time_local\":\"2018-02-12T10:25:02\"}", "line 1 error, %s", PTRS(lines[1]));
  checkx(lines[2] == "{\"time_local\":\"2018-02-12T10:25:58\"}", "line 0 error, %s", PTRS(lines[2]));

  const char *f_10_26_00 = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-26-00");
  checkx(access(f_10_26_00, F_OK) == 0, "logfile 2018-02-12_10-26-00 notfound");

  lines.clear();
  sys::file2vector(f_10_26_00, &lines);
  checkx(lines.size() == 2, "file size error, %s, %d", f_10_26_00, (int) lines.size());
  checkx(lines[0] == "{\"time_local\":\"2018-02-12T10:26:01\"}", "line 0 error, %s", PTRS(lines[0]));
  checkx(lines[1] == "{\"time_local\":\"2018-02-12T10:26:30\"}", "line 1 error, %s", PTRS(lines[1]));

  const char *f_10_27_00 = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-27-00");
  checkx(access(f_10_27_00, F_OK) != 0, "logfile 2018-02-12_10-27-00 found");

  const char *f_10_28_00 = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-28-00");
  checkx(access(f_10_28_00, F_OK) == 0, "logfile 2018-02-12_10-28-00 notfound");

  lines.clear();
  sys::file2vector(f_10_28_00, &lines);
  checkx(lines.size() == 2, "file size error, %s, %d", f_10_28_00, (int) lines.size());
  checkx(lines[0] == "{\"time_local\":\"2018-02-12T10:28:01\"}", "line 0 error, %s", PTRS(lines[0]));
  checkx(lines[1] == "{\"time_local\":\"2018-02-12T10:28:02\"}", "line 1 error, %s", PTRS(lines[1]));

  const char *f_10_29_00 = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-29-00");
  checkx(access(f_10_29_00, F_OK) != 0, "logfile 2018-02-12_10-29-00 found");

  const char *f_10_29_00_current = LUALOGFILE(TOPIC, PARTITION, "2018-02-12_10-29-00.current");
  checkx(access(f_10_29_00_current, F_OK) != 0, "logfile 2018-02-12_10-29-00.current found");
}

DEFINE(prepare)
{
  system("mkdir -p "TOPICDIR);
}

DEFINE(clean)
{
  system("rm -rf "TOPICDIR"/*");
}

int main()
{
  DO(prepare);

  TEST(parseRequest);

  bool withTimeout;
  ENV_SET("WITH_TIMEOUT", &withTimeout);

  withTimeout = true;
  DO(clean);
  TESTX(luaTransformLogRotate, "luaTransformLogRotateWithTimeout");

  withTimeout = false;
  DO(clean);
  TESTX(luaTransformLogRotate, "luaTransformLogRotateWithoutTimeout");
  return 0;
}
