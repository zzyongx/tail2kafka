#include <string>
#include <map>

#include "unittesthelper.h"
#include "logger.h"
#include "common.h"

LOGGER_INIT();

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

int main()
{
  TEST(parseRequest);
  return 0;
}
