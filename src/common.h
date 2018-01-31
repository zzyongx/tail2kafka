#ifndef _COMMON_H_
#define _COMMON_H_

#include <climits>
#include <cassert>
#include <stdint.h>
#include <time.h>
#include <map>
#include <string>
#include <vector>
#include <algorithm>

static const int    UNSET_INT      = INT_MAX;

struct LuaCtx;
typedef std::vector<LuaCtx *>              LuaCtxPtrList;

#define MAX_ERR_LEN    512

bool shell(const char *cmd, std::string *output, char *errbuf);
bool hostAddr(const std::string &host, uint32_t *addr, char *errbuf);
void split(const char *line, size_t nline, std::vector<std::string> *items);
bool iso8601(const std::string &t, std::string *iso, time_t *timestamp = 0);

bool parseRequest(const char *ptr, std::string *method, std::string *path, std::map<std::string, std::string> *query);

inline int absidx(int idx, size_t total)
{
  assert(total != 0);
  return idx > 0 ? idx-1 : total + idx;
}

#endif
