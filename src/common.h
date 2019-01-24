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
void splitn(const char *line, size_t nline, std::vector<std::string> *items,
            int limit = -1, char delimiter = ' ');
bool timeLocalToIso8601(const std::string &t, std::string *iso, time_t *timestamp = 0);
bool parseIso8601(const std::string &t, time_t *timestamp);

inline time_t mktime(int year, int mon, int day, int hour, int min, int sec)
{
  struct tm tm;
  tm.tm_sec  = sec;
  tm.tm_min  = min;
  tm.tm_hour = hour;
  tm.tm_mday = day;
  tm.tm_mon  = mon-1;
  tm.tm_year = year-1900;

  return mktime(&tm);
}

bool parseRequest(const char *ptr, std::string *method, std::string *path, std::map<std::string, std::string> *query);

inline int absidx(int idx, size_t total)
{
  assert(total != 0);
  return idx > 0 ? idx-1 : total + idx;
}

#endif
