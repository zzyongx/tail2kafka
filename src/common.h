#ifndef _COMMON_H_
#define _COMMON_H_

#include <climits>
#include <cassert>
#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include <algorithm>

static const int    UNSET_INT      = INT_MAX;

struct LuaCtx;

typedef std::map<std::string, int>         SIHash;
typedef std::map<std::string, SIHash>      SSIHash;
typedef std::vector<LuaCtx *>              LuaCtxPtrList;
typedef std::map<int, LuaCtx *>            WatchCtxHash;
typedef std::map<std::string, std::string> SSHash;

#define MAX_ERR_LEN    512

bool shell(const char *cmd, std::string *output, char *errbuf);
bool hostAddr(const std::string &host, uint32_t *addr, char *errbuf);
void split(const char *line, size_t nline, std::vector<std::string> *items);
bool iso8601(const std::string &t, std::string *iso);


inline int absidx(int idx, size_t total)
{
  assert(total != 0);
  return idx > 0 ? idx-1 : total + idx;
}

inline std::string to_string(int i)
{
  std::string s;
  do {
    s.append(1, i%10 + '0');
    i /= 10;
  } while (i);
  std::reverse(s.begin(), s.end());
  return s;
}

#endif
