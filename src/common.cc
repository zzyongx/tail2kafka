#include <cstdio>
#include <cstring>
#include <string>
#include <errno.h>
#include <unistd.h>
#include <netdb.h>

#include "util.h"
#include "common.h"

bool shell(const char *cmd, std::string *output, char *errbuf)
{
  FILE *fp = popen(cmd, "r");
  if (!fp) {
    snprintf(errbuf, MAX_ERR_LEN, "%s exec error", cmd);
    return false;
  }

  char buf[256];
  while (fgets(buf, 256, fp)) {
    output->append(buf);
  }

  int status = pclose(fp);
  if (status != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "%s exit %d", cmd, status);
    return false;
  }

  output->assign(util::trim(*output));
  return true;
}

bool hostAddr(const std::string &host, uint32_t *addr, char *errbuf)
{
  struct addrinfo *ai;
  struct addrinfo  hints;

  memset(&hints, 0x00, sizeof(hints));
  hints.ai_family = AF_INET;

  int rc = getaddrinfo(host.c_str(), NULL, &hints, &ai);
  if (rc != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "getaddrinfo() %s error %s\n",
             host.c_str(), rc == EAI_SYSTEM ? strerror(errno) : gai_strerror(rc));
    return false;
  }

  struct sockaddr_in *in = (struct sockaddr_in *) ai->ai_addr;
  *addr = in->sin_addr.s_addr;
  freeaddrinfo(ai);
  return true;
}

void split(const char *line, size_t nline, std::vector<std::string> *items, char delimiter)
{
  bool esc = false;
  char want = '\0';
  size_t pos = 0;

  for (size_t i = 0; i < nline; ++i) {
    if (esc) {
      esc = false;
    } else if (line[i] == '\\') {
      esc = true;
    } else if (want == '"') {
      if (line[i] == '"') {
        want = '\0';
        items->push_back(std::string(line + pos, i-pos));
        pos = i+1;
      }
    } else if (want == ']') {
      if (line[i] == ']') {
        want = '\0';
        items->push_back(std::string(line + pos, i-pos));
        pos = i+1;
      }
    } else {
      if (line[i] == '"') {
        want = line[i];
        pos++;
      } else if (line[i] == '[') {
        want = ']';
        pos++;
      } else if (line[i] == delimiter) {
        if (i != pos) items->push_back(std::string(line + pos, i - pos));
        pos = i+1;
      }
    }
  }
  if (pos != nline) items->push_back(std::string(line + pos, nline - pos));
}

enum DateTimeStatus { WaitYear, WaitMonth, WaitDay, WaitHour, WaitMin, WaitSec };

static const char *MonthAlpha[12] = {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

// 28/Feb/2015:12:30:23 +0800 -> 2015-03-30T16:31:53
bool timeLocalToIso8601(const std::string &t, std::string *iso, time_t *time)
{
  DateTimeStatus status = WaitDay;
  int year, mon, day, hour, min, sec;
  year = mon = day = hour = min = sec = 0;

  const char *p = t.c_str();
  while (*p && *p != ' ') {
    if (*p == '/') {
      if (status == WaitDay) status = WaitMonth;
      else if (status == WaitMonth) status = WaitYear;
      else return false;
    } else if (*p == ':') {
      if (status == WaitYear) status = WaitHour;
      else if (status == WaitHour) status = WaitMin;
      else if (status == WaitMin) status = WaitSec;
      else return false;
    } else if (*p >= '0' && *p <= '9') {
      int n = *p - '0';
      if (status == WaitYear) year = year * 10 + n;
      else if (status == WaitDay) day = day * 10 + n;
      else if (status == WaitHour) hour = hour * 10 + n;
      else if (status == WaitMin) min = min * 10 + n;
      else if (status == WaitSec) sec = sec * 10 + n;
      else return false;
    } else if (status == WaitMonth) {
      size_t i;
      for (i = 0; i < 12; ++i) {
        if (strncmp(p, MonthAlpha[i], 3) == 0) {
          mon = i+1;
          break;
        }
      }
    } else {
      return false;
    }
    p++;
  }

  iso->reserve(sizeof("yyyy-mm-ddThh:mm:ss"));
  iso->resize(sizeof("yyyy-mm-ddThh:mm:ss")-1);
  sprintf((char *) iso->data(), "%04d-%02d-%02dT%02d:%02d:%02d",
          year, mon, day, hour, min, sec);

  if (time) *time = mktime(year, mon, day, hour, min, sec);
  return true;
}

// 2018-02-22 17:40:00.000
bool parseIso8601(const std::string &t, time_t *timestamp)
{
  DateTimeStatus status = WaitYear;
  int year, mon, day, hour, min, sec;
  year = mon = day = hour = min = sec = 0;

  const char *p = t.c_str();
  while (*p && *p != '.') {
    if (*p == '-') {
      if (status == WaitYear) status = WaitMonth;
      else if (status == WaitMonth) status = WaitDay;
      else return false;
    } else if (*p == ' ' || *p == 'T') {
      if (status == WaitDay) status = WaitHour;
      else return false;
    } else if (*p == ':') {
      if (status == WaitHour) status = WaitMin;
      else if (status == WaitMin) status = WaitSec;
      else return false;
    } else if (*p >= '0' && *p <= '9') {
      int n = *p - '0';
      if (status == WaitYear) year = year * 10 + n;
      else if (status == WaitMonth) mon = mon * 10 + n;
      else if (status == WaitDay) day = day * 10 + n;
      else if (status == WaitHour) hour = hour * 10 + n;
      else if (status == WaitMin) min = min * 10 + n;
      else if (status == WaitSec) sec = sec * 10 + n;
      else return false;
    } else {
      return false;
    }
    p++;
  }
  if (status != WaitSec) return false;

  *timestamp = mktime(year, mon, day, hour, min, sec);
  return true;
}

bool parseQuery(const char *r, size_t len, std::string *path, std::map<std::string, std::string> *query)
{
  size_t i = 0;
  const char *ptr = r;

  while (i < len && *ptr && *ptr != '?') {
    ++ptr;
    ++i;
  }
  path->assign(r, ptr - r);

  if (i >= len || !*ptr) return true;

  ++i;
  ++ptr;

  std::string key, value;
  bool wantKey = true;
  while (i < len && *ptr) {
    if (*ptr == '&') {
      if (!key.empty() && !value.empty()) (*query)[key] = value;
      key.clear();
      value.clear();
      wantKey = true;
    } else if (*ptr == '=') {
      wantKey = false;
    } else {
      if (wantKey) {
        key.append(1, *ptr);
      } else {
        if (*ptr == '%' && (i+2 < len && *(ptr+2))) {
          int val;
          if (util::hexToInt(ptr+1, &val)) {
            value.append(1, val);
            i   += 2;
            ptr += 2;
          } else {
            value.append(1, *ptr);
          }
        } else {
          value.append(1, *ptr);
        }
      }
    }
    ++ptr;
    ++i;
  }

  if (!key.empty() && !value.empty()) (*query)[key] = value;
  return true;
}

// GET /path[?k=v] HTTP/1.1
bool parseRequest(const char *r, std::string *method, std::string *path, std::map<std::string, std::string> *query)
{
  const char *fsp = strchr(r, ' ');
  const char *lsp = strrchr(r, ' ');

  if (!fsp || !lsp || lsp <= fsp+1) return false;
  method->assign(r, fsp - r);

  return parseQuery(fsp+1, lsp-(fsp+1), path, query);
}
