#ifndef _UTIL_H_
#define _UTIL_H_

#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

namespace util {

bool split(const char *str, char sp, std::vector<int> *list);

inline bool hexToInt(const char *ptr, int *val)
{
  *val = 0;
  for (int i = 0; i < 2; ++i) {
    int v;
    if (ptr[i] >= '0' && ptr[i] <= '9') v = ptr[i] - '0';
    else if (ptr[i] >= 'a' && ptr[i] <= 'f') v = ptr[i] - 'a' + 10;
    else if (ptr[i] >= 'A' && ptr[i] <= 'F') v = ptr[i] - 'A' + 10;
    else return false;

    *val = *val * 16 + v;
  }
  return true;
}

inline int toInt(const char *ptr, size_t maxlen = -1)
{
  int i = 0;
  size_t len = 0;
  while (len++ < maxlen && *ptr) {
    i = i * 10 + *ptr - '0';
    ++ptr;
  }
  return i;
}

inline long toLong(const char *ptr, size_t maxlen = -1)
{
  long l = 0;
  size_t len = 0;
  while (len++ < maxlen && *ptr) {
    l = l * 10 + *ptr - '0';
    ++ptr;
  }
  return l;
}

template <class IntType>
inline std::string toStr(IntType i, int len = -1, char padding = '0')
{
  std::string s;
  do {
    s.append(1, i%10 + '0');
    i /= 10;
  } while (i);

  if (len > 0 && (int) s.size() < len) s.append(len - (int) s.size(), padding);

  std::reverse(s.begin(), s.end());
  return s;
}

inline std::string trim(const std::string &str, bool left = true, bool right = true, const char *space = " \t\n")
{
  std::string s;
  size_t start = 0, end = str.size();
  for (size_t i = 0; left && i < str.size(); ++i) {
    if (!strchr(space, str[i])) {
      start = i;
      break;
    }
  }

  for (size_t i = str.size(); right && i > start; --i) {
    if (!strchr(space, str[i-1])) {
      end = i;
      break;
    }
  }

  if (start < end) return str.substr(start, end - start);
  else return "";
}

} // namespace util

#endif
