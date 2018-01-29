#ifndef _UTIL_H_
#define _UTIL_H_

#include <string>
#include <vector>
#include <algorithm>

namespace util {

bool split(const char *str, char sp, std::vector<int> *list);

inline int toInt(const char *ptr, size_t maxlen = -1)
{
  int i = 0;
  size_t len = 0;
  while (len < maxlen && *ptr) {
    i = i * 10 + *ptr - '0';
  }
  return i;
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

} // namespace util

#endif
