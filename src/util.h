#ifndef _UTIL_H_
#define _UTIL_H_

#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

namespace util {

bool split(const char *str, char sp, std::vector<int> *list);

template <class Iterator>
std::string join(Iterator begin, Iterator end, char sp)
{
  bool first = true;
  std::string s;
  for (Iterator ite = begin; ite != end; ++ite) {
    if (!first) s.append(1, sp);
    s.append(*ite);
    first = false;
  }
  return s;
}

template <class T>
bool hexToInt(const char *ptr, T *val)
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

#define HEXMAP "0123456789abcdef"
inline const char *binToHex(const unsigned char *bin, size_t len, char *buffer)
{
  const static char *hexmap = HEXMAP;
  char *ptr = buffer;
  for (size_t i = 0; i < len; ++i) {
    *ptr++ = hexmap[(bin[i] >> 4)];
    *ptr++ = hexmap[bin[i] & 0x0F];
  }
  *ptr = '\0';
  return buffer;
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

std::string trim(const std::string &str, bool left = true, bool right = true, const char *space = " \t\n");
std::string &replace(std::string *s, char o, char n);

} // namespace util

#endif
