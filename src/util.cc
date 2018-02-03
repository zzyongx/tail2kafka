#include <vector>
#include "util.h"

namespace util {

bool split(const char *str, char sp, std::vector<int> *list)
{
  int n = 0;
  for (const char *p = str; /* */; ++p) {
    if (!*p || *p == sp) {
      list->push_back(n);
      if (!*p) break;
      n = 0;
    } else if (*p >= '0' && *p <= '9') {
      n = n * 10 + *p - '0';
    } else {
      return false;
    }
  }
  return true;
}

std::string trim(const std::string &str, bool left, bool right, const char *space)
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
