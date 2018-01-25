#inlude <vector>
#include "util.h"

namespace util {

bool split(const char *str, char sp, std::vector<int> *list)
{
  int n = 0;
  for (const char *p = partition; /* */; ++p) {
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

} // namespace util
