#include <cstdio>
#include <cstdlib>
#include <string>
#include <map>

#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_RESET   "\x1b[0m"

template<class T>
struct UNITTEST_HELPER {
  void call() {}
};

#define BTOS(b) ((b) ? "TRUE" : "FALSE")
#define PTRS(s) ((s).c_str())

#define SAFE_DELETE(ptr) do { delete ptr; (ptr) = 0; } while (0)

#define CHECK_IMPL(r, name, fmt, arg...) do {                      \
  if ((r)) break;                                                  \
  fprintf(stderr, "%s@%-5d %s -> ["COLOR_RED fmt COLOR_RESET"]\n", \
          name, __LINE__, #r, ##arg);                              \
  assert((r));                                                    \
} while(0)

#define check(r, fmt, arg...)  CHECK_IMPL(r, __FILE__, fmt, ##arg)
#define checkx(r, fmt, arg...) CHECK_IMPL(r, TEST_NAME_, fmt, ##arg)

inline
void *env_safe_get(const std::map<std::string, void *> &map,
                   const std::string &key, void *def) {
  std::map<std::string, void *>::const_iterator pos = map.find(key);
  if (pos == map.end()) {
    if (def) return def;
    fprintf(stderr, COLOR_RED "ENV %s notfound" COLOR_RESET, key.c_str());
    exit(1);
  } else {
    return pos->second;
  }
}

#define UNITTEST_INIT() static std::map<std::string, void *> UNITTEST_ENV
#define ENV_SET(key, value) UNITTEST_ENV[(key)] = (value)
#define ENV_GET(key, type) (type) (env_safe_get(UNITTEST_ENV, (key), 0))

#define DEFINE(func)  struct TEST_##func {};                        \
  template<> struct UNITTEST_HELPER<TEST_##func> {                  \
    UNITTEST_HELPER(const char *name) : TEST_NAME_(name) {}         \
    void call(); const char *TEST_NAME_; };                         \
  void UNITTEST_HELPER<TEST_##func>::call()

#define TEST_IMPL(func, name, t) do {                                  \
  UNITTEST_HELPER<TEST_##func> test_##func(name); test_##func.call();  \
  if (t) printf("TEST %-60s ["COLOR_GREEN"OK"COLOR_RESET"]\n", name); \
} while(0)

#define DO(func)          TEST_IMPL(func, #func, false)
#define TEST(func)        TEST_IMPL(func, #func, true)
#define TESTX(func, name) TEST_IMPL(func, name, true)
