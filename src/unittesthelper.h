#include <stdio.h>
#include <stdlib.h>

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

#define check(r, fmt, arg...) do {                                                  \
  if ((r)) break;                                                                   \
  fprintf(stderr, "%-5d %s -> ["COLOR_RED fmt COLOR_RESET"]\n", __LINE__, #r, ##arg); \
  exit(1);                                                                          \
} while(0)

#define DEFINE(name)  struct TEST_##name {};                        \
  template<> struct UNITTEST_HELPER<TEST_##name> { void call(); };  \
  void UNITTEST_HELPER<TEST_##name>::call()

#define TEST_IMPL(name, t) do {                                        \
  UNITTEST_HELPER<TEST_##name> test_##name;                            \
  test_##name.call();                                                  \
  if (t) printf("TEST %-20s ["COLOR_GREEN"OK"COLOR_RESET"]\n", #name); \
} while(0)

#define DO(name) TEST_IMPL(name, false)
#define TEST(name) TEST_IMPL(name, true)
