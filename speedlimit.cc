#include <cstdio>
#include <cstdlib>
#include <time.h>
#include <assert.h>
#include <unistd.h>

static void microsleep(int ms)
{
  struct timespec spec = {0, ms * 1000};
  nanosleep(&spec, NULL);
}

int main(int argc, char *argv[])
{
  if (argc != 2) {
    fprintf(stderr, "%s limit(MB)\n", argv[0]);
    return EXIT_FAILURE;
  }

  int limit = atoi(argv[1]);
  if (limit > 500) {
    fprintf(stderr, "limit must <= 500\n");
    return EXIT_FAILURE;
  }
  limit *= 1024 * 1024;

  size_t N = 4096;
  size_t ms = 1000 * 1000 / (limit/N + 1);
  
  char buffer[N];
  ssize_t nn;
  while ((nn = read(STDIN_FILENO, buffer, N)) > 0) {
    ssize_t left = nn;
    while (left > 0) {
      ssize_t nw = write(STDOUT_FILENO, buffer + nn - left, left);
      assert(nw > 0);
      left -= nw;
    }
    microsleep(ms);
  }

  assert(nn >= 0);

  return EXIT_SUCCESS;
}
