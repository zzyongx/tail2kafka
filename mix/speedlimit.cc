#include <cstdio>
#include <cstdlib>
#include <time.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

static void microsleep(float ms)
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

  size_t N = 1024 * 32;
  float micros = 1000 * 1000 / (limit/N + 1);
  int phase = 0;
  int total = 0;
  time_t start = time(0);

  char buffer[N];
  ssize_t nn;
  while ((nn = read(STDIN_FILENO, buffer, N)) > 0) {
    ssize_t left = nn;
    while (left > 0) {
      ssize_t nw = write(STDOUT_FILENO, buffer + nn - left, left);
      assert(nw > 0);
      left -= nw;
    }
    
    phase += nn;
    total += nn;
    if (phase * 5 > limit) {
      time_t end = time(0);
      if (total > (end - start) * limit) {
        micros *= 1.1;
      } else {
        if (micros != 0) micros /= 1.1;
      }
      phase = 0;
    }
    microsleep(micros);
  }

  assert(nn >= 0);

  return EXIT_SUCCESS;
}
