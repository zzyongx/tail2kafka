#ifndef _GNUATOMIC_H_
#define _GNUATOMIC_H_

#include <stdint.h>

namespace util {

template <class IntegralType>
IntegralType atomic_get(IntegralType *ptr) {
  return __sync_add_and_fetch(ptr, 0);
}

template <class IntegralType>
IntegralType atomic_set(IntegralType *ptr, int val) {
  return __sync_lock_test_and_set(ptr, val);
}

template <class IntegralType>
IntegralType atomic_inc(IntegralType *ptr,  int val = 1) {
  return __sync_add_and_fetch(ptr, val);
}

template <class IntegralType>
IntegralType atomic_dec(IntegralType *ptr, int val = 1) {
  return __sync_sub_and_fetch(ptr, val);
}

template <class IntegralType>
IntegralType atomic_and(IntegralType *ptr, uint64_t val) {
  return __sync_and_and_fetch(ptr, val);
}

template <class IntegralType>
IntegralType atomic_or(IntegralType *ptr, uint64_t val) {
  return __sync_or_and_fetch(ptr, val);
}

}  // namespace util
#endif
