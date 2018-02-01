#ifndef _GNUATOMIC_H_
#define _GNUATOMIC_H_

namespace util {

template <class IntegralType>
IntegralType atomic_get(IntegralType *ptr) {
  return __sync_add_and_fetch(ptr, 0);
}

template <class IntegralType>
IntegralType atomic_set(IntegralType *ptr, IntegralType val) {
  return __sync_lock_test_and_set(ptr, val);
}

template <class IntegralType>
void atomic_inc(IntegralType *ptr, int val = 1) {
  __sync_add_and_fetch(ptr, val);
}

template <class IntegralType>
void atomic_dec(IntegralType *ptr, int val = 1) {
  __sync_sub_and_fetch(ptr, val);
}

}  // namespace util
#endif
