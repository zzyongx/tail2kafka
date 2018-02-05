#ifndef _BITS_HELPER_H_
#define _BITS_HELPER_H_

#define bits_set(flags, bit) (flags) |= (bit)
#define bits_clear(flags, bit) (flags) &= ~(bit)
#define bits_test(flags, bit) ((flags) & (bit))

#endif
