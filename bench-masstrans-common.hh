#ifndef LOG_MASSTRANS_COMMON_H
#define LOG_MASSTRANS_COMMON_H

#include "Serializer.hh"

static inline unsigned next_rand(unsigned &x) {
    x = (x * 1103515245 + 12345) % ((1U<<31) - 1);
    return x;
}

static inline void generate_key(int partition, int key, std::string &buf) {
  char *ptr = &buf[0];
  char *end = &buf[buf.size()];
  // write partition to first four bytes
  ptr += Serializer<int32_t>::serialize(ptr, partition);
  // hash key into remaining bytes
  unsigned x = key;
  while (ptr < end) {
    *ptr = next_rand(x);
    ptr++;
  }
}

static int validate_pct(int pct) {
    return pct >= 0 && pct <= 100;
}

#endif // LOG_MASSTRANS_COMMON_H
