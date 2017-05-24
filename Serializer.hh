#pragma once

#include <iostream>
#include <cstring>
#include "compiler.hh"
#include "masstree-beta/str.hh"

#define USE_VARINT

template <typename T, bool simple = mass::is_trivially_copyable<T>::value>
class Serializer {
public:
    static int size(const T &obj);
    static int serialize(char *buf, const T &src);
    static int deserialize(const char *buf, T &dst);
};

template<typename T>
class Serializer<T, true> {
public:
    static_assert(!std::is_same<T, std::string>::value, "bug");
    static_assert(!std::is_same<T, lcdf::Str>::value, "bug");

    static int size(const T &obj) {
        (void) obj;
        return sizeof(T);
    }
    static int serialize(char *buf, const T &src) {
        *(T *) buf = src;
        return size(src);
    }
    static int orig_size(const char *buf) {
        (void) buf;
        return sizeof(T);
    }
    static int deserialize(const char *buf, T &dst) {
        dst = *(T *) buf;
        return size(dst);
    }
};

#ifdef USE_VARINT
template<>
class Serializer<uint64_t, true> {
    static int nbytes(uint64_t obj) {
        if (obj == 0)
            return 0;
        return (64 - __builtin_clzll(obj) + 7) / 8;
    }

public:
    static int size(const uint64_t &obj) {
        return 1 + nbytes(obj);
    }
    static int serialize(char *buf, const uint64_t &src) {
        uint8_t *b = (uint8_t *) buf;
        uint64_t in = src;
        int nb = nbytes(in);
        *b++ = nb;
        for (int i = 0; i < nb; i++) {
            b[i] = in >> (i * 8);
        }
        return size(src);
    }
    static int orig_size(const char *buf) {
        (void) buf;
        return sizeof(uint64_t);
    }
    static int deserialize(const char *buf, uint64_t &dst) {
        const uint8_t *b = (const uint8_t *) buf;
        int nb = *b++;
        uint64_t out = 0;
        for (int i = 0; i < nb; i++) {
            out |= ((uint64_t) b[i]) << (i * 8);
        }
        dst = out;
        return size(dst);
    }
};

template<>
class Serializer<int64_t, true> {
public:
    static int size(const int64_t &obj) {
        return Serializer<uint64_t>::size(obj);
    }
    static int serialize(char *buf, const int64_t &src) {
        return Serializer<uint64_t>::serialize(buf, *(const uint64_t *) &src);
    }
    static int orig_size(const char *buf) {
        return Serializer<uint64_t>::orig_size(buf);
    }
    static int deserialize(const char *buf, int64_t &dst) {
        return Serializer<uint64_t>::deserialize(buf, *(uint64_t *) &dst);
    }
};
#endif

template<>
class Serializer<std::string, false> {
public:
    static int size(const std::string &obj) {
        return Serializer<uint64_t>::size(obj.length()) + obj.length();
    }
    static int serialize(char *buf, const std::string &src) {
        buf += Serializer<uint64_t>::serialize(buf, src.length());
        memcpy(buf, src.data(), src.length());
        return size(src);
    }
    static int orig_size(const char *buf) {
        uint64_t len;
        Serializer<uint64_t>::deserialize(buf, len);
        return len;
    }
    static int deserialize(const char *buf, std::string &dst) {
        uint64_t len;
        buf += Serializer<uint64_t>::deserialize(buf, len);
        // expect that dst is preallocated
        dst.resize(len);
        for (int i = 0; i < len; i++)
            dst[i] = buf[i];
        // dst = std::string(buf, buf + len);
        return size(dst);
    }
};

template<>
class Serializer<lcdf::Str, true> {
public:
    static int size(const lcdf::Str &obj) {
        return Serializer<uint64_t>::size(obj.length()) + obj.length();
    }
    static int serialize(char *buf, const lcdf::Str &src) {
        buf += Serializer<uint64_t>::serialize(buf, src.length());
        memcpy(buf, src.data(), src.length());
        return size(src);
    }
    static int orig_size(const char *buf) {
        uint64_t len;
        Serializer<uint64_t>::deserialize(buf, len);
        return len;
    }
    // returns a reference to the buffer!
    static int deserialize(const char *buf, lcdf::Str &dst) {
        uint64_t len;
        buf += Serializer<uint64_t>::deserialize(buf, len);
        dst.assign(buf, len);
        return size(dst);
    }
};
