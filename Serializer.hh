#pragma once

#include <iostream>
#include <cstring>
#include "compiler.hh"
#include "masstree-beta/str.hh"

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

template<>
class Serializer<std::string, false> {
public:
    static int size(const std::string &obj) {
        return sizeof(uint64_t) + obj.length();
    }
    static int serialize(char *buf, const std::string &src) {
        *(uint64_t *) buf = src.length();
        memcpy(buf + sizeof(uint64_t), src.data(), src.length());
        return size(src);
    }
    static int orig_size(const char *buf) {
        return *(uint64_t *) buf;
    }
    static int deserialize(const char *buf, std::string &dst) {
        uint64_t len = *(uint64_t *) buf;
        buf += sizeof(uint64_t);
        dst = std::string(buf, buf + len);
        return size(dst);
    }
};

template<>
class Serializer<lcdf::Str, true> {
public:
    static int size(const lcdf::Str &obj) {
        return sizeof(uint64_t) + obj.length();
    }
    static int serialize(char *buf, const lcdf::Str &src) {
        *(uint64_t *) buf = src.length();
        memcpy(buf + sizeof(uint64_t), src.data(), src.length());
        return size(src);
    }
    static int orig_size(const char *buf) {
        return *(uint64_t *) buf;
    }
    static int deserialize(const char *buf, lcdf::Str &dst) {
        uint64_t len = *(uint64_t *) buf;
        buf += sizeof(uint64_t);
        memcpy(dst.mutable_data(), buf, len);
        return size(dst);
    }
};
