#pragma once

namespace NetUtils {
    template <typename T>
        T scan(char *&buf) {
            T ret = *(T *) buf;
            buf += sizeof(T);
            return ret;
        }

    static int read_all(int fd, void *ptr, int len) {
        char *p = (char *) ptr;
        int i = len;
        while (i > 0) {
            int n = read(fd, p, i);
            if (n <= 0) {
                if (n < 0)
                    perror("error reading from socket");
                return n;
            }
            p += n;
            i -= n;
        }
        return len;
    }
}
