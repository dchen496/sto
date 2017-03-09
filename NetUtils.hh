#pragma once

namespace NetUtils {
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

    static int write_all(int fd, void *ptr, int len) {
        char *p = (char *) ptr;
        int i = len;
        while (i > 0) {
            int n = write(fd, p, i);
            if (n <= 0) {
                if (n < 0)
                    perror("error writing to socket");
                return n;
            }
            p += n;
            i -= n;
        }
        return len;
    }
}
