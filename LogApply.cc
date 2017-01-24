#include "Transaction.hh"
#include "LogApply.hh"
#include <vector>

pthread_t LogApply::apply_threads[MAX_THREADS];
LogApply::ThreadArgs LogApply::apply_thread_args[MAX_THREADS];
bool LogApply::run;
bool LogApply::debug_txn_log = STO_DEBUG_TXN_LOG;

int LogApply::listen(unsigned num_threads, int start_port) {
    run = true;

    for (unsigned i = 0; i < num_threads; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("couldn't create socket");
            return -1;
        }

        // XXX: nice to have for debugging, but maybe not a good idea in general
        int enable = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *) &enable, sizeof(int));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(start_port + i);
        if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
            perror("couldn't bind to socket");
            return -1;
        }
        if (::listen(fd, 1) < 0) {
            perror("couldn't listen to socket");
            return -1;
        }

        ThreadArgs args;
        args.listen_fd = fd;
        args.thread_id = i;
        apply_thread_args[i] = args;

        pthread_create(&apply_threads[i], nullptr, &applier, (void *) &apply_thread_args[i]);
    }
    for (unsigned i = 0; i < num_threads; i++) {
        pthread_join(apply_threads[i], nullptr);
        close(apply_thread_args[i].listen_fd);
    }
    run = false;
    return 0;
}

void LogApply::stop() {
    run = false;
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



/*
    Log batch format:
    - Length of batch (8 bytes, not included in length)
    - For each transaction:
        - Transaction ID (8 bytes)
        - Number of entries/writes (8 bytes)
        - List of entries (type specific format)
*/
void *LogApply::applier(void *argsptr) {
    ThreadArgs &args = *(ThreadArgs *) argsptr;
    TThread::set_id(args.thread_id);

    int fd = accept(args.listen_fd, NULL, NULL);
    if (fd < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    std::vector<char> buf;
    buf.resize(STO_LOG_BUF_SIZE);
    while (run) {
        uint64_t batch_len;
        int n;

        n = read_all(fd, (void *) &batch_len, sizeof(uint64_t));
        if (n <= 0)
            break;
        assert(batch_len < STO_LOG_MAX_BATCH);

        if (batch_len == 0)
            continue;

        n = read_all(fd, buf.data(), batch_len);
        if (n <= 0)
            break;

        char *ptr = buf.data();
        char *end = ptr + batch_len;
        while (ptr < end)
            ptr = process_txn(ptr);
    }
    close(fd);
    return nullptr;
}

template <typename T>
T scan(char *&buf) {
    T ret = *(T *) buf;
    buf += sizeof(T);
    return ret;
}

char *LogApply::process_txn(char *ptr) {
    Transaction::tid_type tid = scan<Transaction::tid_type>(ptr);
    uint64_t nentries = scan<uint64_t>(ptr);

    if (debug_txn_log) {
        std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
        std::cout << "N=" << nentries << ' ';
    }

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id = scan<uint64_t>(ptr);
        TObject &obj = Transaction::get_registered_object(object_id);

        int bytes_read = 0;
        obj.apply_log_entry(ptr, tid, bytes_read);

        if (debug_txn_log) {
            std::cout << "(" << std::hex << std::setw(2) << object_id << " ";
            for (int i = 0; i < bytes_read; i++) {
                std::cout << std::hex << std::setfill('0') << std::setw(2) << (int) (unsigned char) ptr[i];
            }
            std::cout << ") ";
        }
        ptr += bytes_read;
    }
    if (debug_txn_log)
        std::cout << '\n';
    return ptr;
}
