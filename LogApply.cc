#include "Transaction.hh"
#include "LogApply.hh"

pthread_t LogApply::apply_threads[MAX_THREADS];
LogApply::ThreadArgs LogApply::apply_thread_args[MAX_THREADS];
bool LogApply::run;

void LogApply::listen(unsigned num_threads, int start_port) {
    run = true;

    for (unsigned i = 0; i < num_threads; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            std::cerr << "couldn't create socket\n";
            assert(false);
            return;
        }

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(start_port + i);
        if (bind(fd, (struct sockaddr *) &addr, sizeof(addr))) {
            std::cerr << "couldn't bind socket\n";
            assert(false);
            return;
        }
        listen(fd, 1);

        ThreadArgs args;
        args.listen_fd = fd;
        args.thread_id = i;
        apply_thread_args[i] = args;

        pthread_create(&apply_threads[i], nullptr, &applier, (void *) &apply_thread_args[i]);
    }
}

void LogApply::stop() {
    run = false;
}

void *LogApply::applier(void *argsptr) {
    ThreadArgs &args = *(ThreadArgs *) argsptr;
    TThread::set_id(args.thread_id);

    struct sockaddr_in addr;
    socklen_t addrlen;
    int fd = accept(args.listen_fd, (sockaddr *) &addr, &addrlen);
    if (fd < 0) {
        std::cerr << "couldn't accept connection\n";
        assert(false);
        return nullptr;
    }

    char *buf = new char[STO_LOG_BUF_SIZE];
    while (LogApply::run) {
        int n = read(fd, buf, STO_LOG_BUF_SIZE);
        if (n < 0) {
            std::cerr << "error reading from socket\n";
            assert(false);
            return nullptr;
        }

        process_batch(buf);
    }
    delete[] buf;
    return nullptr;
}

template <typename T>
T scan(char *&buf) {
    T ret = *(T *) buf;
    buf += sizeof(T);
    return ret;
}

void LogApply::process_batch(char *batch) {
    char *ptr = batch;

    Transaction::tid_type tid = scan<Transaction::tid_type>(ptr);
    uint64_t nentries = scan<uint64_t>(ptr);

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id = scan<uint64_t>(ptr);
        TObject &obj = Transaction::get_registered_object(object_id);

        int bytes_read = 0;
        obj.apply_log_entry(ptr, tid, bytes_read);
        ptr += bytes_read;
    }
}
