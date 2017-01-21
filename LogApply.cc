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
            perror("couldn't create socket");
            return;
        }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(start_port + i);
        if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
            perror("couldn't bind to socket");
            return;
        }

        ThreadArgs args;
        args.listen_fd = fd;
        args.thread_id = i;
        apply_thread_args[i] = args;

        pthread_create(&apply_threads[i], nullptr, &applier, (void *) &apply_thread_args[i]);
    }
    for (unsigned i = 0; i < num_threads; i++) {
        pthread_join(apply_threads[i], nullptr);
    }
}

void LogApply::stop() {
    run = false;
}

void *LogApply::applier(void *argsptr) {
    ThreadArgs &args = *(ThreadArgs *) argsptr;
    TThread::set_id(args.thread_id);

    if (::listen(args.listen_fd, 1) < 0) {
        perror("couldn't listen to socket");
        return nullptr;
    }
    int fd = accept(args.listen_fd, NULL, NULL);
    if (fd < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    char *buf = new char[STO_LOG_BUF_SIZE];
    while (run) {
        int n = read(fd, buf, STO_LOG_BUF_SIZE);
        if (n < 0) {
            perror("error reading from socket");
            return nullptr;
        }
        if (n == 0) {
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

#if STO_DEBUG_TXN_LOG
    std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
    std::cout << "N=" << nentries << ' ';
#endif

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id = scan<uint64_t>(ptr);
        TObject &obj = Transaction::get_registered_object(object_id);

        int bytes_read = 0;
        obj.apply_log_entry(ptr, tid, bytes_read);

#if STO_DEBUG_TXN_LOG
        std::cout << "(" << std::hex << std::setw(2) << object_id << " ";
        for (int i = 0; i < bytes_read; i++) {
            std::cout << std::hex << std::setfill('0') << std::setw(2) << (int) ptr[i];
        }
        std::cout << ") ";
#endif
        ptr += bytes_read;
    }
#if STO_DEBUG_TXN_LOG
        std::cout << '\n';
#endif
}
