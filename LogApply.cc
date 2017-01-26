#include "Transaction.hh"
#include "LogApply.hh"
#include "NetUtils.hh"
#include <vector>

bool LogApply::debug_txn_log = STO_DEBUG_TXN_LOG;

int LogApply::nthreads;
int LogApply::listen_fds[MAX_THREADS];
int LogApply::sock_fds[MAX_THREADS];
pthread_t LogApply::apply_threads[MAX_THREADS];
pthread_t LogApply::advance_thread;
LogApply::ThreadArgs LogApply::thread_args[MAX_THREADS];

bool LogApply::run;
Transaction::tid_type LogApply::valid_tids[MAX_THREADS];
Transaction::tid_type LogApply::min_valid_tid = 0; // 0 is lower than any valid TID

int LogApply::listen(unsigned nthreads, int start_port) {
    run = true;
    fence();

    LogApply::nthreads = nthreads;
    for (unsigned i = 0; i < nthreads; i++) {
        sock_fds[i] = -1; // ensure that advance_thread waits for all sockets

        listen_fds[i] = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fds[i] < 0) {
            perror("couldn't create socket");
            return -1;
        }

        // XXX: nice to have for debugging, but maybe not a good idea in general
        int enable = 1;
        setsockopt(listen_fds[i], SOL_SOCKET, SO_REUSEADDR, (void *) &enable, sizeof(int));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(start_port + i);
        if (bind(listen_fds[i], (struct sockaddr *) &addr, sizeof(addr)) < 0) {
            perror("couldn't bind to socket");
            return -1;
        }
        if (::listen(listen_fds[i], 1) < 0) {
            perror("couldn't listen to socket");
            return -1;
        }

        thread_args[i].thread_id = i;
        pthread_create(&apply_threads[i], nullptr, &applier, (void *) &thread_args[i]);
    }

    pthread_create(&advance_thread, nullptr, &advancer, nullptr);

    for (unsigned i = 0; i < nthreads; i++)
        pthread_join(apply_threads[i], nullptr);

    run = false;
    fence();

    pthread_join(advance_thread, nullptr);
    advance();

    // at this point, everyone is done sending
    for (unsigned i = 0; i < nthreads; i++) {
        shutdown(sock_fds[i], SHUT_WR);
        close(sock_fds[i]);
        close(listen_fds[i]);
    }

    return 0;
}

void LogApply::stop() {
    run = false;
}

/*
    Log batch format:
    - Length of batch (8 bytes)
    - Valid TID (8 bytes)
    - For each transaction:
        - Transaction ID (8 bytes)
        - Number of entries/writes (8 bytes)
        - List of entries (type specific format)
*/
void *LogApply::applier(void *argsptr) {
    int id = ((ThreadArgs *) argsptr)->thread_id;
    TThread::set_id(id);

    std::vector<char> buf;
    buf.resize(STO_LOG_BUF_SIZE);

    sock_fds[id] = accept(listen_fds[id], NULL, NULL);
    if (sock_fds[id] < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    // terminates when the primary disconnects
    while (true) {
        // read batch
        uint64_t *hdr = (uint64_t *) buf.data();
        int n = NetUtils::read_all(sock_fds[id], (void *) &hdr[0], sizeof(uint64_t));
        if (n <= 0)
            break;
        uint64_t batch_len = hdr[0];
        assert(batch_len < STO_LOG_BUF_SIZE);

        n = NetUtils::read_all(sock_fds[id], (void *) &hdr[1], batch_len - sizeof(uint64_t));
        if (n <= 0)
            break;

        // record max valid TID for this worker
        uint64_t valid_tid = hdr[1];
        assert(valid_tids[id] <= valid_tid);
        valid_tids[id] = valid_tid;
        fence();

        // apply batch
        char *txn = (char *) &hdr[2];
        char *end = batch_len + (char *) &hdr[0];
        while (txn < end)
            txn = process_txn(txn);
    }
    return nullptr;
}

char *LogApply::process_txn(char *ptr) {
    Transaction::tid_type tid = NetUtils::scan<Transaction::tid_type>(ptr);
    uint64_t nentries = NetUtils::scan<uint64_t>(ptr);

    if (debug_txn_log) {
        std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
        std::cout << "N=" << nentries << ' ';
    }

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id = NetUtils::scan<uint64_t>(ptr);
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

int LogApply::advance() {
    fence();
    Transaction::tid_type new_valid = valid_tids[0];
    for (int i = 1; i < nthreads; i++)
        new_valid = std::min(new_valid, valid_tids[i]);
    min_valid_tid = new_valid;

    for (int i = 0; i < nthreads; i++) {
        int len = sizeof(uint64_t);
        if (write(sock_fds[i], &min_valid_tid, len) < len) {
            perror("short write");
            return -1;
        }
    }
    fence();
    return 0;
}

void *LogApply::advancer(void *) {
    // spin until all sockets are connected
    while (run) {
        bool wait = false;
        for (int i = 0; i < nthreads; i++) {
            if (sock_fds[i] < 0)
                wait = true;
        }
        if (!wait)
            break;
        usleep(10000);
        fence();
    }

    // terminates when the primary disconnects, and all
    // of the apply threads are finished
    while (run) {
        if (advance() < 0)
            return nullptr;
        usleep(50000);
        fence();
    }
    return nullptr;
}
