#include "Transaction.hh"
#include "LogProto.hh"
#include "NetUtils.hh"
#include "Serializer.hh"
#include <deque>

volatile bool LogSend::run;

int LogSend::nsend_threads;
pthread_t LogSend::send_threads[MAX_THREADS];
LogSend::ThreadArgs LogSend::send_thread_args[MAX_THREADS];

std::mutex LogSend::send_mutexes[MAX_THREADS];
std::queue<LogSend::LogBatch> LogSend::batch_queue[MAX_THREADS];
std::condition_variable LogSend::batch_queue_wait[MAX_THREADS];

int LogSend::nworker_threads;
std::mutex LogSend::worker_mutexes[MAX_THREADS];
std::queue<LogSend::LogBatch> LogSend::free_queue[MAX_THREADS];

int LogSend::create_threads(unsigned nsend_threads, unsigned nworker_threads, std::vector<std::string> hosts, int start_port) {
    run = true;
    LogSend::nsend_threads = nsend_threads;
    for (unsigned i = 0; i < nsend_threads; i++) {
        ThreadArgs &args = send_thread_args[i];
        args.thread_id = i;

        for (unsigned j = 0; j < hosts.size(); j++) {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                perror("couldn't create socket");
                return -1;
            }

            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(hosts[j].c_str());
            addr.sin_port = htons(start_port + i);
            if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
                perror("couldn't connect to backup");
                return -1;
            }
            args.fds.push_back(fd);
        }

        pthread_create(&send_threads[i], nullptr, &sender, (void *) &send_thread_args[i]);
    }
    LogSend::nworker_threads = nworker_threads;
    return 0;
}

void LogSend::stop() {
    // stop our own threads
    run = false;
    for (int i = 0; i < nsend_threads; i++) {
        batch_queue_wait[i].notify_all(); // stop the thread from waiting
        pthread_join(send_threads[i], nullptr);
        assert(batch_queue[i].empty());
    }

    // signal to backups that there are no more log entries
    for (int i = 0; i < nsend_threads; i++) {
        for (int fd : send_thread_args[i].fds)
            shutdown(fd, SHUT_WR);
    }

    // wait for backups to finish sending acks, then close the socket
    for (int i = 0; i < nsend_threads; i++) {
        for (int fd : send_thread_args[i].fds) {
            Transaction::tid_type tid;
            while (read(fd, &tid, sizeof(tid)) > 0)
                ;
            close(fd);
        }
        send_thread_args[i].fds.clear();
    }

    // free buffers
    for (int i = 0; i < nworker_threads; i++) {
        std::unique_lock<std::mutex> lk(worker_mutexes[i]);
        while (!free_queue[i].empty()) {
            LogBatch &batch = free_queue[i].front();
            delete[] batch.buf;
            free_queue[i].pop();
        }
    }
}

void LogSend::enqueue_batch(char *buf, int len) {
    int id = TThread::id();
    threadinfo_t &thr = Transaction::tinfo[id];
    int sid = thr.log_send_thread;

    LogBatch batch = { .worker_id = id, .buf = buf, .len = len };

    {
        std::unique_lock<std::mutex> lk(send_mutexes[sid]);

        while ((int) batch_queue[sid].size() >= 2 * nworker_threads / nsend_threads)
            batch_queue_wait[sid].wait(lk);

        batch_queue[sid].push(batch);
        batch_queue_wait[sid].notify_all();
    }
}

char *LogSend::get_buffer() {
    int id = TThread::id();
    char *ret = nullptr;
    {
        std::unique_lock<std::mutex> lk(worker_mutexes[id]);
        if (!free_queue[id].empty()) {
            ret = free_queue[id].front().buf;
            free_queue[id].pop();
        }
    }
    if (!ret)
        ret = new char[STO_LOG_BUF_SIZE];
    return ret;
}

void *LogSend::sender(void *argsptr) {
    ThreadArgs &args = *(ThreadArgs *) argsptr;
    int id = args.thread_id;
    std::queue<LogBatch> &q = batch_queue[id];

    while (true) {
        LogBatch batch;
        {
            std::unique_lock<std::mutex> lk(send_mutexes[id]);
            while (run && q.empty())
                batch_queue_wait[id].wait(lk);
            if (!run && q.empty())
                break;

            batch = q.front();
            q.pop();
            // we only need to wake up one blocked worker
            batch_queue_wait[id].notify_one();
        }
        for (unsigned i = 0; i < args.fds.size(); i++) {
            // XXX: replace with write_all
            if (write(args.fds[i], batch.buf, batch.len) < batch.len) {
                perror("short write");
                return nullptr;
            }
        }
        int wid = batch.worker_id;
        {
            std::unique_lock<std::mutex> lk(worker_mutexes[wid]);
            free_queue[id].push(batch);
        }
    }

    return nullptr;
}


volatile bool LogApply::run;

bool LogApply::debug_txn_log = STO_DEBUG_TXN_LOG;
uint64_t LogApply::txns_processed[MAX_THREADS];

int LogApply::nrecv_threads;
int LogApply::listen_fds[MAX_THREADS];
int LogApply::sock_fds[MAX_THREADS];
pthread_t LogApply::recv_threads[MAX_THREADS];
LogApply::ThreadArgs LogApply::recv_thread_args[MAX_THREADS];

int LogApply::napply_threads;
pthread_t LogApply::apply_threads[MAX_THREADS];
LogApply::ThreadArgs LogApply::apply_thread_args[MAX_THREADS];

pthread_t LogApply::advance_thread;

std::mutex LogApply::apply_mutexes[MAX_THREADS];
std::queue<LogApply::LogBatch> LogApply::batch_queue[MAX_THREADS];
std::condition_variable LogApply::batch_queue_wait[MAX_THREADS];
Transaction::tid_type LogApply::received_tids[MAX_THREADS];
Transaction::tid_type LogApply::min_received_tid = 0; // 0 is lower than any valid TID
Transaction::tid_type LogApply::processed_tids[MAX_THREADS];

std::mutex LogApply::recv_mutexes[MAX_THREADS];
std::queue<LogApply::LogBatch> LogApply::free_queue[MAX_THREADS];

std::vector<std::function<void()>> LogApply::cleanup_callbacks[MAX_THREADS];

int LogApply::listen(unsigned nrecv_threads, unsigned napply_threads, int start_port) {
    run = true;
    min_received_tid = 0;
    fence();

    LogApply::nrecv_threads = nrecv_threads;
    for (unsigned i = 0; i < nrecv_threads; i++) {
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

        recv_thread_args[i].thread_id = i;
        pthread_create(&recv_threads[i], nullptr, &receiver, (void *) &recv_thread_args[i]);
    }

    LogApply::napply_threads = napply_threads;
    for (unsigned i = 0; i < napply_threads; i++) {
        received_tids[i] = 0;
        processed_tids[i] = 0;
        txns_processed[i] = 0;

        apply_thread_args[i].thread_id = i;
        pthread_create(&apply_threads[i], nullptr, &applier, (void *) &apply_thread_args[i]);
    }

    pthread_create(&advance_thread, nullptr, &advancer, nullptr);

    for (unsigned i = 0; i < nrecv_threads; i++)
        pthread_join(recv_threads[i], nullptr);

    run = false;
    for (unsigned i = 0; i < napply_threads; i++)
        batch_queue_wait[i].notify_all();

    for (unsigned i = 0; i < napply_threads; i++)
        pthread_join(apply_threads[i], nullptr);

    fence();

    pthread_join(advance_thread, nullptr);
    advance();

    // at this point, everyone is done sending
    for (unsigned i = 0; i < napply_threads; i++) {
        shutdown(sock_fds[i], SHUT_WR);
        close(sock_fds[i]);
        close(listen_fds[i]);
    }

    return 0;
}

void LogApply::stop() {
    run = false;
}

void *LogApply::receiver(void * argsptr) {
    int id = ((ThreadArgs *) argsptr)->thread_id;

    sock_fds[id] = accept(listen_fds[id], NULL, NULL);
    if (sock_fds[id] < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    std::vector<char *> buffer_pool;

    // terminates when the primary disconnects
    while (run) {
        {
            std::unique_lock<std::mutex> lk(recv_mutexes[id]);
            while (!free_queue[id].empty()) {
                buffer_pool.push_back(free_queue[id].front().buf);
                free_queue[id].pop();
            }
        }

        LogBatch batch;
        if (!read_batch(sock_fds[id], buffer_pool, batch))
            break;

        {
            int bid = batch.thread_id;
            std::unique_lock<std::mutex> lk(apply_mutexes[bid]);

            // batches are sent in order to the apply thread
            assert(received_tids[bid] <= batch.max_tid);
            received_tids[bid] = batch.max_tid;
            batch.recv_thr_id = id;

            // don't accumulate more than 1 buffer per thread
            // waiting here creates back-pressure on the sender
            // XXX: is this the right number?
            while (batch_queue[bid].size() >= 1)
                batch_queue_wait[bid].wait(lk);

            batch_queue[bid].push(batch);
            batch_queue_wait[bid].notify_all();
        }
    }

    return nullptr;
}

/*
    Log batch format:
    - Thread ID (8 bytes)
    - Length of batch (8 bytes)
    - Batch TID (8 bytes)
        - If this batch and all previous batches are applied, the database
          state will reflect at least this TID
    - For each transaction:
        - Transaction ID (8 bytes)
        - Number of entries/writes (8 bytes)
        - List of entries (type specific format)
*/
bool LogApply::read_batch(int sock_fd, std::vector<char *> &buffer_pool, LogBatch &batch) {
    if (buffer_pool.empty()) {
        batch.buf = new char[STO_LOG_BUF_SIZE];
    } else {
        batch.buf = *buffer_pool.rbegin();
        buffer_pool.pop_back();
    }

    batch.start = nullptr;
    batch.end = nullptr;
    batch.needs_free = false;

    char *ptr = batch.buf;
    int n = NetUtils::read_all(sock_fd, ptr, STO_LOG_BATCH_HEADER_SIZE);
    if (n <= 0)
        return false;

    uint64_t thread_id;
    ptr += Serializer<uint64_t>::deserialize(ptr, thread_id);
    assert(thread_id < (uint64_t) napply_threads);

    uint64_t len;
    ptr += Serializer<uint64_t>::deserialize(ptr, len);
    assert(len < STO_LOG_BUF_SIZE);

    uint64_t received_tid;
    ptr += Serializer<uint64_t>::deserialize(ptr, received_tid);

    if (len > STO_LOG_BATCH_HEADER_SIZE) {
        n = NetUtils::read_all(sock_fd, ptr, len - STO_LOG_BATCH_HEADER_SIZE);
        if (n <= 0)
            return false;
    }

    batch.thread_id = thread_id;
    batch.max_tid = received_tid;
    batch.start = batch.buf + STO_LOG_BATCH_HEADER_SIZE;
    batch.end = batch.buf + len;
    return true;
}

void *LogApply::applier(void *argsptr) {
    int id = ((ThreadArgs *) argsptr)->thread_id;
    TThread::set_id(id);

    std::queue<LogBatch> batches;

    while (true) {
        if (batches.empty()) {
            std::unique_lock<std::mutex> lk(apply_mutexes[id]);
            while (run && batch_queue[id].empty())
                batch_queue_wait[id].wait(lk);
            if (!run && batch_queue[id].empty())
                break;
            batches.push(std::move(batch_queue[id].front()));
            batch_queue[id].pop();
            batch_queue_wait[id].notify_one();
        }

        uint64_t max_tid = min_received_tid;
        acquire_fence();

        LogBatch &batch = batches.front();
        if (batch.max_tid <= max_tid) {
            batch.start = process_batch_part(batch, max_tid);
            if (batch.start >= batch.end) {
                if (batch.needs_free) {
                    delete[] batch.buf;
                } else {
                    std::unique_lock<std::mutex> lk(recv_mutexes[batch.recv_thr_id]);
                    free_queue[batch.recv_thr_id].push(batch);
                }
                batches.pop();
            }
        } else {
            usleep(1000);
        }
    }

    return nullptr;
}

char *LogApply::process_batch_part(LogBatch &batch, uint64_t max_tid) {
    while (batch.start < batch.end) {
        uint64_t tid = ((uint64_t *) batch.start)[0];
        if (tid > max_tid)
            break;
        batch.start = process_txn(batch.start);
    }
    return batch.start;
}

char *LogApply::process_txn(char *ptr) {
    int id = TThread::id();
    txns_processed[id]++;

    Transaction::tid_type tid;
    ptr += Serializer<Transaction::tid_type>::deserialize(ptr, tid);
    assert(tid > processed_tids[id]);
    processed_tids[id] = tid;

    uint64_t nentries;
    ptr += Serializer<uint64_t>::deserialize(ptr, nentries);

    if (debug_txn_log) {
        std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
        std::cout << "N=" << nentries << ' ';
    }

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id;
        ptr += Serializer<uint64_t>::deserialize(ptr, object_id);
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
    Transaction::tid_type new_valid = ~0ULL;
    for (int i = 0; i < napply_threads; i++) {
        std::unique_lock<std::mutex> lk(apply_mutexes[i]);
        new_valid = std::min(new_valid, received_tids[i]);
    }
    min_received_tid = new_valid;
    release_fence();

    for (int i = 0; i < nrecv_threads; i++) {
        int len = sizeof(uint64_t);
        if (write(sock_fds[i], &min_received_tid, len) < len) {
            perror("short write");
            return -1;
        }
    }
    return 0;
}

void *LogApply::advancer(void *) {
    // spin until all sockets are connected
    while (run) {
        bool wait = false;
        for (int i = 0; i < napply_threads; i++) {
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
    }
    return nullptr;
}

void LogApply::run_cleanup() {
    int id = TThread::id();
    while (cleanup_callbacks[id].size() > 0) {
        std::vector<std::function<void()>> callbacks = cleanup_callbacks[id];
        cleanup_callbacks[id].clear();
        for (std::function<void()> callback : callbacks)
            callback();
    }
}

void LogApply::cleanup(std::function<void()> callback) {
    cleanup_callbacks[TThread::id()].push_back(callback);
}
