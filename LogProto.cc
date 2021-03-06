#include "Transaction.hh"
#include "LogProto.hh"
#include "NetUtils.hh"
#include "Serializer.hh"

#include <deque>
#include <poll.h>

volatile bool LogPrimary::run;

int LogPrimary::nsend_threads;
LogPrimary::SendThread LogPrimary::send_threads[MAX_THREADS];

int LogPrimary::nworker_threads;
LogPrimary::WorkerThread LogPrimary::worker_threads[MAX_THREADS];

constexpr bool use_cv = true;

int LogPrimary::init_logging(unsigned nthreads, std::vector<std::string> hosts, int start_port) {
    assert(nthreads <= MAX_THREADS);
    run = true;
    nsend_threads = nthreads;
    for (int i = 0; i < nsend_threads; i++) {
        SendThread &thr = send_threads[i];
        thr.thread_id = i;
        thr.active = true;

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
            thr.fds.push_back(fd);
        }

        pthread_create(&thr.handle, nullptr, &sender, (void *) &thr);

#if LOG_CPU_PIN
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % LOG_CPU_PIN, &cpuset);
        pthread_setaffinity_np(thr.handle, sizeof(cpu_set_t), &cpuset);
#endif
    }
    nworker_threads = nthreads;

    for (unsigned i = 0; i < nthreads; i++) {
        threadinfo_t &thr = Transaction::tinfo[i];
        thr.log_buf = new char[STO_LOG_BUF_SIZE];
        thr.log_buf_used = STO_LOG_BATCH_HEADER_SIZE;
        thr.log_stats = log_stats_t();
    }
    return 0;
}

void LogPrimary::stop() {
    // XXX could do some smarter synchronization here...
    usleep(10000);

    // stop our own threads
    run = false;
    for (int i = 0; i < nsend_threads; i++) {
        SendThread &thr = send_threads[i];
        if (use_cv)
            thr.batch_queue_cond.notify_all(); // stop the thread from waiting
        pthread_join(thr.handle, nullptr);
        assert(thr.batch_queue.empty());
    }

    // signal to backups that there are no more log entries
    for (int i = 0; i < nsend_threads; i++) {
        for (int fd : send_threads[i].fds)
            shutdown(fd, SHUT_WR);
    }

    // wait for backups to finish sending acks, then close the socket
    for (int i = 0; i < nsend_threads; i++) {
        for (int fd : send_threads[i].fds) {
            Transaction::tid_type tid;
            while (read(fd, &tid, sizeof(tid)) > 0)
                ;
            close(fd);
        }
        send_threads[i].fds.clear();
    }

    // free buffers
    for (int i = 0; i < nworker_threads; i++) {
        WorkerThread &thr = worker_threads[i];
        std::unique_lock<std::mutex> lk(thr.mu);
        while (!thr.free_queue.empty()) {
            LogBatch &batch = thr.free_queue.front();
            delete[] batch.buf;
            thr.free_queue.pop();
        }
    }
}

void LogPrimary::enqueue_batch(char *buf, int len) {
    int id = TThread::id();
    SendThread &thr = send_threads[id];

    LogBatch batch = { .max_tid = ((uint64_t *) buf)[1], .buf = buf, .len = len };

    {
        std::unique_lock<std::mutex> lk(thr.mu);

        while ((int) thr.batch_queue.size() >= 2 * nworker_threads / nsend_threads) {
            if (use_cv) {
                thr.batch_queue_cond.wait(lk);
            } else {
                lk.unlock();
                usleep(10);
                lk.lock();
            }
        }

        thr.batch_queue.push(batch);
        if (use_cv)
            thr.batch_queue_cond.notify_all();
    }
}

char *LogPrimary::get_buffer() {
    WorkerThread &thr = worker_threads[TThread::id()];
    char *ret = nullptr;
    {
        std::unique_lock<std::mutex> lk(thr.mu);
        if (!thr.free_queue.empty()) {
            ret = thr.free_queue.front().buf;
            thr.free_queue.pop();
        }
    }
    if (!ret)
        ret = new char[STO_LOG_BUF_SIZE];
    return ret;
}

// the log batch must be flushed before disabling a thread!
void LogPrimary::set_active(bool active, int thread_id) {
    SendThread &thr = send_threads[thread_id];
    std::unique_lock<std::mutex> lk(thr.mu);
    thr.active = active;
    if (use_cv)
        thr.batch_queue_cond.notify_all();
}

volatile int nsend;
void *LogPrimary::sender(void *argsptr) {
    SendThread &thr = *(SendThread *) argsptr;

    std::vector<uint64_t> acked_tids(thr.fds.size());

    std::queue<LogBatch> unacked_batches;

    while (true) {
        LogBatch batch;
        bool send_dummy;
        {
            std::unique_lock<std::mutex> lk(thr.mu);
            while (run && thr.batch_queue.empty() && thr.active) {
                if (use_cv) {
                    thr.batch_queue_cond.wait(lk);
                } else {
                    lk.unlock();
                    usleep(10);
                    lk.lock();
                }
            }

            if (!run && thr.batch_queue.empty())
                break;

            send_dummy = !thr.active && thr.batch_queue.empty();
            if (!send_dummy) {
                batch = thr.batch_queue.front();
                thr.batch_queue.pop();
                if (use_cv)
                    thr.batch_queue_cond.notify_all();
            }
        }

        uint64_t buf[2];
        if (send_dummy) {
            // Send an empty batch with the highest TID so far
            // Prevents other threads from stalling if this one is inactive.
            batch.max_tid = Transaction::get_global_tid(); // max tid
            batch.buf = (char *) buf;
            batch.len = STO_LOG_BATCH_HEADER_SIZE;
            buf[0] = STO_LOG_BATCH_HEADER_SIZE; // length
            buf[1] = batch.max_tid;
        }

        for (unsigned i = 0; i < thr.fds.size(); i++) {
            if (NetUtils::write_all(thr.fds[i], batch.buf, batch.len) < batch.len) {
                perror("short write");
                return nullptr;
            }
        }

        if (send_dummy) {
            // Don't send too many empty batches if we're inactive
            usleep(1000);
        } else {
            // We only add real batches to unacked_batches; dummies never need to be resent
            unacked_batches.push(batch);
        }

        // poll to see if batches have been acked
        for (unsigned i = 0; i < thr.fds.size(); i++) {
            struct pollfd pfd;
            pfd.fd = thr.fds[i];
            pfd.events = POLLIN;
            if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLIN)) {
                if (NetUtils::read_all(pfd.fd, &acked_tids[i], sizeof(acked_tids[i])) < (int) sizeof(acked_tids[i]))
                    return nullptr;
            }
        }
        uint64_t min_acked_tid = acked_tids[0];
        for (uint64_t tid : acked_tids)
            min_acked_tid = std::min(min_acked_tid, tid);

        while (!unacked_batches.empty()) {
            LogBatch &b = unacked_batches.front();
            if (b.max_tid > min_acked_tid)
                break;
            {
                WorkerThread &wthr = worker_threads[thr.thread_id];
                std::unique_lock<std::mutex> lk(wthr.mu);
                wthr.free_queue.push(b);
            }
            unacked_batches.pop();
        }
    }

    return nullptr;
}


uint64_t LogBackup::txns_processed[MAX_THREADS];

int LogBackup::nrecv_threads;
LogBackup::RecvThread LogBackup::recv_threads[MAX_THREADS];

int LogBackup::napply_threads;
LogBackup::ApplyThread LogBackup::apply_threads[MAX_THREADS];

pthread_t LogBackup::advance_thread;
Transaction::tid_type LogBackup::tid_bound = 0; // 0 is lower than any valid TID
LogBackup::ApplyState LogBackup::apply_state = ApplyState::IDLE;
std::function<void(uint64_t)> LogBackup::apply_idle_fn;

int LogBackup::listen(unsigned nthreads, int start_port, std::function<void()> apply_init_fn,
        std::function<void(uint64_t)> apply_idle_fn) {

    tid_bound = 0;
    apply_state = ApplyState::IDLE;
    LogBackup::apply_idle_fn = apply_idle_fn;
    fence();

    nrecv_threads = nthreads;
    for (int i = 0; i < nrecv_threads; i++) {
        RecvThread &thr = recv_threads[i];
        thr.thread_id = i;
        thr.sock_fd = -1; // ensure that advance_thread waits for all sockets
        thr.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (thr.listen_fd < 0) {
            perror("couldn't create socket");
            return -1;
        }

        // XXX: nice to have for debugging, but maybe not a good idea in general
        int enable = 1;
        setsockopt(thr.listen_fd, SOL_SOCKET, SO_REUSEADDR, (void *) &enable, sizeof(int));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(start_port + i);
        if (bind(thr.listen_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
            perror("couldn't bind to socket");
            return -1;
        }
        if (::listen(thr.listen_fd, 1) < 0) {
            perror("couldn't listen to socket");
            return -1;
        }
        pthread_create(&thr.handle, nullptr, &receiver, (void *) &thr);

#if LOG_CPU_PIN
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % LOG_CPU_PIN, &cpuset);
        pthread_setaffinity_np(thr.handle, sizeof(cpu_set_t), &cpuset);
#endif
    }

    napply_threads = nthreads;
    for (int i = 0; i < napply_threads; i++) {
        ApplyThread &thr = apply_threads[i];
        thr.thread_id = i;
        thr.init_fn = apply_init_fn;
        thr.received_tid = 0;
        thr.processed_tid = 0;
        thr.cleaned_tid = 0;
        txns_processed[i] = 0;
        pthread_create(&thr.handle, nullptr, &applier, (void *) &thr);

#if LOG_CPU_PIN
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i + LOG_CPU_PIN, &cpuset);
        pthread_setaffinity_np(thr.handle, sizeof(cpu_set_t), &cpuset);
#endif
    }

    pthread_create(&advance_thread, nullptr, &advancer, nullptr);

    // wait for shutdown
    for (int i = 0; i < nrecv_threads; i++)
        pthread_join(recv_threads[i].handle, nullptr);

    pthread_join(advance_thread, nullptr);
    for (int i = 0; i < napply_threads; i++)
        pthread_join(apply_threads[i].handle, nullptr);

    apply_state = ApplyState::IDLE;
    memory_fence();

    // at this point, everyone is done sending
    for (int i = 0; i < nrecv_threads; i++) {
        RecvThread &thr = recv_threads[i];
        shutdown(thr.sock_fd, SHUT_WR);
        close(thr.sock_fd);
        close(thr.listen_fd);
    }

    return 0;
}

void LogBackup::stop() {
}

void *LogBackup::receiver(void* argsptr) {
    RecvThread &thr = *(RecvThread *) argsptr;

    std::vector<char *> buffer_pool;
    thr.sock_fd = accept(thr.listen_fd, NULL, NULL);
    if (thr.sock_fd < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    // terminates when the primary disconnects
    while (true) {
        {
            std::unique_lock<std::mutex> lk(thr.mu);
            while (!thr.free_queue.empty()) {
                buffer_pool.push_back(thr.free_queue.front().buf);
                thr.free_queue.pop();
            }
        }

        LogBatch batch;
        if (!read_batch(thr.sock_fd, buffer_pool, batch))
            break;

        NetUtils::write_all(thr.sock_fd, (char *) &batch.max_tid, sizeof(batch.max_tid));

        {
            ApplyThread &athr = apply_threads[thr.thread_id];
            std::unique_lock<std::mutex> lk(athr.mu);

            // batches are sent in order to the apply thread
            release_fence();
            assert(athr.received_tid <= batch.max_tid);
            athr.received_tid = batch.max_tid;

            if (batch.start == batch.end) {
                // optimization to not waste memory with dummy batches
                batch.start = batch.end = nullptr;
                buffer_pool.push_back(batch.buf);
                batch.buf = nullptr;
            }
            batch.recv_thr_id = thr.thread_id;

            // don't accumulate more than a fixed number of buffers per thread
            // waiting here creates back-pressure on the sender
            // XXX: is this the right number?
            /*
            while (athr.batch_queue.size() >= 100)
                athr.batch_queue_cond.wait(lk);
            */

            athr.batch_queue.push(batch);
            if (use_cv)
                athr.batch_queue_cond.notify_all();
        }
    }

    {
        ApplyThread &athr = apply_threads[thr.thread_id];
        std::unique_lock<std::mutex> lk(athr.mu);
        athr.received_tid = ~0ULL;
        if (use_cv)
            athr.batch_queue_cond.notify_all();
    }

    return nullptr;
}

/*
    Log batch format:
    Header
    - Note that variable length ints are NOT used here
    - Length of batch (8 bytes)
    - Batch TID (8 bytes)
        - If this batch and all previous batches are applied, the database
          state will reflect at least this TID

    For each transaction:
        - Transaction ID (variable length int, 1-9 bytes)
        - Number of entries/writes (variable length int, 1-9 bytes)
        - List of entries (type specific format)
*/
volatile int i, j;
bool LogBackup::read_batch(int sock_fd, std::vector<char *> &buffer_pool, LogBatch &batch) {
    if (buffer_pool.empty()) {
        batch.buf = new char[STO_LOG_BUF_SIZE];
    } else {
        batch.buf = *buffer_pool.rbegin();
        buffer_pool.pop_back();
    }

    batch.start = nullptr;
    batch.end = nullptr;

    char *ptr = batch.buf;
    int n = NetUtils::read_all(sock_fd, ptr, STO_LOG_BATCH_HEADER_SIZE);
    if (n <= 0) {
        buffer_pool.push_back(batch.buf);
        return false;
    }

    uint64_t len = *(uint64_t *) ptr;
    ptr += sizeof(uint64_t);
    assert(len <= STO_LOG_BUF_SIZE);

    uint64_t received_tid = *(uint64_t *) ptr;
    ptr += sizeof(uint64_t);

    if (len > STO_LOG_BATCH_HEADER_SIZE) {
        n = NetUtils::read_all(sock_fd, ptr, len - STO_LOG_BATCH_HEADER_SIZE);
        if (n <= 0) {
            buffer_pool.push_back(batch.buf);
            return false;
        }
    }

    batch.max_tid = received_tid;
    batch.start = batch.buf + STO_LOG_BATCH_HEADER_SIZE;
    batch.end = batch.buf + len;
#if STO_DEBUG_TXN_LOG
    std::cout << "Thread " << TThread::id() << " received " << len << " bytes\n";
#endif
    return true;
}

void *LogBackup::applier(void *argsptr) {
    ApplyThread &thr = *(ApplyThread *) argsptr;
    std::queue<LogBatch> batches;

    TThread::set_id(thr.thread_id);
    thr.init_fn();
    while (true) {
        ApplyState state = apply_state;
        uint64_t max_tid;
        acquire_fence();
        switch (state) {
        case ApplyState::IDLE:
            apply_idle_fn(tid_bound);
            break;
        case ApplyState::APPLY:
            max_tid = tid_bound;
            while (true) {
                if (batches.empty()) {
                    std::unique_lock<std::mutex> lk(thr.mu);
                    while (thr.batch_queue.empty() && thr.received_tid != ~0ULL) {
                        if (use_cv) {
                            thr.batch_queue_cond.wait(lk);
                        } else {
                            lk.unlock();
                            usleep(10);
                            lk.lock();
                        }
                    }
                    while (!thr.batch_queue.empty()) {
                        batches.push(std::move(thr.batch_queue.front()));
                        thr.batch_queue.pop();
                    }
                    if (use_cv)
                        thr.batch_queue_cond.notify_all();
                }

                if (batches.empty() && thr.received_tid == ~0ULL)
                    break;

                LogBatch &batch = batches.front();
                if (!process_batch_part(batch, max_tid))
                    break;
                if (batch.buf != nullptr) {
                    RecvThread &rthr = recv_threads[batch.recv_thr_id];
                    std::unique_lock<std::mutex> lk(rthr.mu);
                    rthr.free_queue.push(batch);
                }
                batches.pop();
            }

            release_fence();
            thr.processed_tid = max_tid;
            break;
        case ApplyState::CLEAN:
            max_tid = tid_bound;
            run_cleanup();
            release_fence();
            thr.cleaned_tid = max_tid;

            break;
        case ApplyState::KILL:
            return nullptr;
        }

        fence();
        // don't sleep if we are running read only transactions
        while (apply_state == state && state != ApplyState::IDLE) {
            usleep(1000);
            fence();
        }
    }
}

void LogBackup::default_apply_idle_fn(uint64_t tid) {
    (void) tid;
    usleep(1000);
}

// returns true if there may still be unprocessed log entries <= max_tid
bool LogBackup::process_batch_part(LogBatch &batch, uint64_t max_tid) {
    while (batch.start < batch.end) {
        uint64_t tid;
        Serializer<uint64_t>::deserialize(batch.start, tid);
        if (tid > max_tid)
            return false;
        batch.start = process_txn(batch.start);
    }
    return batch.max_tid < max_tid;
}

char *LogBackup::process_txn(char *ptr) {
    ApplyThread &thr = apply_threads[TThread::id()];
    txns_processed[thr.thread_id]++;

    Transaction::tid_type tid;
    ptr += Serializer<Transaction::tid_type>::deserialize(ptr, tid);

    uint64_t nentries;
    ptr += Serializer<uint64_t>::deserialize(ptr, nentries);

#if STO_DEBUG_TXN_LOG
    std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
    std::cout << "N=" << nentries << ' ';
#endif

    assert(tid > thr.processed_tid);

    threadinfo_t &tthr = Transaction::tinfo[TThread::id()];
    tthr.epoch = Transaction::global_epochs.global_epoch;
    tthr.rcu_set.clean_until(Transaction::global_epochs.active_epoch);
    if (tthr.trans_start_callback)
        tthr.trans_start_callback();

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id;
        ptr += Serializer<uint64_t>::deserialize(ptr, object_id);
        TObject &obj = Transaction::get_registered_object(object_id);

        int bytes_read = 0;
        obj.apply_log_entry(ptr, tid, bytes_read);

#if STO_DEBUG_TXN_LOG
        std::cout << std::dec << bytes_read << ":(" << std::hex << std::setw(2) << object_id << " ";
        for (int i = 0; i < bytes_read; i++) {
            std::cout << std::hex << std::setfill('0') << std::setw(2) << (int) (unsigned char) ptr[i];
        }
        std::cout << ") ";
#endif
        ptr += bytes_read;
    }
#if STO_DEBUG_TXN_LOG
    std::cout << '\n';
#endif

    if (tthr.trans_end_callback)
        tthr.trans_end_callback();

    release_fence();

    thr.processed_tid = tid;
    return ptr;
}

int LogBackup::advance() {
    assert(apply_state == ApplyState::IDLE);
    fence();

    // compute TID bound (minimum of TIDs received by each worker)
    Transaction::tid_type min_received_tid = ~0ULL;
    for (int i = 0; i < napply_threads; i++)
        min_received_tid = std::min(min_received_tid, apply_threads[i].received_tid);
    acquire_fence();
    assert(min_received_tid >= tid_bound);
    if (min_received_tid == tid_bound)
        return 0;

    tid_bound = min_received_tid;
    release_fence();
    apply_state = ApplyState::APPLY;

    /*
    // send acks to primary
    for (int i = 0; i < nrecv_threads; i++) {
        RecvThread &thr = recv_threads[i];
        int len = sizeof(uint64_t);
        if (NetUtils::write_all(thr.sock_fd, &min_received_tid, len) < len) {
            perror("short write");
            return -1;
        }
    }
    */

    // wait for apply phase to complete
    Transaction::tid_type min_processed_tid = 0;
    while (min_processed_tid < min_received_tid) {
        usleep(10); // should be significantly less than the sleep between advances
        min_processed_tid = ~0ULL;
        for (int i = 0; i < napply_threads; i++)
            min_processed_tid = std::min(min_processed_tid, apply_threads[i].processed_tid);
        acquire_fence();
    }
    assert(min_processed_tid == min_received_tid);

    release_fence();
    apply_state = ApplyState::CLEAN;

    // wait for clean phase to complete
    Transaction::tid_type min_cleaned_tid = 0;
    while (min_cleaned_tid < min_processed_tid) {
        usleep(10);
        min_cleaned_tid = ~0ULL;
        for (int i = 0; i < napply_threads; i++)
            min_cleaned_tid = std::min(min_cleaned_tid, apply_threads[i].cleaned_tid);
        acquire_fence();
    }
    assert(min_cleaned_tid == min_processed_tid);

    // return to idle phase
    release_fence();
    apply_state = ApplyState::IDLE;
    return 0;
}

void *LogBackup::advancer(void *) {
    // spin until all sockets are connected
    while (true) {
        bool wait = false;
        for (int i = 0; i < nrecv_threads; i++) {
            if (recv_threads[i].sock_fd < 0)
                wait = true;
        }
        if (!wait)
            break;
        usleep(10);
        fence();
    }

    // terminates when the primary disconnects, and all
    // of the apply threads are finished
    while (true) {
        int r = advance();
        if (r < 0) {
            return nullptr;
        }
        if (tid_bound == ~0ULL) {
            // no more transactions and we were told to exit
            // kill all the applier threads and exit
            release_fence();
            apply_state = ApplyState::KILL;
            return nullptr;
        }
        usleep(10000);
    }
    return nullptr;
}

void LogBackup::run_cleanup() {
    ApplyThread &thr = apply_threads[TThread::id()];
    for (std::function<void()> callback : thr.cleanup_callbacks)
        callback();
    thr.cleanup_callbacks.clear();
}

void LogBackup::cleanup(std::function<void()> callback) {
    apply_threads[TThread::id()].cleanup_callbacks.push_back(callback);
}
