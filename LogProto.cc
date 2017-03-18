#include "Transaction.hh"
#include "LogProto.hh"
#include "NetUtils.hh"
#include "Serializer.hh"
#include <deque>

volatile bool LogSend::run;

int LogSend::nsend_threads;
LogSend::SendThread LogSend::send_threads[MAX_THREADS];

int LogSend::nworker_threads;
LogSend::WorkerThread LogSend::worker_threads[MAX_THREADS];

constexpr bool use_cv = true;

int LogSend::create_threads(unsigned nthreads, std::vector<std::string> hosts, int start_port) {
    run = true;
    nsend_threads = nthreads;
    for (int i = 0; i < nsend_threads; i++) {
        SendThread &thr = send_threads[i];
        thr.thread_id = i;

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
    }
    nworker_threads = nthreads;
    return 0;
}

void LogSend::stop() {
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

void LogSend::enqueue_batch(char *buf, int len) {
    int id = TThread::id();
    SendThread &thr = send_threads[id];

    LogBatch batch = { .worker_id = id, .buf = buf, .len = len };

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

char *LogSend::get_buffer() {
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

void *LogSend::sender(void *argsptr) {
    SendThread &thr = *(SendThread *) argsptr;

    while (true) {
        LogBatch batch;
        {
            std::unique_lock<std::mutex> lk(thr.mu);
            while (run && thr.batch_queue.empty()) {
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

            batch = thr.batch_queue.front();
            thr.batch_queue.pop();
            if (use_cv)
                thr.batch_queue_cond.notify_all();
        }
        for (unsigned i = 0; i < thr.fds.size(); i++) {
            if (NetUtils::write_all(thr.fds[i], batch.buf, batch.len) < batch.len) {
                perror("short write");
                return nullptr;
            }
        }
        WorkerThread &wthr = worker_threads[batch.worker_id];
        {
            std::unique_lock<std::mutex> lk(wthr.mu);
            wthr.free_queue.push(batch);
        }
    }

    return nullptr;
}


bool LogApply::debug_txn_log = STO_DEBUG_TXN_LOG;
uint64_t LogApply::txns_processed[MAX_THREADS];

int LogApply::nrecv_threads;
LogApply::RecvThread LogApply::recv_threads[MAX_THREADS];

int LogApply::napply_threads;
LogApply::ApplyThread LogApply::apply_threads[MAX_THREADS];

pthread_t LogApply::advance_thread;
Transaction::tid_type LogApply::tid_bound = 0; // 0 is lower than any valid TID
LogApply::ApplyState LogApply::apply_state = ApplyState::IDLE;

int LogApply::listen(unsigned nthreads, int start_port, std::function<void()> apply_init_fn) {
    tid_bound = 0;
    apply_state = ApplyState::IDLE;
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
    }

    pthread_create(&advance_thread, nullptr, &advancer, nullptr);

    // wait for shutdown
    for (int i = 0; i < nrecv_threads; i++)
        pthread_join(recv_threads[i].handle, nullptr);

    pthread_join(advance_thread, nullptr);
    for (int i = 0; i < napply_threads; i++)
        pthread_join(apply_threads[i].handle, nullptr);

    // at this point, everyone is done sending
    for (int i = 0; i < nrecv_threads; i++) {
        RecvThread &thr = recv_threads[i];
        shutdown(thr.sock_fd, SHUT_WR);
        close(thr.sock_fd);
        close(thr.listen_fd);
    }

    return 0;
}

void LogApply::stop() {
    // TODO ?
}

void *LogApply::receiver(void* argsptr) {
    RecvThread &thr = *(RecvThread *) argsptr;

    thr.sock_fd = accept(thr.listen_fd, NULL, NULL);
    if (thr.sock_fd < 0) {
        perror("couldn't accept connection");
        return nullptr;
    }

    std::vector<char *> buffer_pool;

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

        {
            ApplyThread &athr = apply_threads[thr.thread_id];
            std::unique_lock<std::mutex> lk(athr.mu);

            // batches are sent in order to the apply thread
            release_fence();
            assert(athr.received_tid <= batch.max_tid);
            athr.received_tid = batch.max_tid;

            batch.recv_thr_id = thr.thread_id;

            // don't accumulate more than a fixed number of buffers per thread
            // waiting here creates back-pressure on the sender
            // XXX: is this the right number?
            /*
            while (athr.batch_queue.size() >= 100)
                athr.batch_queue_cond.wait(lk);
            */

            athr.batch_queue.push(batch);
            athr.batch_queue_cond.notify_all();
        }
    }

    {
        ApplyThread &athr = apply_threads[thr.thread_id];
        std::unique_lock<std::mutex> lk(athr.mu);
        athr.received_tid = ~0ULL;
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

    uint64_t len = *(uint64_t *) ptr;
    ptr += sizeof(uint64_t);
    assert(len <= STO_LOG_BUF_SIZE);

    uint64_t received_tid = *(uint64_t *) ptr;
    ptr += sizeof(uint64_t);

    if (len > STO_LOG_BATCH_HEADER_SIZE) {
        n = NetUtils::read_all(sock_fd, ptr, len - STO_LOG_BATCH_HEADER_SIZE);
        if (n <= 0)
            return false;
    }

    batch.max_tid = received_tid;
    batch.start = batch.buf + STO_LOG_BATCH_HEADER_SIZE;
    batch.end = batch.buf + len;
    if (debug_txn_log) {
        std::cout << "Thread " << TThread::id() << " received " << len << " bytes\n";
    }
    return true;
}

void *LogApply::applier(void *argsptr) {
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
            // TODO: process some read only transactions
            break;
        case ApplyState::APPLY:
            max_tid = tid_bound;
            while (true) {
                if (batches.empty()) {
                    std::unique_lock<std::mutex> lk(thr.mu);
                    while (thr.batch_queue.empty() && thr.received_tid != ~0ULL)
                        thr.batch_queue_cond.wait(lk);
                    while (!thr.batch_queue.empty()) {
                        batches.push(std::move(thr.batch_queue.front()));
                        thr.batch_queue.pop();
                    }
                    thr.batch_queue_cond.notify_all();
                }

                if (batches.empty() && thr.received_tid == ~0ULL)
                    break;

                LogBatch &batch = batches.front();
                if (!process_batch_part(batch, max_tid))
                    break;
                if (batch.start >= batch.end) {
                    if (batch.needs_free) {
                        delete[] batch.buf;
                    } else {
                        RecvThread &rthr = recv_threads[batch.recv_thr_id];
                        std::unique_lock<std::mutex> lk(rthr.mu);
                        rthr.free_queue.push(batch);
                    }
                    batches.pop();
                }
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
        while (apply_state == state) {
            usleep(10);
            fence();
        }
    }
}

bool LogApply::process_batch_part(LogBatch &batch, uint64_t max_tid) {
    while (batch.start < batch.end) {
        uint64_t tid;
        Serializer<uint64_t>::deserialize(batch.start, tid);
        if (tid > max_tid)
            return false;
        batch.start = process_txn(batch.start);
    }
    return true;
}

char *LogApply::process_txn(char *ptr) {
    ApplyThread &thr = apply_threads[TThread::id()];
    txns_processed[thr.thread_id]++;

    Transaction::tid_type tid;
    ptr += Serializer<Transaction::tid_type>::deserialize(ptr, tid);

    uint64_t nentries;
    ptr += Serializer<uint64_t>::deserialize(ptr, nentries);

    if (debug_txn_log) {
        std::cout << "TID=" << std::hex << std::setfill('0') << std::setw(8) << tid << ' ';
        std::cout << "N=" << nentries << ' ';
    }

    assert(tid > thr.processed_tid);

    for (uint64_t i = 0; i < nentries; i++) {
        uint64_t object_id;
        ptr += Serializer<uint64_t>::deserialize(ptr, object_id);
        TObject &obj = Transaction::get_registered_object(object_id);

        int bytes_read = 0;
        obj.apply_log_entry(ptr, tid, bytes_read);

        if (debug_txn_log) {
            std::cout << std::dec << bytes_read << ":(" << std::hex << std::setw(2) << object_id << " ";
            for (int i = 0; i < bytes_read; i++) {
                std::cout << std::hex << std::setfill('0') << std::setw(2) << (int) (unsigned char) ptr[i];
            }
            std::cout << ") ";
        }
        ptr += bytes_read;
    }
    if (debug_txn_log)
        std::cout << '\n';

    release_fence();
    thr.processed_tid = tid;
    return ptr;
}

int LogApply::advance() {
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

    // send acks to primary
    for (int i = 0; i < nrecv_threads; i++) {
        RecvThread &thr = recv_threads[i];
        int len = sizeof(uint64_t);
        if (NetUtils::write_all(thr.sock_fd, &min_received_tid, len) < len) {
            perror("short write");
            return -1;
        }
    }

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

void *LogApply::advancer(void *) {
    // spin until all sockets are connected
    while (true) {
        bool wait = false;
        for (int i = 0; i < nrecv_threads; i++) {
            if (recv_threads[i].sock_fd < 0)
                wait = true;
        }
        if (!wait)
            break;
        usleep(10000);
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
        usleep(50000);
    }
    return nullptr;
}

void LogApply::run_cleanup() {
    ApplyThread &thr = apply_threads[TThread::id()];
    for (std::function<void()> callback : thr.cleanup_callbacks)
        callback();
    thr.cleanup_callbacks.clear();
}

void LogApply::cleanup(std::function<void()> callback) {
    apply_threads[TThread::id()].cleanup_callbacks.push_back(callback);
}
