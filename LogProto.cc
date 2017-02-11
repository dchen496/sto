#include "Transaction.hh"
#include "LogProto.hh"
#include "NetUtils.hh"
#include "Serializer.hh"
#include <deque>

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

volatile bool LogApply::run;

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

    for (unsigned i = 0; i < napply_threads; i++)
        pthread_join(apply_threads[i], nullptr);

    run = false;
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
            int thid = batch.thread_id;
            std::unique_lock<std::mutex> lk(apply_mutexes[thid]);
            // we require that batches be sent in order to the apply thread
            // to avoid issues with missing batches
            assert(received_tids[thid] <= batch.max_tid);
            received_tids[thid] = batch.max_tid;
            batch.recv_thr_id = id;
            batch_queue[thid].push(batch);
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

    while (run) {
        if (batches.empty()) {
            std::unique_lock<std::mutex> lk(apply_mutexes[id]);
            while (batch_queue[id].empty())
                batch_queue_wait[id].wait(lk);
            batches.push(batch_queue[id].front());
            batch_queue[id].pop();
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
            relax_fence();
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
    Transaction::tid_type new_valid = received_tids[0];
    for (int i = 1; i < napply_threads; i++)
        new_valid = std::min(new_valid, received_tids[i]);
    min_received_tid = new_valid;
    release_fence();

    for (int i = 0; i < napply_threads; i++) {
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
