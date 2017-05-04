#pragma once

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

class LogSend {
    struct LogBatch {
        char *buf;
        int len;
    };

public:
    static int init_logging(unsigned nthreads, std::vector<std::string> hosts, int start_port);
    static void stop();
    static void enqueue_batch(char *buf, int len);
    static char* get_buffer();
    static void set_active(bool active, int thread_id);
    static volatile bool run;

private:
    static void *sender(void *argsptr);


    struct __attribute__((aligned(128))) SendThread {
        int thread_id;
        pthread_t handle;
        std::vector<int> fds;
        std::mutex mu;
        std::queue<LogBatch> batch_queue;
        std::condition_variable batch_queue_cond;
        bool active;
    };

    static int nsend_threads;
    static SendThread send_threads[MAX_THREADS];

    struct __attribute__((aligned(128))) WorkerThread {
        std::mutex mu;
        std::queue<LogBatch> free_queue;
    };

    static int nworker_threads;
    static WorkerThread worker_threads[MAX_THREADS];
};

class LogApply {
    struct ThreadArgs {
        int thread_id;
    };

    struct LogBatch {
        uint64_t recv_thr_id;
        uint64_t max_tid;
        char *buf;
        char *start;
        char *end;
    };

public:
    static int listen(unsigned nthreads, int start_port, std::function<void()> apply_init_fn = []{});
    static void stop();
    static void cleanup(std::function<void()> callback);

    static uint64_t txns_processed[MAX_THREADS];

private:
    static void *receiver(void *argsptr);
    static bool read_batch(int sock_fd, std::vector<char *> &buffer_pool, LogBatch &batch);

    static void *applier(void *argsptr);
    static bool process_batch_part(LogBatch &batch, uint64_t max_tid);
    static char *process_txn(char *ptr);

    static void *advancer(void *argsptr);
    static int advance();
    static void run_cleanup();

    struct __attribute__((aligned(128))) RecvThread {
        int thread_id;
        pthread_t handle;
        int listen_fd;
        int sock_fd;
        std::mutex mu;
        std::queue<LogBatch> free_queue;
    };

    static int nrecv_threads;
    static RecvThread recv_threads[MAX_THREADS];

    struct __attribute__((aligned(128))) ApplyThread {
        int thread_id;
        pthread_t handle;
        std::function<void()> init_fn;

        std::mutex mu;
        std::queue<LogBatch> batch_queue;
        std::condition_variable batch_queue_cond;
        Transaction::tid_type received_tid;
        Transaction::tid_type processed_tid;
        Transaction::tid_type cleaned_tid;
        std::vector<std::function<void()>> cleanup_callbacks;
    };

    static int napply_threads;
    static ApplyThread apply_threads[MAX_THREADS];

    enum ApplyState {
        IDLE = 0,
        APPLY,
        CLEAN,
        KILL
    };
    static pthread_t advance_thread;
    static Transaction::tid_type tid_bound;
    static ApplyState apply_state;
};
