#pragma once

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

class LogSend {
  struct ThreadArgs {
    int thread_id;
  };

public:

};

class LogApply {
  struct ThreadArgs {
    int thread_id;
  };

  struct LogBatch {
    uint64_t recv_thr_id;
    uint64_t thread_id;
    uint64_t max_tid;
    bool needs_free;
    char *buf;
    char *start;
    char *end;
  };

public:
  static int listen(unsigned nrecv_threads, unsigned napply_threads, int start_port);
  static void stop();
  static void cleanup(std::function<void()> callback);

  static bool debug_txn_log;
  static uint64_t txns_processed[MAX_THREADS];

private:
  static void *receiver(void *argsptr);
  static bool read_batch(int sock_fd, std::vector<char *> &buffer_pool, LogBatch &batch);

  static void *applier(void *argsptr);
  static char *process_batch_part(LogBatch &batch, uint64_t max_tid);
  static char *process_txn(char *ptr);

  static void *advancer(void *argsptr);
  static int advance();
  static void run_cleanup();

  static int nrecv_threads;
  static int listen_fds[MAX_THREADS];
  static int sock_fds[MAX_THREADS];
  static pthread_t recv_threads[MAX_THREADS];
  static ThreadArgs recv_thread_args[MAX_THREADS];

  static int napply_threads;
  static pthread_t apply_threads[MAX_THREADS];
  static ThreadArgs apply_thread_args[MAX_THREADS];

  static pthread_t advance_thread;

  static volatile bool run;

  static std::mutex apply_mutexes[MAX_THREADS];
  static std::queue<LogBatch> batch_queue[MAX_THREADS];
  static std::condition_variable batch_queue_wait[MAX_THREADS];
  static Transaction::tid_type received_tids[MAX_THREADS];
  static Transaction::tid_type min_received_tid;
  static Transaction::tid_type processed_tids[MAX_THREADS];

  static std::mutex recv_mutexes[MAX_THREADS];
  static std::queue<LogBatch> free_queue[MAX_THREADS];

  static std::vector<std::function<void()>> cleanup_callbacks[MAX_THREADS];
};
