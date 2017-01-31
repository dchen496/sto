#pragma once

class LogApply {
  struct ThreadArgs {
    int thread_id;
  };

  struct LogBatch {
    uint64_t max_tid;
    bool needs_free;
    char *buf;
    char *start;
    char *end;
  };

public:
  static int listen(unsigned nthreads, int start_port);
  static void stop();
  static void cleanup(std::function<void()> callback);

  static bool debug_txn_log;
  static uint64_t txns_processed[MAX_THREADS];

private:
  static void *applier(void *argsptr);
  static bool read_batch(std::vector<char *> &buffer_pool, LogBatch &batch);
  static char *process_batch_part(LogBatch &batch, uint64_t max_tid);
  static char *process_txn(char *ptr);
  static int advance();
  static void *advancer(void *argsptr);
  static void run_cleanup();

  static int nthreads;
  static int listen_fds[MAX_THREADS];
  static int sock_fds[MAX_THREADS];
  static pthread_t apply_threads[MAX_THREADS];
  static pthread_t advance_thread;
  static ThreadArgs thread_args[MAX_THREADS];

  static bool run;
  static Transaction::tid_type received_tids[MAX_THREADS];
  static Transaction::tid_type min_received_tid;

  static Transaction::tid_type processed_tids[MAX_THREADS];

  static std::vector<std::function<void()>> cleanup_callbacks[MAX_THREADS];
};
