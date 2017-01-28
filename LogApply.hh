#pragma once

class LogApply {
  struct ThreadArgs {
    int thread_id;
  };

public:
  static int listen(unsigned nthreads, int start_port);
  static void stop();

  static bool debug_txn_log;

private:
  static void *applier(void *argsptr);
  static bool read_batch(char *buf, char *&start, char *&end, bool &needs_free);
  static char *process_batch_part(char *start, char *end, uint64_t max_tid);
  static char *process_txn(char *ptr);
  static int advance();
  static void *advancer(void *argsptr);

  static int nthreads;
  static int listen_fds[MAX_THREADS];
  static int sock_fds[MAX_THREADS];
  static pthread_t apply_threads[MAX_THREADS];
  static pthread_t advance_thread;
  static ThreadArgs thread_args[MAX_THREADS];

  static bool run;
  static Transaction::tid_type recvd_tids[MAX_THREADS];
  static Transaction::tid_type min_recvd_tid;
};
