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
  static Transaction::tid_type valid_tids[MAX_THREADS];
  static Transaction::tid_type min_valid_tid;
};
