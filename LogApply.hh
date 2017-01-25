#pragma once

class LogApply {
  struct ApplyThreadArgs {
    int listen_fd;
    int thread_id;
  };
  struct AdvanceThreadArgs {
    int nthreads;
  };

public:
  static int listen(unsigned nthreads, int start_port);
  static void stop();

  static bool debug_txn_log;

private:
  static void *applier(void *argsptr);
  static void *advancer(void *argsptr);
  static char *process_txn(char *ptr);

  static pthread_t apply_threads[MAX_THREADS];
  static ApplyThreadArgs apply_thread_args[MAX_THREADS];
  static pthread_t advance_thread;
  static AdvanceThreadArgs advance_thread_arg;

  static bool run;
  static Transaction::tid_type next_tids[MAX_THREADS];
  static Transaction::tid_type min_next_tid;
};
