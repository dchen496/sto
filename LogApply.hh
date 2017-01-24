#pragma once

class LogApply {
  class ThreadArgs {
  public:
    int listen_fd;
    int thread_id;
  };

public:
  static int listen(unsigned num_threads, int start_port);
  static void stop();
  static bool debug_txn_log;

private:
  static void *applier(void *args);
  static char *process_txn(char *ptr);
  static pthread_t apply_threads[MAX_THREADS];
  static ThreadArgs apply_thread_args[MAX_THREADS];
  static bool run;
};
