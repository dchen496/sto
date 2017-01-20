#pragma once

class LogApply {
  class ThreadArgs {
  public:
    int listen_fd;
    int thread_id;
  };

public:
  static void listen(unsigned num_threads, int start_port);
  static void stop();
private:
  static void *applier(void *args);
  static void process_batch(char *batch);
  static pthread_t apply_threads[MAX_THREADS];
  static ThreadArgs apply_thread_args[MAX_THREADS];
  static bool run;
};
