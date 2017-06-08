#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"

const int startup_delay = 500000;

int nthreads;
int niters;
int txnsize;
std::string backup_host;
int start_port;

template <typename T>
struct ThreadArgs {
    int id;
    TBox<T> *fs;
    bool enable_logging;
};

template <int S>
struct filler {
    static_assert(S % sizeof(int) == 0, "S must be a multiple of the int size");

    int buf[S / sizeof(int)];

    filler() = default;
    filler(const filler &f) = default;
    filler &operator=(const filler &f) = default;

    filler(int x) {
        for (int i = 0; i < S / sizeof(int); i++) {
            buf[i] = x;
        }
    }

    friend std::ostream &operator<<(std::ostream &stream, const filler &f) {
        return stream;
    }
};

template <typename T>
void *test_multithreaded_worker(void *argptr) {
    ThreadArgs<T> &args = *(ThreadArgs<T> *) argptr;
    TThread::set_id(args.id);

    /*
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(args.id, &cpuset);
    CPU_SET(args.id + 32, &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
    */

    TBox<T> *fs = args.fs;
    int val = 0;
    for (int i = 0; i < niters; i++) {
        TransactionGuard t;
        for (int j = 0; j < txnsize; j++) {
            fs[j + args.id * txnsize] = val;
            val++;
        }
    }
    if (args.enable_logging)
        Transaction::flush_log_batch();
    return nullptr;
}

template <typename T>
void test_multithreaded(bool enable_logging) {
    usleep(startup_delay);
    std::vector<TBox<T>> fs(nthreads * txnsize);
    for (unsigned i = 0; i < fs.size(); i++)
        Transaction::register_object(fs[i], i);
    if (enable_logging)
        assert(LogPrimary::init_logging(nthreads, {backup_host}, start_port) == 0);

    std::vector<pthread_t> thrs(nthreads);
    std::vector<ThreadArgs<T>> args(nthreads);

    using hc = std::chrono::high_resolution_clock;

    hc::time_point time_start = hc::now();

    for (int i = 0; i < nthreads; i++) {
        args[i] = { .id = i, .fs = fs.data(), .enable_logging = enable_logging };
        pthread_create(&thrs[i], nullptr, &test_multithreaded_worker<T>, (void *) &args[i]);
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    hc::time_point time_end = hc::now();

    if (enable_logging)
        LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);

    int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_start).count();
    log_stats_t agg;
    if (enable_logging) {
        for (int i = 0; i < nthreads; i++) {
            log_stats_t &s = Transaction::tinfo[i].log_stats;
            agg.bytes += s.bytes;
            agg.bufs += s.bufs;
            agg.ents += s.ents;
            agg.txns += s.txns;
        }
    } else {
        agg.ents = txnsize * nthreads * niters;
        agg.txns = nthreads * niters;
    }

    printf("MB=%.2f bufs=%llu ents=%llu txns=%llu\n", agg.bytes/1.0e6, agg.bufs, agg.ents, agg.txns);
    double s = us / 1.0e6;
    printf("s=%f MB/s=%f bufs/s=%f ents/s=%f txns/s=%f\n", s, agg.bytes/s/1.0e6, agg.bufs/s, agg.ents/s, agg.txns/s);
    printf("b/ent=%f b/txn=%f\n", (double)agg.bytes/agg.ents, (double)agg.bytes/agg.txns);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 6) {
        printf("usage: log-tbox-primary nthreads niters txnsize backup_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    nthreads = atoi(*arg++);
    niters = atoi(*arg++);
    txnsize = atoi(*arg++);
    backup_host = std::string(*arg++);
    start_port = atoi(*arg++);

    TThread::set_id(0);
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_multithreaded<int64_t>(false);
    test_multithreaded<int64_t>(true);
    return 0;
}
