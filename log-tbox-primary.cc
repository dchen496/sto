#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include "Transaction.hh"
#include "TBox.hh"

const int startup_delay = 500000;

int nthreads;
int niters;
int txnsize;
std::string backup_host;
int start_port;

struct ThreadArgs {
    int id;
    TBox<int> *fs;
    bool enable_logging;
};

void *test_multithreaded_int_worker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);

    /*
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(args.id, &cpuset);
    CPU_SET(args.id + 32, &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
    */

    TBox<int> *fs = args.fs;
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

void test_multithreaded_int(bool enable_logging) {
    usleep(startup_delay);
    std::vector<TBox<int>> fs(nthreads * txnsize);
    for (unsigned i = 0; i < fs.size(); i++)
        Transaction::register_object(fs[i], i);
    if (enable_logging)
        assert(Transaction::init_logging(nthreads, {backup_host}, start_port) == 0);

    pthread_t thrs[nthreads];
    ThreadArgs args[nthreads];

    using hc = std::chrono::high_resolution_clock;

    hc::time_point time_start = hc::now();

    for (int i = 0; i < nthreads; i++) {
        args[i] = { .id = i, .fs = fs.data(), .enable_logging = enable_logging };
        pthread_create(&thrs[i], nullptr, test_multithreaded_int_worker, (void *) &args[i]);
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    hc::time_point time_end = hc::now();

    if (enable_logging)
        Transaction::stop_logging();
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
        printf("usage: log-tbox-primary nthreads niters txnsize backup_host start_port");
        return -1;
    }

    nthreads = atoi(argv[1]);
    niters = atoi(argv[2]);
    txnsize = atoi(argv[3]);
    backup_host = std::string(argv[4]);
    start_port = atoi(argv[5]);

    TThread::set_id(0);
    Transaction::debug_txn_log = false;
    test_multithreaded_int(false);
    test_multithreaded_int(true);
    return 0;
}
