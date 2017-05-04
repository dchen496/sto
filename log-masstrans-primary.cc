#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include <cstdlib>

#include "Transaction.hh"
#include "LogProto.hh"
#include "MassTrans.hh"

const int startup_delay = 500000;
typedef MassTrans<std::string, versioned_str_struct, /* opacity */ false> mbta_type;

int nthreads;
int ntrees;
int niters;
int nkeys;
int strsize;
int txnsize;
std::string backup_host;
int start_port;

template <typename T>
std::string to_str(T v) {
    std::stringstream ss;
    ss << v;
    return ss.str();
}

struct ThreadArgs {
    int id;
    mbta_type *fs;
    bool enable_logging;
};

int num_inserts = 0;

void *test_multithreaded_worker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    mbta_type::thread_init();

    mbta_type *fs = args.fs;
    std::string val;
    val.resize(strsize);

    int local_num_inserts = 0;
    for (int i = 0; i < niters; i++) {
        Sto::start_transaction();
        try {
            for (int j = 0; j < txnsize; j++) {
                std::string key_str = to_str(((unsigned) rand()) % nkeys);
                mbta_type &f = fs[((unsigned) rand()) % ntrees];
                val[((unsigned) rand()) % strsize] = (char) rand();

                local_num_inserts += !f.transPut(key_str, val);
            }
            Sto::try_commit();
        } catch (Transaction::Abort e) {
        }
        //usleep(1);
    }
    if (args.enable_logging)
        Transaction::flush_log_batch();
    __sync_fetch_and_add(&num_inserts, local_num_inserts);
    return nullptr;
}

void test_multithreaded(bool enable_logging) {
    num_inserts = 0;
    usleep(startup_delay);
    std::vector<mbta_type> fs(ntrees);
    for (unsigned i = 0; i < fs.size(); i++)
        Transaction::register_object(fs[i], i);
    if (enable_logging)
        assert(LogSend::init_logging(nthreads, {backup_host}, start_port) == 0);

    std::vector<pthread_t> thrs(nthreads);
    std::vector<ThreadArgs> args(nthreads);

    using hc = std::chrono::high_resolution_clock;

    hc::time_point time_start = hc::now();

    for (int i = 0; i < nthreads; i++) {
        args[i] = { .id = i, .fs = fs.data(), .enable_logging = enable_logging };
        pthread_create(&thrs[i], nullptr, &test_multithreaded_worker, (void *) &args[i]);
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    hc::time_point time_end = hc::now();
    printf("done\n");

    if (enable_logging)
        LogSend::stop();
    hc::time_point time_log_end = hc::now();

    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);

    int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_start).count();
    int64_t us2 = std::chrono::duration_cast<std::chrono::microseconds>(time_log_end - time_start).count();
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
    double s2 = us2 / 1.0e6;
    printf("s=%f s2=%f MB/s=%f bufs/s=%f ents/s=%f txns/s=%f\n", s, s2, agg.bytes/s/1.0e6, agg.bufs/s, agg.ents/s, agg.txns/s);
    printf("b/ent=%f b/txn=%f\n", (double)agg.bytes/agg.ents, (double)agg.bytes/agg.txns);
    printf("inserts=%d\n", num_inserts);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 9) {
        printf("usage: log-masstrans-primary nthreads ntrees nkeys niters strsize txnsize backup_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    nthreads = atoi(*arg++);
    ntrees = atoi(*arg++);
    nkeys = atoi(*arg++);
    niters = atoi(*arg++);
    strsize = atoi(*arg++);
    txnsize = atoi(*arg++);
    backup_host = std::string(*arg++);
    start_port = atoi(*arg++);

    mbta_type::static_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_multithreaded(false);
    test_multithreaded(true);
    return 0;
}
