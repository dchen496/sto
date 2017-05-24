#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include <cstdlib>

#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"
#include "MassTrans.hh"
#include "bench-masstrans-common.hh"

const int startup_delay = 2000000;
typedef MassTrans<std::string, versioned_str_struct, /* opacity */ false> mbta_type;
using hc = std::chrono::high_resolution_clock;

int runtime;
int init_keys;
int key_size;
int val_size;
int txn_size;
int read_pct;
int insert_pct;
int update_pct;
int cross_pct;
int nthreads;
std::string backup_host;
int start_port;

static inline unsigned generate_value(unsigned seed, std::string buf) {
  for (unsigned i = 0; i < buf.size(); i++) {
    buf[i] = next_rand(seed);
  }
  return next_rand(seed);
}

hc::time_point run_until;
struct ThreadArgs {
    int id;
    mbta_type *tree;
    bool enable_logging;
    int txns;
    int aborts;
    int reads;
    int inserts;
    int updates;
    int cross_reads;
    int cross_updates;
}  __attribute__ ((aligned (128)));

union padded_int {
    int v;
    char padding[128];
};
std::vector<padded_int> valid_keys;
std::vector<TBox<int>> valid_keys_boxes;

void *init_multithreaded_worker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    mbta_type::thread_init();
    mbta_type &tree = *args.tree;

    std::string key_buf;
    key_buf.resize(key_size);
    std::string val_buf;
    val_buf.resize(val_size);

    unsigned s = args.id;
    for (int j = 0; j < init_keys; j += 100) {
        try {
            Sto::start_transaction();
            for (int i = j; i < init_keys && i < j + 100; i++) {
                generate_key(args.id, i, key_buf);
                s = generate_value(s, val_buf);
                tree.transInsert(key_buf, val_buf);
            }
            assert(Sto::try_commit());
        } catch (Transaction::Abort e) {
            assert(false);
        }
    }

    valid_keys[args.id].v = init_keys;
    Sto::start_transaction();
    try {
        valid_keys_boxes[args.id] = init_keys;
        assert(Sto::try_commit());
    } catch (Transaction::Abort e) {
        assert(false);
    }
    if (args.enable_logging)
        Transaction::flush_log_batch();
    return nullptr;
}

void *test_multithreaded_worker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    mbta_type::thread_init();
    mbta_type &tree = *args.tree;

    std::string key_buf;
    key_buf.resize(key_size);
    std::string val_buf;
    val_buf.resize(val_size);

    unsigned s = args.id;
    next_rand(s);
    int insert_key = init_keys;

    while (true) {
        Sto::start_transaction();
        try {
            // generate one operation type per transaction (so we have true read-only txns)
            int pct1 = next_rand(s) % 100;
            for (int j = 0; j < txn_size; j++) {
                if (pct1 < insert_pct) {
                    // insert (never cross partition)
                    generate_key(args.id, insert_key, key_buf);
                    s = generate_value(s, val_buf);
                    tree.transInsert(key_buf, val_buf);
                    insert_key++;
                    release_fence();
                    valid_keys[args.id].v = insert_key;
                    args.inserts++;
                } else {
                    int partition = args.id;
                    int pct2 = next_rand(s) % 100;
                    if (pct2 < cross_pct) {
                        // cross partition
                        partition = next_rand(s) % nthreads;
                    }

                    int key = next_big_rand(s) % valid_keys[partition].v;
                    acquire_fence();
                    generate_key(partition, key, key_buf);
                    if (pct1 < insert_pct + read_pct) {
                        // read
                        assert(tree.transGet(key_buf, val_buf));
                        args.reads++;
                        if (partition != args.id)
                            args.cross_reads++;
                    } else {
                        // update
                        s = generate_value(s, val_buf);
                        tree.transUpdate(key_buf, val_buf);
                        args.updates++;
                        if (partition != args.id)
                            args.cross_updates++;
                    }
                }
            }
            Sto::try_commit();
        } catch (Transaction::Abort e) {
            args.aborts++;
        }
        args.txns++;

        // occasionally update valid box and check the time
        if (args.txns % 1024 == 0) {
            Sto::start_transaction();
            try {
                valid_keys_boxes[args.id] = valid_keys[args.id].v;
                Sto::try_commit();
            } catch (Transaction::Abort e) {
                assert(false);
            }
            if (hc::now() > run_until)
                break;
        }
    }
    if (args.enable_logging)
        Transaction::flush_log_batch();
    printf("thread %d: %d txns, %d aborts, %d reads, %d inserts, %d updates, %d cross reads, %d cross updates\n",
            args.id, args.txns, args.aborts, args.reads, args.inserts, args.updates, args.cross_reads, args.cross_updates);
    return nullptr;
}

void test_multithreaded(bool enable_logging) {
    mbta_type tree;
    valid_keys.clear();
    valid_keys.resize(nthreads);
    valid_keys_boxes.clear();
    valid_keys_boxes.resize(nthreads);

    if (enable_logging) {
        Transaction::register_object(tree, 0);
        for (int i = 0; i < nthreads; i++)
            Transaction::register_object(valid_keys_boxes[i], i + 1);
        assert(LogSend::init_logging(nthreads, {backup_host}, start_port) == 0);
    }

    std::vector<pthread_t> thrs(nthreads);
    std::vector<ThreadArgs> args(nthreads);

    printf("initializing\n");
    for (int i = 0; i < nthreads; i++) {
        args[i] = ThreadArgs();
        args[i].id = i;
        args[i].tree = &tree;
        args[i].enable_logging = enable_logging;
        pthread_create(&thrs[i], nullptr, &init_multithreaded_worker, (void *) &args[i]);

#if LOG_CPU_PIN
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i + LOG_CPU_PIN, &cpuset);
        pthread_setaffinity_np(thrs[i], sizeof(cpu_set_t), &cpuset);
#endif
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    // hack to force the backup to apply all entries we've sent it
    // also gives it some time to catch up if it's a batch or two behind
    if (enable_logging) {
      for (int i = 0; i < nthreads; i++)
        LogSend::set_active(false, i);
      usleep(100000);
      for (int i = 0; i < nthreads; i++)
        LogSend::set_active(true, i);
    }

    printf("starting\n");

    hc::time_point time_start = hc::now();
    run_until = time_start + std::chrono::seconds(runtime);

    for (int i = 0; i < nthreads; i++) {
        args[i] = ThreadArgs();
        args[i].id = i;
        args[i].tree = &tree;
        args[i].enable_logging = enable_logging;
        pthread_create(&thrs[i], nullptr, &test_multithreaded_worker, (void *) &args[i]);

#if LOG_CPU_PIN
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i + LOG_CPU_PIN, &cpuset);
        pthread_setaffinity_np(thrs[i], sizeof(cpu_set_t), &cpuset);
#endif
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    hc::time_point time_end = hc::now();
    printf("done\n");

    if (enable_logging)
        LogSend::stop();

    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);

    int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_start).count();
    double s = us / 1.0e6;

    log_stats_t agg;
    if (enable_logging) {
        for (int i = 0; i < nthreads; i++) {
            log_stats_t &s = Transaction::tinfo[i].log_stats;
            agg.bytes += s.bytes;
            agg.bufs += s.bufs;
            agg.ents += s.ents;
            agg.txns += s.txns;
        }
        printf("log MB=%.2f bufs=%lu ents=%lu txns=%lu\n", agg.bytes/1.0e6, agg.bufs, agg.ents, agg.txns);
        printf("log MB/s=%f bufs/s=%f ents/s=%f txns/s=%f\n",
                agg.bytes/s/1.0e6, agg.bufs/s, agg.ents/s, agg.txns/s);
        printf("log b/ent=%f b/txn=%f\n", (double)agg.bytes/agg.ents, (double)agg.bytes/agg.txns);
    }

    int txns = 0, aborts = 0, reads = 0, inserts = 0, updates = 0, cross_reads = 0, cross_updates = 0;
    for (int i = 0; i < nthreads; i++) {
        txns += args[i].txns;
        aborts += args[i].aborts;
        reads += args[i].reads;
        inserts += args[i].inserts;
        updates += args[i].updates;
        cross_reads += args[i].cross_reads;
        cross_updates += args[i].cross_updates;
    }
    printf("s=%f txns=%d txns/s=%f aborts=%d aborts/s=%f\n", s, txns, txns / s, aborts, aborts / s);
    printf("reads=%d reads/s=%f cross_reads=%d cross_reads/s=%f\n", reads, reads / s, cross_reads, cross_reads / s);
    printf("inserts=%d inserts/s=%f updates=%d updates/s=%f cross_updates=%d cross_updates/s=%f\n",
            inserts, inserts/s, updates, updates / s, cross_updates, cross_updates / s);

    // csv-formatted
    printf("!H runtime,init_keys,key_size,val_size,txn_size,read_pct,insert_pct,update_pct,cross_pct,nthreads"
            ",s,txns,txns/s,aborts,aborts/s,reads,reads/s,cross_reads,cross_reads/s"
            ",inserts,inserts/s,updates,updates/s,cross_updates,cross_updates/s");
    if (enable_logging)
        printf(",MB,MB/s,bufs,bufs/s,ents,ents/s,txns,txns/s,b/ent,b/txn");
    printf("\n");

    printf("!V %d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f",
            runtime,init_keys,key_size,val_size,txn_size,read_pct,insert_pct,update_pct,cross_pct,nthreads,
            s, txns, txns/s, aborts, aborts/s, reads, reads/s, cross_reads, cross_reads/s,
            inserts, inserts/s, updates, updates/s, cross_updates, cross_updates/s);
    if (enable_logging) {
        printf(",%f,%f,%lu,%f,%lu,%f,%lu,%f,%f,%f",
            agg.bytes/1.0e6, agg.bytes/s/1.0e6, agg.bufs, agg.bufs/s,
            agg.ents, agg.ents/s, agg.txns, agg.txns/s,
            (double)agg.bytes/agg.ents, (double)agg.bytes/agg.txns);
    }
    printf("\n");
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 13) {
        printf("usage: bench-masstrans-primary runtime init_keys key_size val_size txn_size read%% insert%% update%% cross%% nthreads backup_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    runtime = atoi(*arg++);
    printf("runtime: %d, ", runtime);
    init_keys = atoi(*arg++);
    printf("init_keys: %d, ", init_keys);
    key_size = atoi(*arg++);
    printf("key_size: %d, ", key_size);
    val_size = atoi(*arg++);
    printf("val_size: %d, ", val_size);
    txn_size = atoi(*arg++);
    printf("txn_size: %d, ", txn_size);
    read_pct = atoi(*arg++);
    printf("read_pct: %d, ", read_pct);
    insert_pct = atoi(*arg++);
    printf("insert_pct: %d, ", insert_pct);
    update_pct = atoi(*arg++);
    printf("update_pct: %d, ", update_pct);
    cross_pct = atoi(*arg++);
    printf("cross_pct: %d, ", cross_pct);
    nthreads = atoi(*arg++);
    printf("nthreads: %d\n", nthreads);
    backup_host = std::string(*arg++);
    start_port = atoi(*arg++);


    if (key_size < 4) {
        printf("key is too small\n");
        return -1;
    }

    if (!validate_pct(read_pct) || !validate_pct(insert_pct) ||
        !validate_pct(update_pct) || !validate_pct(cross_pct) ||
        read_pct + insert_pct + update_pct != 100) {
        printf("invalid percentages\n");
        return -1;
    }

    mbta_type::static_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    //test_multithreaded(false);
    usleep(startup_delay);
    test_multithreaded(true);
    return 0;
}
