#include <iostream>
#include <cassert>
#include <vector>
#include <chrono>
#include <cstdlib>

#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"
#include "MassTrans.hh"
#include "log-masstrans-common.hh"

const int startup_delay = 500000;
typedef MassTrans<std::string, versioned_str_struct, /* opacity */ false> mbta_type;

int niters;
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

struct ThreadArgs {
    int id;
    mbta_type *tree;
    bool enable_logging;
};

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
    for (int i = 0; i < init_keys; i++) {
        Sto::start_transaction();
        try {
            generate_key(args.id, i, key_buf);
            s = generate_value(s, val_buf);
            tree.transInsert(key_buf, val_buf);
            assert(Sto::try_commit());
        } catch (Transaction::Abort e) {
            assert(false);
        }
    }
    valid_keys[args.id].v = init_keys;
    Sto::start_transaction();
    try {
        valid_keys_boxes[args.id] = init_keys;
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

    int reads = 0, inserts = 0, updates = 0, cross_reads = 0, cross_updates = 0;

    for (int i = 0; i < niters; i++) {
        Sto::start_transaction();
        try {
            // generate one operation type per transaction (so we have true read-only txns)
            int pct1 = (next_rand(s) >> 16) % 100;
            for (int j = 0; j < txn_size; j++) {
                if (pct1 < insert_pct) {
                    // insert (never cross partition)
                    generate_key(args.id, insert_key, key_buf);
                    s = generate_value(s, val_buf);
                    tree.transInsert(key_buf, val_buf);
                    insert_key++;
                    release_fence();
                    valid_keys[args.id].v = insert_key;
                    inserts++;
                } else {
                    int partition = args.id;
                    int pct2 = (next_rand(s) >> 16) % 100;
                    if (pct2 < cross_pct) {
                        // cross partition
                        partition = (next_rand(s) >> 16) % nthreads;
                    }

                    int key = next_rand(s) % valid_keys[partition].v;
                    acquire_fence();
                    generate_key(partition, key, key_buf);
                    if (pct1 < insert_pct + read_pct) {
                        // read
                        assert(tree.transGet(key_buf, val_buf));
                        reads++;
                        if (partition != args.id)
                            cross_reads++;
                    } else {
                        // update
                        s = generate_value(s, val_buf);
                        tree.transUpdate(key_buf, val_buf);
                        updates++;
                        if (partition != args.id)
                            cross_updates++;
                    }
                }
            }
            Sto::try_commit();
        } catch (Transaction::Abort e) {
        }

        // occasionally update valid box
        if (i % 1024 == 0) {
            Sto::start_transaction();
            try {
                valid_keys_boxes[args.id] = valid_keys[args.id].v;
                Sto::try_commit();
            } catch (Transaction::Abort e) {
            }
        }
    }
    if (args.enable_logging)
        Transaction::flush_log_batch();
    printf("thread %d: %d reads, %d inserts, %d updates, %d cross reads, %d cross updates\n", args.id, reads, inserts, updates, cross_reads, cross_updates);
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
        args[i] = { .id = i, .tree = &tree, .enable_logging = enable_logging };
        pthread_create(&thrs[i], nullptr, &init_multithreaded_worker, (void *) &args[i]);
    }
    for (int i = 0; i < nthreads; i++)
        pthread_join(thrs[i], nullptr);

    printf("starting\n");
    using hc = std::chrono::high_resolution_clock;

    hc::time_point time_start = hc::now();

    for (int i = 0; i < nthreads; i++) {
        args[i] = { .id = i, .tree = &tree, .enable_logging = enable_logging };
        pthread_create(&thrs[i], nullptr, &test_multithreaded_worker, (void *) &args[i]);
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

    if (enable_logging) {
        log_stats_t agg;
        for (int i = 0; i < nthreads; i++) {
            log_stats_t &s = Transaction::tinfo[i].log_stats;
            agg.bytes += s.bytes;
            agg.bufs += s.bufs;
            agg.ents += s.ents;
            agg.txns += s.txns;
        }
        printf("log MB=%.2f bufs=%llu ents=%llu txns=%llu\n", agg.bytes/1.0e6, agg.bufs, agg.ents, agg.txns);
        printf("log MB/s=%f bufs/s=%f ents/s=%f txns/s=%f\n",
                agg.bytes/s/1.0e6, agg.bufs/s, agg.ents/s, agg.txns/s);
        printf("log b/ent=%f b/txn=%f\n", (double)agg.bytes/agg.ents, (double)agg.bytes/agg.txns);
    }

    printf("s=%f txns=%d txns/s=%f\n", s, niters * nthreads, niters * nthreads / s);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 13) {
        printf("usage: log-masstrans-primary niters init_keys key_size val_size txn_size read%% insert%% update%% cross%% nthreads backup_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    niters = atoi(*arg++);
    printf("niters: %d\n", niters);
    init_keys = atoi(*arg++);
    printf("init_keys: %d\n", init_keys);
    key_size = atoi(*arg++);
    printf("key_size: %d\n", key_size);
    val_size = atoi(*arg++);
    printf("val_size: %d\n", val_size);
    txn_size = atoi(*arg++);
    printf("txn_size: %d\n", txn_size);
    read_pct = atoi(*arg++);
    printf("read_pct: %d\n", read_pct);
    insert_pct = atoi(*arg++);
    printf("insert_pct: %d\n", insert_pct);
    update_pct = atoi(*arg++);
    printf("update_pct: %d\n", update_pct);
    cross_pct = atoi(*arg++);
    printf("cross_pct: %d\n", cross_pct);
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
