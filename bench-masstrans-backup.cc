#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"
#include "MassTrans.hh"
#include "bench-masstrans-common.hh"

int nthreads;
int key_size;
int val_size;
int txn_size;
int cross_pct;
int start_port;

typedef MassTrans<std::string, versioned_str_struct, /*opacity*/ false> mbta_type;
using hc = std::chrono::system_clock;

struct idle_ctx {
    int iter;
    int reads;
    int cross_reads;
    int valid_keys;
    unsigned s;
    TBox<int> valid_keys_box;
    hc::time_point time_start;
    mbta_type *tree;
} __attribute__ ((aligned (128)));

std::vector<idle_ctx> contexts;

void thread_init() {
    mbta_type::thread_init();
}

auto thread_init_obj = std::function<void()>(&thread_init);

void idle_fn(uint64_t tid) {
    (void) tid;
    int id = TThread::id();
    idle_ctx &ctx = contexts[id];

    // wait for initialization
    if (ctx.valid_keys == 0) {
        Sto::start_transaction();
        try {
            ctx.valid_keys = ctx.valid_keys_box;
            assert(Sto::try_commit());
        } catch (Transaction::Abort e) {
            assert(false);
        }
        usleep(1000);
        // good enough...
        ctx.time_start = hc::now();
        return;
    }

    // periodically update valid keys
    if (ctx.iter % 1024 == 0)
        ctx.valid_keys = ctx.valid_keys_box.nontrans_read();

    std::string key_buf;
    key_buf.resize(key_size);
    std::string val_buf;
    val_buf.resize(val_size);

    unsigned &s = ctx.s;
    Sto::start_transaction();
    try {
        for (int i = 0; i < txn_size; i++) {
                int partition = id;
                int pct = next_rand(s) % 100;
                if (pct < cross_pct) {
                    partition = next_rand(s) % nthreads;
                    if (contexts[partition].valid_keys == 0)
                        partition = id;
                }

                int key = next_big_rand(s) % contexts[partition].valid_keys;
                generate_key(partition, key, key_buf);
                assert(ctx.tree->transGet(key_buf, val_buf));
                ctx.reads++;
                if (partition != id) {
                    ctx.cross_reads++;
                }
        }
        Sto::try_commit();
    } catch (Transaction::Abort e) {
        assert(false);
    }
    ctx.iter++;
}

auto idle_fn_obj = std::function<void(uint64_t)>(&idle_fn);

void test_multithreaded() {
    mbta_type tree;
    Transaction::register_object(tree, 0);
    contexts.clear();
    contexts.resize(nthreads);
    for (int i = 0; i < nthreads; i++) {
        contexts[i].tree = &tree;
        contexts[i].time_start = hc::now();
        contexts[i].s = i;
        Transaction::register_object(contexts[i].valid_keys_box, i + 1);
    }
    assert(LogApply::listen(nthreads, start_port, thread_init_obj, idle_fn_obj) == 0);
    Transaction::clear_registered_objects();
    hc::time_point time_end = hc::now();

    int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(time_end - contexts[0].time_start).count();
    double s = us / 1.0e6;
    int total_reads = 0;
    for (int i = 0; i < nthreads; i++) {
        idle_ctx &ctx = contexts[i];
        printf("thread %d: %d reads, %d cross reads\n", i, ctx.reads, ctx.cross_reads);
        total_reads += ctx.reads;
    }

    printf("s=%f reads=%d reads/s=%f\n", s, total_reads, total_reads / s);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 7) {
        printf("usage: bench-masstrans-backup key_size val_size txn_size cross%% nthreads start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    key_size = atoi(*arg++);
    val_size = atoi(*arg++);
    txn_size = atoi(*arg++);
    cross_pct = atoi(*arg++);
    nthreads = atoi(*arg++);
    start_port = atoi(*arg++);

    if (!validate_pct(cross_pct)) {
        printf("invalid percentages\n");
    }

    mbta_type::static_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_multithreaded();
    return 0;
}
