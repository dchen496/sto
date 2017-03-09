#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "MassTrans.hh"

#define GUARDED if (TransactionGuard tguard{})

const std::string host = "127.0.0.1";
const int port = 2000;
const int niters = 2000000;
const int startup_delay = 500000;

void thread_init() {
    MassTrans<int>::thread_init();
}

template <typename T>
std::string to_str(T v) {
    std::stringstream ss;
    ss << v;
    return ss.str();
}

void test_simple_int(int batch) {
    usleep(startup_delay);
    MassTrans<int> f;
    Transaction::register_object(f, 0);
    assert(Transaction::init_logging(1, {host}, port) == 0);

    // inserts
    for (int i = 0; i < 20; i++) {
        int val = batch ? (i + 50) : i;
        {
            TransactionGuard t;
            std::string key = to_str<int>(i);
            f.transInsert(key, val + 1);
        }
        if (!batch)
            Transaction::flush_log_batch();
    }

    // get and update
    for (int i = 1; i < 20; i += 2) {
        {
            TransactionGuard t;
            int v = 0;
            std::string key1 = to_str<int>(i - 1);
            std::string key2 = to_str<int>(i);
            assert(f.transGet(key1, v));
            f.transUpdate(key2, v * 2);
        }
        if (!batch)
            Transaction::flush_log_batch();
    }

    {
        TransactionGuard t;
        std::string key1 = to_str<int>(19);
        f.transDelete(key1);
    }
    Transaction::flush_log_batch();
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

/*
void test_many_writes(int batch) {
    usleep(startup_delay);
    const int n = 100;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
        fs[i].nontrans_write(i);
        refs[i].nontrans_write(0);
    }
    assert(Transaction::init_logging(1, {host}, port) == 0);

    for (int i = 0; i < niters; i++) {
        int a = (i * 17) % n;
        int b = (i * 19) % n;
        int c = (i * 31) % n;
        {
            TransactionGuard t;
            fs[a] = (23 * fs[b] + fs[c] + 197) % 997;
        }
        if (!batch)
            Transaction::flush_log_batch();
    }
    Transaction::flush_log_batch();

    {
        TransactionGuard t;
        for (int i = 0; i < n; i++) {
            refs[i] = fs[i];
        }
    }
    Transaction::flush_log_batch();

    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

struct ThreadArgs {
    int id;
    bool batch;
    TBox<int> *fs;
    int n;
};

void *test_multithreadedWorker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    TBox<int> *fs = args.fs;
    int n = args.n;
    for (int i = 0; i < niters; i++) {
        int a = (i * 17) % n;
        int b = (i * 19) % n;
        int c = (i * 31) % n;

        try {
            Sto::start_transaction();
            fs[a] = (23 * fs[b] + fs[c] + 197) % 997;
            if (Sto::try_commit() && !args.batch)
                Transaction::flush_log_batch();
        } catch (Transaction::Abort e) {
        }
    }
    Transaction::flush_log_batch();
    return nullptr;
}

void test_multithreaded(int batch) {
    usleep(startup_delay);
    const int n = 20;
    const int nthread = 4;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
        fs[i].nontrans_write(i);
        refs[i].nontrans_write(0);
    }

    assert(Transaction::init_logging(nthread, {host}, port) == 0);
    pthread_t thrs[nthread];
    ThreadArgs args[nthread];
    for (int i = 0; i < nthread; i++) {
        args[i] = { .id = i, .batch = batch, .fs = fs, .n = n };
        pthread_create(&thrs[i], nullptr, test_multithreadedWorker, (void *) &args[i]);
    }
    for (int i = 0; i < nthread; i++)
        pthread_join(thrs[i], nullptr);

    {
        TransactionGuard t;
        for (int i = 0; i < n; i++) {
            refs[i] = fs[i];
        }
    }
    Transaction::flush_log_batch();

    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_simple_string() {
    usleep(startup_delay);
    TBox<std::string> f;
    Transaction::register_object(f, 0);
    assert(Transaction::init_logging(1, {host}, port) == 0);

    for (int i = 0; i < 20; i++) {
        std::stringstream ss;
        ss << i;
        {
            TransactionGuard t;
            f = ss.str();
        }

        {
            TransactionGuard t2;
            std::string f_read = f;
            assert(f_read == ss.str());
        }
        Transaction::flush_log_batch();
    }
    Transaction::flush_log_batch();
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}
*/

int main() {
    thread_init();
    TThread::set_id(0);
    //Transaction::debug_txn_log = false;
    test_simple_int(0);
    //test_simple_int(1);
    /*
    test_many_writes(0);
    test_many_writes(1);
    test_multithreaded(0);
    test_multithreaded(1);
    test_simple_string();
    */
    return 0;
}
