#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"

#define GUARDED if (TransactionGuard tguard{})

const std::string host = "127.0.0.1";
const int port = 2000;
const int niters = 2000000;
const int startup_delay = 500000;

void test_simple_int(int batch) {
    usleep(startup_delay);
    TBox<int> f;
    Transaction::register_object(f, 0);
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    for (int i = 0; i < 20; i++) {
        int val = batch ? (i + 50) : i;
        {
            TransactionGuard t;
            f = val;
        }

        {
            TransactionGuard t2;
            int f_read = f;
            assert(f_read == val);
        }
        if (!batch)
            Transaction::flush_log_batch();
    }
    Transaction::flush_log_batch();

    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

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
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

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
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

struct ThreadArgs {
    int id;
    bool batch;
    TBox<int> *fs;
    int n;
    int ntxns;
};

void *test_multithreaded_worker(void *argptr) {
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
            Sto::commit();
            if (!args.batch)
                Transaction::flush_log_batch();
            args.ntxns++;
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
    TBox<int> ntxns[nthread];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
        fs[i].nontrans_write(i);
        refs[i].nontrans_write(0);
    }
    for (int i = 0; i < nthread; i++)
        Transaction::register_object(ntxns[i], i + 2 * n);

    assert(LogPrimary::init_logging(nthread, {host}, port) == 0);
    pthread_t thrs[nthread];
    ThreadArgs args[nthread];
    for (int i = 0; i < nthread; i++) {
        args[i] = { .id = i, .batch = batch, .fs = fs, .n = n, .ntxns = 0 };
        pthread_create(&thrs[i], nullptr, test_multithreaded_worker, (void *) &args[i]);
    }
    for (int i = 0; i < nthread; i++)
        pthread_join(thrs[i], nullptr);

    {
        TransactionGuard t;
        for (int i = 0; i < n; i++) {
            refs[i] = fs[i];
        }
        ntxns[0] = args[0].ntxns + 1;
        for (int i = 1; i < nthread; i++)
            ntxns[i] = args[i].ntxns;
    }
    Transaction::flush_log_batch();
    LogPrimary::stop();

    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_simple_string() {
    usleep(startup_delay);
    TBox<std::string> f;
    Transaction::register_object(f, 0);
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

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
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main() {
    TThread::set_id(0);
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_simple_int(0);
    test_simple_int(1);
    test_many_writes(1);
    test_multithreaded(1);
    test_simple_string();
    return 0;
}
