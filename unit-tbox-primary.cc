#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"

#define GUARDED if (TransactionGuard tguard{})

void test_simple_int(int batch) {
    usleep(500000);
    TBox<int> f;
    Transaction::register_object(f, 0);
    assert(Transaction::init_logging(1, {"127.0.0.1"}, 2000) == 0);

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
            Transaction::flush_log_buffer();
    }
    if (batch)
        Transaction::flush_log_buffer();

    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    std::flush(std::cout);
}

void test_many_writes(int batch) {
    usleep(500000);
    TBox<int> fs[100];
    for (int i = 0; i < 100; i++) {
        Transaction::register_object(fs[i], i);
        fs[i].nontrans_write(i);
    }
    assert(Transaction::init_logging(1, {"127.0.0.1"}, 2000) == 0);

    for (int i = 0; i < 1000000; i++) {
        int a = (i * 17) % 100;
        int b = (i * 19) % 100;
        int c = (i * 31) % 100;
        {
            TransactionGuard t;
            fs[a] = (fs[b] + fs[c]) % 1000;
        }
        if (!batch)
            Transaction::flush_log_buffer();
    }
    Transaction::flush_log_buffer();

    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    std::flush(std::cout);
}

struct ThreadArgs {
    int id;
    bool batch;
    TBox<int> (*fs)[100];
};

void *test_multithreadedWorker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    TBox<int> (&fs)[100] = *args.fs;
    for (int i = 0; i < 1000000; i++) {
        int a = (i * 17) % 100;
        int b = (i * 19) % 100;
        int c = (i * 31) % 100;
        {
            TransactionGuard t;
            fs[a] = (fs[b] + fs[c]) % 1000;
        }
        if (!args.batch)
            Transaction::flush_log_buffer();
    }
    Transaction::flush_log_buffer();
    return nullptr;
}

void test_multithreaded(int batch) {
    usleep(500000);
    TBox<int> fs[100];
    TBox<int> refs[100];
    for (int i = 0; i < 100; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + 100);
        fs[i].nontrans_write(i);
        refs[i].nontrans_write(0);
    }

    assert(Transaction::init_logging(4, {"127.0.0.1"}, 2000) == 0);
    pthread_t thrs[4];
    ThreadArgs args[4];
    for (int i = 0; i < 4; i++) {
        args[i].id = i;
        args[i].batch = batch;
        args[i].fs = &fs;
        pthread_create(&thrs[i], nullptr, test_multithreadedWorker, (void *) &args[i]);
    }

    for (int i = 0; i < 4; i++)
        pthread_join(thrs[i], nullptr);

    // hacky way of sending the expected values
    for (int i = 0; i < 100; i++) {
        TransactionGuard t;
        refs[i] = fs[i];
    }
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
    std::flush(std::cout);
}

int main() {
    TThread::set_id(0);
    Transaction::debug_txn_log = false;
    test_simple_int(0);
    test_simple_int(1);
    test_many_writes(0);
    test_many_writes(1);
    test_multithreaded(0);
    test_multithreaded(1);
    return 0;
}
