#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "MassTrans.hh"
#include "LogProto.hh"

const int port = 2000;
const int niters = 2000000;

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
    MassTrans<int> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port, std::function<void()>(&thread_init)) == 0);

    int v1 = -1, v2 = -1;
    bool found1 = false, found2 = false;
    printf("here\n");
    fflush(stdout);
    {
        TransactionGuard t;
        std::string key = to_str<int>(19);
        found1 = f.transGet(key, v1);
    }
    assert(!found1);

    printf("here2\n");
    fflush(stdout);
    for (int i = 0; i < 19; i += 2) {
        {
            TransactionGuard t;
            std::string key = to_str<int>(i);
            found1 = f.transGet(key, v1);
        }
        assert(found1);
        assert(v1 == batch ? (i + 51) : (i + 1));
    }

    printf("here3\n");
    fflush(stdout);
    for (int i = 1; i < 19; i += 2) {
        TransactionGuard t;
        std::string key1 = to_str<int>(i);
        std::string key2 = to_str<int>(i - 1);
        found1 = f.transGet(key1, v1);
        found2 = f.transGet(key2, v2);
    }
    assert(found1 && found2);
    assert(v1 == 2 * v2);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

/*
void test_many_writes(int batch) {
    const int n = 100;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    assert(LogApply::listen(1, port) == 0);

    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_multithreaded(int batch) {
    const int n = 20;
    const int nthread = 4;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    assert(LogApply::listen(nthread, port) == 0);

    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_simple_string() {
    TBox<std::string> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port) == 0);

    assert(f.nontrans_read() == "19");

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}
*/

int main() {
    thread_init();
//    LogApply::debug_txn_log = false;
    test_simple_int(0);
    test_simple_int(1);
    /*
    test_many_writes(0);
    test_many_writes(1);
    test_multithreaded(0);
    test_multithreaded(1);
    test_simple_string();
    */
    return 0;
}
