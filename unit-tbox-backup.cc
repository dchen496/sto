#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogProto.hh"

const int port = 2000;
const int niters = 2000000;

void test_simple_int(int batch) {
    TBox<int> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, 1, port) == 0);

    assert(f.nontrans_read() == batch ? 69 : 19);
    assert(LogApply::txns_processed[0] == 20);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_many_writes(int batch) {
    const int n = 100;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    assert(LogApply::listen(1, 1, port) == 0);

    assert(LogApply::txns_processed[0] == niters + 1);
    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_multithreaded(int batch) {
    const int n = 20;
    const int nrecv_thread = 2;
    const int napply_thread = 5;
    TBox<int> fs[n];
    TBox<int> refs[n];
    TBox<int> ntxns[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    for (int i = 0; i < napply_thread; i++)
        Transaction::register_object(ntxns[i], i + 2 * n);

    assert(LogApply::listen(nrecv_thread, napply_thread, port) == 0);

    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    for (int i = 0; i < napply_thread; i++)
        assert((int) LogApply::txns_processed[i] == ntxns[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_simple_string() {
    TBox<std::string> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, 1, port) == 0);

    assert(f.nontrans_read() == "19");
    assert(LogApply::txns_processed[0] == 20);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main() {
    LogApply::debug_txn_log = false;
    test_simple_int(0);
    test_simple_int(1);
    //test_many_writes(0);
    test_many_writes(1);
    //test_multithreaded(0);
    test_multithreaded(1);
    test_simple_string();
    return 0;
}
