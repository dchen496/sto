#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogApply.hh"

const int port = 2000;
const int niters = 2000000;

void test_singlethreaded_int() {
    const int n = 100;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    assert(LogApply::listen(1, port) == 0);
    assert(LogApply::txns_processed[0] == niters + 1);
    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_multithreaded_int() {
    const int n = 20;
    const int nthread = 4;
    TBox<int> fs[n];
    TBox<int> refs[n];
    TBox<int> ntxns[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    for (int i = 0; i < nthread; i++)
        Transaction::register_object(ntxns[i], i + 2 * n);

    assert(LogApply::listen(nthread, port) == 0);

    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    for (int i = 0; i < nthread; i++)
        assert(LogApply::txns_processed[i] == ntxns[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main() {
    LogApply::debug_txn_log = false;
    test_singlethreaded_int();
    test_multithreaded_int();
    return 0;
}
