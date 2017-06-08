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
    assert(LogBackup::listen(1, port) == 0);

    assert(f.nontrans_read() == batch ? 69 : 19);
    assert(LogBackup::txns_processed[0] == 20);

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
    assert(LogBackup::listen(1, port) == 0);

    assert(LogBackup::txns_processed[0] == niters + 1);
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
    TBox<int> ntxns[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    for (int i = 0; i < nthread; i++)
        Transaction::register_object(ntxns[i], i + 2 * n);

    assert(LogBackup::listen(nthread, port) == 0);

    for (int i = 0; i < n; i++)
        assert(fs[i].nontrans_read() == refs[i].nontrans_read());

    for (int i = 0; i < nthread; i++)
        assert((int) LogBackup::txns_processed[i] == ntxns[i].nontrans_read());

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_simple_string() {
    TBox<std::string> f;
    Transaction::register_object(f, 0);
    assert(LogBackup::listen(1, port) == 0);

    assert(f.nontrans_read() == "19");
    assert(LogBackup::txns_processed[0] == 20);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main() {
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
