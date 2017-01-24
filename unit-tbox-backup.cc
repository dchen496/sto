#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogApply.hh"

const int port = 2000;
const int niters = 2000000;

void test_simple_int(int batch) {
    TBox<int> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port) == 0);

    {
        TransactionGuard t;
        int f_read = f;
        assert(f_read == batch ? 69 : 19);
    }
    Transaction::clear_registered_objects();
    std::cout << "BACKUP PASS: " << __FUNCTION__ << "(" << batch << ")\n";
    std::flush(std::cout);
}

void test_many_writes(int batch) {
    const int n = 100;
    TBox<int> fs[n];
    TBox<int> refs[n];
    for (int i = 0; i < n; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + n);
    }
    assert(LogApply::listen(1, port) == 0);

    for (int i = 0; i < n; i++) {
        TransactionGuard t;
        int f_read = fs[i];
        int ref_read = refs[i];
        assert(f_read == ref_read);
    }
    Transaction::clear_registered_objects();
    std::cout << "BACKUP PASS: " << __FUNCTION__ << "(" << batch << ")\n";
    std::flush(std::cout);
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

    for (int i = 0; i < n; i++) {
        TransactionGuard t;
        int f_read = fs[i];
        int ref_read = refs[i];
        assert(ref_read == f_read);
    }
    Transaction::clear_registered_objects();
    std::cout << "BACKUP PASS: " << __FUNCTION__ << "(" << batch << ")\n";
    std::flush(std::cout);
}

int main() {
    LogApply::debug_txn_log = false;
    test_simple_int(0);
    test_simple_int(1);
    test_many_writes(0);
    test_many_writes(1);
    test_multithreaded(0);
    test_multithreaded(1);
    return 0;
}
