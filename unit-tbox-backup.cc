#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogApply.hh"

void test_simple_int(int batch) {
    TBox<int> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, 2000) == 0);

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
    TBox<int> fs[100];
    int es[100];
    for (int i = 0; i < 100; i++) {
        Transaction::register_object(fs[i], i);
        fs[i].nontrans_write(0);
        es[i] = i;
    }
    assert(LogApply::listen(1, 2000) == 0);

    for (int i = 0; i < 1000000; i++) {
        int a = (i * 17) % 100;
        int b = (i * 19) % 100;
        int c = (i * 31) % 100;
        es[a] = (es[b] + es[c]) % 1000;
    }

    for (int i = 0; i < 100; i++) {
        TransactionGuard t;
        int f_read = fs[i];
        assert(f_read == es[i]);
    }
    Transaction::clear_registered_objects();
    std::cout << "BACKUP PASS: " << __FUNCTION__ << "(" << batch << ")\n";
    std::flush(std::cout);
}

void test_multithreaded(int batch) {
    TBox<int> fs[100];
    TBox<int> refs[100];
    for (int i = 0; i < 100; i++) {
        Transaction::register_object(fs[i], i);
        Transaction::register_object(refs[i], i + 100);
        fs[i].nontrans_write(i);
        refs[i].nontrans_write(-i);
    }
    assert(LogApply::listen(4, 2000) == 0);

    for (int i = 0; i < 100; i++) {
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
}
