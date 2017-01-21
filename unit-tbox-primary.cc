#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"

#define GUARDED if (TransactionGuard tguard{})

void testSimpleInt() {
    TBox<int> f;
    Transaction::register_object(f, 0);
    Transaction::init_logging(4, {"127.0.0.1"}, 2000);

    {
        TransactionGuard t;
        f = 100;
    }

    {
        TransactionGuard t2;
        int f_read = f;
        assert(f_read == 100);
    }

    printf("PASS: %s\n", __FUNCTION__);
    Transaction::stop_logging();
    Transaction::clear_registered_objects();
}

int main() {
    testSimpleInt();
    return 0;
}
