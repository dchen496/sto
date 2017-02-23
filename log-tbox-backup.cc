#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogProto.hh"

int nthreads;
int niters;
int txnsize;
std::string listen_host;
int start_port;

void test_multithreaded_int() {
    std::vector<TBox<int64_t>> fs(nthreads * txnsize);
    for (unsigned i = 0; i < fs.size(); i++)
        Transaction::register_object(fs[i], i);
    assert(LogApply::listen(nthreads, start_port) == 0);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 6) {
        printf("usage: log-tbox-backup nthreads niters txnsize listen_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    nthreads = atoi(*arg++);
    niters = atoi(*arg++);
    txnsize = atoi(*arg++);
    listen_host = std::string(*arg++);
    start_port = atoi(*arg++);

    LogApply::debug_txn_log = false;
    test_multithreaded_int();
    return 0;
}
