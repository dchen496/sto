#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "LogProto.hh"
#include "MassTrans.hh"

int nthreads;
int ntrees;
std::string listen_host;
int start_port;

typedef MassTrans<std::string, versioned_str_struct, /*opacity*/ false> mbta_type;

void thread_init() {
    mbta_type::thread_init();
}

auto thread_init_obj = std::function<void()>(&thread_init);

void test_multithreaded() {
    std::vector<mbta_type> fs(ntrees);
    for (unsigned i = 0; i < fs.size(); i++)
        Transaction::register_object(fs[i], i);
    assert(LogBackup::listen(nthreads, start_port, thread_init_obj) == 0);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc != 5) {
        printf("usage: log-masstrans-backup nthreads ntrees listen_host start_port\n");
        return -1;
    }

    char **arg = &argv[1];
    nthreads = atoi(*arg++);
    ntrees = atoi(*arg++);
    listen_host = std::string(*arg++);
    start_port = atoi(*arg++);

    mbta_type::static_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_multithreaded();
    return 0;
}
