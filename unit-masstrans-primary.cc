#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include <map>
#include "Transaction.hh"
#include "LogProto.hh"
#include "TBox.hh"
#include "MassTrans.hh"

#define GUARDED if (TransactionGuard tguard{})

const std::string host = "127.0.0.1";
const int port = 2000;
const int niters = 100000;
const int startup_delay = 500000;

void thread_init() {
    MassTrans<int>::thread_init();
    MassTrans<Masstree::Str, versioned_str_struct>::thread_init();
}

template <typename T>
std::string to_str(T v) {
    std::stringstream ss;
    ss << v;
    return ss.str();
}

std::string copy_mtstr(Masstree::Str str) {
    return std::string(str.data(), str.data() + str.length());
}

void test_simple_int(int batch) {
    usleep(startup_delay);
    MassTrans<int> f;
    Transaction::register_object(f, 0);
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    // inserts
    for (int i = 0; i < 20; i++) {
        int val = batch ? (i + 50) : i;
        {
            TransactionGuard t;
            std::string key = to_str(i);
            f.transInsert(key, val + 1);
        }
        if (!batch)
            Transaction::flush_log_batch();
    }

    // get and update
    for (int i = 1; i < 20; i += 2) {
        {
            TransactionGuard t;
            int v = 0;
            std::string key1 = to_str(i - 1);
            std::string key2 = to_str(i);
            assert(f.transGet(key1, v));
            f.transUpdate(key2, v * 2);
        }
        if (!batch)
            Transaction::flush_log_batch();
    }

    // deletes
    {
        TransactionGuard t;
        std::string key1 = to_str(19);
        f.transDelete(key1);
    }
    Transaction::flush_log_batch();
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_many_puts_int() {
    usleep(startup_delay);

    MassTrans<int> f, ref;
    TBox<std::string> first_key, last_key;
    TBox<int> value_hash;

    Transaction::register_object(f, 0);
    Transaction::register_object(ref, 1);
    Transaction::register_object(first_key, 2);
    Transaction::register_object(last_key, 3);
    Transaction::register_object(value_hash, 4);

    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    std::map<std::string, int> m;
    srand(1234);
    for (int i = 0; i < niters; i++) {
        int a = rand() % niters;
        TransactionGuard t;
        std::string key = to_str(a);
        assert((m.count(key) > 0) == f.transPut(key, i));
        m[key] = i;
    }

    Transaction::flush_log_batch();

    int hash = 1;
    for (auto it = m.begin(); it != m.end(); it++) {
        TransactionGuard t;
        assert(ref.transInsert(it->first, it->second));
        hash = hash * 17 + it->second;
    }

    Transaction::flush_log_batch();

    {
        TransactionGuard t;
        first_key = m.begin()->first;
        last_key = m.rbegin()->first;
        value_hash = hash;
    }
    Transaction::flush_log_batch();

    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_many_ops_int() {
    usleep(startup_delay);

    int n = 100;
    MassTrans<int> f, ref;
    TBox<std::string> first_key, last_key;
    TBox<int> value_hash;

    Transaction::register_object(f, 0);
    Transaction::register_object(ref, 1);
    Transaction::register_object(first_key, 2);
    Transaction::register_object(last_key, 3);
    Transaction::register_object(value_hash, 4);

    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    std::map<std::string, int> m;
    srand(1234);
    for (int i = 0; i < niters; i++) {
        int a = rand() % n;
        int op = (rand() / 1000) % 2;
        TransactionGuard t;
        std::string key = to_str(a);
        switch (op) {
        case 0:
            assert((m.count(key) > 0) == f.transPut(key, i));
            m[key] = i;
            break;
        case 1:
            f.transDelete(key);
            m.erase(key);
            break;
        }
    }

    Transaction::flush_log_batch();

    int hash = 1;
    for (auto it = m.begin(); it != m.end(); it++) {
        TransactionGuard t;
        assert(ref.transInsert(it->first, it->second));
        hash = hash * 17 + it->second;
    }

    Transaction::flush_log_batch();

    {
        TransactionGuard t;
        first_key = m.begin()->first;
        last_key = m.rbegin()->first;
        value_hash = hash;
    }
    Transaction::flush_log_batch();

    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_simple_string() {
    usleep(startup_delay);
    MassTrans<Masstree::Str, versioned_str_struct> f;
    Transaction::register_object(f, 0);
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    // inserts
    for (int i = 0; i < 20; i++) {
        {
            TransactionGuard t;
            std::string key = to_str(i);
            std::string value = to_str(i + 50);
            f.transInsert(key, value);
        }
    }
    Transaction::flush_log_batch();

    // get and update
    for (int i = 1; i < 20; i++) {
        {
            TransactionGuard t;
            std::string key1 = to_str(i - 1);
            std::string key2 = to_str(i);
            Masstree::Str v1, v2;
            assert(f.transGet(key1, v1));
            assert(f.transGet(key2, v2));
            std::string new_v = copy_mtstr(v1) + copy_mtstr(v2);
            // this may cause resizing
            f.transUpdate(key1, new_v);
        }
    }
    Transaction::flush_log_batch();

    // deletes
    {
        TransactionGuard t;
        std::string key1 = to_str(19);
        f.transDelete(key1);
    }

    Transaction::flush_log_batch();
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_string_resize() {
    usleep(startup_delay);
    MassTrans<Masstree::Str, versioned_str_struct> f;
    Transaction::register_object(f, 0);
    assert(LogPrimary::init_logging(1, {host}, port) == 0);

    std::string key = "k";
    std::string value = "a";
    {
        TransactionGuard t;
        f.transInsert(key, Masstree::Str(value));
    }

    for (int i = 0; i < 14; i++) {
        TransactionGuard t;
        value = value + value;
        f.transPut(key, Masstree::Str(value));
    }

    Transaction::flush_log_batch();
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

struct ThreadArgs {
    int id;
    int n;
    MassTrans<int> *f;
    std::string min_key;
    std::string max_key;
    int ntxns;
};

void *test_multithreaded_worker(void *argptr) {
    ThreadArgs &args = *(ThreadArgs *) argptr;
    TThread::set_id(args.id);
    thread_init();

    MassTrans<int> &f = *args.f;
    srand(1234 + args.id);
    for (int i = 0; i < niters; i++) {
        int a = rand() % args.n;
        int op = (rand() / 16) % 2; // rand just alternates the last bit
        std::string key = to_str(a);
        try {
            Sto::start_transaction();
            switch (op) {
            case 0:
                f.transPut(key, i);
                break;
            case 1:
                f.transDelete(key);
                break;
            }
            Sto::commit();

            args.min_key = (args.min_key == "") ? key : std::min(args.min_key, key);
            args.max_key = (args.max_key == "") ? key : std::max(args.max_key, key);
            args.ntxns++;
        } catch (Transaction::Abort e) {
        }
    }
    Transaction::flush_log_batch();
    return nullptr;
}

void test_multithreaded_int(bool mostly_inserts) {
    usleep(startup_delay);

    const int nthread = 4;
    const int n = mostly_inserts ? niters : 1000;
    MassTrans<int> f;
    MassTrans<int> ref;
    TBox<std::string> first_key, last_key;
    TBox<int> value_hash;
    TBox<int> ntxns[nthread];
    Transaction::register_object(f, 0);
    Transaction::register_object(ref, 1);
    Transaction::register_object(first_key, 2);
    Transaction::register_object(last_key, 3);
    Transaction::register_object(value_hash, 4);
    for (int i = 0; i < nthread; i++)
        Transaction::register_object(ntxns[i], 5 + i);

    assert(LogPrimary::init_logging(nthread, {host}, port) == 0);

    pthread_t thrs[nthread];
    ThreadArgs args[nthread];
    for (int i = 0; i < nthread; i++) {
        args[i] = { .id = i, .n = n, .f = &f, .min_key = "", .max_key = "", .ntxns = 0 };
        pthread_create(&thrs[i], nullptr, test_multithreaded_worker, (void *) &args[i]);
    }
    std::string min_key, max_key;
    for (int i = 0; i < nthread; i++) {
        pthread_join(thrs[i], nullptr);
        min_key = (min_key == "") ? args[i].min_key : std::min(min_key, args[i].min_key);
        max_key = (max_key == "") ? args[i].max_key : std::max(max_key, args[i].max_key);
    }

    // copy f to ref
    const int batch_size = 16;
    bool has_more = true;
    std::string start = to_str(min_key);
    int hash = 1;
    int g = 0;
    while (has_more) {
        TransactionGuard t;
        int i = 0;
        std::string next_start;
        auto callback = [&](Masstree::Str key, int value) -> bool {
            std::string k(key.data(), key.data() + key.length());
            i++;
            if (i > batch_size) {
                has_more = true;
                next_start = k;
                i = 0;
                return false;
            }
            ref.transInsert(Masstree::Str(k), value);
            g++;
            hash = hash * 17 + value;
            return true;
        };
        has_more = false;
        f.transQuery(Masstree::Str(to_str(start)), Masstree::Str(to_str(max_key) + "a"), callback); // + "a" since we want to include max_key
        start = next_start;
    }
    {
        TransactionGuard t;
        first_key = to_str(min_key);
        last_key = to_str(max_key);
        value_hash = hash;
    }
    for (int i = 0; i < nthread; i++) {
        TransactionGuard t;
        ntxns[i] = args[i].ntxns;
    }

    Transaction::flush_log_batch();
    LogPrimary::stop();
    Transaction::clear_registered_objects();
    printf("PRIMARY PASS: %s(%d)\n", __FUNCTION__, mostly_inserts);
    fflush(stdout);
}


int main() {
    TThread::set_id(0);
    thread_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_simple_int(0);
    test_simple_int(1);
    test_many_puts_int();
    test_many_ops_int();
    test_simple_string();
    test_string_resize();
    test_multithreaded_int(0);
    test_multithreaded_int(1);
    return 0;
}
