#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include <map>
#include "Transaction.hh"
#include "TBox.hh"
#include "MassTrans.hh"
#include "LogProto.hh"

const int port = 2000;
const int niters = 2000000;

void thread_init() {
    MassTrans<int>::thread_init();
    MassTrans<Masstree::Str, versioned_str_struct>::thread_init();
}

auto thread_init_obj = std::function<void()>(&thread_init);

template <typename T>
std::string to_str(T v) {
    std::stringstream ss;
    ss << v;
    return ss.str();
}

std::string copy_mtstr(Masstree::Str str) {
    return std::string(str.data(), str.data() + str.length());
}

template <typename V, typename T = MassTrans<V>>
std::map<std::string, V> copy_to_map(T mt, std::string start, std::string end) {
    const int batch_size = 16;
    std::map<std::string, V> ret;
    end += "a"; // we want the range to be inclusive, but transQuery is exclusive for the upper bound

    bool has_more = true;
    while (has_more) {
        TransactionGuard t;
        int i = 0;
        std::string next_start;
        auto callback = [&](Masstree::Str key, V value) -> bool {
            std::string k(key.data(), key.data() + key.length());
            i++;
            if (i > batch_size) {
                has_more = true;
                next_start = k;
                i = 0;
                return false;
            }

            ret[k] = value;
            return true;
        };
        has_more = false;
        mt.transQuery(Masstree::Str(start), Masstree::Str(end), callback);
        start = next_start;
    }
    return ret;
}

void test_simple_int(int batch) {
    MassTrans<int> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port, thread_init_obj) == 0);

    {
        bool found;
        int v;
        TransactionGuard t;
        std::string key = to_str<int>(19);
        found = f.transGet(key, v);
        assert(!found);
    }

    for (int i = 0; i < 19; i += 2) {
        bool found;
        int v;
        TransactionGuard t;
        std::string key = to_str(i);
        found = f.transGet(key, v);
        assert(found);
        assert(v == batch ? (i + 51) : (i + 1));
    }

    for (int i = 1; i < 19; i += 2) {
        TransactionGuard t;
        bool found1 = false, found2 = false;
        int v1 = -1, v2 = -1;
        std::string key1 = to_str(i);
        std::string key2 = to_str(i - 1);
        found1 = f.transGet(key1, v1);
        found2 = f.transGet(key2, v2);
        assert(v1 == 2 * v2);
        assert(found1 && found2);
    }

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, batch);
    fflush(stdout);
}

void test_many_puts_int() {
    MassTrans<int> f, ref;
    TBox<std::string> first_key, last_key;
    TBox<int> value_hash;

    Transaction::register_object(f, 0);
    Transaction::register_object(ref, 1);
    Transaction::register_object(first_key, 2);
    Transaction::register_object(last_key, 3);
    Transaction::register_object(value_hash, 4);

    assert(LogApply::listen(1, port, thread_init_obj) == 0);

    std::map<std::string, int> m1 = copy_to_map<int>(f,
            first_key.nontrans_read(), last_key.nontrans_read());
    std::map<std::string, int> m2 = copy_to_map<int>(ref,
            first_key.nontrans_read(), last_key.nontrans_read());

    auto it1 = m1.begin();
    auto it2 = m2.begin();

    int hash = 1;
    while (it1 != m1.end()) {
        assert(it2 != m2.end());
        assert(*it1 == *it2);
        hash = hash * 17 + it1->second;
        it1++; it2++;
    }
    assert(it2 == m2.end());
    assert(value_hash.nontrans_read() == hash);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_many_ops_int() {
    MassTrans<int> f, ref;
    TBox<std::string> first_key, last_key;
    TBox<int> value_hash;

    Transaction::register_object(f, 0);
    Transaction::register_object(ref, 1);
    Transaction::register_object(first_key, 2);
    Transaction::register_object(last_key, 3);
    Transaction::register_object(value_hash, 4);

    assert(LogApply::listen(1, port, thread_init_obj) == 0);

    std::map<std::string, int> m1 = copy_to_map<int>(f,
            first_key.nontrans_read(), last_key.nontrans_read());
    std::map<std::string, int> m2 = copy_to_map<int>(ref,
            first_key.nontrans_read(), last_key.nontrans_read());

    auto it1 = m1.begin();
    auto it2 = m2.begin();

    int hash = 1;
    while (it1 != m1.end()) {
        assert(it2 != m2.end());
        assert(*it1 == *it2);
        hash = hash * 17 + it1->second;
        it1++; it2++;
    }
    assert(it2 == m2.end());
    assert(value_hash.nontrans_read() == hash);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}


void test_simple_string() {
    MassTrans<Masstree::Str, versioned_str_struct> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port, thread_init_obj) == 0);

    bool found;
    {
        TransactionGuard t;
        std::string key = to_str<int>(19);
        Masstree::Str v;
        found = f.transGet(key, v);
        assert(!found);
    }

    for (int i = 0; i < 19; i++) {
        TransactionGuard t;
        std::string key = to_str(i);
        Masstree::Str v;
        found = f.transGet(key, v);
        assert(found);
        assert(copy_mtstr(v) == to_str(i + 50) + to_str(i + 51));
    }

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_string_resize() {
    MassTrans<Masstree::Str, versioned_str_struct> f;
    Transaction::register_object(f, 0);
    assert(LogApply::listen(1, port, thread_init_obj) == 0);

    std::string value;
    bool found;
    {
        TransactionGuard g;
        std::string key = "k";
        Masstree::Str v;
        found = f.transGet(key, v);
        value = copy_mtstr(v);
    }
    assert(found);
    assert(value.size() == 1 << 14);
    for (unsigned i = 0; i < value.size(); i++) {
        assert(value[i] == 'a');
    }

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s()\n", __FUNCTION__);
    fflush(stdout);
}

void test_multithreaded_int(bool mostly_inserts) {
    const int nthread = 4;
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

    assert(LogApply::listen(nthread, port, thread_init_obj) == 0);

    std::map<std::string, int> m1 = copy_to_map<int>(f,
            first_key.nontrans_read(), last_key.nontrans_read());
    std::map<std::string, int> m2 = copy_to_map<int>(ref,
            first_key.nontrans_read(), last_key.nontrans_read());

    auto it1 = m1.begin();
    auto it2 = m2.begin();
    int hash = 1;
    while (it1 != m1.end()) {
        assert(it2 != m2.end());
        assert(*it1 == *it2);
        hash = hash * 17 + it1->second;
        it1++; it2++;
    }
    assert(it2 == m2.end());
    assert(value_hash.nontrans_read() == hash);

    Transaction::clear_registered_objects();
    printf("BACKUP PASS: %s(%d)\n", __FUNCTION__, mostly_inserts);
    fflush(stdout);
}

// TODO: multithreaded string resize tests?

int main() {
    TThread::set_id(0);
    thread_init();
    LogApply::debug_txn_log = false;
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
