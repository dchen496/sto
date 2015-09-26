#pragma once

#include <cassert>
#include <utility>
#include "TaggedLow.hh"
#include "Transaction.hh"
#include "versioned_value.hh"
#include "VersionFunctions.hh"
#include "RBTreeInternal.hh"

template <typename K, typename T>
class RBTree;

template <typename T>
class rbwrapper : public T {
  public:
    template <typename... Args> inline rbwrapper(Args&&... args)
    : T(std::forward<Args>(args)...) {
    }
    inline rbwrapper(const T& x)
    : T(x) {
    }
    inline rbwrapper(T&& x) noexcept
    : T(std::move(x)) {
    }
    inline const T& value() const {
        return *this;
    }
    inline T& mutable_value() {
        return *this;
    }
    rblinks<rbwrapper<T> > rblinks_;
};

// Define a custom key-value pair type that contains versions and also
// has proper comparison methods
template <typename K, typename T>
class rbpair {
public:
    typedef TransactionTid::type Version;
    typedef versioned_value_struct<T> versioned_value;

    static constexpr Version insert_bit = TransactionTid::user_bit1;

    explicit rbpair(const K& key, const T& value) {
        init(key, value);
    }
    explicit rbpair(std::pair<K, T>& kvp) {
        init(kvp.first, kvp.second);
    }

    // move ctor
    inline rbpair(rbpair&& cp) noexcept {
        versioned_pair_.first = cp.key();
        versioned_pair_.second = cp.vervalue();
        cp.versioned_pair_.first = K();
        cp.versioned_pair_.second = nullptr;
    }

    ~rbpair(){delete versioned_pair_.second;}

    inline const K& key() const {
        return versioned_pair_.first;
    }
    inline versioned_value *vervalue() {
        return versioned_pair_.second;
    }
    inline T& writeable_value() {
        //XXX typo in versioned_value.hh... should be writable :(
        return versioned_pair_.second->writeable_value();
    }
    inline bool operator<(const rbpair& rhs) const {
        return (key() < rhs.key());
    }
    inline bool operator>(const rbpair& rhs) const {
        return (key() > rhs.key());
    }
    rbpair& operator= (const T& v) {
        rbtree_->insert(v);
        return *this;
    }

private:
    void init(const K& key, const T& value) {
        // initialize with "insert_bit" set (i.e. inserted but not committed)
        // erase at commit time 
        versioned_value *val = versioned_value::make(value,
            TransactionTid::increment_value + insert_bit);
        versioned_pair_.first = key;
        versioned_pair_.second = val;
    }
   
    RBTree<K, T>* rbtree_;
    std::pair<K, versioned_value*> versioned_pair_;
};

template <typename K, typename T>
class RBTree : public Shared {
    typedef TransactionTid::type Version;
    typedef versioned_value_struct<T> versioned_value;

    static constexpr TransItem::flags_type insert_tag = TransItem::user0_bit;
    static constexpr TransItem::flags_type delete_tag = TransItem::user0_bit<<1;
    static constexpr Version insert_bit = TransactionTid::user_bit1;
    static constexpr Version delete_bit = TransactionTid::user_bit1<<1;
    static constexpr Version dirty_bit = TransactionTid::user_bit1<<2;

public:
    RBTree() {
        treelock_ = 0;
    }
    
    // lookup
    typedef rbwrapper<rbpair<K, T>> wrapper_type;
    typedef rbtree<wrapper_type> internal_tree_type;
    inline size_t count(const K& key) const;

    // element access
    // XXX problem with overriding operator[]?
    inline const T& operator[](const K& key);
    inline rbpair<K, T>& operator[](const K& key);
    
    // modifiers
    inline int erase(K& key);
    inline void insert(std::pair<K, T>& n);

    void lock(versioned_value *e) {
        lock(&e->version());
    }
    void unlock(versioned_value *e) {
        unlock(&e->version());
    }
    inline void lock(TransItem& item);
    inline void unlock(TransItem& item);
    inline bool check(const TransItem& item, const Transaction& trans);
    inline void install(TransItem& item, const Transaction& t);
    inline void cleanup(TransItem& item, bool committed);

private:
    // Find and return a pointer to the rbwrapper. Abort if value inserted and not yet committed.
    inline rbwrapper<rbpair<K, T>>* find_or_abort(rbwrapper<rbpair<K, T>>& rbkvp) {
        // XXX problem with overloading lock -- this doesn't work?
        lock(treelock_);
        auto x = wrapper_tree_.find_any(rbkvp,
            rbpriv::make_compare<wrapper_type, wrapper_type>(wrapper_tree_.r_.get_compare()));
        unlock(treelock_);
        if (x) {
            // x->mutable_value() returns the rbpair
            versioned_value* v = x->mutable_value().vervalue();
            // check if rbpair has version "inserted" (someone inserted w/out commit)
            if (is_inserted(v->version())) {
                auto item = Sto::item(this, v);
                // check if item was inserted by this transaction
                if (has_insert(item)) {
                    return x; 
                } else {
                    // some other transaction inserted this node and hasn't committed
                    Sto::abort();
                    return NULL;
                }
           }
        }
        // item was committed or DNE, so return pointer
        return x;
    }

    static void lock(Version *v) {
        TransactionTid::lock(*v);
    }
    static void unlock(Version *v) {
        TransactionTid::unlock(*v);
    }
    static bool has_insert(const TransItem& item) {
        return item.flags() & insert_tag;
    }
    static bool has_delete(const TransItem& item) {
        return item.flags() & delete_tag;
    }
    static bool is_locked(Version v) {
        return TransactionTid::is_locked(v);
    }
    static bool is_inserted(Version v) {
        return v & insert_bit;
    }
    static void erase_inserted(Version* v) {
        *v = *v & (~insert_bit);
    }
    static void mark_inserted(Version* v) {
        *v = *v | insert_bit;
    }
    static bool is_dirty(Version v) {
        return v & dirty_bit;
    }
    static void erase_dirty(Version* v) {
        *v = *v & (~dirty_bit);
    }
    static void mark_dirty(Version* v) {
        *v = *v | dirty_bit;
    }
    static bool is_deleted(Version v) {
        return v & delete_bit;
    }
    static void erase_deleted(Version* v) {
        *v = *v & (~delete_bit);
    }
    static void mark_deleted(Version* v) {
        *v = *v | delete_bit;
    }

    internal_tree_type wrapper_tree_;
    Version treelock_;
};

template <typename K, typename T>
inline size_t RBTree<K, T>::count(const K& key) const {
    rbwrapper<rbpair<K, T>> idx_pair(rbpair<K, T>(key, T()));
    return ((find_or_abort(idx_pair)) ? 1 : 0);
}

template <typename K, typename T>
inline const T& RBTree<K, T>::operator[](const K& key) {
    rbwrapper<rbpair<K, T>> idx_pair(rbpair<K, T>(key, T()));
    auto x = find_or_abort(idx_pair);
    if (!x) {
        // if item DNE, return ref to newly inserted new key-value pair with empty value
        // lock entire tree during insert
        auto n = new rbwrapper<rbpair<K, T> >(  rbpair<K, T>(key, T())  );
        lock(&treelock_);
        wrapper_tree_.insert(*n);
        unlock(&treelock_);
        // XXX add write and insert flag of item (value of rbpair) with value T()
        // Sto::item(this, n->mutable_value().vervalue()).add_write(T()).add_flags(insert_tag);
        
        // return the reference to the actual value 
        return n->mutable_value().writeable_value();
    }
    return x->mutable_value().writeable_value();
}

template <typename K, typename T>
inline rbpair<K, T>& RBTree<K, T>::operator[](const K& key) {
    rbwrapper<rbpair<K, T>> idx_pair(rbpair<K, T>(key, T()));
    auto x = find_or_abort(idx_pair);
    if (!x) {
        // if item DNE, return ref to newly inserted new key-value pair with empty value
        // lock entire tree during insert
        auto n = new rbwrapper<rbpair<K, T> >(  rbpair<K, T>(key, T())  );
        lock(&treelock_);
        wrapper_tree_.insert(*n);
        unlock(&treelock_);
        // XXX add write and insert flag of item (value of rbpair) with value T()
        // Sto::item(this, n->mutable_value().vervalue()).add_write(T()).add_flags(insert_tag);
        
        // XXX return the reference to the rbpair to ensure operator= will be used
        return n->mutable_value();
    }
    return x->mutable_value();
}

template <typename K, typename T>
inline void RBTree<K, T>::insert(std::pair<K, T>& kvp) {
    rbpair<K, T> rbkvp(kvp);
    lock(&treelock_);
    wrapper_tree_.insert(*new rbwrapper<rbpair<K, T> >(  std::move(rbkvp)  ));
    unlock(&treelock_);
}

template <typename K, typename T>
inline int RBTree<K, T>::erase(K& key) {
    rbwrapper<rbpair<K, T>> idx_pair(rbpair<K, T>(key, T()));
    rbpair<K, T>* x = find_or_abort(idx_pair);
    if (x) {
        lock(&treelock_);
        wrapper_tree_.erase(x);
        unlock(&treelock_);
        return 1;
    }
    return 0;
}

template <typename K, typename T>
inline void RBTree<K, T>::lock(TransItem& item) {
    (void) item;
}

template <typename K, typename T>
inline void RBTree<K, T>::unlock(TransItem& item) {
    (void) item;
}
template <typename K, typename T>
inline bool RBTree<K, T>::check(const TransItem& item, const Transaction& trans) {
    (void) item;
    (void) trans;
    return false;
}

template <typename K, typename T>
inline void RBTree<K, T>::install(TransItem& item, const Transaction& t) {
    (void) item;
    (void) t;
}

template <typename K, typename T>
inline void RBTree<K, T>::cleanup(TransItem& item, bool committed) {
    (void) item;
        (void) committed;
}