#pragma once
#include "Interface.hh"
#include "TWrapped.hh"

template <typename T, typename W = TWrapped<T> >
class TBox : public TObject {
public:
    typedef typename W::read_type read_type;
    typedef typename W::version_type version_type;

    TBox() {
    }
    template <typename... Args>
    explicit TBox(Args&&... args)
        : v_(std::forward<Args>(args)...) {
    }

    read_type read() const {
        auto item = Sto::item(this, 0);
        if (item.has_write())
            return item.template write_value<T>();
        else
            return v_.read(item, vers_);
    }
    void write(const T& x) {
        Sto::item(this, 0).add_write(x);
    }
    void write(T&& x) {
        Sto::item(this, 0).add_write(std::move(x));
    }
    template <typename... Args>
    void write(Args&&... args) {
        Sto::item(this, 0).template add_write<T>(std::forward<Args>(args)...);
    }

    operator read_type() const {
        return read();
    }
    TBox<T, W>& operator=(const T& x) {
        write(x);
        return *this;
    }
    TBox<T, W>& operator=(T&& x) {
        write(std::move(x));
        return *this;
    }
    template <typename V>
    TBox<T, W>& operator=(V&& x) {
        write(std::forward<V>(x));
        return *this;
    }
    TBox<T, W>& operator=(const TBox<T, W>& x) {
        write(x.read());
        return *this;
    }

    const T& nontrans_read() const {
        return v_.access();
    }
    T& nontrans_access() {
        return v_.access();
    }
    void nontrans_write(const T& x) {
        v_.access() = x;
    }
    void nontrans_write(T&& x) {
        v_.access() = std::move(x);
    }

    // transactional methods
    bool lock(TransItem& item, Transaction& txn) override {
        return txn.try_lock(item, vers_);
    }
    bool check(TransItem& item, Transaction&) override {
        return item.check_version(vers_);
    }
    void install(TransItem& item, Transaction& txn) override {
        v_.write(std::move(item.template write_value<T>()));
        txn.set_version_unlock(vers_, item);
    }
    void unlock(TransItem&) override {
        vers_.unlock();
    }
    void print(std::ostream& w, const TransItem& item) const override {
        w << "{TBox<" << typeid(T).name() << "> " << (void*) this;
        if (item.has_read())
            w << " R" << item.read_value<version_type>();
        if (item.has_write())
            w << " =" << item.write_value<T>();
        w << "}";
    }

    int log_entry_size(TransItem &item) {
        (void) item;
        return sizeof(T);
    }
    int write_log_entry(TransItem &item, char *buf) {
        (void) item, (void) buf;
        *(T *) buf = item.template write_value<T>();
        return sizeof(T);
    }
    bool apply_log_entry(char *entry, TransactionTid::type log_tid, int &bytes_read) {
        bytes_read = sizeof(T);
        if (!vers_.lock_if_older(version_type(log_tid))) {
            return false;
        }
        v_.write(std::move(*(T *) entry));
        vers_.set_version_unlock(log_tid);
        return true;
    }

protected:
    version_type vers_;
    W v_;
};
