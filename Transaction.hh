#include <vector>
#include <algorithm>

#include "Interface.hh"

#pragma once

#define INIT_SET_SIZE 8

#define READER_BIT (1<<0)
#define WRITER_BIT (1<<1)

template <typename T>
T* readObj(T* obj) {
  //assert(!isReadObj(obj));
  return (T*)((intptr_t)obj | READER_BIT);
}
template <typename T>
T* writeObj(T* obj) {
  //  assert(!isWriteObj(obj));
  return (T*)((intptr_t)obj | WRITER_BIT);
}
template <typename T>
T* untag(T* obj) {
  return (T*)((intptr_t)obj & ~(WRITER_BIT|READER_BIT));
}
template <typename T>
bool isReadObj(T* obj) {
  return (intptr_t)obj & READER_BIT;
}
template <typename T>
bool isWriteObj(T* obj) {
  return (intptr_t)obj & WRITER_BIT;
}

class Transaction {
public:
  struct TransItem {
    TransItem(Shared *s, TransData data) : shared(s), data(data) {}

    Shared *sharedObj() const {
      return untag(shared);
    }

    bool has_write() const {
      return isWriteObj(shared);
    }
    bool has_read() const {
      return isReadObj(shared);
    }
    bool same_item(const TransItem& x) const {
      return sharedObj() == x.sharedObj() && data.key == x.data.key;
    }

    template <typename T>
    T write_value() const {
      assert(isWriteObj(shared));
      return unpack<T>(data.wdata);
    }
    template <typename T>
    T read_value() const {
      assert(isReadObj(shared));
      return unpack<T>(data.rdata);
    }

    template <typename T>
    void add_write(T wdata) {
      shared = writeObj(shared);
      data.wdata = pack(wdata);
    }
    template <typename T>
    void add_read(T rdata) {
      shared = readObj(shared);
      data.rdata = pack(rdata);
    }

    inline bool operator<(const TransItem& t2) const {
      return data < t2.data
                    || (data == t2.data && shared < t2.shared);
    }
    inline bool operator==(const TransItem& t2) const {
      return data == t2.data;
    }

  private:
    typedef Shared TaggedShared;
    TaggedShared *shared;
  public:
    TransData data;
  };

  typedef std::vector<TransItem> TransSet;

  Transaction() : transSet_(), readMyWritesOnly_(true) {
    transSet_.reserve(INIT_SET_SIZE);
  }

  // adds item without checking its presence in the array
  template <typename T>
  TransItem& add_item(Shared *s, T key) {
    readMyWritesOnly_ = false;
    void *k = pack(key);
    // TODO: if user of this forgets to do add_read or add_write, we end up with a non-read, non-write, weirdo item
    transSet_.emplace_back(s, TransData(k, NULL, NULL));
    return transSet_[transSet_.size()-1];
  }

  // tries to find an existing item with this key, otherwise adds it
  template <typename T>
  TransItem& item(Shared *s, T key) {
    void *k = pack(key);
    for (TransItem& ti : transSet_) {
      if (ti.sharedObj() == s && ti.data.key == k) {
        return ti;
      }
    }
    return add_item(s, key);
  }

#if 0
  void read(Shared *s, TransData data) {
    transSet_.emplace_back(readObj(r), data);
  }

  void write(Shared *s, TransData data) {
    transSet_.emplace_back(writeObj(w), data);
  }
#endif

#if 0
  // TODO: should this be a different virtual object or?
  void onAbort(Writer *w, TransData data) {
    abortSet_.emplace_back(w, data);
  }

  void onCommit(Writer *w, TransData data) {
    commitSet_.emplace_back(w, data);
  }
#endif

  bool commit() {
    bool success = true;

    //phase1
    if (readMyWritesOnly_) {
      std::sort(transSet_.begin(), transSet_.end());
    } else {
      std::stable_sort(transSet_.begin(), transSet_.end());
    }
    TransItem* trans_first = transSet_.data();
    TransItem* trans_last = trans_first + transSet_.size();
    for (TransItem* it = trans_first; it != trans_last; )
        if (it->has_write()) {
            TransItem* me = it;
            me->sharedObj()->lock(me->data);
            for (++it; it != trans_last && it->same_item(*me); ++it)
                /* do nothing */;
        } else
            ++it;

    /* fence(); */

    //phase2
    for (TransItem* it = trans_first; it != trans_last; ++it)
        if (it->has_read()) {
            bool has_write = it->has_write();
            if (!has_write)
                for (TransItem* it2 = it + 1;
                     it2 != trans_last && it2->same_item(*it);
                     ++it2)
                    if (it2->has_write()) {
                        has_write = true;
                        break;
                    }
            if (!it->sharedObj()->check(it->data, has_write)) {
                success = false;
                goto end;
            }
        }

    //phase3
    for (TransItem& ti : transSet_) {
      if (ti.has_write()) {
        ti.sharedObj()->install(ti.data);
      }
    }

  end:

    for (TransItem* it = trans_first; it != trans_last; )
        if (it->has_write()) {
            TransItem* me = it;
            me->sharedObj()->unlock(me->data);
            for (++it; it != trans_last && it->same_item(*me); ++it)
                /* do nothing */;
        } else
            ++it;

#if 0
    if (success) {
      commitSuccess();
    } else {
      abort();
    }
#endif

    return success;

  }

#if 0
  void abort() {
    for (WriterItem& w : abortSet_) {
      w.writer->undo(w.data);
    }
  }

private:
  void commitSuccess() {
    for (WriterItem& w : commitSet_) {
      w.writer->afterT(w.data);
    }
  }
#endif

private:
  TransSet transSet_;
  bool readMyWritesOnly_;

};
