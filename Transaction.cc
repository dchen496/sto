#include "Transaction.hh"

Transaction::testing_type Transaction::testing;
threadinfo_t Transaction::tinfo[MAX_THREADS];
__thread int TThread::the_id;
threadinfo_t::epoch_type __attribute__((aligned(64))) Transaction::global_epoch;
bool Transaction::run_epochs = true;
__thread Transaction *TThread::txn = nullptr;
std::function<void(threadinfo_t::epoch_type)> Transaction::epoch_advance_callback;
TransactionTid::type __attribute__((aligned(128))) Transaction::_TID = TransactionTid::valid_bit;

static void __attribute__((used)) check_static_assertions() {
    static_assert(sizeof(threadinfo_t) % 128 == 0, "threadinfo is 2-cache-line aligned");
}

void* Transaction::epoch_advancer(void*) {
    // don't bother epoch'ing til things have picked up
    usleep(100000);
    while (run_epochs) {
        auto g = global_epoch;
        for (auto&& t : tinfo) {
            if (t.epoch != 0 && t.epoch < g)
                g = t.epoch;
        }

        global_epoch = ++g;

        if (epoch_advance_callback)
            epoch_advance_callback(global_epoch);

        for (auto&& t : tinfo) {
            acquire_spinlock(t.spin_lock);
            auto deletetil = t.callbacks.begin();
            for (auto it = t.callbacks.begin(); it != t.callbacks.end(); ++it) {
                if (threadinfo_t::signed_epoch_type(g - it->first) >= 2) {
                    it->second();
                    ++deletetil;
                } else {
                    // callbacks are in ascending order so if this one is too soon of an epoch the rest will be too
                    break;
                }
            }
            if (t.callbacks.begin() != deletetil) {
                t.callbacks.erase(t.callbacks.begin(), deletetil);
            }
            auto deletetil2 = t.needs_free.begin();
            for (auto it = t.needs_free.begin(); it != t.needs_free.end(); ++it) {
                if (threadinfo_t::signed_epoch_type(g - it->first) >= 2) {
                    free(it->second);
                    ++deletetil2;
                } else {
                    break;
                }
            }
            if (t.needs_free.begin() != deletetil2) {
                t.needs_free.erase(t.needs_free.begin(), deletetil2);
            }
            release_spinlock(t.spin_lock);
        }
        usleep(100000);
    }
    return NULL;
}

void Transaction::hard_check_opacity(TransactionTid::type t) {
    // ignore opacity checks during commit; we're in the middle of checking
    // things anyway
    if (state_ == s_committing)
        return;

    INC_P(txp_hco);
    if (t & TransactionTid::lock_bit) {
        INC_P(txp_hco_lock);
    abort:
        INC_P(txp_hco_abort);
        abort();
    }
    if (!(t & TransactionTid::valid_bit))
        INC_P(txp_hco_invalid);

    start_tid_ = _TID;
    release_fence();
    for (auto it = transSet_.begin(); it != transSet_.end(); ++it)
        if (it->has_read()) {
            INC_P(txp_total_check_read);
            if (!it->owner()->check(*it, *this)
                && !preceding_read_exists(*it))
                goto abort;
        } else if (it->has_predicate()) {
            INC_P(txp_total_check_predicate);
            if (!it->owner()->check_predicate(*it, *this, false))
                goto abort;
        }
}

 void Transaction::stop(bool committed) {
    if (!committed)
        INC_P(txp_total_aborts);
    if (any_writes_ && state_ == s_committing_locked) {
        for (auto it = transSet_.begin() + first_write_; it != transSet_.end(); ++it)
            if (it->needs_unlock())
                it->owner()->unlock(*it);
    }
    if (any_writes_) {
        for (auto it = transSet_.begin() + first_write_; it != transSet_.end(); ++it)
            if (it->has_write())
                it->owner()->cleanup(*it, committed);
    }
    // TODO: this will probably mess up with nested transactions
    tinfo[TThread::id()].epoch = 0;
    if (tinfo[TThread::id()].trans_end_callback)
        tinfo[TThread::id()].trans_end_callback();
    // XXX should reset trans_end_callback after calling it...
    state_ = s_aborted + committed;
}

bool Transaction::try_commit() {
#if ASSERT_TX_SIZE
    if (transSet_.size() > TX_SIZE_LIMIT) {
        std::cerr << "transSet_ size at " << transSet_.size()
            << ", abort." << std::endl;
        assert(false);
    }
#endif
    MAX_P(txp_max_set, transSet_.size());
    ADD_P(txp_total_n, transSet_.size());

    assert(state_ == s_in_progress || state_ >= s_aborted);
    if (state_ >= s_aborted)
        return state_ > s_aborted;

    state_ = s_committing;

    int writeset[transSet_.size()];
    int nwriteset = 0;
    writeset[0] = transSet_.size();

    for (auto it = transSet_.begin(); it != transSet_.end(); ++it) {
        if (it->has_predicate()) {
            INC_P(txp_total_check_predicate);
            if (!it->owner()->check_predicate(*it, *this, true))
                goto abort;
        }
        if (it->has_write())
            writeset[nwriteset++] = it - transSet_.begin();
#ifdef DETAILED_LOGGING
        if (it->has_read())
            INC_P(txp_total_r);
#endif
    }

    first_write_ = writeset[0];

    //phase1
#if !NOSORT
    std::sort(writeset, writeset + nwriteset, [&] (int i, int j) {
        return transSet_[i] < transSet_[j];
    });
#endif
    if (nwriteset) {
        state_ = s_committing_locked;
        auto writeset_end = writeset + nwriteset;
        for (auto it = writeset; it != writeset_end; ) {
            TransItem* me = &transSet_[*it];
            if (!me->owner()->lock(*me, *this))
                goto abort;
            me->__or_flags(TransItem::lock_bit);
            ++it;
            if (may_duplicate_items_)
                for (; it != writeset_end && transSet_[*it].same_item(*me); ++it)
                    /* do nothing */;
        }
    }


#if CONSISTENCY_CHECK
    fence();
    commit_tid();
    fence();
#endif

    //phase2
    for (auto it = transSet_.begin(); it != transSet_.end(); ++it)
        if (it->has_read()) {
            INC_P(txp_total_check_read);
            if (!it->owner()->check(*it, *this)
                && !preceding_read_exists(*it))
                goto abort;
        }

    // fence();

    //phase3
    for (auto it = transSet_.begin() + first_write_; it != transSet_.end(); ++it) {
        TransItem& ti = *it;
        if (ti.has_write()) {
            INC_P(txp_total_w);
            ti.owner()->install(ti, *this);
        }
    }

    // fence();
    stop(true);
    return true;

abort:
    // fence();
    INC_P(txp_commit_time_aborts);
    stop(false);
    return false;
}

void Transaction::print_stats() {
    threadinfo_t out = tinfo_combined();
    if (txp_count >= txp_max_set) {
        fprintf(stderr, "$ %llu starts, %llu max read set, %llu commits",
                out.p(txp_total_starts),
                out.p(txp_max_set),
                out.p(txp_total_starts) - out.p(txp_total_aborts));
        if (out.p(txp_total_aborts)) {
            fprintf(stderr, ", %llu (%.3f%%) aborts",
                    out.p(txp_total_aborts),
                    100.0 * (double) out.p(txp_total_aborts) / out.p(txp_total_starts));
            if (out.p(txp_commit_time_aborts))
                fprintf(stderr, "\n$ %llu (%.3f%%) of aborts at commit time",
                        out.p(txp_commit_time_aborts),
                        100.0 * (double) out.p(txp_commit_time_aborts) / out.p(txp_total_aborts));
        }
        fprintf(stderr, "\n");
    }
    if (txp_count >= txp_hco_abort)
        fprintf(stderr, "$ %llu HCO (%llu lock, %llu invalid, %llu aborts)\n",
                out.p(txp_hco), out.p(txp_hco_lock), out.p(txp_hco_invalid), out.p(txp_hco_abort));
    if (txp_count >= txp_hash_collision)
        fprintf(stderr, "$ %llu hash collisions\n", out.p(txp_hash_collision));
}

void Transaction::print(std::ostream& w) const {
    static const char* names[] = {"in-progress", "committing", "committing-locked", "aborted", "committed"};
    w << "T0x" << (void*) this << " " << names[state_] << " [";
    for (auto& ti : transSet_) {
        if (&ti != &transSet_[0])
            w << " ";
        ti.owner()->print(w, ti);
    }
    w << "]\n";
}

void TObject::print(std::ostream& w, const TransItem& item) const {
    w << "{" << (void*) this << "." << item.key<void*>();
    if (item.has_read())
        w << " ?" << item.read_value<void*>();
    if (item.has_write())
        w << " =" << item.write_value<void*>();
    if (item.has_predicate())
        w << " P" << item.predicate_value<void*>();
    w << "}";
}
