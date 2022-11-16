/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#ifndef COZOROCKS_ITER_H
#define COZOROCKS_ITER_H

#include "common.h"
#include "slice.h"
#include "status.h"
#include "db.h"

struct IterBridge {
    Transaction *tx;
    unique_ptr<Iterator> iter;
    string lower_storage;
    string upper_storage;
    Slice lower_bound;
    Slice upper_bound;
    size_t cf;
    shared_ptr<RocksDbBridge> db_bridge;
    ReadOptions r_opts;

    explicit IterBridge(shared_ptr<RocksDbBridge> db_bridge_)
        : lower_bound(),
          upper_bound(),
          db_bridge(db_bridge_),
          r_opts() {
        r_opts.ignore_range_deletions = true;
        r_opts.auto_prefix_mode = true;
    }

    inline void set_cf(size_t cf_) {
        cf = cf_;
    }

    inline void set_snapshot(const Snapshot *snapshot) {
        r_opts.snapshot = snapshot;
    }

//    inline ReadOptions &get_r_opts() {
//        return *r_opts;
//    }

    inline void verify_checksums(bool val) {
        r_opts.verify_checksums = val;
    }

    inline void fill_cache(bool val) {
        r_opts.fill_cache = val;
    }

    inline void tailing(bool val) {
        r_opts.tailing = val;
    }

    inline void total_order_seek(bool val) {
        r_opts.total_order_seek = val;
    }

    inline void auto_prefix_mode(bool val) {
        r_opts.auto_prefix_mode = val;
    }

    inline void prefix_same_as_start(bool val) {
        r_opts.prefix_same_as_start = val;
    }

    inline void pin_data(bool val) {
        r_opts.pin_data = val;
    }

    inline void clear_bounds() {
        r_opts.iterate_lower_bound = nullptr;
        r_opts.iterate_upper_bound = nullptr;
        lower_bound.clear();
        upper_bound.clear();
    }

    inline void set_lower_bound(RustBytes bound) {
        lower_storage = convert_slice_to_string(bound);
        lower_bound = lower_storage;
        r_opts.iterate_lower_bound = &lower_bound;
    }

    inline void set_upper_bound(RustBytes bound) {
        upper_storage = convert_slice_to_string(bound);
        upper_bound = upper_storage;
        r_opts.iterate_upper_bound = &upper_bound;
    }

    inline void start() {
        auto cf_handle = db_bridge->cf_handles[cf];
        if (tx) {
            iter.reset(tx->GetIterator(r_opts, cf_handle));
        } else {
            iter.reset(db_bridge->db->NewIterator(r_opts, cf_handle));
        }
    }

    inline void reset() {
        iter.reset();
        clear_bounds();
    }

    inline void to_start() {
        iter->SeekToFirst();
    }

    inline void to_end() {
        iter->SeekToLast();
    }

    inline void seek(RustBytes key) {
        iter->Seek(convert_slice(key));
    }

    inline void seek_backward(RustBytes key) {
        iter->SeekForPrev(convert_slice(key));
    }

    inline bool is_valid() const {
        return iter->Valid();
    }

    inline void next() {
        iter->Next();
    }

    inline void prev() {
        iter->Prev();
    }

    inline void status(RocksDbStatus &status) const {
        write_status(iter->status(), status);
    }

    [[nodiscard]] inline RustBytes key() const {
        return convert_slice_back(iter->key());
    }

    [[nodiscard]] inline RustBytes val() const {
        return convert_slice_back(iter->value());
    }
};

#endif //COZOROCKS_ITER_H
