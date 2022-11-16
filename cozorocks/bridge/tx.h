/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include "common.h"
#include "slice.h"
#include "status.h"
#include "iter.h"
#include "db.h"

struct TxBridge
{
    unique_ptr<Transaction> tx;
    WriteOptions w_opts;
    ReadOptions r_opts;
    TransactionOptions tx_opts;
    shared_ptr<RocksDbBridge> db;

    explicit TxBridge(shared_ptr<RocksDbBridge> _db)
        : db(_db)
    {
        r_opts.ignore_range_deletions = true;
    }

    inline WriteOptions &get_w_opts()
    {
        return w_opts;
    }

    //    inline ReadOptions &get_r_opts() {
    //        return *r_opts;
    //    }

    inline void verify_checksums(bool val)
    {
        r_opts.verify_checksums = val;
    }

    inline void fill_cache(bool val)
    {
        r_opts.fill_cache = val;
    }

    inline unique_ptr<IterBridge> iterator() const
    {
        auto iter = make_unique<IterBridge>(db);
        iter->tx = &*tx;
        return iter;
    };

    inline void set_snapshot(bool val)
    {
        if (tx != nullptr)
        {
            if (val)
            {
                tx->SetSnapshot();
            }
        }
        else
        {
            tx_opts.set_snapshot = val;
        }
    }

    inline void clear_snapshot()
    {
        tx->ClearSnapshot();
    }

    void start();

    inline unique_ptr<PinnableSlice> get(size_t cf, RustBytes key, bool for_update, RocksDbStatus &status) const
    {
        Slice key_ = convert_slice(key);
        auto ret = make_unique<PinnableSlice>();
        auto cf_handle = db->cf_handles[cf];
        if (for_update)
        {
            auto s = tx->GetForUpdate(r_opts, cf_handle, key_, &*ret);
            write_status(s, status);
        }
        else
        {
            auto s = tx->Get(r_opts, cf_handle, key_, &*ret);
            write_status(s, status);
        }
        return ret;
    }

    inline void put(size_t cf, RustBytes key, RustBytes val, RocksDbStatus &status)
    {
        auto cf_handle = db->cf_handles[cf];
        write_status(tx->Put(cf_handle, convert_slice(key), convert_slice(val)), status);
    }

    inline void del(size_t cf, RustBytes key, RocksDbStatus &status)
    {
        auto cf_handle = db->cf_handles[cf];
        write_status(tx->Delete(cf_handle, convert_slice(key)), status);
    }

    inline void commit(RocksDbStatus &status)
    {
        write_status(tx->Commit(), status);
    }

    inline void rollback(RocksDbStatus &status)
    {
        write_status(tx->Rollback(), status);
    }

    inline void rollback_to_savepoint(RocksDbStatus &status)
    {
        write_status(tx->RollbackToSavePoint(), status);
    }

    inline void pop_savepoint(RocksDbStatus &status)
    {
        write_status(tx->PopSavePoint(), status);
    }

    inline void set_savepoint()
    {
        tx->SetSavePoint();
    }
};
