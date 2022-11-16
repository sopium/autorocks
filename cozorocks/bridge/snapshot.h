#pragma once

#include "common.h"
#include "db.h"

struct SnapshotBridge
{
    const Snapshot *snapshot;
    shared_ptr<RocksDbBridge> db;

    explicit SnapshotBridge(const Snapshot *snapshot_, shared_ptr<RocksDbBridge> db_) : snapshot(snapshot_), db(db_) {}

    unique_ptr<PinnableSlice> get(size_t cf, RustBytes key, RocksDbStatus &status) const
    {
        Slice key_ = convert_slice(key);
        auto ret = make_unique<PinnableSlice>();
        auto cf_handle = db->cf_handles[cf];
        auto r_opts = ReadOptions();
        r_opts.snapshot = snapshot;
        write_status(db->db->Get(r_opts, cf_handle, key_, &*ret), status);
        return ret;
    }

    ~SnapshotBridge()
    {
        db->db->ReleaseSnapshot(snapshot);
    }
};
