#pragma once

#include "common.h"
#include "db.h"

/// Owned Snapshot.
struct SnapshotBridge
{
    const Snapshot *snapshot;
    shared_ptr<RocksDbBridge> db;
    PinnableSlice slice;

    explicit SnapshotBridge(const Snapshot *snapshot_, shared_ptr<RocksDbBridge> db_) : snapshot(snapshot_), db(db_) {}

    PinnableSlice &get(size_t cf, RustBytes key, RocksDbStatus &status)
    {
        Slice key_ = convert_slice(key);
        auto cf_handle = db->cf_handles[cf];
        auto r_opts = ReadOptions();
        r_opts.snapshot = snapshot;
        write_status(db->db->Get(r_opts, cf_handle, key_, &slice), status);
        return slice;
    }

    ~SnapshotBridge()
    {
        db->db->ReleaseSnapshot(snapshot);
    }
};
