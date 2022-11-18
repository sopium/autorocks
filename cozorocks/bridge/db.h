/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include <utility>

#include "iostream"
#include "common.h"
#include "status.h"
#include "slice.h"

struct SstFileWriterBridge
{
    SstFileWriter inner;

    SstFileWriterBridge(EnvOptions eopts, Options opts) : inner(eopts, opts)
    {
    }

    inline void finish(RocksDbStatus &status)
    {
        write_status(inner.Finish(), status);
    }

    inline void put(RustBytes key, RustBytes val, RocksDbStatus &status)
    {
        write_status(inner.Put(convert_slice(key), convert_slice(val)), status);
    }
};

struct RocksDbBridge
{
    unique_ptr<TransactionDB> db;

    std::vector<ColumnFamilyHandle *> cf_handles;

    inline unique_ptr<SstFileWriterBridge> get_sst_writer(rust::Str path, RocksDbStatus &status) const
    {
        DB *db_ = get_base_db();
        auto cf = db->DefaultColumnFamily();
        Options options_ = db_->GetOptions(cf);
        auto sst_file_writer = std::make_unique<SstFileWriterBridge>(EnvOptions(), options_);
        string path_(path);

        write_status(sst_file_writer->inner.Open(path_), status);
        return sst_file_writer;
    }

    inline void ingest_sst(rust::Str path, RocksDbStatus &status) const
    {
        IngestExternalFileOptions ifo;
        DB *db_ = get_base_db();
        string path_(path);
        auto cf = db->DefaultColumnFamily();
        write_status(db_->IngestExternalFile(cf, {std::move(path_)}, ifo), status);
    }

    inline void del_range(RustBytes start, RustBytes end, RocksDbStatus &status) const
    {
        WriteBatch batch;
        auto cf = db->DefaultColumnFamily();
        auto s = batch.DeleteRange(cf, convert_slice(start), convert_slice(end));
        if (!s.ok())
        {
            write_status(s, status);
            return;
        }
        WriteOptions w_opts;
        TransactionDBWriteOptimizations optimizations;
        optimizations.skip_concurrency_control = true;
        optimizations.skip_duplicate_key_check = true;
        auto s2 = db->Write(w_opts, optimizations, &batch);
        write_status(s2, status);
    }

    void compact_range(size_t cf, RustBytes start, RustBytes end, RocksDbStatus &status) const
    {
        CompactRangeOptions options;
        auto cf_handle = cf_handles[cf];
        auto start_s = convert_slice(start);
        auto end_s = convert_slice(end);
        auto s = db->CompactRange(options, cf_handle, &start_s, &end_s);
        write_status(s, status);
    }

    DB *get_base_db() const
    {
        return db->GetBaseDB();
    }
};

struct TxBridge;

unique_ptr<TxBridge>
transact(shared_ptr<RocksDbBridge> db);

struct SnapshotBridge;

unique_ptr<SnapshotBridge>
snapshot(shared_ptr<RocksDbBridge> db);

shared_ptr<RocksDbBridge>
open_db(const DbOpts &opts, RocksDbStatus &status);
