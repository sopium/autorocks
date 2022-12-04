/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#pragma once

#include <memory>
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/options_util.h"

using namespace std;
using namespace rocksdb;

inline vector<ColumnFamilyDescriptor>
new_column_family_descriptor_vec(size_t len)
{
    vector<ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(len + 1);
    for (size_t i = 0; i < len; i++)
    {
        descriptors.emplace_back(to_string(i), ColumnFamilyOptions());
    }
    descriptors.emplace_back("default", ColumnFamilyOptions());
    return descriptors;
}

TransactionDBOptions new_transaction_db_options()
{
    return TransactionDBOptions();
}

// Autocxx cannot access fields of non-pod type...
struct ReadOptionsWrapper : ReadOptions
{
    void set_snapshot(const Snapshot *snapshot_)
    {
        snapshot = snapshot_;
    }
};

struct DbOptionsWrapper
{
    string path;
    DBOptions db_options;
    vector<ColumnFamilyDescriptor> cf_descriptors;

    DbOptionsWrapper(string path_)
        : DbOptionsWrapper(path_, 0)
    {
    }

    DbOptionsWrapper(string path_, size_t columns)
        : path(path_), cf_descriptors(new_column_family_descriptor_vec(columns))
    {
    }

    DbOptionsWrapper(Slice path_, size_t columns)
        : path(path_.ToString()), cf_descriptors(new_column_family_descriptor_vec(columns))
    {
    }

    void set_create_if_missing(bool val)
    {
        db_options.create_if_missing = val;
    }

    void set_create_missing_column_families(bool val)
    {
        db_options.create_missing_column_families = val;
    }

    Status load(Slice options_file)
    {
        return LoadOptionsFromFile(options_file.ToString(), Env::Default(), &db_options, &cf_descriptors);
    }

    ColumnFamilyOptions *get_cf_option(size_t index)
    {
        return &cf_descriptors[index].options;
    }

    /// Sort and complete missing column families.
    void sort_and_complete_missing(size_t columns)
    {
        unordered_map<string, ColumnFamilyDescriptor> cf_map;
        for (auto desc : cf_descriptors)
        {
            cf_map.emplace(desc.name, move(desc));
        }
        auto default_cf = cf_map["default"];
        cf_map.erase("default");

        cf_descriptors.clear();
        cf_descriptors.reserve(columns + 1);
        for (size_t i = 0; i < columns; i++)
        {
            auto name = to_string(i);
            auto it = cf_map.find(name);
            if (it != cf_map.end())
            {
                cf_descriptors.emplace_back(move(it->second));
            }
            else
            {
                cf_descriptors.emplace_back(name, default_cf.options);
            }
        }
        cf_descriptors.emplace_back(move(default_cf));
    }
};

// Note: make sure TransactionDBWrapper is Unpin.
struct TransactionDBWrapper
{
    unique_ptr<TransactionDB> db;
    std::vector<ColumnFamilyHandle *> cf_handles;

    Status open(
        DbOptionsWrapper &&options,
        const TransactionDBOptions &transaction_db_options)
    {
        TransactionDB *ptr;
        Status status = TransactionDB::Open(
            options.db_options,
            transaction_db_options,
            options.path,
            options.cf_descriptors,
            &cf_handles,
            &ptr);
        if (status.ok())
        {
            db.reset(ptr);
        }
        return status;
    }

    ColumnFamilyHandle *get_cf(size_t cf) const
    {
        if (cf >= cf_handles.size())
        {
            return nullptr;
        }
        return cf_handles[cf];
    }

    Status get(const ReadOptions &options, ColumnFamilyHandle *cf, const Slice &key, PinnableSlice *slice) const
    {
        return db->Get(options, cf, key, slice);
    }

    Status put(const WriteOptions &options, ColumnFamilyHandle *cf, const Slice &key, const Slice &value) const
    {
        return db->Put(options, cf, key, value);
    }

    Status del(const WriteOptions &options, ColumnFamilyHandle *cf, const Slice &key) const
    {
        return db->Delete(options, cf, key);
    }

    unique_ptr<Iterator> iter(const ReadOptions &options, ColumnFamilyHandle *cf) const
    {
        return unique_ptr<Iterator>(db->NewIterator(options, cf));
    }

    DB *as_db() const
    {
        return &*db;
    }
};

// Note: make sure TransactionWrapper is Unpin.
struct TransactionWrapper
{
    unique_ptr<Transaction> tx;

    static TransactionWrapper begin(const TransactionDBWrapper *db, const WriteOptions &write_options, const TransactionOptions &transaction_options)
    {
        return {unique_ptr<Transaction>(db->db->BeginTransaction(write_options, transaction_options))};
    }

    Status get(const ReadOptions &options, ColumnFamilyHandle *cf, const Slice &key, PinnableSlice *slice) const
    {
        return tx->Get(options, cf, key, slice);
    }

    Status put(ColumnFamilyHandle *cf, const Slice &key, const Slice &value) const
    {
        return tx->Put(cf, key, value);
    }

    Status del(ColumnFamilyHandle *cf, const Slice &key) const
    {
        return tx->Delete(cf, key);
    }

    const Snapshot *snapshot() const
    {
        return tx->GetSnapshot();
    }

    Status commit() const
    {
        return tx->Commit();
    }

    unique_ptr<Iterator> iter(const ReadOptions &options, ColumnFamilyHandle *cf) const
    {
        return unique_ptr<Iterator>(tx->GetIterator(options, cf));
    }
};
