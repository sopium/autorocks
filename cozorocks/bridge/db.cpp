/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#include <iostream>
#include <memory>
#include "db.h"
#include "cozorocks/src/bridge/mod.rs.h"

Options default_db_options() {
    Options options = Options();
    options.bottommost_compression = kZSTD;
    options.compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_background_jobs = 6;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = kMinOverlappingRatio;
    BlockBasedTableOptions table_options;
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 5;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

ColumnFamilyOptions default_cf_options() {
    ColumnFamilyOptions options = ColumnFamilyOptions();
    options.bottommost_compression = kZSTD;
    options.compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.compaction_pri = kMinOverlappingRatio;
    BlockBasedTableOptions table_options;
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 5;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

shared_ptr<RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status) {
    auto options = default_db_options();
    // Load options from file.
    auto cf_descs = vector<ColumnFamilyDescriptor>();
    if (!opts.options_path.empty()) {
        write_status(
            LoadOptionsFromFile(string(opts.options_path), Env::Default(), &options, &cf_descs),
            status
        );
        if (status.code != StatusCode::kOk) {
            return make_shared<RocksDbBridge>();
        }
    }
    // Fill missing column family descriptors.
    auto cf_map = map<string, ColumnFamilyDescriptor>();
    for (ColumnFamilyDescriptor desc: cf_descs) {
        cf_map.emplace(desc.name, move(desc));
    }
    cf_descs.clear();
    auto cf_default = cf_map["default"];
    cf_map.erase("default");
    for (size_t i = 0; i < opts.column_families; i++) {
        auto name = to_string(i);
        cf_map.emplace(name, ColumnFamilyDescriptor(name, cf_default.options));
    }
    for (auto&& pair: cf_map) {
        cf_descs.push_back(pair.second);
    }
    cf_descs.push_back(cf_default);

    if (opts.prepare_for_bulk_load) {
        options.PrepareForBulkLoad();
    }
    if (opts.increase_parallelism > 0) {
        options.IncreaseParallelism(opts.increase_parallelism);
    }
    if (opts.optimize_level_style_compaction) {
        options.OptimizeLevelStyleCompaction();
    }
    options.create_if_missing = opts.create_if_missing;
    options.paranoid_checks = opts.paranoid_checks;
    if (opts.enable_blob_files) {
        options.enable_blob_files = true;

        options.min_blob_size = opts.min_blob_size;

        options.blob_file_size = opts.blob_file_size;

        options.enable_blob_garbage_collection = opts.enable_blob_garbage_collection;
    }
    if (opts.use_bloom_filter) {
        BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(NewBloomFilterPolicy(opts.bloom_filter_bits_per_key, false));
        table_options.whole_key_filtering = opts.bloom_filter_whole_key_filtering;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    if (opts.use_capped_prefix_extractor) {
        options.prefix_extractor.reset(NewCappedPrefixTransform(opts.capped_prefix_extractor_len));
    }
    if (opts.use_fixed_prefix_extractor) {
        options.prefix_extractor.reset(NewFixedPrefixTransform(opts.fixed_prefix_extractor_len));
    }
    options.create_missing_column_families = true;

    shared_ptr<RocksDbBridge> db = make_shared<RocksDbBridge>();

    db->db_path = string(opts.db_path);

    TransactionDB *txn_db = nullptr;
    vector<ColumnFamilyHandle*> cf_handles;
    write_status(
            TransactionDB::Open(options, TransactionDBOptions(), db->db_path, cf_descs, &db->cf_handles, &txn_db),
            status);
    db->db.reset(txn_db);
    db->destroy_on_exit = opts.destroy_on_exit;


    return db;
}

RocksDbBridge::~RocksDbBridge() {
    if (destroy_on_exit && (db != nullptr)) {
        cerr << "destroying database on exit: " << db_path << endl;
        auto status = db->Close();
        if (!status.ok()) {
            cerr << status.ToString() << endl;
        }
        db.reset();
        Options options{};
        auto status2 = DestroyDB(db_path, options);
        if (!status2.ok()) {
            cerr << status2.ToString() << endl;
        }
    }
}

unique_ptr<TxBridge> transact(shared_ptr<RocksDbBridge> db) {
    return make_unique<TxBridge>(db);
}
