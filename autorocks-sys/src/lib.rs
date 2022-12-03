#![allow(clippy::all)]

autocxx::include_cpp! {
    #include "rocksdb/slice.h"
    #include "rocksdb/options.h"
    #include "rocksdb/iterator.h"
    #include "rocksdb/status.h"
    #include "db.h"

    safety!(unsafe_ffi)

    generate_pod!("rocksdb::WriteOptions")
    generate_pod!("rocksdb::TransactionOptions")
    generate!("rocksdb::DB")
    generate!("rocksdb::Iterator")
    generate_pod!("rocksdb::Slice")
    generate!("rocksdb::PinnableSlice")
    generate!("rocksdb::Options")
    generate!("rocksdb::ReadOptions")
    generate!("rocksdb::Status")
    generate!("rocksdb::ColumnFamilyOptions")
    generate!("rocksdb::ColumnFamilyDescriptor")
    // Unfortunately cannot generate these because of shared_ptr<const Snapshot>.
    //
    // generate!("rocksdb::TransactionDB")
    // generate!("rocksdb::Transaction")

    generate!("new_transaction_db_options")
    generate!("ReadOptionsWrapper")
    generate!("DbOptionsWrapper")
    generate!("TransactionDBWrapper")
    generate!("TransactionWrapper")
}

pub use ffi::*;

impl Unpin for TransactionDBWrapper {}
impl Unpin for TransactionWrapper {}

// RocksDB is thread safe.
unsafe impl Send for TransactionDBWrapper {}
unsafe impl Sync for TransactionDBWrapper {}
unsafe impl Send for TransactionWrapper {}
unsafe impl Sync for TransactionWrapper {}
