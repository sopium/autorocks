/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#![warn(rust_2018_idioms, future_incompatible)]
#![allow(clippy::type_complexity)]

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
    generate!("rocksdb::Slice")
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
    generate!("DbOptionsWrapper")
    generate!("TransactionDBWrapper")
    generate!("TransactionWrapper")
}

use ::std::fmt;

pub use ffi::*;

pub struct RocksDBStatusError {
    msg: String,
    pub code: rocksdb::Status_Code,
}

impl fmt::Debug for RocksDBStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksDBStatusError")
            .field("msg", &self.msg)
            .field("code", &(self.code.clone() as u8))
            .finish()
    }
}

impl fmt::Display for RocksDBStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl ::std::error::Error for RocksDBStatusError {}

pub fn into_result(status: &rocksdb::Status) -> Result<(), RocksDBStatusError> {
    if status.ok() {
        Ok(())
    } else {
        Err(RocksDBStatusError {
            code: status.code(),
            msg: status.ToString().to_string_lossy().into(),
        })
    }
}
