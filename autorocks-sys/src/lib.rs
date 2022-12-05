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
    generate!("rocksdb::WriteBatch")
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
    generate!("new_write_batch")
    generate!("ReadOptionsWrapper")
    generate!("DbOptionsWrapper")
    generate!("TransactionDBWrapper")
    generate!("TransactionWrapper")
}

pub use ffi::*;

impl Unpin for TransactionDBWrapper {}
impl Unpin for TransactionWrapper {}

// Thread safe.
unsafe impl Send for TransactionDBWrapper {}
unsafe impl Sync for TransactionDBWrapper {}

unsafe impl Send for TransactionWrapper {}
unsafe impl Send for rocksdb::WriteBatch {}

impl From<&[u8]> for rocksdb::Slice {
    fn from(s: &[u8]) -> rocksdb::Slice {
        rocksdb::Slice {
            data_: s.as_ptr() as *const _,
            size_: s.len(),
        }
    }
}
