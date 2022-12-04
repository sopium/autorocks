use std::os::unix::prelude::OsStrExt;

use autorocks_sys::{new_transaction_db_options, rocksdb::Status_Code};
use tempfile::tempdir;

use super::*;

fn open_temp(columns: usize) -> TransactionDb {
    let dir = tempdir().unwrap();
    let path: Slice = dir.path().as_os_str().as_bytes().into();
    moveit! {
        let mut options = DbOptionsWrapper::new2(path, columns);
        let txn_db_options = new_transaction_db_options();
    }
    options.as_mut().set_create_if_missing(true);
    options.as_mut().set_create_missing_column_families(true);
    let db = TransactionDb::open(options, &txn_db_options).unwrap();
    db
}

#[test]
fn test_db_open_put_get_delete() {
    let db = open_temp(1);
    db.put(0, b"key", b"value").unwrap();
    moveit! {
        let mut slice = PinnableSlice::new();
    }
    let v = db.get(0, b"key", slice.as_mut()).unwrap();
    assert_eq!(v.unwrap(), b"value");
    db.delete(0, b"key").unwrap();
    let v = db.get(0, b"key", slice.as_mut()).unwrap();
    assert!(v.is_none());
}

#[test]
fn test_snapshot() {
    let db = open_temp(1);
    db.put(0, b"key", b"value").unwrap();
    let snap = db.snapshot();
    db.put(0, b"key", b"value1").unwrap();
    let snap1 = db.snapshot();
    moveit! {
        let mut slice = PinnableSlice::new();
    }
    let v = snap.get(0, b"key", slice.as_mut()).unwrap();
    assert_eq!(v.unwrap(), b"value");
    let v = snap1.get(0, b"key", slice.as_mut()).unwrap();
    assert_eq!(v.unwrap(), b"value1");
    let v = snap1.get(0, b"key1", slice.as_mut()).unwrap();
    assert!(v.is_none());
}

#[test]
fn test_tx_and_tx_snapshot() {
    let db = open_temp(1);
    db.put(0, b"key", b"value").unwrap();
    moveit! {
        let mut slice = PinnableSlice::new();
    }
    let tx = db.begin_transaction();

    db.put(0, b"key", b"value1").unwrap();

    let snap = tx.snapshot();
    let v = snap.get(0, b"key", slice.as_mut()).unwrap().unwrap();
    assert_eq!(v, b"value");
    let v = tx.get(0, b"key", slice.as_mut()).unwrap().unwrap();
    assert_eq!(v, b"value1");

    tx.put(0, b"key1", b"value1").unwrap();
    let err = tx.put(0, b"key", b"value2").unwrap_err();
    assert!(err.code == Status_Code::kBusy);
    tx.delete(0, b"key1").unwrap();
    let v = tx.get(0, b"key1", slice.as_mut()).unwrap();
    assert!(v.is_none());

    tx.commit().unwrap();
}

#[test]
fn test_iter() {
    let db = open_temp(1);
    db.put(0, b"key", b"value").unwrap();
    let tx = db.begin_transaction();
    let snap1 = tx.snapshot();
    let snap = db.snapshot();
    db.put(0, b"key1", b"value1").unwrap();
    assert_eq!(snap.iter(0).count(), 1);
    assert_eq!(snap1.iter(0).count(), 1);
    assert_eq!(db.iter(0).count(), 2);
    assert_eq!(tx.iter(0).count(), 2);
}
