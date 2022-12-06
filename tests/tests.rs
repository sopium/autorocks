use autorocks::*;
use autorocks_sys::rocksdb::{CompressionType, PinnableSlice, Status_Code};
use moveit::moveit;
use tempfile::tempdir;

fn open_temp(columns: usize) -> TransactionDb {
    let dir = tempdir().unwrap();
    DbBuilder::new(dir.path(), columns)
        .create_if_missing(true)
        .create_missing_column_families(true)
        .build()
        .unwrap()
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

#[cfg(feature = "snappy")]
#[test]
fn test_db_open_snappy() {
    let dir = tempdir().unwrap();
    let db = DbBuilder::new(dir.path(), 1)
        .create_if_missing(true)
        .create_missing_column_families(true)
        .compression(CompressionType::kSnappyCompression)
        .build()
        .unwrap();
    db.put(0, b"key", b"value").unwrap();
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

#[test]
fn test_write_batch() {
    let db = open_temp(1);
    db.put(0, b"key", b"value").unwrap();
    let mut wb = db.new_write_batch();
    wb.put(0, b"key1", b"value1").unwrap();
    wb.delete(0, b"key").unwrap();
    db.write(&mut wb).unwrap();
    moveit! {
        let mut buf = PinnableSlice::new();
    }
    assert!(db.get(0, b"key", buf.as_mut()).unwrap().is_none());
    assert!(db.get(0, b"key1", buf.as_mut()).unwrap().is_some());
}
