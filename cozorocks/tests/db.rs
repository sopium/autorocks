use cozorocks::DbBuilder;

#[test]
fn test_db_open() {
    let db = DbBuilder::default()
        .path("./db")
        .column_families(3)
        .create_if_missing(true)
        .options_path("./db.toml")
        .build()
        .unwrap();

    let mut tx = db.transact().start();
    tx.put(2, b"key2", b"value2").unwrap();
    tx.commit().unwrap();

    let mut tx = db.transact().start();
    tx.put(2, b"key", b"value").unwrap();
    tx.put(2, b"key1", b"value1").unwrap();
    assert_eq!(
        &*tx.get(2, b"key2", false, false).unwrap().unwrap(),
        b"value2"
    );

    let mut iter = tx.iterator(false).cf(2).start();
    iter.seek(b"key1");
    assert_eq!(&*iter.key().unwrap().unwrap(), b"key1");
    iter.next();
    assert_eq!(&*iter.key().unwrap().unwrap(), b"key2");
    iter.next();
    assert!(!iter.is_valid());

    tx.commit().unwrap();
}

#[test]
fn test_snapshot() {
    let db = DbBuilder::default()
        .path("./db1")
        .create_if_missing(true)
        .build()
        .unwrap();

    let mut tx = db.transact().start();
    tx.del(0, b"key").unwrap();
    tx.commit().unwrap();

    let mut snap = db.get_snapshot();

    let mut tx = db.transact().set_snapshot(true).start();
    tx.put(0, b"key", b"value").unwrap();
    tx.commit().unwrap();

    assert!(snap.get(0, b"key").unwrap().is_none());
}
