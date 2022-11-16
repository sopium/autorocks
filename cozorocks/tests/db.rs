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
    assert_eq!(&*tx.get(2, b"key2", false).unwrap().unwrap(), b"value2");

    let mut iter = tx
        .iterator()
        .cf(2)
        .start();
    iter.seek(b"key1");
    assert_eq!(&*iter.key().unwrap().unwrap(), b"key1");
    iter.next();
    assert_eq!(&*iter.key().unwrap().unwrap(), b"key2");
    iter.next();
    assert!(!iter.is_valid());

    tx.commit().unwrap();
}
