use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    #[cfg(feature = "autorocks")]
    c.bench_function("autorocks db get", |b| {
        use ::std::os::unix::prelude::OsStrExt;
        use autorocks::{
            autorocks_sys::{rocksdb::*, *},
            moveit::moveit,
            *,
        };

        let dir = tempfile::tempdir().unwrap();
        let path: Slice = dir.path().as_os_str().as_bytes().into();
        moveit! {
            let mut options = DbOptionsWrapper::new2(path, 1);
            let txn_db_options = new_transaction_db_options();
        }
        options.as_mut().set_create_if_missing(true);
        options.as_mut().set_create_missing_column_families(true);
        let db = TransactionDb::open(options, &txn_db_options).unwrap();
        db.put(0, b"key", b"value").unwrap();
        moveit! {
            let mut buf = PinnableSlice::new();
        }
        b.iter(|| {
            db.get(0, b"key", buf.as_mut()).unwrap().unwrap();
        })
    });

    #[cfg(feature = "ckb-rocksdb")]
    c.bench_function("ckb-rocksdb db get", |b| {
        use ckb_rocksdb::{prelude::*, Options, DB};

        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // TransactionDB does not support get_pinned_cf.
        let db = DB::open_cf(&opts, dir.path(), ["0"]).unwrap();
        db.put_cf(db.cf_handle("0").unwrap(), b"key", b"value")
            .unwrap();
        b.iter(|| {
            db.get_pinned_cf(db.cf_handle("0").unwrap(), b"key")
                .unwrap()
                .unwrap();
        })
    });

    #[cfg(feature = "ckb-rocksdb")]
    c.bench_function("ckb-rocksdb db get cached cf", |b| {
        use ckb_rocksdb::{prelude::*, Options, DB};

        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // TransactionDB does not support get_pinned_cf.
        let db = DB::open_cf(&opts, dir.path(), ["0"]).unwrap();
        let cf = db.cf_handle("0").unwrap();
        db.put_cf(cf, b"key", b"value").unwrap();
        b.iter(|| {
            db.get_pinned_cf(cf, b"key").unwrap().unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
