use std::{fmt, marker::PhantomData, mem::MaybeUninit, pin::Pin, sync::Arc};

use autocxx::prelude::UniquePtr;
use autorocks_sys::{
    rocksdb::{
        Iterator, PinnableSlice, ReadOptions, Slice, Status, Status_Code, TransactionDBOptions,
        TransactionOptions, WriteOptions, DB,
    },
    DbOptionsWrapper, ReadOptionsWrapper, TransactionDBWrapper, TransactionWrapper,
};
use moveit::{moveit, Emplace, New};

pub extern crate autorocks_sys;
pub extern crate moveit;

pub struct RocksDBStatusError {
    msg: String,
    pub code: Status_Code,
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

impl std::error::Error for RocksDBStatusError {}

pub type Result<T, E = RocksDBStatusError> = std::result::Result<T, E>;

pub fn into_result(status: &Status) -> Result<()> {
    if status.ok() {
        Ok(())
    } else {
        Err(RocksDBStatusError {
            code: status.code(),
            msg: status.ToString().to_string_lossy().into(),
        })
    }
}

#[derive(Clone)]
pub struct TransactionDb {
    inner: Arc<TransactionDBWrapper>,
}

impl TransactionDb {
    pub fn open(
        options: impl autocxx::RValueParam<DbOptionsWrapper>,
        txn_db_options: &TransactionDBOptions,
    ) -> Result<TransactionDb> {
        let db = Arc::emplace(TransactionDBWrapper::new());
        let mut db = Pin::into_inner(db);
        let db_mut = Arc::get_mut(&mut db).unwrap();
        moveit! {
            let status = Pin::new(db_mut).open(options, txn_db_options);
        }
        into_result(&status)?;
        Ok(TransactionDb { inner: db })
    }

    pub fn put(&self, col: usize, key: &[u8], value: &[u8]) -> Result<()> {
        moveit! {
            let options = WriteOptions::new();
        }
        self.put_with_options(&options, col, key, value)
    }

    pub fn put_with_options(
        &self,
        options: &WriteOptions,
        col: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let cf = self.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.as_inner().put(options, cf, &key.into(), &value.into()) };
        }
        into_result(&status)
    }

    pub fn delete_with_options(
        &self,
        options: &WriteOptions,
        col: usize,
        key: &[u8],
    ) -> Result<()> {
        let cf = self.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.as_inner().del(options, cf, &key.into()) };
        }
        into_result(&status)
    }

    pub fn delete(&self, col: usize, key: &[u8]) -> Result<()> {
        moveit! {
            let options = WriteOptions::new();
        }
        self.delete_with_options(&options, col, key)
    }

    pub fn get<'b>(
        &self,
        col: usize,
        key: &[u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        moveit! {
            let options = ReadOptions::new();
        }
        self.get_with_options(&options, col, key, buf)
    }

    pub fn get_with_options<'b>(
        &self,
        options: &ReadOptions,
        col: usize,
        key: &[u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        let slice = unsafe { buf.get_unchecked_mut() };
        let cf = self.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.as_inner().get(options, cf, &key.into(), slice) };
        }
        if status.IsNotFound() {
            return Ok(None);
        }
        into_result(&status)?;
        Ok(Some(as_rust_slice(slice)))
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            inner: unsafe { self.as_mut_db().GetSnapshot() },
            db: self.clone(),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        moveit! {
            let write_options = WriteOptions::new();
            let mut transaction_options = TransactionOptions::new();
        }
        transaction_options.set_snapshot = true;
        self.begin_transaction_with_options(&write_options, &transaction_options)
    }

    pub fn begin_transaction_with_options(
        &self,
        write_options: &WriteOptions,
        transaction_options: &TransactionOptions,
    ) -> Transaction {
        let mut tx: MaybeUninit<TransactionWrapper> = MaybeUninit::uninit();
        unsafe {
            TransactionWrapper::begin(self.as_inner(), write_options, transaction_options)
                .new(Pin::new(&mut tx))
        };
        Transaction {
            inner: unsafe { tx.assume_init() },
            db: self.clone(),
        }
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let options = ReadOptions::new();
        }
        self.iter_with_options(&options, col)
    }

    pub fn iter_with_options<'a>(
        &'a self,
        options: &ReadOptions,
        col: usize,
    ) -> DbIterator<&'a Self> {
        let cf = self.as_inner().get_cf(col);
        assert!(!cf.is_null());
        let mut iter = unsafe { self.as_inner().iter(options, cf) };
        iter.as_mut().unwrap().SeekToFirst();
        DbIterator {
            inner: iter,
            just_seeked: true,
            phantom: PhantomData,
        }
    }

    pub fn as_inner(&self) -> &TransactionDBWrapper {
        &self.inner
    }

    unsafe fn as_mut_db(&self) -> Pin<&mut DB> {
        unsafe { Pin::new_unchecked(&mut *self.as_inner().as_db()) }
    }
}

pub struct Snapshot {
    inner: *const autorocks_sys::rocksdb::Snapshot,
    db: TransactionDb,
}

impl Snapshot {
    pub fn get<'b>(
        &self,
        col: usize,
        key: &[u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        self.db
            .get_with_options(ReadOptionsWrapper::as_ref(&options), col, key, buf)
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        let iter = self
            .db
            .iter_with_options(ReadOptionsWrapper::as_ref(&options), col);
        DbIterator {
            inner: iter.inner,
            just_seeked: iter.just_seeked,
            phantom: PhantomData,
        }
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.as_mut_db().ReleaseSnapshot(self.inner);
        }
    }
}

pub struct SnapshotRef<'a> {
    inner: &'a autorocks_sys::rocksdb::Snapshot,
    tx: &'a Transaction,
}

impl<'a> SnapshotRef<'a> {
    pub fn get<'b>(
        &'a self,
        col: usize,
        key: &'a [u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        self.tx
            .get_with_options(ReadOptionsWrapper::as_ref(&options), col, key, buf)
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        let iter = self
            .tx
            .iter_with_options(ReadOptionsWrapper::as_ref(&options), col);
        DbIterator {
            inner: iter.inner,
            just_seeked: iter.just_seeked,
            phantom: PhantomData,
        }
    }
}

pub struct Transaction {
    inner: TransactionWrapper,
    db: TransactionDb,
}

impl Transaction {
    pub fn put(&self, col: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.db.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.inner.put(cf, &key.into(), &value.into()) };
        }
        into_result(&status)
    }

    pub fn delete(&self, col: usize, key: &[u8]) -> Result<()> {
        let cf = self.db.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.inner.del(cf, &key.into()) };
        }
        into_result(&status)
    }

    pub fn get<'b>(
        &self,
        col: usize,
        key: &[u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        moveit! {
            let options = ReadOptions::new();
        }
        self.get_with_options(&options, col, key, buf)
    }

    pub fn get_with_options<'b>(
        &self,
        options: &ReadOptions,
        col: usize,
        key: &[u8],
        buf: Pin<&'b mut PinnableSlice>,
    ) -> Result<Option<&'b [u8]>> {
        let slice = unsafe { buf.get_unchecked_mut() };
        let cf = self.db.as_inner().get_cf(col);
        assert!(!cf.is_null());
        moveit! {
            let status = unsafe { self.as_inner().get(options, cf, &key.into(), slice) };
        }
        if status.IsNotFound() {
            return Ok(None);
        }
        into_result(&status)?;
        Ok(Some(as_rust_slice(slice)))
    }

    /// # Panics
    ///
    /// If there are no snapshot set for this transaction.
    pub fn snapshot(&self) -> SnapshotRef<'_> {
        let snap = self.as_inner().snapshot();
        SnapshotRef {
            inner: unsafe { snap.as_ref() }.unwrap(),
            tx: self,
        }
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let options = ReadOptions::new();
        }
        self.iter_with_options(&options, col)
    }

    pub fn iter_with_options<'a>(
        &'a self,
        options: &ReadOptions,
        col: usize,
    ) -> DbIterator<&'a Self> {
        let cf = self.db.as_inner().get_cf(col);
        assert!(!cf.is_null());
        let mut iter = unsafe { self.as_inner().iter(options, cf) };
        iter.as_mut().unwrap().SeekToFirst();
        DbIterator {
            inner: iter,
            just_seeked: true,
            phantom: PhantomData,
        }
    }

    pub fn commit(&self) -> Result<()> {
        moveit! {
            let status = self.inner.commit();
        }
        into_result(&status)
    }

    pub fn as_inner(&self) -> &TransactionWrapper {
        &self.inner
    }
}

pub struct DbIterator<T> {
    inner: UniquePtr<Iterator>,
    just_seeked: bool,
    phantom: PhantomData<T>,
}

impl<T> core::iter::Iterator for DbIterator<T> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = self.inner.as_mut().unwrap();
        if !self.just_seeked {
            inner.as_mut().Next();
        } else {
            self.just_seeked = false;
        }
        if inner.Valid() {
            let v = (
                as_rust_slice1(&inner.key()).into(),
                as_rust_slice1(&inner.value()).into(),
            );
            Some(v)
        } else {
            None
        }
    }
}

fn as_rust_slice1(s: &Slice) -> &[u8] {
    unsafe { core::slice::from_raw_parts(s.data_ as *const _, s.size_) }
}

fn as_rust_slice(s: &PinnableSlice) -> &[u8] {
    let s = s.as_ref();
    unsafe { core::slice::from_raw_parts(s.data_ as *const _, s.size_) }
}

#[cfg(test)]
mod tests;
