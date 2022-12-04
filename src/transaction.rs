use std::{marker::PhantomData, pin::Pin};

use autorocks_sys::{
    rocksdb::{PinnableSlice, ReadOptions},
    TransactionWrapper,
};
use moveit::moveit;

use crate::{into_result, slice::as_rust_slice, DbIterator, Result, SnapshotRef, TransactionDb};

pub struct Transaction {
    pub(crate) inner: TransactionWrapper,
    pub(crate) db: TransactionDb,
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
