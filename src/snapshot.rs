use std::{marker::PhantomData, pin::Pin};

use autorocks_sys::{rocksdb::PinnableSlice, ReadOptionsWrapper};
use moveit::moveit;

use crate::{DbIterator, Result, Transaction, TransactionDb};

pub struct Snapshot {
    pub(crate) inner: *const autorocks_sys::rocksdb::Snapshot,
    pub(crate) db: TransactionDb,
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
        self.db.get_with_options((*options).as_ref(), col, key, buf)
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        let iter = self.db.iter_with_options((*options).as_ref(), col);
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
    pub(crate) inner: &'a autorocks_sys::rocksdb::Snapshot,
    pub(crate) tx: &'a Transaction,
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
        self.tx.get_with_options((*options).as_ref(), col, key, buf)
    }

    pub fn iter(&self, col: usize) -> DbIterator<&'_ Self> {
        moveit! {
            let mut options = ReadOptionsWrapper::new();
        }
        unsafe {
            options.as_mut().set_snapshot(self.inner);
        }
        let iter = self.tx.iter_with_options((*options).as_ref(), col);
        DbIterator {
            inner: iter.inner,
            just_seeked: iter.just_seeked,
            phantom: PhantomData,
        }
    }
}
