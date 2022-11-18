/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::pin::Pin;

use cxx::*;

use crate::bridge::ffi::*;
use crate::bridge::iter::IterBuilder;

pub struct TxBuilder {
    pub(crate) inner: UniquePtr<TxBridge>,
}

pub struct PinSliceRef<'a> {
    pub(crate) inner: Pin<&'a mut PinnableSlice>,
}

impl Deref for PinSliceRef<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        convert_pinnable_slice_back(&self.inner)
    }
}

impl Debug for PinSliceRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let to_d: &[u8] = self;
        write!(f, "{:?}", to_d)
    }
}

impl Drop for PinSliceRef<'_> {
    fn drop(&mut self) {
        self.inner.as_mut().Reset();
    }
}

impl TxBuilder {
    #[inline]
    pub fn start(mut self) -> Tx {
        self.inner.pin_mut().start();
        Tx { inner: self.inner }
    }
    #[inline]
    pub fn set_snapshot(mut self, val: bool) -> Self {
        self.inner.pin_mut().set_snapshot(val);
        self
    }
    #[inline]
    pub fn sync(mut self, val: bool) -> Self {
        set_w_opts_sync(self.inner.pin_mut().get_w_opts(), val);
        self
    }

    #[inline]
    pub fn no_slowdown(mut self, val: bool) -> Self {
        set_w_opts_no_slowdown(self.inner.pin_mut().get_w_opts(), val);
        self
    }

    #[inline]
    pub fn disable_wal(mut self, val: bool) -> Self {
        set_w_opts_disable_wal(self.inner.pin_mut().get_w_opts(), val);
        self
    }

    #[inline]
    pub fn verify_checksums(mut self, val: bool) -> Self {
        self.inner.pin_mut().verify_checksums(val);
        self
    }

    #[inline]
    pub fn fill_cache(mut self, val: bool) -> Self {
        self.inner.pin_mut().fill_cache(val);
        self
    }
}

pub struct Tx {
    pub(crate) inner: UniquePtr<TxBridge>,
}

impl Tx {
    #[inline]
    pub fn set_snapshot(&mut self) {
        self.inner.pin_mut().set_snapshot(true)
    }
    #[inline]
    pub fn clear_snapshot(&mut self) {
        self.inner.pin_mut().clear_snapshot()
    }
    #[inline]
    pub fn put(&mut self, cf: usize, key: &[u8], val: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().put(cf, key, val, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn del(&mut self, cf: usize, key: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().del(cf, key, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn get<'a, 'b>(
        &'a mut self,
        cf: usize,
        key: &'b [u8],
        for_update: bool,
        use_snapshot: bool,
    ) -> Result<Option<PinSliceRef<'a>>, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        let ret = self
            .inner
            .pin_mut()
            .get(cf, key, for_update, use_snapshot, &mut status);
        match status.code {
            StatusCode::kOk => Ok(Some(PinSliceRef { inner: ret })),
            StatusCode::kNotFound => Ok(None),
            _ => Err(status),
        }
    }
    #[inline]
    pub fn commit(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().commit(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn rollback(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().rollback(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn rollback_to_save(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().rollback_to_savepoint(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn save(&mut self) {
        self.inner.pin_mut().set_savepoint();
    }
    #[inline]
    pub fn pop_save(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().pop_savepoint(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn iterator(&self, use_snapshot: bool) -> IterBuilder {
        IterBuilder {
            inner: self.inner.iterator(use_snapshot),
        }
        .auto_prefix_mode(true)
    }
}
