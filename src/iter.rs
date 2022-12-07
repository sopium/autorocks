use std::{hint::unreachable_unchecked, marker::PhantomData};

use autocxx::prelude::UniquePtr;
use autorocks_sys::rocksdb::Iterator;

use crate::slice::as_rust_slice1;

pub enum Direction {
    Forward,
    Backward,
}

pub struct DbIterator<T> {
    pub(crate) inner: UniquePtr<Iterator>,
    pub(crate) just_seeked: bool,
    pub(crate) direction: Direction,
    pub(crate) phantom: PhantomData<T>,
}

impl<T> DbIterator<T> {
    pub(crate) fn new(mut inner: UniquePtr<Iterator>, direction: Direction) -> Self {
        match direction {
            Direction::Forward => inner.as_mut().unwrap().SeekToFirst(),
            Direction::Backward => inner.as_mut().unwrap().SeekToLast(),
        }
        Self {
            inner,
            just_seeked: true,
            direction,
            phantom: PhantomData,
        }
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.inner.as_mut().unwrap().Seek(&key.into());
        self.just_seeked = true;
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) {
        self.inner.as_mut().unwrap().SeekForPrev(&key.into());
        self.just_seeked = true;
    }

    pub fn valid(&self) -> bool {
        self.inner.as_ref().unwrap().Valid()
    }

    pub fn key(&self) -> Option<&[u8]> {
        if self.valid() {
            Some(unsafe { as_rust_slice1(self.inner.as_ref().unwrap().key()) })
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.valid() {
            Some(unsafe { as_rust_slice1(self.inner.as_ref().unwrap().value()) })
        } else {
            None
        }
    }
}

impl<T> core::iter::Iterator for DbIterator<T> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = self.inner.as_mut().unwrap();
        if !self.just_seeked {
            match self.direction {
                Direction::Backward => inner.as_mut().Prev(),
                Direction::Forward => inner.as_mut().Next(),
            }
        } else {
            self.just_seeked = false;
        }
        if inner.Valid() {
            let v = (
                unsafe { unwrap_unchecked(self.key()) }.into(),
                unsafe { unwrap_unchecked(self.value()) }.into(),
            );
            Some(v)
        } else {
            None
        }
    }
}

unsafe fn unwrap_unchecked<T>(x: Option<T>) -> T {
    match x {
        Some(x) => x,
        None => unreachable_unchecked(),
    }
}
