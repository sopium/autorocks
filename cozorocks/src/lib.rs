/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

#![warn(rust_2018_idioms, future_incompatible)]
#![allow(clippy::type_complexity)]

pub use bridge::db::DbBuilder;
pub use bridge::db::RocksDb;
pub use bridge::ffi::RocksDbStatus;
pub use bridge::ffi::SnapshotBridge;
pub use bridge::ffi::StatusCode;
pub use bridge::ffi::StatusSeverity;
pub use bridge::ffi::StatusSubCode;
pub use bridge::iter::DbIter;
pub use bridge::iter::IterBuilder;
pub use bridge::tx::PinSliceRef;
pub use bridge::tx::Tx;
pub use bridge::tx::TxBuilder;

pub(crate) mod bridge;
