// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io;
use std::ops::{Deref, RangeInclusive};
use std::rc::Rc;

pub mod file_storage;
pub mod prepare_journal;
pub mod superblock;

pub trait Journal<S>
where
    S: Storage,
{
    type Header;
    type Entry;
    type HeaderRef<'a>: Deref<Target = Self::Header>
    where
        Self: 'a;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>>;
    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>>;

    fn append(&self, entry: Self::Entry) -> impl Future<Output = io::Result<()>>;
    fn entry(&self, header: &Self::Header) -> impl Future<Output = Option<Self::Entry>>;

    /// Number of entries that can be appended before the journal would need
    /// to evict un-snapshotted slots. Returns `None` for journals that don't persist to disk.
    fn remaining_capacity(&self) -> Option<usize> {
        None
    }

    /// Remove entries with ops in `ops` from the journal,
    /// returning the removed entries sorted by op.
    ///
    /// Implementations that persist to disk should rewrite the underlying
    /// storage to reclaim space. The default is a no-op for journals that
    /// do not persist to disk.
    ///
    /// # Errors
    /// Returns an I/O error if the drain fails.
    fn drain(
        &self,
        _ops: RangeInclusive<u64>,
    ) -> impl Future<Output = io::Result<Vec<Self::Entry>>> {
        async { Ok(Vec::new()) }
    }

    /// Snapshot watermark: entries at or below it are evictable. `0` for
    /// journals without snapshot bookkeeping.
    fn snapshot_op(&self) -> u64 {
        0
    }

    /// Advance the snapshot watermark (see [`Self::snapshot_op`]). No-op for
    /// journals without snapshot bookkeeping. State transfer uses this to
    /// mark pre-transfer residents superseded by the installed snapshot.
    fn set_snapshot_op(&self, _op: u64) {}
}

// TODO: Move to other crate.
pub trait Storage {
    type Buffer;

    fn write_at(&self, offset: usize, buf: Self::Buffer)
    -> impl Future<Output = io::Result<usize>>;

    fn read_at(
        &self,
        offset: usize,
        buffer: Self::Buffer,
    ) -> impl Future<Output = io::Result<Self::Buffer>>;
}

pub trait JournalHandle {
    type Storage: Storage;
    type Target: Journal<Self::Storage>;

    fn handle(&self) -> &Self::Target;
}

/// Forwarding impl so a journal held behind an `Rc` still drives the shard
/// through [`JournalHandle`]. The deterministic simulator needs this to retain
/// the metadata WAL across a replica restart: the bytes and index survive the
/// shard being dropped and rebuilt.
impl<T: JournalHandle> JournalHandle for Rc<T> {
    type Storage = T::Storage;
    type Target = T::Target;

    fn handle(&self) -> &Self::Target {
        (**self).handle()
    }
}
