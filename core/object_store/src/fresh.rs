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

//! The fresh object: the messages of batches from many partitions, stored
//! before any of those batches is committed.
//!
//! ```text
//! [magic: 8 bytes][blob][blob]...
//! ```
//!
//! A blob is the message frames of one batch. The object does not say where
//! a blob starts or whose it is: the partition that commits the batch keeps
//! its position and length, and reads exactly that range. The magic names
//! the version, so an object that describes itself can follow without a
//! reader taking one for the other.

use bytes::{Bytes, BytesMut};

const MAGIC: &[u8; 8] = b"IGGYFRS1";

/// The id of a fresh object, which its key carries.
///
/// No two objects of a cluster share one: the id names the replica and the
/// shard that wrote the object, when that shard started, and the object's
/// number since. A shard that restarts takes a later start, so it never
/// writes under an id of its earlier life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FreshObjectId(u128);

impl FreshObjectId {
    /// Objects one shard can number between two starts.
    pub const SEQUENCE_MAX: u64 = (1 << 40) - 1;

    /// The id of object `sequence` of `shard` on `replica`, which started
    /// `started_at` microseconds after the Unix epoch. `None` for a sequence
    /// above [`Self::SEQUENCE_MAX`].
    #[must_use]
    pub const fn new(replica: u8, shard: u16, started_at: u64, sequence: u64) -> Option<Self> {
        if sequence > Self::SEQUENCE_MAX {
            return None;
        }
        Some(Self(
            ((started_at as u128) << 64)
                | ((replica as u128) << 56)
                | ((shard as u128) << 40)
                | sequence as u128,
        ))
    }

    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Collects blobs into one fresh object.
#[derive(Debug)]
pub struct FreshObject {
    bytes: BytesMut,
}

impl Default for FreshObject {
    fn default() -> Self {
        Self::new()
    }
}

impl FreshObject {
    #[must_use]
    pub fn new() -> Self {
        let mut bytes = BytesMut::with_capacity(MAGIC.len());
        bytes.extend_from_slice(MAGIC);
        Self { bytes }
    }

    /// Add `blob`, and return where it starts in the object.
    pub fn push(&mut self, blob: &[u8]) -> u64 {
        let position = self.bytes.len() as u64;
        self.bytes.extend_from_slice(blob);
        position
    }

    /// Bytes the object holds so far, its magic included.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Whether the object holds no blob.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.len() == MAGIC.len()
    }

    /// The object's bytes, as they go to the store.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes.freeze()
    }
}

/// Whether `bytes`, the start of an object, are those of a fresh object this
/// build reads ranges of.
#[must_use]
pub fn is_fresh_object(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_blobs_when_pushing_should_return_where_each_one_starts() {
        let mut object = FreshObject::new();
        assert!(object.is_empty());

        let first = object.push(b"abc");
        let second = object.push(b"defgh");

        assert!(!object.is_empty());
        assert_eq!(object.len(), 16);
        let bytes = object.into_bytes();
        assert!(is_fresh_object(&bytes));
        let (first, second) = (
            usize::try_from(first).unwrap(),
            usize::try_from(second).unwrap(),
        );
        assert_eq!(&bytes[first..first + 3], b"abc");
        assert_eq!(&bytes[second..second + 5], b"defgh");
    }

    #[test]
    fn given_another_object_when_asking_should_not_take_it_for_a_fresh_object() {
        assert!(!is_fresh_object(b"IGGYFRS2 and more"));
        assert!(!is_fresh_object(b"IGGY"));
    }

    #[test]
    fn given_writers_and_sequences_when_building_ids_should_give_each_its_own() {
        let ids = [
            FreshObjectId::new(0, 0, 1_000, 0),
            FreshObjectId::new(0, 0, 1_000, 1),
            FreshObjectId::new(0, 1, 1_000, 0),
            FreshObjectId::new(1, 0, 1_000, 0),
            FreshObjectId::new(0, 0, 1_001, 0),
            FreshObjectId::new(u8::MAX, u16::MAX, u64::MAX, FreshObjectId::SEQUENCE_MAX),
        ]
        .map(Option::unwrap);

        let mut distinct = ids.to_vec();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), ids.len());
        assert_eq!(ids[5].get(), u128::MAX);
    }

    #[test]
    fn given_a_sequence_past_the_most_when_building_an_id_should_give_none() {
        assert_eq!(
            FreshObjectId::new(0, 0, 0, FreshObjectId::SEQUENCE_MAX + 1),
            None
        );
    }
}
