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

//! Concurrent segmented slab with structural sharing for RCU patterns.
//!
//! # Design Goals
//!
//! - O(1) access time
//! - Structural sharing via Arc-wrapped segments (cheap clones)
//! - Lock-free reads with copy-on-write modifications
//! - Slab-assigned keys with slot reuse via free list
//!
//! # Architecture
//!
//! Data is divided into fixed-size segments, each backed by a `Slab<T>`.
//! Keys are encoded as: `global_key = (segment_idx << SEGMENT_BITS) | local_key`
//!
//! Slab handles ID assignment internally:
//! - `insert(value)` returns the assigned key
//! - Removed slots are reused via Slab's free list
//! - No external ID generation needed
//!
//! # Structural Sharing
//!
//! Each segment is wrapped in `Arc`. On modification:
//! - `Arc::make_mut` clones only if segment is shared
//! - Unmodified segments remain shared across snapshots
//! - Clone cost is O(num_segments), not O(num_entries)

use slab::Slab;
use std::sync::Arc;

/// Concurrent segmented slab with structural sharing.
///
/// # Performance Characteristics
///
/// | Operation | Time Complexity | Notes |
/// |-----------|-----------------|-------|
/// | get       | O(1)            | Direct segment + slab indexing |
/// | insert    | O(1) amortized  | May clone segment if shared |
/// | remove    | O(1) amortized  | May clone segment if shared |
/// | clone     | O(num_segments) | Just Arc clones, not data copies |
///
/// # Key Encoding
///
/// Keys are `usize` where:
/// - High bits `(key >> SEGMENT_BITS)` = segment index
/// - Low bits `(key & SEGMENT_MASK)` = local slab key
///
/// # Type Parameter
///
/// `SEGMENT_CAPACITY` must be a power of 2. This is enforced at compile time.
#[derive(Clone)]
pub struct SegmentedSlab<T, const SEGMENT_CAPACITY: usize> {
    segments: Vec<Arc<Slab<T>>>,
    len: usize,
}

impl<T: std::fmt::Debug, const SEGMENT_CAPACITY: usize> std::fmt::Debug
    for SegmentedSlab<T, SEGMENT_CAPACITY>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedSlab")
            .field("len", &self.len)
            .field("segments", &self.segments.len())
            .field("segment_capacity", &SEGMENT_CAPACITY)
            .finish()
    }
}

impl<T, const SEGMENT_CAPACITY: usize> Default for SegmentedSlab<T, SEGMENT_CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const SEGMENT_CAPACITY: usize> SegmentedSlab<T, SEGMENT_CAPACITY> {
    const SEGMENT_BITS: usize = {
        assert!(SEGMENT_CAPACITY > 0, "SEGMENT_CAPACITY must be positive");
        assert!(
            SEGMENT_CAPACITY.is_power_of_two(),
            "SEGMENT_CAPACITY must be a power of 2"
        );
        SEGMENT_CAPACITY.trailing_zeros() as usize
    };
    const SEGMENT_MASK: usize = SEGMENT_CAPACITY - 1;

    pub fn new() -> Self {
        // Force evaluation to trigger compile-time assertions
        let _ = Self::SEGMENT_BITS;
        Self {
            segments: Vec::new(),
            len: 0,
        }
    }

    /// Create from key-value pairs (for bootstrap/recovery).
    ///
    /// Keys determine segment placement. Uses slab's `FromIterator<(usize, T)>`
    /// to place entries at specific keys within each segment.
    ///
    /// # Panics
    ///
    /// Panics if duplicate keys are provided.
    pub fn from_entries(entries: impl IntoIterator<Item = (usize, T)>) -> Self {
        use std::collections::HashSet;

        let mut segment_entries: Vec<Vec<(usize, T)>> = Vec::new();
        let mut seen_keys: HashSet<usize> = HashSet::new();
        let mut len = 0;

        for (key, value) in entries {
            assert!(seen_keys.insert(key), "duplicate key {key} in from_entries");

            let (seg_idx, local_key) = Self::decode_key(key);

            while segment_entries.len() <= seg_idx {
                segment_entries.push(Vec::new());
            }

            segment_entries[seg_idx].push((local_key, value));
            len += 1;
        }

        let segments = segment_entries
            .into_iter()
            .map(|entries| Arc::new(entries.into_iter().collect::<Slab<T>>()))
            .collect();

        Self { segments, len }
    }

    /// Decodes global key into (segment_index, local_key).
    #[inline]
    const fn decode_key(key: usize) -> (usize, usize) {
        (key >> Self::SEGMENT_BITS, key & Self::SEGMENT_MASK)
    }

    /// Encodes segment and local indices into global key.
    #[inline]
    const fn encode_key(segment_idx: usize, local_key: usize) -> usize {
        (segment_idx << Self::SEGMENT_BITS) | local_key
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns reference to value at key, or None if not present.
    #[inline]
    pub fn get(&self, key: usize) -> Option<&T> {
        let (seg_idx, local_key) = Self::decode_key(key);
        self.segments.get(seg_idx)?.get(local_key)
    }

    /// Returns true if key is present.
    #[inline]
    pub fn contains_key(&self, key: usize) -> bool {
        let (seg_idx, local_key) = Self::decode_key(key);
        self.segments
            .get(seg_idx)
            .is_some_and(|slab| slab.contains(local_key))
    }

    /// Returns iterator over (key, &value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &T)> + '_ {
        self.segments
            .iter()
            .enumerate()
            .flat_map(|(seg_idx, slab)| {
                slab.iter()
                    .map(move |(local_key, value)| (Self::encode_key(seg_idx, local_key), value))
            })
    }

    /// Returns iterator over all keys.
    pub fn keys(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter().map(|(k, _)| k)
    }

    /// Returns iterator over all values.
    pub fn values(&self) -> impl Iterator<Item = &T> + '_ {
        self.iter().map(|(_, v)| v)
    }
}

impl<T: Clone, const SEGMENT_CAPACITY: usize> SegmentedSlab<T, SEGMENT_CAPACITY> {
    /// Insert value, returning (new_slab, assigned_key).
    ///
    /// Slab assigns the key, reusing freed slots when available.
    /// Only the affected segment is cloned if shared.
    pub fn insert(mut self, value: T) -> (Self, usize) {
        let seg_idx = self
            .segments
            .iter()
            .position(|slab| slab.vacant_key() < SEGMENT_CAPACITY)
            .unwrap_or(self.segments.len());

        if seg_idx >= self.segments.len() {
            self.segments.push(Arc::new(Slab::new()));
        }

        let slab = Arc::make_mut(&mut self.segments[seg_idx]);
        let local_key = slab.insert(value);
        let global_key = Self::encode_key(seg_idx, local_key);

        self.len += 1;
        (self, global_key)
    }

    /// Update value at existing key, returning (new_slab, success).
    ///
    /// Returns (self, false) if key doesn't exist (no-op).
    pub fn update(mut self, key: usize, value: T) -> (Self, bool) {
        let (seg_idx, local_key) = Self::decode_key(key);

        let Some(slab_arc) = self.segments.get_mut(seg_idx) else {
            return (self, false);
        };

        if !slab_arc.contains(local_key) {
            return (self, false);
        }

        let slab = Arc::make_mut(slab_arc);
        slab[local_key] = value;
        (self, true)
    }

    /// Remove entry at key, returning (new_slab, removed_value).
    ///
    /// Freed slot will be reused by future inserts.
    pub fn remove(mut self, key: usize) -> (Self, Option<T>) {
        let (seg_idx, local_key) = Self::decode_key(key);

        let Some(slab_arc) = self.segments.get_mut(seg_idx) else {
            return (self, None);
        };

        if !slab_arc.contains(local_key) {
            return (self, None);
        }

        let slab = Arc::make_mut(slab_arc);
        let value = slab.remove(local_key);
        self.len -= 1;

        (self, Some(value))
    }

    /// Remove entry at key, returning new slab. Ignores removed value.
    #[inline]
    pub fn without(self, key: usize) -> Self {
        self.remove(key).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CAPACITY: usize = 1024;
    type TestSlab<T> = SegmentedSlab<T, TEST_CAPACITY>;

    #[test]
    fn test_key_encoding() {
        assert_eq!(TestSlab::<()>::decode_key(0), (0, 0));
        assert_eq!(TestSlab::<()>::decode_key(42), (0, 42));
        assert_eq!(TestSlab::<()>::decode_key(1024), (1, 0));
        assert_eq!(TestSlab::<()>::decode_key(1025), (1, 1));
        assert_eq!(TestSlab::<()>::decode_key(2048), (2, 0));

        assert_eq!(TestSlab::<()>::encode_key(0, 0), 0);
        assert_eq!(TestSlab::<()>::encode_key(0, 42), 42);
        assert_eq!(TestSlab::<()>::encode_key(1, 0), 1024);
        assert_eq!(TestSlab::<()>::encode_key(1, 1), 1025);
    }

    #[test]
    fn test_insert_assigns_sequential_keys() {
        let slab = TestSlab::new();

        let (slab, key0) = slab.insert("a");
        let (slab, key1) = slab.insert("b");
        let (slab, key2) = slab.insert("c");

        assert_eq!(key0, 0);
        assert_eq!(key1, 1);
        assert_eq!(key2, 2);
        assert_eq!(slab.len(), 3);
    }

    #[test]
    fn test_get() {
        let slab = TestSlab::new();
        let (slab, key) = slab.insert("hello");

        assert_eq!(slab.get(key), Some(&"hello"));
        assert_eq!(slab.get(999), None);
    }

    #[test]
    fn test_update() {
        let slab = TestSlab::new();
        let (slab, key) = slab.insert("original");

        let (slab, success) = slab.update(key, "updated");
        assert!(success);
        assert_eq!(slab.get(key), Some(&"updated"));
        assert_eq!(slab.len(), 1);

        // Update non-existent key returns false
        let (_, success) = slab.update(999, "nope");
        assert!(!success);
    }

    #[test]
    fn test_remove_and_reuse() {
        let slab = TestSlab::new();
        let (slab, key0) = slab.insert("a");
        let (slab, key1) = slab.insert("b");
        let (slab, key2) = slab.insert("c");

        assert_eq!(key0, 0);
        assert_eq!(key1, 1);
        assert_eq!(key2, 2);

        // Remove middle entry
        let (slab, removed) = slab.remove(key1);
        assert_eq!(removed, Some("b"));
        assert_eq!(slab.len(), 2);
        assert_eq!(slab.get(key1), None);

        // New insert reuses freed slot
        let (slab, key3) = slab.insert("d");
        assert_eq!(key3, 1); // Reused!
        assert_eq!(slab.get(key3), Some(&"d"));
        assert_eq!(slab.len(), 3);
    }

    #[test]
    fn test_structural_sharing() {
        let slab = TestSlab::new();
        let (slab, _) = slab.insert("value");

        // Clone shares segment via Arc
        let snapshot = slab.clone();

        // Modify original
        let (slab, key1) = slab.insert("new");

        // Snapshot still sees original state
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.get(key1), None);

        // Modified slab has both
        assert_eq!(slab.len(), 2);
    }

    #[test]
    fn test_arc_make_mut_no_clone_when_unique() {
        let slab = TestSlab::new();
        let (slab, _) = slab.insert("a");

        let ptr_before = Arc::as_ptr(&slab.segments[0]);
        let (slab, _) = slab.insert("b");
        let ptr_after = Arc::as_ptr(&slab.segments[0]);

        // Same pointer - no clone occurred
        assert_eq!(ptr_before, ptr_after);
    }

    #[test]
    fn test_arc_make_mut_clones_when_shared() {
        let slab = TestSlab::new();
        let (slab, _) = slab.insert("a");

        let _snapshot = slab.clone(); // Creates shared reference
        let ptr_before = Arc::as_ptr(&slab.segments[0]);

        let (slab, _) = slab.insert("b");
        let ptr_after = Arc::as_ptr(&slab.segments[0]);

        // Different pointer - clone occurred
        assert_ne!(ptr_before, ptr_after);
    }

    #[test]
    fn test_cross_segment() {
        let mut slab = TestSlab::new();

        // Fill first segment
        for i in 0..TEST_CAPACITY {
            let (new_slab, key) = slab.insert(i);
            slab = new_slab;
            assert_eq!(key, i);
        }

        // Next insert should go to second segment
        let (slab, key) = slab.insert(TEST_CAPACITY);
        assert_eq!(key, TEST_CAPACITY);
        assert_eq!(slab.segments.len(), 2);
    }

    #[test]
    fn test_from_entries() {
        let entries = vec![(0, "zero"), (5, "five"), (1024, "segment2")];

        let slab = TestSlab::from_entries(entries);

        assert_eq!(slab.len(), 3);
        assert_eq!(slab.get(0), Some(&"zero"));
        assert_eq!(slab.get(5), Some(&"five"));
        assert_eq!(slab.get(1024), Some(&"segment2"));
        assert_eq!(slab.segments.len(), 2);
    }

    #[test]
    fn test_iter() {
        let slab = TestSlab::new();
        let (slab, _) = slab.insert("a");
        let (slab, _) = slab.insert("b");
        let (slab, _) = slab.insert("c");

        let items: Vec<_> = slab.iter().collect();
        assert_eq!(items, vec![(0, &"a"), (1, &"b"), (2, &"c")]);
    }

    #[test]
    fn test_keys_and_values() {
        let slab = TestSlab::new();
        let (slab, _) = slab.insert(10);
        let (slab, _) = slab.insert(20);

        let keys: Vec<_> = slab.keys().collect();
        assert_eq!(keys, vec![0, 1]);

        let values: Vec<_> = slab.values().collect();
        assert_eq!(values, vec![&10, &20]);
    }

    #[test]
    fn test_contains_key() {
        let slab = TestSlab::new();
        let (slab, key) = slab.insert("x");

        assert!(slab.contains_key(key));
        assert!(!slab.contains_key(999));
        assert!(!slab.contains_key(2048)); // Non-existent segment
    }

    #[test]
    fn test_without() {
        let slab = TestSlab::new();
        let (slab, key) = slab.insert("value");

        // without() removes and ignores result
        let slab = slab.without(key);
        assert_eq!(slab.get(key), None);
        assert_eq!(slab.len(), 0);

        // without() on non-existent key is no-op
        let slab = slab.without(999);
        assert_eq!(slab.len(), 0);
    }

    #[test]
    fn test_empty_slab() {
        let slab: TestSlab<i32> = TestSlab::new();

        assert!(slab.is_empty());
        assert_eq!(slab.len(), 0);
        assert_eq!(slab.get(0), None);
        assert!(!slab.contains_key(0));
        assert_eq!(slab.iter().count(), 0);
    }

    #[test]
    #[should_panic(expected = "duplicate key 0 in from_entries")]
    fn test_from_entries_panics_on_duplicate_keys() {
        let entries = vec![(0, "first"), (0, "second")];
        let _ = TestSlab::from_entries(entries);
    }
}
