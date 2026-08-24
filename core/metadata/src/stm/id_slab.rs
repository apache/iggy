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

//! Key-stable arena with deterministic key allocation. Replaces `slab::Slab`
//! for the catalogs whose keys are committed state.
//!
//! `Slab` recycles keys LIFO from a free list no snapshot carries and it
//! exposes no way to restore, rebuilding it by ascending scan. The next key
//! therefore depended on local delete order, so a replica restored from a
//! snapshot assigned a different key than a live one for the same log entry.
//! Stream and topic keys pack into the consensus group id, making that a
//! different shard owner, consensus group, directory and authorization scope:
//! a silent permanent fork.
//!
//! [`IdSlab::vacant_key`] is the lowest unoccupied key, a pure function of the
//! occupied set, which is exactly what a snapshot carries. Restored and live
//! arenas agree by construction.

use std::collections::BTreeSet;
use std::ops::{Index, IndexMut};

/// Arena addressed by stable `usize` keys, allocating the lowest free key.
#[derive(Debug, Clone)]
pub struct IdSlab<T> {
    /// Dense by key; `None` is an unoccupied slot below the high-water mark.
    entries: Vec<Option<T>>,
    len: usize,
    /// Unoccupied keys below `entries.len()`, ascending. A memo so allocation
    /// stays O(log n) instead of scanning. Always derived from `entries`, never
    /// serialized: that is what keeps a restored arena identical to a live one.
    free: BTreeSet<usize>,
}

impl<T> IdSlab<T> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            len: 0,
            free: BTreeSet::new(),
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Key the next [`Self::insert`] will use: the lowest unoccupied one.
    /// Depends only on the occupied set, so it survives a snapshot round trip.
    /// Callers stamp it into committed state before inserting, which is only
    /// sound because of that.
    #[must_use]
    pub fn vacant_key(&self) -> usize {
        self.free.first().copied().unwrap_or(self.entries.len())
    }

    /// Inserts at [`Self::vacant_key`] and returns that key.
    pub fn insert(&mut self, value: T) -> usize {
        self.len += 1;
        if let Some(key) = self.free.pop_first() {
            self.entries[key] = Some(value);
            return key;
        }
        self.entries.push(Some(value));
        self.entries.len() - 1
    }

    #[must_use]
    pub fn contains(&self, key: usize) -> bool {
        self.get(key).is_some()
    }

    #[must_use]
    pub fn get(&self, key: usize) -> Option<&T> {
        self.entries.get(key)?.as_ref()
    }

    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        self.entries.get_mut(key)?.as_mut()
    }

    /// Removes `key`, returning its value, or `None` if it was unoccupied.
    ///
    /// A trailing removal is not truncated: the slot stays in `free` for a later
    /// insert. Truncating would make the high-water mark depend on removal
    /// order, the divergence this type exists to remove.
    pub fn try_remove(&mut self, key: usize) -> Option<T> {
        let value = self.entries.get_mut(key)?.take()?;
        self.len -= 1;
        self.free.insert(key);
        Some(value)
    }

    /// # Panics
    /// If `key` is unoccupied. Mirrors `Slab::remove`.
    pub fn remove(&mut self, key: usize) -> T {
        self.try_remove(key)
            .unwrap_or_else(|| panic!("no entry at key {key}"))
    }

    /// Ascending by key.
    #[must_use]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self.entries.iter().enumerate(),
        }
    }

    /// Ascending by key.
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            inner: self.entries.iter_mut().enumerate(),
        }
    }
}

/// Named rather than `impl Iterator` so `IntoIterator` can name it without
/// boxing. These run on per-request metadata reads.
pub struct Iter<'a, T> {
    inner: std::iter::Enumerate<std::slice::Iter<'a, Option<T>>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (usize, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, slot) = self.inner.next()?;
            if let Some(value) = slot {
                return Some((key, value));
            }
        }
    }
}

pub struct IterMut<'a, T> {
    inner: std::iter::Enumerate<std::slice::IterMut<'a, Option<T>>>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = (usize, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, slot) = self.inner.next()?;
            if let Some(value) = slot {
                return Some((key, value));
            }
        }
    }
}

pub struct IntoIter<T> {
    inner: std::iter::Enumerate<std::vec::IntoIter<Option<T>>>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = (usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, slot) = self.inner.next()?;
            if let Some(value) = slot {
                return Some((key, value));
            }
        }
    }
}

impl<T> Default for IdSlab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Index<usize> for IdSlab<T> {
    type Output = T;

    fn index(&self, key: usize) -> &Self::Output {
        self.get(key)
            .unwrap_or_else(|| panic!("no entry at key {key}"))
    }
}

impl<T> IndexMut<usize> for IdSlab<T> {
    fn index_mut(&mut self, key: usize) -> &mut Self::Output {
        self.get_mut(key)
            .unwrap_or_else(|| panic!("no entry at key {key}"))
    }
}

/// Rebuilds from `(key, value)` pairs, the shape every snapshot stores.
///
/// The high-water mark becomes one past the largest key, and every gap below it
/// becomes free. Both derive from the pairs alone, so the same pairs always
/// yield the same [`IdSlab::vacant_key`]. Duplicate keys keep the last value.
impl<T> FromIterator<(usize, T)> for IdSlab<T> {
    fn from_iter<I: IntoIterator<Item = (usize, T)>>(iter: I) -> Self {
        let mut slab = Self::new();
        for (key, value) in iter {
            if key >= slab.entries.len() {
                slab.entries.resize_with(key + 1, || None);
            }
            if slab.entries[key].replace(value).is_none() {
                slab.len += 1;
            }
        }
        slab.free = (0..slab.entries.len())
            .filter(|&key| slab.entries[key].is_none())
            .collect();
        slab
    }
}

impl<'a, T> IntoIterator for &'a IdSlab<T> {
    type Item = (usize, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut IdSlab<T> {
    type Item = (usize, &'a mut T);
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> IntoIterator for IdSlab<T> {
    type Item = (usize, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.entries.into_iter().enumerate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdSlab;

    /// The property the type exists for. `Slab` fails it: LIFO reuse makes the
    /// head depend on delete order while a rebuild scans ascending, so
    /// descending deletes diverge.
    #[test]
    fn given_any_delete_order_when_rebuilding_should_keep_vacant_key() {
        for order in [[1, 0], [0, 1]] {
            let mut slab: IdSlab<char> = IdSlab::new();
            for value in ['a', 'b', 'c'] {
                slab.insert(value);
            }
            for key in order {
                slab.remove(key);
            }
            let before = slab.vacant_key();

            let rebuilt: IdSlab<char> = slab
                .iter()
                .map(|(key, value)| (key, *value))
                .collect::<Vec<_>>()
                .into_iter()
                .collect();

            assert_eq!(
                rebuilt.vacant_key(),
                before,
                "delete order {order:?} must not survive a rebuild"
            );
        }
    }

    #[test]
    fn given_a_hole_when_inserting_should_take_the_lowest_key() {
        let mut slab: IdSlab<char> = IdSlab::new();
        for value in ['a', 'b', 'c'] {
            slab.insert(value);
        }
        slab.remove(2);
        slab.remove(0);

        assert_eq!(slab.vacant_key(), 0);
        assert_eq!(slab.insert('d'), 0);
        assert_eq!(slab.insert('e'), 2);
        assert_eq!(slab.insert('f'), 3, "then extends past the high-water mark");
        assert_eq!(slab.len(), 4);
    }

    /// A trailing removal must not shrink the high-water mark, or it would
    /// again depend on removal order.
    #[test]
    fn given_a_trailing_removal_when_inserting_should_refill_the_slot() {
        let mut slab: IdSlab<char> = IdSlab::new();
        for value in ['a', 'b'] {
            slab.insert(value);
        }
        slab.remove(1);

        assert_eq!(slab.vacant_key(), 1);
        assert_eq!(slab.insert('c'), 1);
    }

    #[test]
    fn given_removed_keys_when_reading_should_report_absent() {
        let mut slab: IdSlab<char> = IdSlab::new();
        let key = slab.insert('a');
        assert_eq!(slab.get(key), Some(&'a'));

        assert_eq!(slab.remove(key), 'a');
        assert!(!slab.contains(key));
        assert_eq!(slab.get(key), None);
        assert_eq!(slab.try_remove(key), None);
        assert!(slab.is_empty());
    }

    #[test]
    fn given_holes_when_iterating_should_yield_ascending_occupied_keys() {
        let mut slab: IdSlab<char> = IdSlab::new();
        for value in ['a', 'b', 'c', 'd'] {
            slab.insert(value);
        }
        slab.remove(2);
        slab.remove(0);

        let seen: Vec<(usize, char)> = slab.iter().map(|(key, value)| (key, *value)).collect();
        assert_eq!(seen, vec![(1, 'b'), (3, 'd')]);
    }
}
