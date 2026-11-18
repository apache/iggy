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

//! Ordered walks behind the list pages.
//!
//! A walk yields the entries after a position in one direction: the larger
//! keys, or the smaller keys when it is descending. The entry at the position
//! itself need not exist any more, so a client can continue after a deleted
//! entry.

use std::ops::Bound;

/// The items of a double-ended iterator, from its front or, when descending,
/// from its back. Both directions of a walk then share one iterator type.
#[derive(Debug, Clone)]
pub struct Directed<I> {
    inner: I,
    descending: bool,
}

impl<I> Directed<I> {
    pub(crate) const fn new(inner: I, descending: bool) -> Self {
        Self { inner, descending }
    }
}

impl<I: DoubleEndedIterator> Iterator for Directed<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.descending {
            self.inner.next_back()
        } else {
            self.inner.next()
        }
    }
}

/// The range of an ordered map or set that holds the keys after `after`.
#[must_use]
pub(crate) const fn range_after<K>(after: Option<&K>, descending: bool) -> (Bound<&K>, Bound<&K>) {
    match (after, descending) {
        (None, _) => (Bound::Unbounded, Bound::Unbounded),
        (Some(key), false) => (Bound::Excluded(key), Bound::Unbounded),
        (Some(key), true) => (Bound::Unbounded, Bound::Excluded(key)),
    }
}

/// The items of `sorted` after `after`, where `key` ascends over `sorted`.
pub fn slice_after<T, K: Ord>(
    sorted: &[T],
    key: impl Fn(&T) -> K,
    after: Option<K>,
    descending: bool,
) -> Directed<std::slice::Iter<'_, T>> {
    let range = match after {
        None => sorted,
        Some(after) if descending => &sorted[..sorted.partition_point(|item| key(item) < after)],
        Some(after) => &sorted[sorted.partition_point(|item| key(item) <= after)..],
    };
    Directed::new(range.iter(), descending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn given_a_position_when_walking_a_set_should_yield_the_keys_past_it_in_its_direction() {
        let set: BTreeSet<u64> = [1, 3, 5, 7].into_iter().collect();
        let walk = |after: Option<u64>, descending| {
            Directed::new(
                set.range(range_after(after.as_ref(), descending)),
                descending,
            )
            .copied()
            .collect::<Vec<_>>()
        };
        assert_eq!(walk(None, false), [1, 3, 5, 7]);
        assert_eq!(walk(None, true), [7, 5, 3, 1]);
        assert_eq!(walk(Some(3), false), [5, 7]);
        assert_eq!(walk(Some(3), true), [1]);
        assert_eq!(
            walk(Some(4), false),
            [5, 7],
            "an absent position still splits"
        );
        assert_eq!(walk(Some(4), true), [3, 1]);
        assert_eq!(walk(Some(7), false), Vec::<u64>::new());
        assert_eq!(walk(Some(1), true), Vec::<u64>::new());
    }

    #[test]
    fn given_a_position_when_walking_a_sorted_slice_should_match_the_set_walk() {
        let sorted = [2u64, 4, 6, 8, 10];
        let set: BTreeSet<u64> = sorted.into_iter().collect();
        for descending in [false, true] {
            for after in [None, Some(0), Some(2), Some(5), Some(10), Some(11)] {
                let from_slice: Vec<u64> = slice_after(&sorted, |item| *item, after, descending)
                    .copied()
                    .collect();
                let from_set: Vec<u64> = Directed::new(
                    set.range(range_after(after.as_ref(), descending)),
                    descending,
                )
                .copied()
                .collect();
                assert_eq!(
                    from_slice, from_set,
                    "after {after:?}, descending {descending}"
                );
            }
        }
    }
}
