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

use rand::{Rng, RngExt};

/// Trait for items stored in a `ReadyQueue`. Must expose a tick at which
/// the item becomes ready for delivery.
/// This trait will be used in the storage simulator in the future.
pub trait Ready {
    fn ready_at(&self) -> u64;
}

/// A min-heap priority queue with reservoir-sampled random ready removal.
///
/// `remove_ready()` picks a uniformly random item
/// from among all items whose `ready_at <= tick`, using reservoir sampling
/// over the heap's tree structure. The min-heap property allows pruning entire
/// subtrees whose root is not yet ready.
#[derive(Debug)]
pub struct ReadyQueue<T> {
    items: Vec<T>,
}

impl<T: Ready> Default for ReadyQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ready> ReadyQueue<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
        }
    }

    /// Push an item onto the queue, maintaining the min-heap property.
    pub fn push(&mut self, item: T) {
        self.items.push(item);
        let last = self.items.len() - 1;
        self.sift_up(last);
    }

    /// Peek at the item with the smallest `ready_at`.
    pub fn peek(&self) -> Option<&T> {
        self.items.first()
    }

    /// Reset the queue, removing all items but retaining the allocation.
    /// Matches TigerBeetle's `reset()` which sets `items.len = 0`.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Remove a uniformly random ready item (one whose `ready_at <= tick`).
    ///
    /// Returns `None` if no items are ready.
    pub fn remove_ready(&mut self, prng: &mut impl Rng, tick: u64) -> Option<T> {
        let top = self.peek()?;
        if top.ready_at() > tick {
            return None;
        }

        let root = self.pick_random_ready(prng, tick, 0);
        debug_assert!(root.count > 0);
        debug_assert!(root.pick < self.items.len());

        let result = self.remove_at(root.pick);
        debug_assert!(result.ready_at() <= tick);
        Some(result)
    }

    /// Remove a uniformly random item (regardless of readiness).
    /// Used for capacity eviction.
    pub fn remove_random(&mut self, prng: &mut impl Rng) -> Option<T> {
        if self.items.is_empty() {
            return None;
        }
        let index = prng.random_range(0..self.items.len());
        Some(self.remove_at(index))
    }

    /// Remove the item at `index`, maintaining the heap property.
    /// Swaps with the last element, then sifts up or down as needed.
    pub(crate) fn remove_at(&mut self, index: usize) -> T {
        assert!(index < self.items.len());
        let last = self.items.len() - 1;
        if index == last {
            return self.items.pop().unwrap();
        }
        self.items.swap(index, last);
        let removed = self.items.pop().unwrap();

        // The element now at `index` came from `last`. It might need to move
        // up or down to restore the heap property.
        let new_pos = self.sift_up(index);
        if new_pos == index {
            self.sift_down(index);
        }

        removed
    }

    /// Number of items in the queue.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Access all items in unspecified order.
    pub fn as_slice(&self) -> &[T] {
        &self.items
    }

    /// Sift an element up toward the root. Returns the final index.
    fn sift_up(&mut self, mut index: usize) -> usize {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.items[index].ready_at() < self.items[parent].ready_at() {
                self.items.swap(index, parent);
                index = parent;
            } else {
                break;
            }
        }
        index
    }

    /// Sift an element down away from the root.
    fn sift_down(&mut self, mut index: usize) {
        let len = self.items.len();
        loop {
            let left = index * 2 + 1;
            let right = index * 2 + 2;
            let mut smallest = index;

            if left < len && self.items[left].ready_at() < self.items[smallest].ready_at() {
                smallest = left;
            }
            if right < len && self.items[right].ready_at() < self.items[smallest].ready_at() {
                smallest = right;
            }

            if smallest == index {
                break;
            }
            self.items.swap(index, smallest);
            index = smallest;
        }
    }

    /// Reservoir sampling over the heap subtree rooted at `index`.
    ///
    /// Returns the total count of ready items in the subtree and the index
    /// of a uniformly random ready item among them.
    fn pick_random_ready(&self, prng: &mut impl Rng, tick: u64, index: usize) -> SubtreePick {
        if index >= self.items.len() || self.items[index].ready_at() > tick {
            return SubtreePick { pick: 0, count: 0 };
        }

        // Start with the current node as our pick.
        let mut result = SubtreePick {
            pick: index,
            count: 1,
        };

        // Visit both children.
        for child_index in [index * 2 + 1, index * 2 + 2] {
            let subtree = self.pick_random_ready(prng, tick, child_index);
            if subtree.count > 0 {
                let denominator = result.count + subtree.count;
                // Replace our pick with the subtree's pick with probability
                // subtree.count / denominator (reservoir sampling).
                if prng.random_range(0..denominator) < subtree.count {
                    result.pick = subtree.pick;
                }
                result.count = denominator;
            }
        }

        result
    }
}

struct SubtreePick {
    pick: usize,
    count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestItem {
        ready_at: u64,
    }

    impl Ready for TestItem {
        fn ready_at(&self) -> u64 {
            self.ready_at
        }
    }

    fn make_prng() -> rand_xoshiro::Xoshiro256Plus {
        use rand_xoshiro::rand_core::SeedableRng;
        rand_xoshiro::Xoshiro256Plus::seed_from_u64(42)
    }

    #[test]
    fn test_heap_property() {
        let mut q = ReadyQueue::new();
        q.push(TestItem { ready_at: 10 });
        q.push(TestItem { ready_at: 5 });
        q.push(TestItem { ready_at: 15 });
        q.push(TestItem { ready_at: 1 });

        assert_eq!(q.len(), 4);
        assert_eq!(q.peek().unwrap().ready_at, 1);
    }

    #[test]
    fn test_remove_ready_returns_ready_items() {
        let mut q = ReadyQueue::new();
        q.push(TestItem { ready_at: 5 });
        q.push(TestItem { ready_at: 10 });
        q.push(TestItem { ready_at: 3 });

        let mut prng = make_prng();

        // At tick 4, only item 3 (ready_at=3) is ready
        let item = q.remove_ready(&mut prng, 4).unwrap();
        assert_eq!(item.ready_at, 3);
        assert_eq!(q.len(), 2);

        // At tick 4, item 1 (ready_at=5) is not yet ready
        assert!(q.remove_ready(&mut prng, 4).is_none());
    }
}
