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

//! Client-side consumer-group + partitioning state for the VSR transport.
//!
//! Under VSR the client routes partition ops by namespace, so it resolves
//! partitioning locally (the broker never picks a partition, matching Kafka):
//! - consumer-group polls select the next of the member's assigned partitions
//!   (round-robin) from the cached assignment synced from the coordinator;
//! - `Balanced` produce round-robins per topic; `MessagesKey` hashes the key.
//!
//! Cursors must persist across calls, so this lives on the long-lived
//! transport. The coordinator fences stale selections (rebalance), prompting a
//! re-sync that resets the cursor.

use crate::Identifier;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default, Clone)]
struct GroupAssignment {
    partitions: Vec<u32>,
    generation: u64,
    cursor: usize,
}

/// Per-transport cache of consumer-group assignments + partition counts used to
/// resolve partitioning client-side. Keys are `stream|topic[|group]` strings
/// built from the request identifiers.
#[derive(Debug, Default)]
pub struct ConsumerGroupClientState {
    assignments: Mutex<HashMap<String, GroupAssignment>>,
    balanced_cursors: Mutex<HashMap<String, usize>>,
    partition_counts: Mutex<HashMap<String, u32>>,
    /// Identifiers of the joined groups, so the heartbeat can rebuild a sync
    /// request without re-deriving them from the cache key. Keyed by the same
    /// `stream|topic|group` string as `assignments`.
    joined_groups: Mutex<HashMap<String, (Identifier, Identifier, Identifier)>>,
}

impl ConsumerGroupClientState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// True if a non-empty assignment is cached for the group.
    #[must_use]
    pub fn has_assignment(&self, key: &str) -> bool {
        self.assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .is_some_and(|assignment| !assignment.partitions.is_empty())
    }

    /// Replace the cached assignment for a group. A generation change (a
    /// rebalance) resets the round-robin cursor so selection restarts cleanly.
    pub fn set_assignment(&self, key: String, generation: u64, partitions: Vec<u32>) {
        let mut map = self
            .assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = map.entry(key).or_default();
        if entry.generation != generation {
            entry.cursor = 0;
        }
        entry.generation = generation;
        entry.partitions = partitions;
    }

    /// Drop a group's cached assignment (after a fence rejection) so the next
    /// poll re-syncs.
    pub fn invalidate_assignment(&self, key: &str) {
        self.assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    /// The next assigned partition for a group poll, advancing the cursor.
    /// `None` when nothing is cached / the assignment is empty.
    #[must_use]
    pub fn next_group_partition(&self, key: &str) -> Option<u32> {
        let mut map = self
            .assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let assignment = map.get_mut(key)?;
        if assignment.partitions.is_empty() {
            return None;
        }
        let index = assignment.cursor % assignment.partitions.len();
        assignment.cursor = assignment.cursor.wrapping_add(1);
        Some(assignment.partitions[index])
    }

    /// The next `Balanced` produce partition for a topic, advancing the cursor.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn next_balanced_partition(&self, key: &str, partition_count: u32) -> u32 {
        if partition_count == 0 {
            return 0;
        }
        let mut map = self
            .balanced_cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cursor = map.entry(key.to_owned()).or_default();
        let partition = (*cursor % partition_count as usize) as u32;
        *cursor = cursor.wrapping_add(1);
        partition
    }

    #[must_use]
    pub fn partition_count(&self, key: &str) -> Option<u32> {
        self.partition_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .copied()
    }

    pub fn set_partition_count(&self, key: String, partition_count: u32) {
        self.partition_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, partition_count);
    }

    /// Record a joined group's identifiers so the heartbeat can re-sync it.
    pub fn register_group(
        &self,
        key: String,
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    ) {
        self.joined_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, (stream_id, topic_id, group_id));
    }

    /// Forget a group (after leave / delete) so the heartbeat stops re-syncing.
    pub fn deregister_group(&self, key: &str) {
        self.joined_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    /// True if the last assignment sync observed this client as a member. A
    /// member mid-rebalance or holding zero partitions is still registered, so
    /// this is a different question than [`Self::has_assignment`].
    #[must_use]
    pub fn is_registered(&self, key: &str) -> bool {
        self.joined_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(key)
    }

    /// Identifiers of every group the client has joined on this transport.
    #[must_use]
    pub fn registered_groups(&self) -> Vec<(Identifier, Identifier, Identifier)> {
        self.joined_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_partition_round_robins_then_wraps() {
        let state = ConsumerGroupClientState::new();
        state.set_assignment("s|t|g".to_owned(), 1, vec![0, 1, 2]);
        let picks: Vec<u32> = (0..4)
            .map(|_| state.next_group_partition("s|t|g").unwrap())
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0]);
    }

    #[test]
    fn generation_change_resets_cursor() {
        let state = ConsumerGroupClientState::new();
        state.set_assignment("s|t|g".to_owned(), 1, vec![0, 1, 2]);
        assert_eq!(state.next_group_partition("s|t|g"), Some(0));
        assert_eq!(state.next_group_partition("s|t|g"), Some(1));
        // Rebalance: new generation, one partition, cursor reset.
        state.set_assignment("s|t|g".to_owned(), 2, vec![5]);
        assert_eq!(state.next_group_partition("s|t|g"), Some(5));
    }

    #[test]
    fn balanced_round_robins() {
        let state = ConsumerGroupClientState::new();
        let picks: Vec<u32> = (0..4)
            .map(|_| state.next_balanced_partition("s|t", 3))
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0]);
    }

    #[test]
    fn missing_assignment_yields_none() {
        let state = ConsumerGroupClientState::new();
        assert!(!state.has_assignment("s|t|g"));
        assert_eq!(state.next_group_partition("s|t|g"), None);
    }

    #[test]
    fn member_holding_no_partitions_stays_registered() {
        let state = ConsumerGroupClientState::new();
        let id = Identifier::named("g").unwrap();
        state.register_group("s|t|g".to_owned(), id.clone(), id.clone(), id);
        state.set_assignment("s|t|g".to_owned(), 1, Vec::new());
        // A poll distinguishes "member, nothing assigned" from "not a member"
        // by membership alone, so the two must not track each other.
        assert!(!state.has_assignment("s|t|g"));
        assert!(state.is_registered("s|t|g"));

        state.deregister_group("s|t|g");
        assert!(!state.is_registered("s|t|g"));
    }
}
