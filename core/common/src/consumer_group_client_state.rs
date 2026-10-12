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
//! The SDK resolves partition operations locally before encoding their namespace:
//! - consumer-group polls select the next of the member's assigned partitions
//!   (round-robin) from the cached assignment synced from the coordinator;
//! - `Balanced` produce round-robins per topic; `MessagesKey` hashes the key.
//!
//! Cursors must persist across calls, so this lives on the long-lived
//! transport. The coordinator fences stale selections (rebalance), prompting a
//! re-sync that resets the cursor.

use crate::Identifier;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default, Clone)]
struct GroupAssignment {
    partitions: Vec<u32>,
    generation: u64,
    cursor: usize,
}

#[derive(Debug, Default)]
struct TopicPartitions {
    count: Option<u32>,
    contexts: HashMap<u32, PartitionContext>,
    cursor: usize,
    /// HTTP only: the client may send to the topic but not read its details, the only HTTP
    /// source of send contexts, so its sends there carry none.
    send_contexts_refused: bool,
}

/// Per-transport cache of group assignments, producer cursors, partition counts
/// and send contexts. Topic lookups borrow identifiers without formatting keys.
#[derive(Debug, Default)]
pub struct ConsumerGroupClientState {
    assignments: Mutex<HashMap<String, GroupAssignment>>,
    topic_partitions: Mutex<HashMap<Identifier, HashMap<Identifier, TopicPartitions>>>,
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
    pub fn next_balanced_partition(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_count: u32,
    ) -> u32 {
        if partition_count == 0 {
            return 0;
        }
        let mut map = self
            .topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(topic) = map
            .get_mut(stream_id)
            .and_then(|topics| topics.get_mut(topic_id))
        {
            let partition = (topic.cursor % partition_count as usize) as u32;
            topic.cursor = topic.cursor.wrapping_add(1);
            return partition;
        }
        map.entry(stream_id.clone()).or_default().insert(
            topic_id.clone(),
            TopicPartitions {
                cursor: 1,
                ..Default::default()
            },
        );
        0
    }

    #[must_use]
    pub fn partition_count(&self, stream_id: &Identifier, topic_id: &Identifier) -> Option<u32> {
        self.topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(stream_id)?
            .get(topic_id)?
            .count
    }

    pub fn set_topic_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: &[crate::Partition],
    ) {
        let Ok(count) = u32::try_from(partitions.len()) else {
            return;
        };
        let mut map = self
            .topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let topic = map
            .entry(stream_id.clone())
            .or_default()
            .entry(topic_id.clone())
            .or_default();
        topic.count = Some(count);
        topic.contexts = partitions
            .iter()
            .map(|partition| (partition.id, partition.context))
            .collect();
        topic.send_contexts_refused = false;
    }

    #[must_use]
    pub fn partition_context(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
    ) -> Option<PartitionContext> {
        self.topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(stream_id)?
            .get(topic_id)?
            .contexts
            .get(&partition_id)
            .copied()
    }

    pub fn set_partition_context(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        context: PartitionContext,
    ) {
        self.topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(stream_id.clone())
            .or_default()
            .entry(topic_id.clone())
            .or_default()
            .contexts
            .insert(partition_id, context);
    }

    /// Remember that the topic details were refused to this client, so its sends to the topic go
    /// without a context instead of asking for the details again.
    pub fn refuse_send_contexts(&self, stream_id: &Identifier, topic_id: &Identifier) {
        self.topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(stream_id.clone())
            .or_default()
            .entry(topic_id.clone())
            .or_default()
            .send_contexts_refused = true;
    }

    #[must_use]
    pub fn send_contexts_refused(&self, stream_id: &Identifier, topic_id: &Identifier) -> bool {
        self.topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(stream_id)
            .and_then(|topics| topics.get(topic_id))
            .is_some_and(|topic| topic.send_contexts_refused)
    }

    pub fn invalidate_topic(&self, stream_id: &Identifier, topic_id: &Identifier) {
        if let Some(topic) = self
            .topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(stream_id)
            .and_then(|topics| topics.get_mut(topic_id))
        {
            topic.count = None;
            topic.contexts.clear();
            topic.send_contexts_refused = false;
        }
    }

    /// Names and numeric identifiers can cache the same partition under different keys.
    /// Already captured request contexts are independent of this discovery cache.
    pub fn invalidate_topic_discovery(&self) {
        let mut map = self
            .topic_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for topic in map.values_mut().flat_map(HashMap::values_mut) {
            topic.count = None;
            topic.contexts.clear();
            topic.send_contexts_refused = false;
        }
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

    /// Drop what the consensus session owned. Membership is per connection
    /// and the coordinator fences assignments by a generation it tracks per
    /// session, so nothing synced under the old session holds once it is
    /// reset. The balanced cursors and the partition counts stay: they belong
    /// to a topic, not to a session, and clearing them would restart the
    /// produce round-robin at partition 0 and cost a metadata round trip per
    /// topic on every reconnect.
    pub fn clear_session_scoped(&self) {
        self.assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.joined_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partitions(count: u32) -> Vec<crate::Partition> {
        (0..count)
            .map(|id| crate::Partition {
                id,
                created_at: Default::default(),
                segments_count: 0,
                current_offset: 0,
                size: Default::default(),
                messages_count: 0,
                context: PartitionContext::default(),
            })
            .collect()
    }

    #[test]
    fn topic_identifiers_do_not_alias_and_discovery_refresh_preserves_the_cursor() {
        let state = ConsumerGroupClientState::new();
        let stream = Identifier::numeric(1).unwrap();
        let numeric = Identifier::numeric(2).unwrap();
        let named = Identifier::named("2").unwrap();
        state.set_topic_partitions(&stream, &numeric, &partitions(3));
        assert_eq!(state.partition_count(&stream, &named), None);
        assert_eq!(state.next_balanced_partition(&stream, &numeric, 3), 0);
        state.invalidate_topic_discovery();
        assert_eq!(state.partition_count(&stream, &numeric), None);
        state.set_topic_partitions(&stream, &numeric, &partitions(3));
        assert_eq!(state.next_balanced_partition(&stream, &numeric, 3), 1);
    }

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
            .map(|_| {
                state.next_balanced_partition(
                    &Identifier::named("s").unwrap(),
                    &Identifier::named("t").unwrap(),
                    3,
                )
            })
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0]);
    }

    #[test]
    fn send_context_discovery_does_not_invent_a_partition_count() {
        let stream = Identifier::named("s").unwrap();
        let topic = Identifier::named("t").unwrap();
        let state = ConsumerGroupClientState::new();
        let context = PartitionContext::default();
        state.set_partition_context(&stream, &topic, 2, context);
        assert_eq!(state.partition_context(&stream, &topic, 2), Some(context));
        assert_eq!(state.partition_count(&stream, &topic), None);

        state.set_topic_partitions(&stream, &topic, &partitions(3));
        assert_eq!(state.partition_count(&stream, &topic), Some(3));
        assert_eq!(state.partition_context(&stream, &topic, 2), Some(context));

        state.invalidate_topic(&stream, &topic);
        assert_eq!(state.partition_context(&stream, &topic, 2), None);
        assert_eq!(state.partition_count(&stream, &topic), None);
    }

    #[test]
    fn refused_send_contexts_are_forgotten_with_the_topic_state() {
        let stream = Identifier::named("s").unwrap();
        let topic = Identifier::named("t").unwrap();
        let state = ConsumerGroupClientState::new();
        assert!(!state.send_contexts_refused(&stream, &topic));

        state.refuse_send_contexts(&stream, &topic);
        assert!(state.send_contexts_refused(&stream, &topic));
        state.invalidate_topic(&stream, &topic);
        assert!(!state.send_contexts_refused(&stream, &topic));

        state.refuse_send_contexts(&stream, &topic);
        state.invalidate_topic_discovery();
        assert!(!state.send_contexts_refused(&stream, &topic));

        state.refuse_send_contexts(&stream, &topic);
        state.set_topic_partitions(&stream, &topic, &partitions(1));
        assert!(!state.send_contexts_refused(&stream, &topic));
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

    #[test]
    fn session_reset_drops_membership_and_keeps_topic_state() {
        let stream = Identifier::named("s").unwrap();
        let topic = Identifier::named("t").unwrap();
        let state = ConsumerGroupClientState::new();
        let id = Identifier::named("g").unwrap();
        state.register_group("s|t|g".to_owned(), id.clone(), id.clone(), id);
        state.set_assignment("s|t|g".to_owned(), 1, vec![0, 1]);
        assert_eq!(
            state.next_balanced_partition(
                &Identifier::named("s").unwrap(),
                &Identifier::named("t").unwrap(),
                3
            ),
            0
        );
        state.set_topic_partitions(&stream, &topic, &partitions(3));

        state.clear_session_scoped();

        assert!(state.registered_groups().is_empty());
        assert!(!state.is_registered("s|t|g"));
        assert!(!state.has_assignment("s|t|g"));
        // Topic state outlives the session: the produce cursor carries on and
        // the partition count is still cached.
        assert_eq!(
            state.next_balanced_partition(
                &Identifier::named("s").unwrap(),
                &Identifier::named("t").unwrap(),
                3
            ),
            1
        );
        assert_eq!(state.partition_count(&stream, &topic), Some(3));
    }
}
