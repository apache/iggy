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

//! The state of one coordinator log partition, built by reading its records in order.
//!
//! The partition decides its owner. A gateway that Iggy assigns the partition appends a claim
//! numbered above every claim before it, and the first claim with the highest number owns the
//! partition. A record whose claim is below a claim that came before it is void: its writer had
//! already lost the partition when the record landed. So a stale coordinator can append, but
//! nothing it appends after a newer claim counts, and it learns so when it reads its own record.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::time::Duration;

use crate::coordinator::record::{Body, CommittedOffset, NO_CHECKPOINT, Record};
use crate::group::GroupSnapshot;

/// A Kafka topic partition, by name and index.
pub type TopicPartition = (String, i32);

/// What a checkpoint rewrites of one group: its state, or its offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointUnit {
    Group(String),
    Offsets(String),
}

/// The offsets of one group that the log holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupOffsets {
    /// The log holds every offset of the group, so a partition with no entry has none committed.
    /// False until a deletion of the whole group: before that, an entry missing here can still be
    /// in Iggy, where the gateway kept offsets before the log.
    pub complete: bool,
    /// `None` marks an offset deleted after the log began to hold it.
    pub entries: BTreeMap<TopicPartition, Option<CommittedOffset>>,
}

impl GroupOffsets {
    /// Drops each offset of an older incarnation of its topic than `incarnation_of` names. Iggy
    /// deleted that topic with its offsets, so the topic of that name now holds none of them.
    /// An offset whose incarnation the gateway did not know stays.
    pub fn drop_older_incarnations(&mut self, incarnation_of: impl Fn(&str) -> Option<u64>) {
        self.entries.retain(|(topic, _), committed| {
            let logged = committed
                .as_ref()
                .and_then(|committed| committed.topic_incarnation);
            logged
                .zip(incarnation_of(topic))
                .is_none_or(|(logged, current)| logged >= current)
        });
    }
}

/// What one record did to the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The record counts.
    Valid,
    /// A newer claim came before it, so it does not count.
    Void,
    /// The record is a winning claim: `node_id` owns the partition from here on.
    Claimed { claim: u64, node_id: u16 },
}

/// The state of one coordinator log partition after the records read so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionState {
    /// The last winning claim, or the claim a checkpoint names, and the gateway that made it.
    /// `None` before either.
    pub owner: Option<(u64, u16)>,
    /// The highest claim of any record so far.
    pub highest: u64,
    /// Where the last complete checkpoint begins, or [`NO_CHECKPOINT`].
    pub checkpoint: u64,
    /// When Iggy stored the end of that checkpoint, or the first record read before one
    /// completed, in microseconds since the Unix epoch. A load reads nothing older.
    pub checkpointed_at: Option<u64>,
    /// Valid records since that checkpoint completed.
    pub since_checkpoint: u64,
    pub groups: HashMap<String, GroupSnapshot>,
    pub offsets: HashMap<String, GroupOffsets>,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self {
            owner: None,
            highest: 0,
            checkpoint: NO_CHECKPOINT,
            checkpointed_at: None,
            since_checkpoint: 0,
            groups: HashMap::new(),
            offsets: HashMap::new(),
        }
    }
}

impl PartitionState {
    /// The highest claim so far, 0 before the first. A new claim goes above it.
    #[must_use]
    pub const fn claim(&self) -> u64 {
        self.highest
    }

    /// Applies `record`, read at `offset` of the partition, which Iggy stored at `stored_at`
    /// microseconds since the Unix epoch.
    pub fn apply(&mut self, offset: u64, stored_at: u64, record: Record) -> Applied {
        self.checkpointed_at.get_or_insert(stored_at);
        let highest = self.highest;
        if let Body::Claim { node_id } = record.body {
            if record.claim <= highest {
                return Applied::Void;
            }
            self.highest = record.claim;
            self.owner = Some((record.claim, node_id));
            self.since_checkpoint += 1;
            return Applied::Claimed {
                claim: record.claim,
                node_id,
            };
        }
        if record.claim < highest {
            return Applied::Void;
        }
        // Only the winner of a claim writes under it. So the claim counts even when its own
        // record expired, and a later claim must go above it.
        self.highest = record.claim;
        self.since_checkpoint += 1;
        match record.body {
            Body::Claim { .. } => {}
            // A load that starts at a checkpoint learns the owner here. The writer checked its
            // claim when it appended this record, so no higher claim comes before it.
            Body::CheckpointBegin { node_id } => {
                if self.owner.is_none_or(|(claim, _)| record.claim > claim) {
                    self.owner = Some((record.claim, node_id));
                }
            }
            Body::CheckpointEnd { begin } => {
                // Only an end that follows its begin completes a checkpoint.
                if begin < offset {
                    self.checkpoint = begin;
                    self.checkpointed_at = Some(stored_at);
                    self.since_checkpoint = 0;
                }
            }
            Body::Group { group_id, snapshot } => match snapshot {
                Some(snapshot) => {
                    self.groups.insert(group_id, snapshot);
                }
                None => {
                    self.groups.remove(&group_id);
                }
            },
            Body::Offset {
                group_id,
                topic,
                partition,
                committed,
            } => {
                self.offsets
                    .entry(group_id)
                    .or_default()
                    .entries
                    .insert((topic, partition), committed);
            }
            Body::GroupOffsetsDeleted { group_id } => {
                self.offsets.insert(
                    group_id,
                    GroupOffsets {
                        complete: true,
                        entries: BTreeMap::new(),
                    },
                );
            }
            // Removed, not marked deleted: Iggy deleted its copies with the topic. An offset that
            // names a later incarnation of that name stays, as Kafka keeps one by topic id.
            Body::TopicOffsetsDeleted { topic, incarnation } => {
                for offsets in self.offsets.values_mut() {
                    offsets.entries.retain(|(name, _), committed| {
                        *name != topic || !deleted_with(committed.as_ref(), incarnation)
                    });
                }
                self.offsets
                    .retain(|_, offsets| offsets.complete || !offsets.entries.is_empty());
            }
            Body::GroupOffsetsExpired { group_id } => {
                self.offsets.remove(&group_id);
            }
            Body::GroupOffsetsComplete { group_id } => {
                self.offsets.entry(group_id).or_default().complete = true;
            }
        }
        Applied::Valid
    }

    /// Whether a checkpoint is due at `now`, in microseconds since the Unix epoch. After
    /// `records` valid records, once they also outnumber the entries a checkpoint rewrites, so a
    /// checkpoint never costs more than the records it saves a load. And once the records a load
    /// needs are `age` old, so they never expire, also when the owner changes each day.
    #[must_use]
    pub fn checkpoint_due(&self, records: u64, age: Duration, now: u64) -> bool {
        let age = u64::try_from(age.as_micros()).unwrap_or(u64::MAX);
        let aged = self
            .checkpointed_at
            .is_some_and(|at| now.saturating_sub(at) >= age);
        // Counting the entries walks every offset, so only once enough records came.
        aged || (self.since_checkpoint >= records && self.since_checkpoint >= self.live_entries())
    }

    /// Whether a deletion of incarnation `incarnation` of `topic` removes an entry.
    #[must_use]
    pub fn holds_topic(&self, topic: &str, incarnation: u64) -> bool {
        self.offsets.values().any(|offsets| {
            offsets.entries.iter().any(|((name, _), committed)| {
                name == topic && deleted_with(committed.as_ref(), incarnation)
            })
        })
    }

    /// The deletions that match the log to the topics Iggy holds, for deletions a gateway missed.
    /// `incarnation_of` names the incarnation of each topic Iggy holds. One deletion per older
    /// incarnation the log holds offsets of, and one for a topic Iggy no longer holds.
    #[must_use]
    pub fn deletions_against(&self, incarnation_of: impl Fn(&str) -> Option<u64>) -> Vec<Body> {
        let mut deletions = Vec::new();
        for (topic, logged) in self.logged_incarnations() {
            let current = incarnation_of(topic);
            let mut stale: Vec<u64> = logged
                .known
                .into_iter()
                .filter(|incarnation| current.is_none_or(|current| *incarnation < current))
                .collect();
            // Op 0 creates no topic, so its deletion takes only the entries of no known
            // incarnation.
            if stale.is_empty() && current.is_none() {
                stale.push(0);
            }
            deletions.extend(
                stale
                    .into_iter()
                    .map(|incarnation| Body::TopicOffsetsDeleted {
                        topic: topic.to_owned(),
                        incarnation,
                    }),
            );
        }
        deletions
    }

    /// The deletions of each topic incarnation older than the one `incarnation_of` names, so a
    /// topic created again under a deleted one's name holds none of its offsets, also when no
    /// gateway heard of the deletion. A topic that `incarnation_of` names none for stays, since a
    /// change log that lags shows a new topic late. So does a topic with an offset of unknown
    /// incarnation, which such a deletion takes too.
    #[must_use]
    pub fn older_incarnations(&self, incarnation_of: impl Fn(&str) -> Option<u64>) -> Vec<Body> {
        let mut deletions = Vec::new();
        for (topic, logged) in self.logged_incarnations() {
            let Some(current) = incarnation_of(topic).filter(|_| !logged.unknown) else {
                continue;
            };
            deletions.extend(
                logged
                    .known
                    .into_iter()
                    .filter(|incarnation| *incarnation < current)
                    .map(|incarnation| Body::TopicOffsetsDeleted {
                        topic: topic.to_owned(),
                        incarnation,
                    }),
            );
        }
        deletions
    }

    /// The incarnations the log holds offsets of, for each topic it holds an entry of.
    fn logged_incarnations(&self) -> BTreeMap<&str, LoggedIncarnations> {
        let mut logged: BTreeMap<&str, LoggedIncarnations> = BTreeMap::new();
        for offsets in self.offsets.values() {
            for ((topic, _), committed) in &offsets.entries {
                let held = logged.entry(topic.as_str()).or_default();
                match committed
                    .as_ref()
                    .map(|committed| committed.topic_incarnation)
                {
                    Some(Some(incarnation)) => {
                        held.known.insert(incarnation);
                    }
                    Some(None) => held.unknown = true,
                    None => {}
                }
            }
        }
        logged
    }

    /// The groups whose offsets the log drops at `now_ms`: no stored state, so no members here,
    /// and no commit in the last `retention_ms`. A group with no commit, which holds only
    /// deletions, goes at once: Iggy deleted those offsets too.
    #[must_use]
    pub fn expired_groups(&self, retention_ms: i64, now_ms: i64) -> Vec<String> {
        self.offsets
            .iter()
            .filter(|(group_id, offsets)| {
                !self.groups.contains_key(*group_id)
                    && offsets
                        .entries
                        .values()
                        .flatten()
                        .map(|committed| committed.commit_timestamp_ms)
                        .max()
                        .is_none_or(|last| now_ms.saturating_sub(last) >= retention_ms)
            })
            .map(|(group_id, _)| group_id.clone())
            .collect()
    }

    /// The records a checkpoint writes, without building them.
    #[must_use]
    pub fn live_entries(&self) -> u64 {
        let offsets: usize = self
            .offsets
            .values()
            .map(|offsets| {
                if offsets.complete {
                    1 + offsets
                        .entries
                        .values()
                        .filter(|entry| entry.is_some())
                        .count()
                } else {
                    offsets.entries.len()
                }
            })
            .sum();
        u64::try_from(self.groups.len() + offsets).unwrap_or(u64::MAX)
    }

    /// Every unit a checkpoint rewrites now.
    #[must_use]
    pub fn checkpoint_units(&self) -> Vec<CheckpointUnit> {
        self.groups
            .keys()
            .cloned()
            .map(CheckpointUnit::Group)
            .chain(self.offsets.keys().cloned().map(CheckpointUnit::Offsets))
            .collect()
    }

    /// Hands `write` each record that rewrites `unit` as the state holds it now, with the offset
    /// it rewrites, until `write` returns false. Offsets go in key order, from the one after
    /// `after`. A group whose offsets the log alone holds starts with
    /// [`Body::GroupOffsetsComplete`] when `after` is `None`. Nothing when the state no longer
    /// holds the unit.
    ///
    /// Every record repeats what the state holds, so a replay that reads only part of a
    /// checkpoint, after the records it repeats, ends in the same state. A deletion of one offset
    /// stays, since without it a load would find the offset in Iggy again.
    pub fn rewrite_unit(
        &self,
        unit: &CheckpointUnit,
        after: Option<&TopicPartition>,
        mut write: impl FnMut(Body, Option<&TopicPartition>) -> bool,
    ) {
        let group_id = match unit {
            CheckpointUnit::Group(group_id) => {
                if let Some(snapshot) = self.groups.get(group_id) {
                    let body = Body::Group {
                        group_id: group_id.clone(),
                        snapshot: Some(snapshot.clone()),
                    };
                    write(body, None);
                }
                return;
            }
            CheckpointUnit::Offsets(group_id) => group_id,
        };
        let Some(offsets) = self.offsets.get(group_id) else {
            return;
        };
        let complete = Body::GroupOffsetsComplete {
            group_id: group_id.clone(),
        };
        if offsets.complete && after.is_none() && !write(complete, None) {
            return;
        }
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let entries = offsets
            .entries
            .range::<TopicPartition, _>((start, Bound::Unbounded));
        for (key, committed) in entries {
            if offsets.complete && committed.is_none() {
                continue;
            }
            let body = Body::Offset {
                group_id: group_id.clone(),
                topic: key.0.clone(),
                partition: key.1,
                committed: committed.clone(),
            };
            if !write(body, Some(key)) {
                return;
            }
        }
    }

    /// Every record a checkpoint writes now, in one go.
    #[cfg(test)]
    #[must_use]
    pub fn checkpoint_bodies(&self) -> Vec<Body> {
        let mut bodies = Vec::new();
        for unit in self.checkpoint_units() {
            self.rewrite_unit(&unit, None, |body, _| {
                bodies.push(body);
                true
            });
        }
        bodies
    }
}

/// The incarnations of one topic that the log holds offsets of.
#[derive(Debug, Default)]
struct LoggedIncarnations {
    known: BTreeSet<u64>,
    /// An offset names no incarnation, since the gateway did not know it.
    unknown: bool,
}

/// Whether a deletion of incarnation `incarnation` of a topic removes `committed`, an entry of
/// that topic: a deletion, an offset of that incarnation, or one whose incarnation the gateway
/// did not know.
fn deleted_with(committed: Option<&CommittedOffset>, incarnation: u64) -> bool {
    committed.is_none_or(|committed| {
        committed
            .topic_incarnation
            .is_none_or(|other| other == incarnation)
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::group::MemberSnapshot;

    fn record(claim: u64, body: Body) -> Record {
        Record {
            claim,
            checkpoint: NO_CHECKPOINT,
            body,
        }
    }

    fn group(generation_id: i32) -> GroupSnapshot {
        GroupSnapshot {
            generation_id,
            protocol_type: "consumer".to_string(),
            protocol_name: Some("range".to_string()),
            leader: Some("m-1".to_string()),
            members: vec![MemberSnapshot {
                member_id: "m-1".to_string(),
                group_instance_id: None,
                client_id: String::new(),
                client_host: String::new(),
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 45_000,
                metadata: Bytes::new(),
                assignment: Bytes::new(),
            }],
        }
    }

    fn offset(group_id: &str, value: i64) -> Body {
        commit(group_id, "orders", 0, value, None)
    }

    fn commit(
        group_id: &str,
        topic: &str,
        partition: i32,
        value: i64,
        topic_incarnation: Option<u64>,
    ) -> Body {
        Body::Offset {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
            committed: Some(CommittedOffset {
                offset: value,
                leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: value,
                topic_incarnation,
            }),
        }
    }

    fn committed(state: &PartitionState, group_id: &str) -> Option<i64> {
        state.offsets[group_id].entries[&("orders".to_string(), 0)]
            .as_ref()
            .map(|committed| committed.offset)
    }

    #[test]
    fn given_a_newer_claim_when_the_old_owner_appends_after_it_should_void_its_records() {
        let mut state = PartitionState::default();
        let records = [
            record(1, Body::Claim { node_id: 1 }),
            record(1, offset("orders", 10)),
            record(2, Body::Claim { node_id: 2 }),
            record(1, offset("orders", 5)),
            record(2, offset("orders", 11)),
        ];
        let applied: Vec<Applied> = records
            .into_iter()
            .enumerate()
            .map(|(at, record)| state.apply(at as u64, 0, record))
            .collect();
        assert_eq!(
            applied,
            [
                Applied::Claimed {
                    claim: 1,
                    node_id: 1
                },
                Applied::Valid,
                Applied::Claimed {
                    claim: 2,
                    node_id: 2
                },
                Applied::Void,
                Applied::Valid,
            ]
        );
        assert_eq!(committed(&state, "orders"), Some(11));
        assert_eq!(state.owner, Some((2, 2)));
    }

    #[test]
    fn given_two_claims_with_one_number_when_replayed_should_give_the_partition_to_the_first() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(3, Body::Claim { node_id: 1 }));
        assert_eq!(
            state.apply(1, 0, record(3, Body::Claim { node_id: 2 })),
            Applied::Void
        );
        assert_eq!(state.owner, Some((3, 1)));
        assert_eq!(
            state.apply(2, 0, record(3, offset("orders", 4))),
            Applied::Valid,
            "the winner's records count"
        );
    }

    #[test]
    fn given_group_records_when_replayed_should_keep_the_last_state_and_drop_an_emptied_group() {
        let mut state = PartitionState::default();
        state.apply(
            0,
            0,
            record(
                1,
                Body::Group {
                    group_id: "a".to_string(),
                    snapshot: Some(group(1)),
                },
            ),
        );
        state.apply(
            1,
            0,
            record(
                1,
                Body::Group {
                    group_id: "a".to_string(),
                    snapshot: Some(group(2)),
                },
            ),
        );
        assert_eq!(state.groups["a"].generation_id, 2);
        state.apply(
            2,
            0,
            record(
                1,
                Body::Group {
                    group_id: "a".to_string(),
                    snapshot: None,
                },
            ),
        );
        assert!(state.groups.is_empty());
    }

    #[test]
    fn given_a_checkpoint_when_replayed_from_its_begin_should_rebuild_the_same_state() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, Body::Claim { node_id: 1 }));
        state.apply(
            1,
            0,
            record(
                1,
                Body::Group {
                    group_id: "a".to_string(),
                    snapshot: Some(group(3)),
                },
            ),
        );
        state.apply(2, 0, record(1, offset("a", 7)));
        state.apply(
            3,
            0,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "b".to_string(),
                },
            ),
        );
        state.apply(4, 0, record(1, offset("b", 2)));

        let begin = 5;
        let mut log = vec![record(1, Body::CheckpointBegin { node_id: 1 })];
        log.extend(
            state
                .checkpoint_bodies()
                .into_iter()
                .map(|body| record(1, body)),
        );
        log.push(record(1, Body::CheckpointEnd { begin }));
        for (at, entry) in log.iter().cloned().enumerate() {
            state.apply(begin + at as u64, 0, entry);
        }
        assert_eq!(state.checkpoint, begin);
        assert_eq!(state.since_checkpoint, 0);

        let mut rebuilt = PartitionState::default();
        for (at, entry) in log.into_iter().enumerate() {
            rebuilt.apply(begin + at as u64, 0, entry);
        }
        assert_eq!(rebuilt.owner, Some((1, 1)), "the begin names the owner");
        assert_eq!(
            rebuilt.apply(99, 0, record(0, offset("a", 1))),
            Applied::Void,
            "a record of an older claim after the checkpoint is void"
        );
        assert_eq!(rebuilt.groups, state.groups);
        assert_eq!(rebuilt.offsets, state.offsets);
        assert!(rebuilt.offsets["b"].complete);
    }

    /// A gateway that stops between two sends of a checkpoint leaves only its first part in the
    /// log. The next load replays that part after the records it repeats, so the part must change
    /// nothing, also for a group that a deletion made complete.
    #[test]
    fn given_a_checkpoint_cut_short_when_replayed_after_what_it_repeats_should_keep_every_offset() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, Body::Claim { node_id: 1 }));
        state.apply(
            1,
            0,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "a".to_string(),
                },
            ),
        );
        for (offset, partition) in (2..).zip(0..3) {
            state.apply(
                offset,
                0,
                record(1, commit("a", "orders", partition, 5, Some(1))),
            );
        }
        let held = state.offsets.clone();

        let begin = 5;
        state.apply(begin, 0, record(1, Body::CheckpointBegin { node_id: 1 }));
        let mut written = Vec::new();
        state.rewrite_unit(
            &CheckpointUnit::Offsets("a".to_string()),
            None,
            |body, _| {
                written.push(body);
                written.len() < 2
            },
        );
        assert_eq!(written.len(), 2, "the mark and the first offset");
        for (offset, body) in (begin + 1..).zip(written) {
            assert_eq!(state.apply(offset, 0, record(1, body)), Applied::Valid);
        }

        assert_eq!(state.offsets, held);
        assert_eq!(state.checkpoint, NO_CHECKPOINT, "no checkpoint completed");
    }

    #[test]
    fn given_an_end_without_its_begin_before_it_should_not_move_the_checkpoint() {
        let mut state = PartitionState::default();
        state.apply(4, 0, record(0, Body::CheckpointEnd { begin: 9 }));
        assert_eq!(state.checkpoint, NO_CHECKPOINT);
    }

    #[test]
    fn given_records_of_a_claim_whose_record_expired_should_claim_above_it() {
        let mut state = PartitionState::default();
        assert_eq!(
            state.apply(10, 0, record(5, offset("a", 1))),
            Applied::Valid
        );
        assert_eq!(state.claim(), 5, "a new claim goes above the owner's");
        assert_eq!(
            state.apply(11, 0, record(5, Body::Claim { node_id: 2 })),
            Applied::Void,
            "a claim at the owner's number loses"
        );
        assert_eq!(state.apply(12, 0, record(4, offset("a", 2))), Applied::Void);
        assert_eq!(committed(&state, "a"), Some(1));
    }

    #[test]
    fn given_a_deleted_topic_should_remove_its_offsets_from_every_group() {
        let mut state = PartitionState::default();
        let topic_offset = |group_id: &str, topic: &str| Body::Offset {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition: 0,
            committed: None,
        };
        state.apply(0, 0, record(1, offset("a", 1)));
        state.apply(1, 0, record(1, topic_offset("a", "payments")));
        state.apply(2, 0, record(1, commit("b", "orders", 0, 2, Some(7))));
        state.apply(
            3,
            0,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "c".to_string(),
                },
            ),
        );
        state.apply(4, 0, record(1, offset("c", 3)));

        let deleted = Body::TopicOffsetsDeleted {
            topic: "orders".to_string(),
            incarnation: 7,
        };
        assert_eq!(state.apply(5, 0, record(1, deleted)), Applied::Valid);
        assert_eq!(
            state.offsets["a"].entries.keys().collect::<Vec<_>>(),
            [&("payments".to_string(), 0)],
            "an offset of unknown incarnation goes with any"
        );
        assert!(!state.offsets.contains_key("b"), "an emptied group goes");
        assert!(
            state.offsets["c"].complete && state.offsets["c"].entries.is_empty(),
            "a deleted group stays complete"
        );
    }

    /// The deletion can reach the log after a commit to the topic created again under the name,
    /// for example when a gateway loads the partition within a minute of the deletion.
    #[test]
    fn given_a_deletion_of_an_older_incarnation_should_keep_the_offsets_of_the_newer() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, commit("a", "orders", 0, 5, Some(3))));
        state.apply(1, 0, record(1, commit("a", "orders", 1, 6, Some(9))));
        let deleted = Body::TopicOffsetsDeleted {
            topic: "orders".to_string(),
            incarnation: 3,
        };
        state.apply(2, 0, record(1, deleted));
        assert_eq!(
            state.offsets["a"].entries.keys().collect::<Vec<_>>(),
            [&("orders".to_string(), 1)]
        );
    }

    /// A receiver that lagged lost the deletions it missed. The topics Iggy holds still name them.
    #[test]
    fn given_the_topics_iggy_holds_should_delete_the_offsets_of_the_others() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, commit("a", "orders", 0, 5, Some(3))));
        state.apply(1, 0, record(1, commit("a", "orders", 1, 6, Some(9))));
        state.apply(2, 0, record(1, commit("a", "payments", 0, 7, Some(5))));
        state.apply(3, 0, record(1, commit("b", "refunds", 0, 8, None)));
        state.apply(4, 0, record(1, commit("b", "shipped", 0, 9, None)));
        let held = |topic: &str| match topic {
            "orders" => Some(9),
            "shipped" => Some(2),
            _ => None,
        };

        let deletions = state.deletions_against(held);

        let deleted = |topic: &str, incarnation| Body::TopicOffsetsDeleted {
            topic: topic.to_string(),
            incarnation,
        };
        assert_eq!(
            deletions,
            [
                deleted("orders", 3),
                deleted("payments", 5),
                deleted("refunds", 0),
            ]
        );
        for (offset, deletion) in (5..).zip(deletions) {
            state.apply(offset, 0, record(1, deletion));
        }
        assert_eq!(
            state.offsets["a"].entries.keys().collect::<Vec<_>>(),
            [&("orders".to_string(), 1)]
        );
        assert_eq!(
            state.offsets["b"].entries.keys().collect::<Vec<_>>(),
            [&("shipped".to_string(), 0)],
            "an offset of no known incarnation stays while Iggy holds the topic"
        );
    }

    /// A deletion while no gateway ran reaches no gateway, but the change log shows the topic
    /// created again under a newer incarnation.
    #[test]
    fn given_a_newer_incarnation_shown_should_delete_only_the_offsets_of_older_ones() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, commit("a", "orders", 0, 5, Some(3))));
        state.apply(1, 0, record(1, commit("a", "orders", 1, 6, Some(9))));
        state.apply(2, 0, record(1, commit("a", "payments", 0, 7, Some(5))));
        state.apply(3, 0, record(1, commit("b", "refunds", 0, 8, Some(2))));
        state.apply(4, 0, record(1, commit("b", "refunds", 1, 9, None)));
        let shown = |topic: &str| match topic {
            "orders" => Some(9),
            "refunds" => Some(4),
            _ => None,
        };

        assert_eq!(
            state.older_incarnations(shown),
            [Body::TopicOffsetsDeleted {
                topic: "orders".to_string(),
                incarnation: 3,
            }],
            "a topic the change log does not show, and one with an offset of unknown \
             incarnation, keep their offsets"
        );
    }

    #[test]
    fn given_an_older_incarnation_shown_newer_should_drop_its_offsets_from_an_answer() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, commit("a", "orders", 0, 5, Some(3))));
        state.apply(1, 0, record(1, commit("a", "orders", 1, 6, Some(9))));
        state.apply(2, 0, record(1, commit("a", "orders", 2, 7, None)));
        state.apply(3, 0, record(1, commit("a", "payments", 0, 8, Some(5))));
        state.apply(4, 0, record(1, commit("a", "orders", 3, 9, Some(12))));
        let mut offsets = state.offsets["a"].clone();

        offsets.drop_older_incarnations(|topic| (topic == "orders").then_some(9));

        assert_eq!(
            offsets.entries.keys().collect::<Vec<_>>(),
            [
                &("orders".to_string(), 1),
                &("orders".to_string(), 2),
                &("orders".to_string(), 3),
                &("payments".to_string(), 0),
            ],
            "an unknown incarnation, a newer one than a lagging change log shows, and a topic it \
             does not show stay"
        );
    }

    #[test]
    fn given_groups_without_members_or_recent_commits_should_expire_their_offsets() {
        let mut state = PartitionState::default();
        state.apply(0, 0, record(1, commit("old", "orders", 0, 100, None)));
        state.apply(1, 0, record(1, commit("recent", "orders", 0, 900, None)));
        state.apply(2, 0, record(1, commit("live", "orders", 0, 100, None)));
        state.apply(
            3,
            0,
            record(
                1,
                Body::Group {
                    group_id: "live".to_string(),
                    snapshot: Some(group(1)),
                },
            ),
        );
        state.apply(
            4,
            0,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "deleted".to_string(),
                },
            ),
        );

        let mut expired = state.expired_groups(500, 1_000);
        expired.sort();
        assert_eq!(expired, ["deleted", "old"]);

        let expiry = Body::GroupOffsetsExpired {
            group_id: "deleted".to_string(),
        };
        assert_eq!(state.apply(5, 0, record(1, expiry)), Applied::Valid);
        assert!(
            !state.offsets.contains_key("deleted"),
            "Iggy answers for the group again"
        );
    }

    /// A checkpoint goes on with a group's offsets where its last chunk ended.
    #[test]
    fn given_a_resumed_unit_should_rewrite_the_offsets_after_the_last_one_written() {
        let mut state = PartitionState::default();
        state.apply(
            0,
            0,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "a".to_string(),
                },
            ),
        );
        for (offset, partition) in (1..).zip(0..3) {
            state.apply(
                offset,
                0,
                record(1, commit("a", "orders", partition, 1, None)),
            );
        }
        let unit = CheckpointUnit::Offsets("a".to_string());
        let partitions = |after: Option<&TopicPartition>| {
            let mut written = Vec::new();
            state.rewrite_unit(&unit, after, |body, _| {
                written.push(match body {
                    Body::Offset { partition, .. } => partition,
                    _ => -1,
                });
                true
            });
            written
        };
        assert_eq!(
            partitions(None),
            [-1, 0, 1, 2],
            "the mark first, from the start"
        );
        assert_eq!(partitions(Some(&("orders".to_string(), 0))), [1, 2]);
    }

    #[test]
    fn given_fewer_records_than_live_entries_should_wait_for_the_checkpoint() {
        let mut state = PartitionState::default();
        for at in 0..4 {
            state.apply(at, 0, record(1, offset(&format!("g{at}"), 1)));
        }
        state.apply(4, 0, record(1, Body::CheckpointBegin { node_id: 1 }));
        state.apply(5, 0, record(1, Body::CheckpointEnd { begin: 4 }));
        assert_eq!(state.since_checkpoint, 0);
        assert_eq!(state.live_entries(), 4);

        let day = Duration::from_hours(24);
        for at in 6..9 {
            state.apply(at, 0, record(1, offset("g0", 2)));
        }
        assert!(
            !state.checkpoint_due(2, day, 0),
            "3 records do not pay for rewriting 4 entries"
        );
        state.apply(9, 0, record(1, offset("g0", 3)));
        assert!(state.checkpoint_due(2, day, 0));
        assert!(
            !state.checkpoint_due(5, day, 0),
            "nor before the configured count"
        );
    }

    #[test]
    fn given_an_old_checkpoint_should_make_one_due_whatever_the_count() {
        let day = Duration::from_hours(24);
        let day_micros = 86_400_000_000;
        let mut state = PartitionState::default();
        assert!(!state.checkpoint_due(10, day, day_micros), "nothing read");

        state.apply(0, 1_000, record(1, Body::Claim { node_id: 1 }));
        assert_eq!(state.checkpointed_at, Some(1_000), "the first record read");
        assert!(!state.checkpoint_due(10, day, 1_000 + day_micros - 1));
        assert!(state.checkpoint_due(10, day, 1_000 + day_micros));

        state.apply(1, 2_000, record(1, Body::CheckpointBegin { node_id: 1 }));
        state.apply(2, 3_000, record(1, Body::CheckpointEnd { begin: 1 }));
        assert_eq!(
            state.checkpointed_at,
            Some(3_000),
            "the end of the checkpoint"
        );
        assert!(!state.checkpoint_due(10, day, 1_000 + day_micros));
    }
}
