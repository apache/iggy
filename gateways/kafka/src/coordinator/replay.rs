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

use std::collections::{BTreeMap, HashMap};

use crate::coordinator::record::{Body, CommittedOffset, NO_CHECKPOINT, Record};
use crate::group::GroupSnapshot;

/// A Kafka topic partition, by name and index.
pub type TopicPartition = (String, i32);

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
    /// The highest claim so far, and the gateway that made it. `None` before the first claim.
    pub owner: Option<(u64, u16)>,
    /// Where the last complete checkpoint begins, or [`NO_CHECKPOINT`].
    pub checkpoint: u64,
    /// Valid records since that checkpoint completed.
    pub since_checkpoint: u64,
    pub groups: HashMap<String, GroupSnapshot>,
    pub offsets: HashMap<String, GroupOffsets>,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self {
            owner: None,
            checkpoint: NO_CHECKPOINT,
            since_checkpoint: 0,
            groups: HashMap::new(),
            offsets: HashMap::new(),
        }
    }
}

impl PartitionState {
    /// The highest claim so far, 0 before the first.
    #[must_use]
    pub fn claim(&self) -> u64 {
        self.owner.map_or(0, |(claim, _)| claim)
    }

    /// Applies `record`, read at `offset` of the partition.
    pub fn apply(&mut self, offset: u64, record: Record) -> Applied {
        let highest = self.claim();
        if let Body::Claim { node_id } = record.body {
            if record.claim <= highest {
                return Applied::Void;
            }
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
        self.since_checkpoint += 1;
        match record.body {
            Body::Claim { .. } => {}
            // A load that starts at a checkpoint learns the owner here. The writer checked its
            // claim when it appended this record, so no higher claim comes before it.
            Body::CheckpointBegin { node_id } => {
                if record.claim > highest {
                    self.owner = Some((record.claim, node_id));
                }
            }
            Body::CheckpointEnd { begin } => {
                // Only an end that follows its begin completes a checkpoint.
                if begin < offset {
                    self.checkpoint = begin;
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
        }
        Applied::Valid
    }

    /// The records that rewrite every live group and offset, for a checkpoint. A deletion that
    /// the log alone knows stays, since without it a load would find the offset in Iggy again.
    #[must_use]
    pub fn checkpoint_bodies(&self) -> Vec<Body> {
        let mut bodies = Vec::with_capacity(self.groups.len() + self.offsets.len());
        for (group_id, snapshot) in &self.groups {
            bodies.push(Body::Group {
                group_id: group_id.clone(),
                snapshot: Some(snapshot.clone()),
            });
        }
        for (group_id, offsets) in &self.offsets {
            if offsets.complete {
                bodies.push(Body::GroupOffsetsDeleted {
                    group_id: group_id.clone(),
                });
            }
            for ((topic, partition), committed) in &offsets.entries {
                if offsets.complete && committed.is_none() {
                    continue;
                }
                bodies.push(Body::Offset {
                    group_id: group_id.clone(),
                    topic: topic.clone(),
                    partition: *partition,
                    committed: committed.clone(),
                });
            }
        }
        bodies
    }
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
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 45_000,
                metadata: Bytes::new(),
                assignment: Bytes::new(),
            }],
        }
    }

    fn offset(group_id: &str, value: i64) -> Body {
        Body::Offset {
            group_id: group_id.to_string(),
            topic: "orders".to_string(),
            partition: 0,
            committed: Some(CommittedOffset {
                offset: value,
                leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
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
            .map(|(at, record)| state.apply(at as u64, record))
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
        state.apply(0, record(3, Body::Claim { node_id: 1 }));
        assert_eq!(
            state.apply(1, record(3, Body::Claim { node_id: 2 })),
            Applied::Void
        );
        assert_eq!(state.owner, Some((3, 1)));
        assert_eq!(
            state.apply(2, record(3, offset("orders", 4))),
            Applied::Valid,
            "the winner's records count"
        );
    }

    #[test]
    fn given_group_records_when_replayed_should_keep_the_last_state_and_drop_an_emptied_group() {
        let mut state = PartitionState::default();
        state.apply(
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
        state.apply(0, record(1, Body::Claim { node_id: 1 }));
        state.apply(
            1,
            record(
                1,
                Body::Group {
                    group_id: "a".to_string(),
                    snapshot: Some(group(3)),
                },
            ),
        );
        state.apply(2, record(1, offset("a", 7)));
        state.apply(
            3,
            record(
                1,
                Body::GroupOffsetsDeleted {
                    group_id: "b".to_string(),
                },
            ),
        );
        state.apply(4, record(1, offset("b", 2)));

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
            state.apply(begin + at as u64, entry);
        }
        assert_eq!(state.checkpoint, begin);
        assert_eq!(state.since_checkpoint, 0);

        let mut rebuilt = PartitionState::default();
        for (at, entry) in log.into_iter().enumerate() {
            rebuilt.apply(begin + at as u64, entry);
        }
        assert_eq!(rebuilt.owner, Some((1, 1)), "the begin names the owner");
        assert_eq!(
            rebuilt.apply(99, record(0, offset("a", 1))),
            Applied::Void,
            "a record of an older claim after the checkpoint is void"
        );
        assert_eq!(rebuilt.groups, state.groups);
        assert_eq!(rebuilt.offsets, state.offsets);
        assert!(rebuilt.offsets["b"].complete);
    }

    #[test]
    fn given_an_end_without_its_begin_before_it_should_not_move_the_checkpoint() {
        let mut state = PartitionState::default();
        state.apply(4, record(0, Body::CheckpointEnd { begin: 9 }));
        assert_eq!(state.checkpoint, NO_CHECKPOINT);
    }
}
