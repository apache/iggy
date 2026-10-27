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

use crate::Simulator;
use consensus::MetadataHandle;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireConsumerGroupItemBody, WireMemberItemBody, WireMetadataChange, WireMetadataChangeStatus,
    WireMetadataCurrentItem, WirePartitionsItemBody, WireSessionBody, WireStreamBody,
    WireTokenBody, WireTopicItemBody, WireTopicKey, WireUserBody,
};
use iggy_common::{
    MetadataChangeClass, MetadataChangeStart, MetadataChangesQuery, MetadataCurrentPageToken,
};
use metadata::change_log::{ChangeEntrySnapshot, ChangeLog, ChangeLogSnapshot};
use metadata::stm::user::ROOT_USER_CREATED_AT_MICROS;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Per tick, on one live replica: no change record above the applied
/// frontier, and the newest record op never moves back within one
/// incarnation.
pub(crate) fn check_change_log_head(
    sim: &Simulator,
    replica_idx: u8,
    heads: &mut HashMap<u8, (u128, u64)>,
    seed: u64,
) {
    let replica = &sim.replicas[usize::from(replica_idx)];
    let metadata = replica.shards[0].plane.metadata();
    let Some(last_record_op) = metadata.change_log().read(ChangeLog::last_record_op) else {
        return;
    };
    let frontier = metadata.applied_frontier().get();
    assert!(
        last_record_op <= frontier,
        "replica {replica_idx} holds a change record at op {last_record_op} above its applied \
         frontier {frontier} (seed={seed:#x})"
    );
    let incarnation = replica.metadata_incarnation;
    if let Some((previous_incarnation, previous)) =
        heads.insert(replica_idx, (incarnation, last_record_op))
        && previous_incarnation == incarnation
    {
        assert!(
            last_record_op >= previous,
            "replica {replica_idx} change log moved back from op {previous} to {last_record_op} \
             (seed={seed:#x})"
        );
    }
}

/// At quiesce: live replicas hold byte-identical records for every operation
/// they all applied and all still retain. Returns the records compared.
pub(crate) fn assert_change_logs_agree(sim: &Simulator, live: &[usize], seed: u64) -> usize {
    let logs: Vec<(usize, u64, ChangeLogSnapshot)> = live
        .iter()
        .filter_map(|&replica_idx| {
            let metadata = sim.replicas[replica_idx].shards[0].plane.metadata();
            let applied = metadata.consensus.as_ref()?.commit_min();
            Some((replica_idx, applied, metadata.change_log().snapshot()))
        })
        .collect();
    let Some((first_idx, first_applied, first)) = logs.first() else {
        return 0;
    };
    let mut compared = 0;
    for (replica_idx, applied, log) in &logs[1..] {
        if first.log_id.is_some() && log.log_id.is_some() {
            assert_eq!(
                first.log_id, log.log_id,
                "replicas {first_idx} and {replica_idx} report different change log ids \
                 (seed={seed:#x})"
            );
        }
        let common_applied = (*first_applied).min(*applied);
        for (class, left, right) in [
            (
                MetadataChangeClass::Resource,
                &first.resource,
                &log.resource,
            ),
            (
                MetadataChangeClass::Activity,
                &first.activity,
                &log.activity,
            ),
        ] {
            let floor = (left.evicted_through_op, left.evicted_through_ordinal)
                .max((right.evicted_through_op, right.evicted_through_ordinal));
            let window = |entries: &[ChangeEntrySnapshot]| -> Vec<ChangeEntrySnapshot> {
                entries
                    .iter()
                    .filter(|entry| entry.op <= common_applied && (entry.op, entry.ordinal) > floor)
                    .cloned()
                    .collect()
            };
            let (left, right) = (window(&left.entries), window(&right.entries));
            assert_eq!(
                left, right,
                "replicas {first_idx} and {replica_idx} hold different {class:?} change records \
                 through op {common_applied} (seed={seed:#x})"
            );
            compared += left.len();
        }
    }
    compared
}

/// At quiesce: a mirror built from the leader's records alone equals the
/// leader's current state pages, entities the harness seeded without a
/// committed operation aside. Returns the items compared, zero when the log
/// already evicted history the mirror needs.
pub(crate) fn assert_change_log_projection(sim: &Simulator, leader: usize, seed: u64) -> usize {
    let metadata = sim.replicas[leader].shards[0].plane.metadata();
    let log = metadata.change_log().snapshot();
    if (
        log.resource.evicted_through_op,
        log.activity.evicted_through_op,
    ) != (0, 0)
    {
        return 0;
    }
    let mut mirror = Mirror::default();
    let mut entries: Vec<&ChangeEntrySnapshot> = log
        .resource
        .entries
        .iter()
        .chain(&log.activity.entries)
        .collect();
    entries.sort_by_key(|entry| (entry.op, entry.ordinal));
    for entry in entries {
        let Ok(Some(change)) = WireMetadataChange::decode(entry.group, entry.kind, &entry.body)
        else {
            panic!(
                "leader holds an undecodable change record at op {} (seed={seed:#x})",
                entry.op
            );
        };
        mirror.apply(change);
    }

    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        limit: 1000,
        ..MetadataChangesQuery::default()
    };
    let mut current = Vec::new();
    loop {
        let Ok(page) = metadata.metadata_current_state(&query) else {
            return 0;
        };
        if page.status != WireMetadataChangeStatus::Ok {
            return 0;
        }
        current.extend(page.items);
        match page.current_page_token {
            Some(token) => {
                query.start =
                    MetadataChangeStart::Current(Some(MetadataCurrentPageToken::from(token)));
            }
            None => break,
        }
    }
    let projected: BTreeSet<Vec<u8>> = current
        .iter()
        .filter_map(|item| {
            let Ok(Some(decoded)) = WireMetadataCurrentItem::decode(item.group, &item.body) else {
                panic!("leader served an undecodable current item (seed={seed:#x})");
            };
            (!seeded_by_harness(&decoded)).then(|| encoded(&decoded))
        })
        .collect();
    let mirrored: BTreeSet<Vec<u8>> = mirror
        .items()
        .iter()
        .filter(|item| !seeded_by_harness(item))
        .map(encoded)
        .collect();
    assert_eq!(
        mirrored, projected,
        "a mirror of the leader's change records differs from its current state \
         (leader={leader}, seed={seed:#x})"
    );
    projected.len()
}

/// The model a client builds from change records alone.
#[derive(Debug, Default)]
struct Mirror {
    streams: BTreeMap<u32, WireStreamBody>,
    topics: BTreeMap<(u32, u32), (WireTopicItemBody, u32)>,
    groups: BTreeMap<(u32, u32, u32), WireConsumerGroupItemBody>,
    members: BTreeMap<(u32, u32, u32, u32), WireMemberItemBody>,
    users: BTreeMap<u32, WireUserBody>,
    tokens: BTreeMap<(u32, String), WireTokenBody>,
    sessions: BTreeMap<u128, WireSessionBody>,
}

impl Mirror {
    #[allow(clippy::too_many_lines)]
    fn apply(&mut self, change: WireMetadataChange) {
        match change {
            WireMetadataChange::StreamCreated(body) | WireMetadataChange::StreamUpdated(body) => {
                self.streams.insert(body.key.stream_id, body);
            }
            WireMetadataChange::StreamDeleted(deleted) => {
                let stream_id = deleted.key.stream_id;
                self.streams.remove(&stream_id);
                self.topics.retain(|key, _| key.0 != stream_id);
                self.groups.retain(|key, _| key.0 != stream_id);
                self.members.retain(|key, _| key.0 != stream_id);
            }
            WireMetadataChange::TopicCreated(body) | WireMetadataChange::TopicUpdated(body) => {
                self.topics.insert(
                    (body.key.stream.stream_id, body.key.topic_id),
                    (
                        WireTopicItemBody {
                            key: body.key,
                            created_at: body.created_at,
                            name: body.name,
                            options: body.options,
                        },
                        body.partitions_count,
                    ),
                );
            }
            WireMetadataChange::TopicDeleted(deleted) => {
                let topic = (deleted.key.stream.stream_id, deleted.key.topic_id);
                self.topics.remove(&topic);
                self.groups.retain(|key, _| (key.0, key.1) != topic);
                self.members.retain(|key, _| (key.0, key.1) != topic);
            }
            WireMetadataChange::PartitionsChanged(changed) => {
                let topic = (changed.topic.stream.stream_id, changed.topic.topic_id);
                if let Some((_, partitions_count)) = self.topics.get_mut(&topic) {
                    *partitions_count = changed.partitions_count;
                }
                for (key, member) in &mut self.members {
                    if (key.0, key.1) == topic {
                        member.partition_ids.retain(|partition_id| {
                            !changed.removed_partition_ids.contains(partition_id)
                        });
                    }
                }
            }
            WireMetadataChange::ConsumerGroupCreated(created) => {
                self.groups.insert(
                    (
                        created.topic.stream.stream_id,
                        created.topic.topic_id,
                        created.group_id,
                    ),
                    WireConsumerGroupItemBody {
                        topic: created.topic,
                        group_id: created.group_id,
                        name: created.name,
                    },
                );
            }
            WireMetadataChange::ConsumerGroupDeleted(deleted) => {
                let group = (
                    deleted.topic.stream.stream_id,
                    deleted.topic.topic_id,
                    deleted.group_id,
                );
                self.groups.remove(&group);
                self.members.retain(|key, _| (key.0, key.1, key.2) != group);
            }
            WireMetadataChange::ConsumerGroupMemberJoined(joined) => {
                self.members.insert(
                    (
                        joined.topic.stream.stream_id,
                        joined.topic.topic_id,
                        joined.group_id,
                        joined.member_id,
                    ),
                    WireMemberItemBody {
                        topic: joined.topic,
                        group_id: joined.group_id,
                        member_id: joined.member_id,
                        client_id: joined.client_id,
                        partition_ids: joined.partition_ids,
                    },
                );
            }
            WireMetadataChange::ConsumerGroupMemberLeft(left) => {
                self.members.remove(&(
                    left.topic.stream.stream_id,
                    left.topic.topic_id,
                    left.group_id,
                    left.member_id,
                ));
            }
            WireMetadataChange::ConsumerGroupPartitionHandedOver(handed) => {
                let group = (
                    handed.topic.stream.stream_id,
                    handed.topic.topic_id,
                    handed.group_id,
                );
                for (key, member) in &mut self.members {
                    if (key.0, key.1, key.2) != group {
                        continue;
                    }
                    member
                        .partition_ids
                        .retain(|partition_id| *partition_id != handed.partition_id);
                    if Some(key.3) == handed.member_id {
                        member.partition_ids.push(handed.partition_id);
                        member.partition_ids.sort_unstable();
                    }
                }
            }
            WireMetadataChange::UserCreated(body) | WireMetadataChange::UserUpdated(body) => {
                self.users.insert(body.key.user_id, body);
            }
            WireMetadataChange::UserDeleted(deleted) => {
                let user_id = deleted.key.user_id;
                self.users.remove(&user_id);
                self.tokens.retain(|key, _| key.0 != user_id);
            }
            WireMetadataChange::UserPasswordChanged(_) => {}
            WireMetadataChange::UserPermissionsUpdated(updated) => {
                if let Some(user) = self.users.get_mut(&updated.key.user_id) {
                    user.permissions = updated.permissions;
                }
            }
            WireMetadataChange::PersonalAccessTokenCreated(token) => {
                self.tokens
                    .insert((token.user.user_id, token.name.as_str().to_owned()), token);
            }
            WireMetadataChange::PersonalAccessTokenDeleted(deleted) => {
                self.tokens
                    .remove(&(deleted.user.user_id, deleted.name.as_str().to_owned()));
            }
            WireMetadataChange::SessionRegistered(session) => {
                self.sessions.insert(session.client_id, session);
            }
            WireMetadataChange::SessionClosed(closed) => {
                self.sessions.remove(&closed.client_id);
            }
        }
    }

    fn items(&self) -> Vec<WireMetadataCurrentItem> {
        let streams = self
            .streams
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::Stream);
        let topics = self
            .topics
            .values()
            .map(|(topic, _)| WireMetadataCurrentItem::Topic(topic.clone()));
        let partitions = self.topics.values().map(|(topic, partitions_count)| {
            WireMetadataCurrentItem::Partitions(WirePartitionsItemBody {
                topic: topic.key,
                partitions_count: *partitions_count,
            })
        });
        let groups = self
            .groups
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::ConsumerGroup);
        let members = self
            .members
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::ConsumerGroupMember);
        let users = self
            .users
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::User);
        let tokens = self
            .tokens
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::PersonalAccessToken);
        let sessions = self
            .sessions
            .values()
            .cloned()
            .map(WireMetadataCurrentItem::Session);
        streams
            .chain(topics)
            .chain(partitions)
            .chain(groups)
            .chain(members)
            .chain(users)
            .chain(tokens)
            .chain(sessions)
            .collect()
    }
}

/// Whether the harness seeded this entity straight into the state machines,
/// without the committed operation a record would come from.
fn seeded_by_harness(item: &WireMetadataCurrentItem) -> bool {
    let topic_seeded =
        |topic: &WireTopicKey| topic.stream.stream_created_op == 0 || topic.topic_created_op == 0;
    match item {
        WireMetadataCurrentItem::Stream(stream) => stream.key.stream_created_op == 0,
        WireMetadataCurrentItem::Topic(topic) => topic_seeded(&topic.key),
        WireMetadataCurrentItem::Partitions(partitions) => topic_seeded(&partitions.topic),
        WireMetadataCurrentItem::ConsumerGroup(group) => topic_seeded(&group.topic),
        WireMetadataCurrentItem::ConsumerGroupMember(member) => topic_seeded(&member.topic),
        WireMetadataCurrentItem::User(user) => {
            user.key.user_id == 0 && user.key.user_created_at == ROOT_USER_CREATED_AT_MICROS
        }
        WireMetadataCurrentItem::PersonalAccessToken(_) | WireMetadataCurrentItem::Session(_) => {
            false
        }
    }
}

fn encoded(item: &WireMetadataCurrentItem) -> Vec<u8> {
    let mut bytes = vec![item.group().as_u8()];
    bytes.extend_from_slice(&item.body_bytes());
    bytes
}
