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

use super::*;
use crate::stm::consumer_group::{ConsumerGroupAssignment, ConsumerGroupMember};
use crate::stm::result::{
    CreateConsumerGroupResult, DeleteConsumerGroupResult, LeaveConsumerGroupResult,
};
use crate::stm::snapshot::MetadataSnapshot;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest as WireCreatePartitionsRequest, CreatePartitionsWithAssignmentsRequest,
    InstallConsumerGroupOwnerRequest,
};
use iggy_binary_protocol::requests::streams::RewrapStreamKeyEntry;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest as WireCreateTopicRequest, CreateTopicWithAssignmentsRequest,
};
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_common::{
    CompressionAlgorithm, DEFAULT_SEGMENT_SIZE, Durability, GeoMode, HeaderKey, HeaderKind,
    IggyError, OptionKey, OptionValue, OptionValueCodec, ServerKeyContext, StreamEncryptionKey,
    StreamKeyOrigin, WrappedStreamKey, derived_defaults, topic_option_keys,
};
use std::str::FromStr;
use std::time::Duration;

mod apply_commands;
mod capture;
mod key_rewrap;
mod ordering;
mod partition_lifecycle;
mod replication;
mod resource_limits;
mod snapshot_restore;
mod stream_creation;
mod topic_options;

fn create_stream(inner: &mut StreamsInner, name: &str) {
    let request = CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options: WireOptions::empty(),
    };
    let _ = StateHandler::apply(&request, inner, IggyTimestamp::now());
}

const OLD_CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const NEW_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const THIRD_CONFIG_KEY: &str = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";

fn key_context(key: &str, previous: &[&str]) -> ServerKeyContext {
    let previous: Vec<String> = previous.iter().map(|key| (*key).to_owned()).collect();
    ServerKeyContext::from_config(key, &previous, true).unwrap()
}

fn envelope(context: &ServerKeyContext, seed: u8, origin: StreamKeyOrigin) -> WrappedStreamKey {
    context
        .wrap(
            &StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap(),
            origin,
        )
        .unwrap()
}

fn create_encrypted_stream(inner: &mut StreamsInner, name: &str, set: &WrapSet, op: u64) {
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKeyWrapped),
        OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
    );
    inner.apply_context.metadata_op = op;
    let request = CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
    };
    assert_eq!(
        StateHandler::apply(&request, inner, IggyTimestamp::now()).code,
        0
    );
}

fn rewrap_entry(
    stream_id: u32,
    created_op: u64,
    stored: &WrapSet,
    set: &[u8],
) -> RewrapStreamKeyEntry {
    RewrapStreamKeyEntry {
        stream_id,
        created_op,
        key_fingerprint: stored.key_fingerprint_bytes().try_into().unwrap(),
        wrap_set: Bytes::copy_from_slice(set),
    }
}

fn replaced_count(reply: &ApplyReply) -> u32 {
    u32::from_le_bytes(reply.body.as_ref().try_into().unwrap())
}

/// Apply the create of a one-partition topic in `stream_id` with the
/// `explicit` options, and the defaults the primary derives from them.
fn apply_create_topic(
    inner: &mut StreamsInner,
    stream_id: u32,
    name: &str,
    explicit: &TopicCreateOptions,
    consensus_group_id: u64,
) -> ApplyReply {
    apply_create_topic_block(
        inner,
        stream_id,
        name,
        &explicit.to_option_map().unwrap(),
        consensus_group_id,
    )
}

/// [`apply_create_topic`] with the explicit block as a client sent it,
/// which can leave out keys that `TopicCreateOptions` always sends.
fn apply_create_topic_block(
    inner: &mut StreamsInner,
    stream_id: u32,
    name: &str,
    explicit: &ResourceOptions,
    consensus_group_id: u64,
) -> ApplyReply {
    let derived = derived_defaults(OptionsScope::Topic, explicit).unwrap();
    let request = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            partitions_count: 1,
            name: WireName::new(name).unwrap(),
            options: resource_options_to_wire(explicit, OptionsProvenance::All).unwrap(),
        },
        derived_options: resource_options_to_wire(&derived, OptionsProvenance::All).unwrap(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id,
            members: Vec::new(),
        }],
    };
    StateHandler::apply(&request, inner, IggyTimestamp::now())
}

fn geo_topic() -> TopicCreateOptions {
    TopicCreateOptions {
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    }
}

/// The explicit block of `options` without the durability keys, as a
/// client that names neither sends it.
fn without_durability(options: &TopicCreateOptions) -> ResourceOptions {
    let mut block = options.to_option_map().unwrap();
    block.remove(&HeaderKey::from(OptionKey::Durability));
    block.remove(&HeaderKey::from(OptionKey::ConsumerOffsetDurability));
    block
}

/// The durability `topic_id` in stream 0 runs with, and its stored entry.
fn stored_durability(inner: &StreamsInner, topic_id: usize) -> (Durability, OptionValue) {
    let topic = &inner.items[0].topics[topic_id];
    let stored = topic
        .options
        .get(&HeaderKey::from(OptionKey::Durability))
        .expect("a create stores the durability")
        .clone();
    (topic.resolved.runtime.durability, stored)
}

fn durability_entry(durability: Durability, explicit: bool) -> OptionValue {
    let value = durability.encode().unwrap();
    if explicit {
        OptionValue::explicit(value)
    } else {
        OptionValue::derived(value)
    }
}

fn make_topic_request(stream_id: u32, partitions_count: u32, name: &str) -> WireCreateTopicRequest {
    WireCreateTopicRequest {
        stream_id: WireIdentifier::numeric(stream_id),
        partitions_count,
        name: WireName::new(name).unwrap(),
        options: WireOptions::empty(),
    }
}

fn streams_with_resize_policy(policy: Option<PartitionResizePolicy>) -> StreamsInner {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let mut request = make_topic_request(0, 2, "topic");
    request.options = TopicCreateOptions {
        partition_resize_policy: policy,
        ..TopicCreateOptions::default()
    }
    .to_wire()
    .unwrap();
    let created = CreateTopicWithAssignmentsRequest {
        request,
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 1,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 2,
                members: Vec::new(),
            },
        ],
    };
    assert_eq!(
        StateHandler::apply(&created, &mut inner, IggyTimestamp::now()).code,
        0
    );
    inner
}

fn topic_cap_update(max_topic_size: u64, name: Option<&str>) -> UpdateTopicResolvedRequest {
    UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: name.map(|name| WireName::new(name).unwrap()),
        explicit_options: TopicCreateOptions {
            max_topic_size: Some(MaxTopicSize::from(max_topic_size)),
            ..TopicCreateOptions::default()
        }
        .to_explicit_wire(|key| key == topic_option_keys::MAX_TOPIC_SIZE)
        .unwrap(),
        derived_options: WireOptions::empty(),
    }
}

fn changes_since(inner: &StreamsInner, revision: u64) -> Vec<(u64, PartitionChange)> {
    inner
        .recent_partition_changes
        .since(revision)
        .expect("the feed holds every change after the cursor")
        .copied()
        .collect()
}

fn create_topic_with_partitions(inner: &mut StreamsInner, partitions_count: u32) {
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, partitions_count, "topic"),
        derived_options: WireOptions::empty(),
        partitions: (0..partitions_count)
            .map(|partition_id| CreatedPartitionAssignment {
                initial_view: 0,
                partition_id,
                consensus_group_id: u64::from(partition_id) + 1,
                members: Vec::new(),
            })
            .collect(),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create_topic,
        inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);
}

fn make_create_stream_prepare(
    name: &str,
    op: u64,
) -> server_common::Message<iggy_binary_protocol::PrepareHeader> {
    use iggy_binary_protocol::{Command, Operation, PrepareHeader};
    use server_common::Message;
    use server_common::iobuf::Owned;
    use std::mem::size_of;

    let body = CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options: WireOptions::empty(),
    }
    .to_bytes();
    let header_size = size_of::<PrepareHeader>();
    let total = header_size + body.len();
    let mut buffer = Owned::<4096>::zeroed(total);
    {
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..header_size],
        );
        header.command = Command::Prepare;
        header.operation = Operation::CreateStream;
        header.op = op;
        header.size = u32::try_from(total).unwrap();
    }
    buffer.as_mut_slice()[header_size..].copy_from_slice(&body);
    Message::try_from(buffer).unwrap()
}

/// One stream, one topic, one partition, materialized in the registry the
/// way the data plane does at bootstrap.
fn inner_with_registered_partition() -> StreamsInner {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "alpha");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "logs"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
    let topic_stats = inner.items[0].topics[0].stats.clone();
    inner
        .stats_registry
        .partition(0, 0, &committed_partition(&inner, 0, 0, 0), topic_stats);
    inner
}

/// The committed record for one partition, which is what the registry keys
/// its entry's identity off.
fn committed_partition(
    inner: &StreamsInner,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Partition {
    inner.items[stream_id].topics[topic_id]
        .partitions
        .iter()
        .find(|partition| partition.id == partition_id)
        .expect("committed partition")
        .clone()
}

/// What [`StreamsInner::resource_counts`] must equal: the committed tree,
/// walked.
fn walked_counts(inner: &StreamsInner) -> ResourceCounts {
    let topics = || {
        inner
            .items
            .iter()
            .flat_map(|(_, stream)| stream.topics.iter())
    };
    ResourceCounts {
        streams: inner.items.len(),
        topics: topics().count(),
        partitions: topics().map(|(_, topic)| topic.partitions.len()).sum(),
        consumer_groups: topics().map(|(_, topic)| topic.consumer_groups.len()).sum(),
    }
}

/// Commits `request`, its lifecycle included, and checks the counts
/// against a walk.
fn commit_counted<R: StateHandler<State = StreamsInner>>(inner: &mut StreamsInner, request: &R) {
    let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        request,
        inner,
        IggyTimestamp::now(),
    );
    assert_eq!(reply.code, 0);
    assert_eq!(inner.resource_counts(), walked_counts(inner));
}

fn partition_assignments(first_group: u64, count: u32) -> Vec<CreatedPartitionAssignment> {
    (0..count)
        .map(|partition_id| CreatedPartitionAssignment {
            initial_view: 0,
            partition_id,
            consensus_group_id: first_group + u64::from(partition_id),
            members: Vec::new(),
        })
        .collect()
}

fn create_counted_topic(
    inner: &mut StreamsInner,
    stream_id: u32,
    name: &str,
    first_group: u64,
    partitions: u32,
) {
    commit_counted(
        inner,
        &CreateTopicWithAssignmentsRequest {
            request: make_topic_request(stream_id, partitions, name),
            derived_options: WireOptions::empty(),
            partitions: partition_assignments(first_group, partitions),
        },
    );
}

fn create_counted_group(inner: &mut StreamsInner, stream_id: u32, topic_id: u32, name: &str) {
    commit_counted(
        inner,
        &CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
            name: WireName::new(name).unwrap(),
        },
    );
}

fn node_totals(inner: &StreamsInner) -> (u64, u64, u64, u64) {
    let node = inner.stats_registry.node();
    (
        node.size_bytes_inconsistent(),
        node.messages_count_inconsistent(),
        node.segments_count_inconsistent(),
        node.partitions_with_segments_inconsistent(),
    )
}

fn delete_stream(inner: &mut StreamsInner, id: usize) {
    let request = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(u32::try_from(id).unwrap()),
    };
    let reply = StateHandler::apply(&request, inner, IggyTimestamp::now());
    assert_eq!(reply.code, 0, "stream {id} must delete");
}

/// More calls than any pass over a test tree takes, so a pass that never
/// ends fails instead of hanging.
const PASS_CALLS_MAX: usize = 64;

/// Runs `pass` to its end, `limit` partitions per call, and returns its
/// counts with the number of calls it took.
fn full_pass(
    streams: &Streams,
    live: &[bool],
    now: Instant,
    stale_before: Option<Instant>,
    pass: &mut PartitionReplicationPass,
    limit: usize,
) -> (PartitionReplication, usize) {
    (1..=PASS_CALLS_MAX)
        .find_map(|calls| {
            streams
                .partition_replication(live, now, stale_before, pass, limit)
                .map(|counts| (counts, calls))
        })
        .expect("a pass over a test tree ends")
}

fn stream_creation_keys(inner: &StreamsInner) -> BTreeSet<(u64, usize)> {
    inner
        .items
        .iter()
        .map(|(id, stream)| (stream.created_at.as_micros(), id))
        .collect()
}

fn create_stream_at(inner: &mut StreamsInner, name: &str, micros: u64) {
    let create = CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options: WireOptions::empty(),
    };
    let reply = StateHandler::apply(&create, inner, IggyTimestamp::from(micros));
    assert_eq!(reply.code, 0);
}

fn delete_stream_at(inner: &mut StreamsInner, stream_id: usize, micros: u64) {
    let delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(u32::try_from(stream_id).unwrap()),
    };
    let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        inner,
        IggyTimestamp::from(micros),
    );
    assert_eq!(reply.code, 0);
}

const CAPTURED_CLIENT: u128 = 7;
const UNCAPTURED_CLIENT: u128 = 8;

fn numeric_id(id: usize) -> WireIdentifier {
    WireIdentifier::numeric(u32::try_from(id).unwrap())
}

/// Streams `orders` (encrypted, with non-zero totals), `events` and
/// `audit`. Each has topic `logs` with explicit options, two partitions on
/// nodes 0 to 2 and group `readers`, joined by `CAPTURED_CLIENT` in the
/// first two streams and by `UNCAPTURED_CLIENT` in `audit`. `audit` also
/// holds topic `trash`, whose deletion is pending.
pub fn populated_catalog() -> StreamsInner {
    let mut inner = StreamsInner::new();
    let set = WrapSet::from(envelope(
        &key_context(OLD_CONFIG_KEY, &[]),
        1,
        StreamKeyOrigin::Server,
    ));
    create_encrypted_stream(&mut inner, "orders", &set, 1);
    for name in ["events", "audit"] {
        commit_counted(
            &mut inner,
            &CreateStreamRequest {
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
        );
    }
    let topic_options = TopicCreateOptions {
        message_expiry: Some(IggyExpiry::from(5_000_000u64)),
        ..TopicCreateOptions::default()
    }
    .to_explicit_wire(|key| key == topic_option_keys::MESSAGE_EXPIRY)
    .unwrap();
    for stream_id in 0..3u32 {
        let mut request = make_topic_request(stream_id, 2, "logs");
        request.options = topic_options.clone();
        commit_counted(
            &mut inner,
            &CreateTopicWithAssignmentsRequest {
                request,
                derived_options: WireOptions::empty(),
                partitions: (0..2)
                    .map(|partition_id| CreatedPartitionAssignment {
                        initial_view: 0,
                        partition_id,
                        consensus_group_id: u64::from(stream_id * 2 + partition_id + 1),
                        members: vec![0, 1, 2],
                    })
                    .collect(),
            },
        );
        create_counted_group(&mut inner, stream_id, 0, "readers");
        commit_counted(
            &mut inner,
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(stream_id),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(0),
                client_id: if stream_id < 2 {
                    CAPTURED_CLIENT
                } else {
                    UNCAPTURED_CLIENT
                },
                session: 1,
            },
        );
    }
    create_counted_topic(&mut inner, 2, "trash", 10, 1);
    inner.apply_context.metadata_op += 1;
    let pending = StateHandler::apply(
        &DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(2),
            topic_id: WireIdentifier::numeric(1),
        },
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(pending.code, 0);
    assert_eq!(inner.lifecycle_intents.len(), 1);
    inner.items[0].stats.store_from_snapshot(512, 4, 1);
    inner.items[0].topics[0]
        .stats
        .store_from_snapshot(512, 4, 1);
    inner
}

/// For each stream of `capture`, whether the live catalog still holds the
/// same allocation.
fn shared_with_capture(capture: &StreamsCapture, inner: &StreamsInner) -> Vec<bool> {
    capture
        .items
        .iter()
        .map(|(stream_id, captured)| {
            inner
                .items
                .get(*stream_id)
                .is_some_and(|live| Arc::ptr_eq(live, captured))
        })
        .collect()
}

fn encode_capture(capture: StreamsCapture) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut encoder = StreamsEncoder::from(capture);
    while encoder.encode_next(&mut bytes).unwrap() {}
    bytes
}

/// Applies `request` over a capture of `inner`, then checks the reply code
/// and that the live catalog still holds every captured stream.
fn assert_copies_no_stream<R: StateHandler<State = StreamsInner>>(
    inner: &mut StreamsInner,
    request: &R,
    code: u32,
    op: &str,
) {
    let capture = inner.capture();
    let reply = request.apply(inner, IggyTimestamp::now());
    assert_eq!(reply.code, code, "{op}");
    assert_eq!(
        shared_with_capture(&capture, inner),
        [true; 3],
        "{op} must copy no stream"
    );
}

/// The kinds of op [`apply_random_op`] runs, by index.
const RANDOM_OP_KINDS: [&str; 19] = [
    "CreateStream",
    "UpdateStreamResolved",
    "DeleteStream",
    "CreateTopicWithAssignments",
    "UpdateTopicResolved",
    "RewrapStreamKeys",
    "DeleteTopic",
    "CreatePartitionsWithAssignments",
    "DeletePartitions",
    "TruncatePartition",
    "RecordRetirementDebt",
    "CreateConsumerGroup",
    "DeleteConsumerGroup",
    "JoinConsumerGroup",
    "LeaveConsumerGroup",
    "RemoveConsumerGroupMembers",
    "CompleteConsumerGroupRevocation",
    "RefreshConsumerGroupSession",
    "CompleteLifecycle",
];

fn next_random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn pick<T>(items: &[T], random: u64) -> Option<&T> {
    items.get(usize::try_from(random).ok()? % items.len().max(1))
}

fn commit_at<R: StateHandler<State = StreamsInner>>(
    inner: &mut StreamsInner,
    request: &R,
    step: u64,
) {
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        request,
        inner,
        IggyTimestamp::from(step),
    );
}

/// Runs op `kind` of [`RANDOM_OP_KINDS`] on a target `random` picks from
/// the live catalog, so most ops apply. Does nothing without a target.
#[allow(clippy::too_many_lines)]
fn apply_random_op(
    inner: &mut StreamsInner,
    kind: usize,
    random: u64,
    step: u64,
    old_key: &ServerKeyContext,
    rotated_key: &ServerKeyContext,
) {
    let streams: Vec<usize> = inner.items.iter().map(|(id, _)| id).collect();
    let topics: Vec<(usize, usize)> = inner
        .items
        .iter()
        .flat_map(|(stream_id, stream)| {
            stream
                .topics
                .iter()
                .map(move |(topic_id, _)| (stream_id, topic_id))
        })
        .collect();
    let partitions: Vec<(usize, usize, &Partition)> = inner
        .items
        .iter()
        .flat_map(|(stream_id, stream)| {
            stream.topics.iter().flat_map(move |(topic_id, topic)| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| (stream_id, topic_id, partition))
            })
        })
        .collect();
    let partition = pick(&partitions, random).map(|&(stream_id, topic_id, partition)| {
        (
            stream_id,
            topic_id,
            partition.id,
            partition.created_revision,
            partition.deleted_up_to_offset,
        )
    });
    let groups: Vec<(usize, usize, &ConsumerGroup)> = inner
        .items
        .iter()
        .flat_map(|(stream_id, stream)| {
            stream.topics.iter().flat_map(move |(topic_id, topic)| {
                topic
                    .consumer_groups
                    .values()
                    .map(move |group| (stream_id, topic_id, group.as_ref()))
            })
        })
        .collect();
    let group = pick(&groups, random).map(|&(stream_id, topic_id, group)| {
        let member = pick(&group.members.iter().collect::<Vec<_>>(), random >> 16)
            .map(|(_, member)| member.client_id);
        (stream_id, topic_id, group.id, member)
    });
    let pending: Vec<(usize, usize, usize, InstallConsumerGroupOwnerRequest)> = groups
        .iter()
        .flat_map(|&(stream_id, topic_id, group)| {
            group
                .assignments
                .iter()
                .filter_map(move |(&partition_id, assignment)| {
                    let pending = assignment.pending?;
                    Some((
                        stream_id,
                        topic_id,
                        partition_id,
                        InstallConsumerGroupOwnerRequest {
                            incarnation: assignment.incarnation,
                            group_id: group.id,
                            owner: pending.owner,
                            metadata_op: pending.metadata_op,
                        },
                    ))
                })
        })
        .collect();
    let pending = pick(&pending, random).copied();
    let rewraps: Vec<(usize, u64, WrapSet, WrapSet)> = inner
        .items
        .iter()
        .filter_map(|(stream_id, stream)| {
            let stored = stream_wrap_set(&stream.options).ok()??;
            let target = rotated_key.target_set(&stored).ok()??;
            Some((stream_id, stream.created_op, stored, target))
        })
        .collect();
    let rewrap = pick(&rewraps, random).cloned();
    let stream = pick(&streams, random).copied();
    let topic = pick(&topics, random).copied();
    let name = |prefix: &str| WireName::new(format!("{prefix}{step}")).unwrap();
    let session = 1 + (random >> 24) % 3;

    match kind {
        0 if random.is_multiple_of(4) => {
            let seed = u8::try_from(1 + step % 200).unwrap();
            let set = WrapSet::from(envelope(old_key, seed, StreamKeyOrigin::Server));
            let op = inner.apply_context.metadata_op + 1;
            create_encrypted_stream(inner, &format!("e{step}"), &set, op);
        }
        0 => commit_at(
            inner,
            &CreateStreamRequest {
                name: name("s"),
                options: WireOptions::empty(),
            },
            step,
        ),
        1 => {
            let Some(stream_id) = stream else { return };
            commit_at(
                inner,
                &UpdateStreamResolvedRequest {
                    stream_id: numeric_id(stream_id),
                    name: Some(name("r")),
                    explicit_options: WireOptions::empty(),
                    derived_options: WireOptions::empty(),
                },
                step,
            );
        }
        2 => {
            let Some(stream_id) = stream else { return };
            commit_at(
                inner,
                &DeleteStreamRequest {
                    stream_id: numeric_id(stream_id),
                },
                step,
            );
        }
        3 => {
            let Some(stream_id) = stream else { return };
            let count = 1 + u32::try_from(random % 3).unwrap();
            let members = if random.is_multiple_of(2) {
                vec![0, 1, 2]
            } else {
                Vec::new()
            };
            commit_at(
                inner,
                &CreateTopicWithAssignmentsRequest {
                    request: WireCreateTopicRequest {
                        stream_id: numeric_id(stream_id),
                        partitions_count: count,
                        name: name("t"),
                        options: WireOptions::empty(),
                    },
                    derived_options: WireOptions::empty(),
                    partitions: (0..count)
                        .map(|partition_id| CreatedPartitionAssignment {
                            initial_view: 0,
                            partition_id,
                            consensus_group_id: step * 8 + u64::from(partition_id),
                            members: members.clone(),
                        })
                        .collect(),
                },
                step,
            );
        }
        4 => {
            let Some((stream_id, topic_id)) = topic else {
                return;
            };
            commit_at(
                inner,
                &UpdateTopicResolvedRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    name: Some(name("u")),
                    explicit_options: WireOptions::empty(),
                    derived_options: WireOptions::empty(),
                },
                step,
            );
        }
        5 => {
            let Some((stream_id, created_op, stored, target)) = rewrap else {
                return;
            };
            commit_at(
                inner,
                &RewrapStreamKeysRequest {
                    entries: vec![rewrap_entry(
                        u32::try_from(stream_id).unwrap(),
                        created_op,
                        &stored,
                        target.as_ref(),
                    )],
                },
                step,
            );
        }
        // Left pending, so captures hold lifecycle intents. Kind 18
        // finishes it.
        6 => {
            let Some((stream_id, topic_id)) = topic else {
                return;
            };
            inner.apply_context.metadata_op += 1;
            let _ = StateHandler::apply(
                &DeleteTopicRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                },
                inner,
                IggyTimestamp::from(step),
            );
        }
        7 => {
            let Some((stream_id, topic_id)) = topic else {
                return;
            };
            commit_at(
                inner,
                &CreatePartitionsWithAssignmentsRequest {
                    request: WireCreatePartitionsRequest {
                        stream_id: numeric_id(stream_id),
                        topic_id: numeric_id(topic_id),
                        partitions_count: 1,
                    },
                    partitions: vec![CreatedPartitionAssignment {
                        initial_view: 0,
                        partition_id: 0,
                        consensus_group_id: step * 8,
                        members: Vec::new(),
                    }],
                },
                step,
            );
        }
        8 => {
            let Some((stream_id, topic_id, ..)) = partition else {
                return;
            };
            commit_at(
                inner,
                &DeletePartitionsRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    partitions_count: 1,
                },
                step,
            );
        }
        9 => {
            let Some((stream_id, topic_id, partition_id, created_revision, watermark)) = partition
            else {
                return;
            };
            commit_at(
                inner,
                &TruncatePartitionRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    partition_id: u32::try_from(partition_id).unwrap(),
                    up_to_offset: watermark + 1 + random % 7,
                    expected_history: Some(created_revision),
                },
                step,
            );
        }
        10 => {
            let Some((stream_id, topic_id, partition_id, created_revision, _)) = partition else {
                return;
            };
            commit_at(
                inner,
                &RecordRetirementDebtRequest {
                    debts: vec![RetirementDebt {
                        namespace: IggyNamespace::new(stream_id, topic_id, partition_id).inner(),
                        created_revision,
                    }],
                },
                step,
            );
        }
        11 => {
            let Some((stream_id, topic_id)) = topic else {
                return;
            };
            commit_at(
                inner,
                &CreateConsumerGroupRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    name: name("g"),
                },
                step,
            );
        }
        12 => {
            let Some((stream_id, topic_id, group_id, _)) = group else {
                return;
            };
            commit_at(
                inner,
                &DeleteConsumerGroupRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    group_id: WireIdentifier::numeric(u32::try_from(group_id).unwrap()),
                },
                step,
            );
        }
        13 => {
            let Some((stream_id, topic_id, group_id, _)) = group else {
                return;
            };
            commit_at(
                inner,
                &JoinConsumerGroupRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    group_id: WireIdentifier::numeric(u32::try_from(group_id).unwrap()),
                    client_id: u128::from(1 + (random >> 32) % 4),
                    session,
                },
                step,
            );
        }
        14 => {
            let Some((stream_id, topic_id, group_id, Some(client_id))) = group else {
                return;
            };
            commit_at(
                inner,
                &LeaveConsumerGroupRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    group_id: WireIdentifier::numeric(u32::try_from(group_id).unwrap()),
                    client_id,
                },
                step,
            );
        }
        15 => {
            let Some((.., Some(client_id))) = group else {
                return;
            };
            commit_at(
                inner,
                &RemoveConsumerGroupMembersRequest {
                    // A second client, drawn as a join draws its client.
                    client_ids: vec![client_id, u128::from(1 + (random >> 32) % 4)],
                    actor_user_id: None,
                },
                step,
            );
        }
        16 => {
            let Some((stream_id, topic_id, partition_id, installation)) = pending else {
                return;
            };
            commit_at(
                inner,
                &CompleteConsumerGroupRevocationRequest {
                    stream_id: numeric_id(stream_id),
                    topic_id: numeric_id(topic_id),
                    partition_id: u32::try_from(partition_id).unwrap(),
                    installation,
                    partition_op: step,
                },
                step,
            );
        }
        17 => {
            let Some((.., Some(client_id))) = group else {
                return;
            };
            commit_at(
                inner,
                &RefreshConsumerGroupSessionRequest {
                    client_id,
                    session: step,
                    actor_user_id: None,
                },
                step,
            );
        }
        18 => {
            let Some(&intent_op) = inner.lifecycle_intents.keys().next() else {
                return;
            };
            crate::stm::lifecycle::complete_lifecycle(intent_op, inner, IggyTimestamp::from(step));
        }
        _ => unreachable!("no op kind {kind}"),
    }
}
