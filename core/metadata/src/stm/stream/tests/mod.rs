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
use crate::stm::snapshot::MetadataSnapshot;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest as WireCreatePartitionsRequest, CreatePartitionsWithAssignmentsRequest,
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
