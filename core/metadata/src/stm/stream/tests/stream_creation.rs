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

#[test]
fn create_topic_stores_merged_options_and_typed_fields() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    // Client explicitly pins message_expiry; admission derives the rest.
    let explicit = TopicCreateOptions {
        message_expiry: Some(IggyExpiry::from(5_000_000u64)),
        partitions_count: Some(1),
        ..TopicCreateOptions::default()
    };
    let derived = TopicCreateOptions {
        max_topic_size: Some(MaxTopicSize::from(10_000_000_000u64)),
        compression_algorithm: Some(CompressionAlgorithm::None),
        ..TopicCreateOptions::default()
    };
    let request = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("t").unwrap(),
            options: explicit
                .to_explicit_wire(|key| key == topic_option_keys::MESSAGE_EXPIRY)
                .unwrap(),
        },
        derived_options: derived.to_wire().unwrap(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::from(1));
    assert_eq!(reply.code, 0, "create must succeed");

    let stream = inner.items.get(0).unwrap();
    let (_, topic) = stream.topics.iter().next().unwrap();
    assert_eq!(
        topic.resolved.message_expiry,
        IggyExpiry::from(5_000_000u64)
    );
    assert_eq!(
        topic.resolved.max_topic_size,
        MaxTopicSize::from(10_000_000_000u64)
    );
    assert_eq!(
        topic.resolved.compression_algorithm,
        CompressionAlgorithm::None
    );

    // partitions_count is create-consumed, never persisted.
    assert_eq!(topic.options.len(), 5);
    let expiry_key = HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY).unwrap();
    let expiry = topic.options.get(&expiry_key).unwrap();
    assert!(expiry.explicit, "client-sent key keeps its provenance");
    assert_eq!(expiry.value.kind(), HeaderKind::Uint64);
    for policy in [
        topic_option_keys::DURABILITY,
        topic_option_keys::CONSUMER_OFFSET_DURABILITY,
    ] {
        let key = HeaderKey::from_str(policy).unwrap();
        let option = topic.options.get(&key).unwrap();
        assert!(
            !option.explicit,
            "unsupplied durability comes from admission defaults"
        );
        assert_eq!(option.value.kind(), HeaderKind::String);
        assert_eq!(option.value.as_str().unwrap(), "replicated");
    }
    let size_key = HeaderKey::from_str(topic_option_keys::MAX_TOPIC_SIZE).unwrap();
    assert!(
        !topic.options.get(&size_key).unwrap().explicit,
        "derived key is marked derived"
    );
}

#[test]
fn given_committed_assignments_when_replaying_should_preserve_each_initial_view_and_create_op() {
    const TOPIC_OP: u64 = 5;
    const PARTITIONS_OP: u64 = 6;
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let create_topic = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 2,
            name: WireName::new("t").unwrap(),
            options: WireOptions::empty(),
        },
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
                initial_view: 7,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                partition_id: 1,
                consensus_group_id: 2,
                initial_view: 8,
                members: Vec::new(),
            },
        ],
    };
    let create_topic =
        CreateTopicWithAssignmentsRequest::decode_from(&create_topic.to_bytes()).unwrap();
    inner.apply_context.metadata_op = TOPIC_OP;
    let reply = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::from(1));
    assert_eq!(reply.code, 0, "create topic must succeed");

    let create_partitions = CreatePartitionsWithAssignmentsRequest {
        request: CreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        partitions: vec![
            CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 3,
                initial_view: 9,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                partition_id: 1,
                consensus_group_id: 4,
                initial_view: 10,
                members: Vec::new(),
            },
        ],
    };
    let committed_body = create_partitions.to_bytes();
    let streams: Streams = inner.into();
    let mut snapshot = MetadataSnapshot::new(1);
    snapshot.streams = Some(streams.to_snapshot());
    let decoded = MetadataSnapshot::decode(&snapshot.encode().unwrap()).unwrap();
    let mut restored = StreamsInner::inner_from_snapshot(decoded.streams.unwrap(), Arc::default());

    let replayed = CreatePartitionsWithAssignmentsRequest::decode_from(&committed_body).unwrap();
    restored.apply_context.metadata_op = PARTITIONS_OP;
    let reply = StateHandler::apply(&replayed, &mut restored, IggyTimestamp::from(2));
    assert_eq!(reply.code, 0, "create partitions must succeed");

    let restored: Streams = restored.into();
    let restored = Streams::from_snapshot(restored.to_snapshot()).unwrap();
    let assignments = restored.read(|inner| {
        let topic = inner.items.get(0).unwrap().topics.get(0).unwrap();
        topic
            .partitions
            .iter()
            .map(|partition| {
                (
                    partition.id,
                    partition.consensus_group_id,
                    partition.created_view,
                    partition.created_op,
                )
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(
        assignments,
        [
            (0, 1, 7, TOPIC_OP),
            (1, 2, 8, TOPIC_OP),
            (2, 3, 9, PARTITIONS_OP),
            (3, 4, 10, PARTITIONS_OP)
        ]
    );
}

/// A client may send the literal 0 that means "resolve the default". The
/// merge lets that explicit entry win over the derived one, so the map has
/// to be rewritten from the resolved value: otherwise the persisted map
/// reports 0 while the typed field reports the resolved default, and the
/// single `GetTopic` response contradicts itself.
#[test]
fn create_topic_replaces_a_client_sent_sentinel_with_the_resolved_default() {
    use iggy_common::{HeaderKey, TopicCreateOptions, topic_option_keys};
    use std::str::FromStr;

    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let explicit = TopicCreateOptions {
        max_topic_size: Some(MaxTopicSize::from(0u64)),
        ..TopicCreateOptions::default()
    };
    let derived = TopicCreateOptions {
        max_topic_size: Some(MaxTopicSize::from(10_000_000_000u64)),
        ..TopicCreateOptions::default()
    };
    // `to_wire` normalizes the sentinel away, so the block is hand-built to
    // carry the literal 0 the CLI's `--set max_topic_size=server_default`
    // puts on the wire through the raw map.
    let mut sentinel = bytes::BytesMut::new();
    iggy_binary_protocol::primitives::user_headers::encode_user_headers(
        &[(
            2,
            topic_option_keys::MAX_TOPIC_SIZE.as_bytes(),
            12,
            &0u64.to_le_bytes(),
        )],
        &mut sentinel,
    );
    let request = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("t").unwrap(),
            options: WireOptions::from_bytes(sentinel.freeze()).unwrap(),
        },
        derived_options: derived.to_wire().unwrap(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    assert!(explicit.max_topic_size.is_some(), "sentinel was sent");

    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::from(1));
    assert_eq!(reply.code, 0, "create must succeed");

    let stream = inner.items.get(0).unwrap();
    let (_, topic) = stream.topics.iter().next().unwrap();
    assert_eq!(
        topic.resolved.max_topic_size,
        MaxTopicSize::from(10_000_000_000u64)
    );
    let size_key = HeaderKey::from_str(topic_option_keys::MAX_TOPIC_SIZE).unwrap();
    let stored = topic.options.get(&size_key).unwrap();
    assert_eq!(
        stored.value.as_bytes(),
        &10_000_000_000u64.to_le_bytes(),
        "the map must carry the resolved value, not the sentinel"
    );
}
