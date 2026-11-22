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
fn current_partition_count_scans_existing_topic_state() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "topic"),
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
    let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
    let streams: Streams = inner.into();

    assert_eq!(
        streams.current_partition_count(&WireIdentifier::numeric(0), &WireIdentifier::numeric(0)),
        Some(2)
    );
}

#[test]
fn applying_enriched_create_commands_stores_consensus_group_ids() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "topic"),
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 10,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 11,
                members: Vec::new(),
            },
        ],
    };
    let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

    let create_partitions = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 12,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 13,
                members: Vec::new(),
            },
        ],
    };
    let _ = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());

    assert_eq!(inner.items[0].topics[0].partitions.len(), 4);
    assert_eq!(inner.items[0].topics[0].partitions[2].id, 2);
    assert_eq!(inner.items[0].topics[0].partitions[3].id, 3);
    assert_eq!(
        inner.items[0].topics[0].partitions[0].consensus_group_id,
        10
    );
    assert_eq!(
        inner.items[0].topics[0].partitions[3].consensus_group_id,
        13
    );
}

#[test]
fn create_topic_apply_returns_get_topic_response_compatible_bytes() {
    // STM apply must emit `[TopicResponse][PartitionResponse]*` so existing
    // SDK decoders (`decode_response::<GetTopicResponse>`) parse the reply
    // without a wire-schema break.
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "topic"),
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 100,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 101,
                members: Vec::new(),
            },
        ],
    };

    let apply = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
    assert_eq!(apply.code, 0);
    let (reply, consumed) = GetTopicResponse::decode(&apply.body).expect("reply decodes");
    assert_eq!(consumed, apply.body.len());
    assert_eq!(reply.topic.id, 0);
    assert_eq!(reply.topic.partitions_count, 2);
    assert_eq!(reply.topic.name.as_str(), "topic");
    assert_eq!(reply.partitions.len(), 2);
    assert_eq!(reply.partitions[0].id, 0);
    assert_eq!(reply.partitions[1].id, 1);
}

#[test]
fn create_partitions_apply_resolves_ids_and_returns_empty_reply() {
    // STM resolves request-relative ids against the topic's current
    // partition count; the wire reply is empty (matches legacy
    // empty-ok response; SDK ignores the body).
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "topic"),
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 50,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 51,
                members: Vec::new(),
            },
        ],
    };
    let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

    let create_partitions = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        // request-relative offsets 0..=1; base is 2 (next after the
        // two topic-creation partitions), so resolved ids are 2 and 3.
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 60,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 61,
                members: Vec::new(),
            },
        ],
    };

    let apply = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());
    assert_eq!(apply.code, 0);
    assert!(apply.body.is_empty());

    let partitions = &inner.items[0].topics[0].partitions;
    assert_eq!(partitions.len(), 4);
    assert_eq!(partitions[2].id, 2);
    assert_eq!(partitions[2].consensus_group_id, 60);
    assert_eq!(partitions[3].id, 3);
    assert_eq!(partitions[3].consensus_group_id, 61);
}

#[test]
fn given_missing_topic_when_apply_create_partitions_should_return_topic_not_found() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    // Topic missing => validation failure path
    let create_partitions = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(99),
            partitions_count: 1,
        },
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let apply = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());
    assert_eq!(apply.code, u32::from(CreatePartitionsResult::TopicNotFound));
    assert!(apply.body.is_empty());
}

/// Over-count deletes were acked ok as a silent no-op; they must commit the
/// legacy `InvalidPartitionsCount` rejection. Zero stays an ok no-op at the
/// apply (rejected pre-consensus; a replayed entry keeps its historical ack).
#[test]
fn given_delete_partitions_counts_when_applied_should_reject_over_count() {
    let cases: &[(u32, u32, u32, usize)] = &[
        // (partitions in topic, count to delete, expected code, remaining)
        (
            3,
            4,
            u32::from(DeletePartitionsResult::InvalidPartitionsCount),
            3,
        ),
        (
            0,
            1,
            u32::from(DeletePartitionsResult::InvalidPartitionsCount),
            0,
        ),
        (3, 0, 0, 3),
        (3, 3, 0, 0),
        (3, 2, 0, 1),
    ];
    for &(partitions_count, count_to_delete, expected_code, expected_remaining) in cases {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, partitions_count, "topic"),
            derived_options: WireOptions::empty(),
            partitions: (0..partitions_count)
                .map(|partition_id| CreatedPartitionAssignment {
                    initial_view: 0,
                    partition_id,
                    consensus_group_id: 1,
                    members: Vec::new(),
                })
                .collect(),
        };
        let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
            &create_topic,
            &mut inner,
            IggyTimestamp::now(),
        );

        let delete = DeletePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: count_to_delete,
        };
        let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
            &delete,
            &mut inner,
            IggyTimestamp::now(),
        );

        assert_eq!(
            apply.code, expected_code,
            "deleting {count_to_delete} of {partitions_count} partitions"
        );
        assert!(apply.body.is_empty());
        assert_eq!(
            inner.items[0].topics[0].partitions.len(),
            expected_remaining,
            "deleting {count_to_delete} of {partitions_count} partitions"
        );
    }
}

/// The owner resolves `up_to_offset` against one partition incarnation. A
/// truncate that commits after a delete and re-create of that partition
/// carries an offset from the old offset space, so it must not move the
/// new watermark.
#[test]
fn given_mismatched_created_revision_when_apply_truncate_should_return_history_changed() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "topic"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    let created_revision = inner.items[0].topics[0].partitions[0].created_revision;

    let mut truncate = TruncatePartitionRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partition_id: 0,
        up_to_offset: 500,
        expected_history: Some(created_revision.wrapping_sub(1)),
    };
    let revision_before = inner.revision;
    let stale = StateHandler::apply(&truncate, &mut inner, IggyTimestamp::now());
    assert_eq!(
        stale.code,
        u32::from(TruncatePartitionResult::HistoryChanged)
    );
    assert_eq!(inner.revision, revision_before);
    assert_eq!(
        inner.items[0].topics[0].partitions[0].deleted_up_to_offset,
        0
    );

    truncate.expected_history = Some(created_revision);
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &truncate,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);
    assert_eq!(
        inner.items[0].topics[0].partitions[0].deleted_up_to_offset,
        500
    );
}
