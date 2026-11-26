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
fn given_a_capture_when_the_last_step_of_a_stream_ends_should_release_that_stream() {
    let inner = populated_catalog();
    let holders = |inner: &StreamsInner| -> Vec<usize> {
        inner
            .items
            .iter()
            .map(|(_, stream)| Arc::strong_count(stream))
            .collect()
    };
    let mut encoder = StreamsEncoder::from(inner.capture());
    let mut first_stream = Vec::new();
    encode_array_len(&mut first_stream, STREAMS_SNAPSHOT_FIELDS).unwrap();
    encode_array_len(&mut first_stream, inner.items.len()).unwrap();
    encode_value(
        &mut first_stream,
        &(0usize, StreamSnapshot::from(inner.items[0].as_ref())),
    )
    .unwrap();

    let mut bytes = Vec::new();
    while holders(&inner) == [2, 2, 2] {
        assert!(
            bytes.len() < first_stream.len(),
            "the stream is still held after its last byte"
        );
        assert!(encoder.encode_next(&mut bytes).unwrap());
    }

    assert_eq!(holders(&inner), [1, 2, 2]);
    assert_eq!(bytes, first_stream);
}

/// A topic can hold very many partitions. Each of them is a row of the
/// topic, of every group of the topic and of a pending delete, so none of
/// those lists may land in one step.
#[test]
fn given_a_large_topic_when_encoding_its_capture_should_write_one_row_a_step() {
    // Above 15 entries a msgpack array or map takes the long form of its
    // header.
    const PARTITIONS: u32 = 40;
    let mut inner = StreamsInner::new();
    for name in ["kept", "deleted"] {
        commit_counted(
            &mut inner,
            &CreateStreamRequest {
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
        );
    }
    create_counted_topic(&mut inner, 0, "empty", 1, 0);
    create_counted_topic(&mut inner, 0, "large", 1, PARTITIONS);
    create_counted_topic(&mut inner, 0, "trash", 100, PARTITIONS);
    create_counted_topic(&mut inner, 1, "gone", 200, PARTITIONS);
    create_counted_group(&mut inner, 0, 1, "readers");
    for client_id in [CAPTURED_CLIENT, UNCAPTURED_CLIENT] {
        commit_counted(
            &mut inner,
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(1),
                group_id: WireIdentifier::numeric(0),
                client_id,
                session: 1,
            },
        );
    }
    inner.apply_context.metadata_op += 1;
    let topic_delete = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(2),
    };
    assert_eq!(
        StateHandler::apply(&topic_delete, &mut inner, IggyTimestamp::now()).code,
        0
    );
    inner.apply_context.metadata_op += 1;
    let stream_delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(1),
    };
    assert_eq!(
        StateHandler::apply(&stream_delete, &mut inner, IggyTimestamp::now()).code,
        0
    );
    let streams = Streams::from(inner);
    let snapshot = streams.to_snapshot();
    let (_, large) = &snapshot.items[0].1.topics[1];
    let (_, group) = &large.consumer_groups[0];
    let intents: Vec<_> = snapshot.lifecycle_intents.values().collect();
    assert_eq!(large.partitions.len(), PARTITIONS as usize);
    assert_eq!(group.members.len(), 2);
    assert_eq!(group.assignments.len(), PARTITIONS as usize);
    assert!(
        group
            .assignments
            .values()
            .any(|assignment| assignment.pending.is_some()),
        "the second join leaves handovers pending"
    );
    assert_eq!(
        intents
            .iter()
            .map(|intent| (intent.topic_id.is_some(), intent.partitions.len()))
            .collect::<Vec<_>>(),
        [(true, PARTITIONS as usize), (false, PARTITIONS as usize)]
    );
    let smallest_list = [
        rmp_serde::to_vec(&large.partitions).unwrap().len(),
        rmp_serde::to_vec(&group.assignments).unwrap().len(),
        rmp_serde::to_vec(&intents[0].partitions).unwrap().len(),
    ]
    .into_iter()
    .min()
    .unwrap();

    let mut encoder = StreamsEncoder::from(streams.capture());
    let mut bytes = Vec::new();
    let mut largest_step = 0;
    loop {
        let before = bytes.len();
        let more = encoder.encode_next(&mut bytes).unwrap();
        largest_step = largest_step.max(bytes.len() - before);
        if !more {
            break;
        }
    }

    assert_eq!(bytes, rmp_serde::to_vec(&snapshot).unwrap());
    assert!(
        largest_step < smallest_list / 4,
        "a step of {largest_step} bytes holds several of the {PARTITIONS} rows of a list"
    );
}

#[test]
fn given_an_empty_or_populated_catalog_when_encoding_its_capture_should_write_the_snapshot_bytes() {
    let empty = Streams::default();
    assert_eq!(
        encode_capture(empty.capture()),
        rmp_serde::to_vec(&empty.to_snapshot()).unwrap()
    );
    let streams = Streams::from(populated_catalog());
    let snapshot = streams.to_snapshot();

    let (_, orders) = &snapshot.items[0];
    assert_eq!(snapshot.items.len(), 3);
    assert!(!orders.options.is_empty());
    assert_eq!(orders.stats.size_bytes, 512);
    assert_eq!(orders.topics[0].1.stats.messages_count, 4);
    assert_eq!(snapshot.lifecycle_intents.len(), 1);
    for (_, stream) in &snapshot.items {
        let (_, logs) = &stream.topics[0];
        assert!(!logs.options.is_empty());
        assert!(
            logs.partitions
                .iter()
                .all(|partition| partition.members == [0, 1, 2])
        );
        assert!(!logs.consumer_groups[0].1.members.is_empty());
    }

    assert_eq!(
        encode_capture(streams.capture()),
        rmp_serde::to_vec(&snapshot).unwrap()
    );
}

/// Sets the fields directly: the encoder reads the catalog, whatever op
/// wrote it.
#[test]
fn given_shadow_state_and_a_retired_origin_when_encoding_its_capture_should_write_the_snapshot_bytes()
 {
    let mut inner = populated_catalog();
    let stream = inner.items.make_mut(0).unwrap();
    stream.shadow = Some(ShadowStream {
        origin: "eu".to_owned(),
        cluster_uid: u128::MAX - 1,
        stream_id: 7,
    });
    let topic = &mut stream.topics[0];
    topic.shadow = Some(ShadowTopic {
        created_revision: 3,
        deleted_at: Some(5),
        retention: ShadowRetention {
            message_expiry: 11,
            max_topic_size: 13,
        },
        geo_mode: GeoMode::GeoTiered,
    });
    topic.recovered_from = Some(u128::MAX);
    inner.retired_origins.insert(
        "ap".to_owned(),
        RetiredOrigin {
            cluster_uids: vec![1, u128::MAX],
            retired_at: 17,
        },
    );
    let streams = Streams::from(inner);
    let snapshot = streams.to_snapshot();

    let (_, orders) = &snapshot.items[0];
    let (_, logs) = &orders.topics[0];
    assert!(orders.shadow.is_some());
    assert!(logs.shadow.is_some());
    assert!(logs.recovered_from.is_some());
    assert_eq!(snapshot.retired_origins.len(), 1);

    assert_eq!(
        encode_capture(streams.capture()),
        rmp_serde::to_vec(&snapshot).unwrap()
    );
}

#[test]
fn given_a_capture_when_an_op_is_refused_should_copy_no_stream() {
    let mut inner = populated_catalog();
    let orders_op = inner.items[0].created_op;
    let created_revision = inner.items[1].topics[0].partitions[0].created_revision;
    let stored = stream_wrap_set(&inner.items[0].options).unwrap().unwrap();
    let target = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY])
        .target_set(&stored)
        .unwrap()
        .unwrap();
    let truncate = |expected_history| TruncatePartitionRequest {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(0),
        partition_id: 0,
        up_to_offset: 0,
        expected_history: Some(expected_history),
    };

    assert_copies_no_stream(
        &mut inner,
        &DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(9),
        },
        u32::from(DeleteConsumerGroupResult::ConsumerGroupNotFound),
        "a delete of an unknown group",
    );
    assert_copies_no_stream(
        &mut inner,
        &CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 99,
                members: vec![0],
            }],
        },
        u32::from(CreatePartitionsResult::InvalidReplicationFactor),
        "a partition with members its topic does not take",
    );
    assert_copies_no_stream(
        &mut inner,
        &truncate(created_revision + 1),
        u32::from(TruncatePartitionResult::HistoryChanged),
        "a truncate of an older history",
    );
    assert_copies_no_stream(
        &mut inner,
        &truncate(created_revision),
        0,
        "a truncate below the watermark",
    );
    assert_copies_no_stream(
        &mut inner,
        &RewrapStreamKeysRequest {
            entries: vec![rewrap_entry(0, orders_op + 1, &stored, target.as_ref())],
        },
        0,
        "a rewrap of an earlier incarnation",
    );
    assert_copies_no_stream(
        &mut inner,
        &topic_cap_update(1, None),
        u32::from(UpdateTopicResult::InvalidTopicSize),
        "a topic cap below the segment size",
    );
    assert_copies_no_stream(
        &mut inner,
        &DeletePartitionsRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 3,
        },
        u32::from(DeletePartitionsResult::InvalidPartitionsCount),
        "a delete of more partitions than the topic has",
    );
    assert_copies_no_stream(
        &mut inner,
        &CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(0),
            name: WireName::new("readers").unwrap(),
        },
        u32::from(CreateConsumerGroupResult::NameAlreadyExists),
        "a group of a taken name",
    );
}

#[test]
fn given_a_capture_when_a_leave_or_a_debt_changes_nothing_should_copy_no_stream() {
    let mut inner = populated_catalog();
    let created_revision = inner.items[1].topics[0].partitions[0].created_revision;
    let leave = |group_id, client_id| LeaveConsumerGroupRequest {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(0),
        group_id: WireIdentifier::numeric(group_id),
        client_id,
    };
    assert_copies_no_stream(
        &mut inner,
        &leave(9, CAPTURED_CLIENT),
        u32::from(LeaveConsumerGroupResult::ConsumerGroupNotFound),
        "a leave of an unknown group",
    );
    assert_copies_no_stream(
        &mut inner,
        &leave(0, UNCAPTURED_CLIENT),
        u32::from(LeaveConsumerGroupResult::ConsumerGroupMemberNotFound),
        "a leave of a client that never joined",
    );
    assert_copies_no_stream(
        &mut inner,
        &RecordRetirementDebtRequest {
            debts: vec![RetirementDebt {
                namespace: IggyNamespace::new(1, 0, 0).inner(),
                created_revision: created_revision + 1,
            }],
        },
        0,
        "a debt of another incarnation",
    );
}

#[test]
fn given_a_capture_when_an_update_patches_nothing_should_copy_no_stream() {
    let mut inner = populated_catalog();
    let stream = &inner.items[1];
    let topic = &stream.topics[0];
    let same_stream = UpdateStreamResolvedRequest {
        stream_id: WireIdentifier::numeric(1),
        name: Some(WireName::new(&*stream.name).unwrap()),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };
    let (explicit_options, derived_options) =
        resource_options_to_wire_split(&topic.options).unwrap();
    assert!(
        !explicit_options.is_empty(),
        "the topic patch must carry an option"
    );
    let same_topic = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(0),
        name: Some(WireName::new(&*topic.name).unwrap()),
        explicit_options,
        derived_options,
    };
    assert_copies_no_stream(
        &mut inner,
        &same_stream,
        0,
        "a stream patch of its own name",
    );
    assert_copies_no_stream(
        &mut inner,
        &same_topic,
        0,
        "a topic patch of its own name and options",
    );
}

/// The reconciler reports a handover on each pass until its view shows
/// the commit, so a second completion of the same handover commits too.
#[test]
fn given_a_capture_when_a_revocation_completion_is_stale_should_copy_no_stream() {
    let mut inner = populated_catalog();
    let group = &inner.items[1].topics[0].consumer_groups[&0];
    let (&partition_id, assignment) = group.assignments.iter().next().unwrap();
    let stale = CompleteConsumerGroupRevocationRequest {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(0),
        partition_id: u32::try_from(partition_id).unwrap(),
        installation: InstallConsumerGroupOwnerRequest {
            incarnation: assignment.incarnation,
            group_id: group.id,
            owner: assignment.owner.unwrap_or_default(),
            metadata_op: u64::MAX,
        },
        partition_op: 1,
    };
    assert_copies_no_stream(&mut inner, &stale, 0, "a completion that is not pending");
}

#[test]
fn given_a_capture_when_a_membership_op_changes_nothing_should_copy_no_stream() {
    let mut inner = populated_catalog();
    assert_copies_no_stream(
        &mut inner,
        &JoinConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
            client_id: CAPTURED_CLIENT,
            session: 1,
        },
        0,
        "a re-join in the same session",
    );
    assert_copies_no_stream(
        &mut inner,
        &RefreshConsumerGroupSessionRequest {
            client_id: CAPTURED_CLIENT,
            session: 1,
            actor_user_id: None,
        },
        0,
        "a session refresh that changes nothing",
    );
}

#[test]
fn given_a_capture_when_removing_a_client_should_copy_only_the_streams_holding_it() {
    let mut inner = populated_catalog();
    let capture = inner.capture();

    let reply = StateHandler::apply(
        &RemoveConsumerGroupMembersRequest {
            client_ids: vec![CAPTURED_CLIENT],
            actor_user_id: None,
        },
        &mut inner,
        IggyTimestamp::now(),
    );

    assert_eq!(reply.code, 0);
    assert_eq!(reply.changes.len(), 2);
    assert!(!inner.consumer_group_members.contains_key(&CAPTURED_CLIENT));
    assert!(
        inner
            .consumer_group_members
            .contains_key(&UNCAPTURED_CLIENT)
    );
    assert_eq!(shared_with_capture(&capture, &inner), [false, false, true]);
}

/// A handler that wrote through a shared stream would change what an
/// earlier capture encodes. The totals are shared atomics read at encode
/// time, so this run keeps them at zero.
#[test]
fn given_random_ops_after_captures_when_encoding_them_again_should_write_the_same_bytes() {
    const STEPS: u64 = 1_500;
    const CAPTURE_EVERY: u64 = 100;
    // Creates and joins repeat so the catalog keeps something to change.
    const SCHEDULE: [usize; 24] = [
        0, 0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 13, 14, 15, 16, 16, 17, 18,
    ];
    let old_key = key_context(OLD_CONFIG_KEY, &[]);
    let rotated_key = key_context(NEW_CONFIG_KEY, &[OLD_CONFIG_KEY]);
    let mut inner = StreamsInner::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut changed = [0usize; RANDOM_OP_KINDS.len()];
    let mut captures = Vec::new();
    let mut encoded = encode_capture(inner.capture());
    for step in 1..=STEPS {
        let random = next_random(&mut seed);
        let kind = *pick(&SCHEDULE, random).unwrap();
        apply_random_op(&mut inner, kind, random >> 5, step, &old_key, &rotated_key);
        // Member removals find their groups through this index alone.
        let mut rebuilt = inner.clone();
        rebuilt.recompute_consumer_group_metadata();
        assert_eq!(
            rebuilt.consumer_group_members, inner.consumer_group_members,
            "{} left a stale membership index at step {step}",
            RANDOM_OP_KINDS[kind]
        );
        let after = encode_capture(inner.capture());
        if after != encoded {
            changed[kind] += 1;
        }
        encoded = after;
        if step.is_multiple_of(CAPTURE_EVERY) && step < STEPS {
            captures.push((step, inner.capture(), encoded.clone()));
        }
    }

    for (kind, count) in RANDOM_OP_KINDS.iter().zip(changed) {
        assert!(count > 0, "no {kind} changed the catalog");
    }
    for (step, capture, bytes) in captures {
        let again = encode_capture(capture);
        assert_eq!(again, bytes, "capture of step {step}");
        assert_ne!(again, encoded, "the catalog changed after step {step}");
    }
}
