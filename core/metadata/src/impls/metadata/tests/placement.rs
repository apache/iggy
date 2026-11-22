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
fn given_a_configured_default_factor_when_admitting_a_create_without_one_should_place_that_many_members()
 {
    for (configured, expected) in [(None, 3), (Some(2), 2), (Some(5), 3)] {
        let metadata = placement_metadata();
        metadata.set_default_replication_factor(configured);
        assert_eq!(metadata.default_replication_factor(3), expected);
        let prepare = metadata
            .prepare_request(placement_request(1, "topic", 2))
            .unwrap();
        let request = PersistedCreateTopicRequest::decode_from(prepare.body()).unwrap();
        assert!(
            request
                .partitions
                .iter()
                .all(|partition| partition.members.len() == usize::from(expected)),
            "configured {configured:?}"
        );
    }
}

#[test]
fn given_an_offline_replica_when_admitting_creates_should_spread_over_responsive_replicas() {
    let metadata = placement_metadata();
    metadata.observe_replica_readiness(&[false, true, true]);
    let first = metadata
        .prepare_request(placement_request(1, "first", 4))
        .unwrap();
    assert_eq!(assigned_views(&first), [1, 2, 1, 2]);
    metadata
        .consensus
        .as_ref()
        .unwrap()
        .pipeline_message(PlaneKind::Metadata, &first);
    let second = metadata
        .prepare_request(placement_request(2, "second", 2))
        .unwrap();
    assert_eq!(assigned_views(&second), [1, 2]);
    metadata.observe_replica_readiness(&[true; 3]);
    let returned = metadata
        .prepare_request(placement_request(3, "returned", 2))
        .unwrap();
    assert_eq!(assigned_views(&returned), [0, 0]);
}

#[test]
fn given_pending_placement_when_admitting_another_batch_should_spread_before_wal_append() {
    let metadata = placement_metadata();
    let consensus = metadata.consensus.as_ref().unwrap();
    let first = metadata
        .prepare_request(placement_request(1, "first", 4))
        .unwrap();
    assert_eq!(assigned_views(&first), [0, 1, 2, 0]);
    consensus.pipeline_message(PlaneKind::Metadata, &first);
    let second = metadata
        .prepare_request(placement_request(2, "second", 2))
        .unwrap();
    assert_eq!(
        assigned_views(&second),
        [1, 2],
        "admitted assignments reserve load before append can yield"
    );
    assert!(metadata.journal.is_none());
}

#[test]
fn given_a_pending_create_topic_when_admitting_create_partitions_should_refuse_transiently() {
    let add_partitions = || {
        let body = WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::named("pending").unwrap(),
            partitions_count: 1,
        }
        .to_bytes();
        pat_request(2, 1, Operation::CreatePartitions, &body)
    };
    let metadata = placement_metadata();
    assert!(
        metadata.prepare_request(add_partitions()).is_ok(),
        "a topic nobody is creating is apply's TopicNotFound"
    );

    let metadata = placement_metadata();
    let create = metadata
        .prepare_request(placement_request(1, "pending", 2))
        .unwrap();
    metadata
        .consensus
        .as_ref()
        .unwrap()
        .pipeline_message(PlaneKind::Metadata, &create);
    assert!(matches!(
        metadata.prepare_request(add_partitions()),
        Err(IggyError::TransientNotAccepted)
    ));
}

#[test]
fn given_rolled_back_placement_when_admitting_another_batch_should_release_reservations() {
    let metadata = placement_metadata();
    let consensus = metadata.consensus.as_ref().unwrap();
    let first = metadata
        .prepare_request(placement_request(1, "first", 2))
        .unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &first);
    assert_eq!(
        consensus.rollback_pipelined_prepare(first.header()),
        PrepareRollback::Unwound
    );
    let second = metadata
        .prepare_request(placement_request(2, "second", 1))
        .unwrap();
    assert_eq!(assigned_views(&second), [0]);
}

#[test]
fn given_rejected_create_when_committed_should_release_its_placement_reservations() {
    let metadata = placement_metadata();
    let consensus = metadata.consensus.as_ref().unwrap();
    let first = metadata
        .prepare_request(placement_request(1, "missing-stream", 2))
        .unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &first);
    consensus.with_pipeline_mut(|pipeline| {
        pipeline.pop().unwrap();
    });
    let op = first.header().op;
    let reply = metadata.apply_prepare(first);
    assert_ne!(iggy_binary_protocol::result_code(reply.body()), Some(0));
    consensus.advance_commit_max(op);
    consensus.advance_commit_min(op);
    let second = metadata
        .prepare_request(placement_request(2, "next", 1))
        .unwrap();
    assert_eq!(
        assigned_views(&second),
        [0],
        "a committed rejection leaves no partition load"
    );
}

#[test]
fn given_committed_create_when_admitting_another_should_count_it_once() {
    let mut metadata = placement_metadata();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    let consensus = metadata.consensus.as_ref().unwrap();
    let first = metadata
        .prepare_request(placement_request(1, "new-topic", 2))
        .unwrap();
    assert_eq!(assigned_views(&first), [1, 2]);
    consensus.pipeline_message(PlaneKind::Metadata, &first);
    consensus.with_pipeline_mut(|pipeline| {
        pipeline.pop().unwrap();
    });
    let op = first.header().op;
    let reply = metadata.apply_prepare(first);
    assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
    consensus.advance_commit_max(op);
    consensus.advance_commit_min(op);
    let second = metadata
        .prepare_request(placement_request(2, "next", 1))
        .unwrap();
    assert_eq!(assigned_views(&second), [0]);
}

#[test]
fn given_a_shadow_adoption_when_assigning_its_partitions_should_mint_each_within_the_cap() {
    let mut metadata = placement_metadata();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    let adoption = |partitions_count| PersistedCreateTopicRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count,
            name: WireName::new("payments").unwrap(),
            options: WireOptions::empty(),
        },
        derived_options: WireOptions::empty(),
        partitions: Vec::new(),
    };

    let mut create = adoption(2);
    metadata.assign_shadow_partitions(&mut create).unwrap();
    let ids: Vec<u32> = create
        .partitions
        .iter()
        .map(|partition| partition.partition_id)
        .collect();
    assert_eq!(ids, [0, 1]);
    assert_ne!(
        create.partitions[0].consensus_group_id,
        create.partitions[1].consensus_group_id
    );
    assert!(
        PersistedCreateTopicRequest::decode_from(&create.to_bytes()).is_ok(),
        "the assigned create encodes as a topic create does"
    );

    metadata.set_partitions_max(2);
    assert!(matches!(
        metadata.assign_shadow_partitions(&mut adoption(2)),
        Err(IggyError::PartitionsLimitReached)
    ));
}

#[test]
fn given_a_restored_geo_topic_when_assigning_its_partitions_should_store_persisted_as_derived() {
    let mut metadata = placement_metadata();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    let create = |options: &TopicCreateOptions| PersistedCreateTopicRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("payments").unwrap(),
            options: options.to_wire().unwrap(),
        },
        derived_options: WireOptions::empty(),
        partitions: Vec::new(),
    };
    let durability = HeaderKey::from(OptionKey::Durability);
    let replicated = OptionValue::explicit(Durability::Replicated.encode().unwrap());

    let mut restored = create(&TopicCreateOptions {
        topic_type: Some(TopicType::Geo),
        geo_mode: Some(GeoMode::Geo),
        ..TopicCreateOptions::default()
    });
    metadata.assign_shadow_partitions(&mut restored).unwrap();
    let explicit = resource_options_from_wire(&restored.request.options, true).unwrap();
    let derived = resource_options_from_wire(&restored.derived_options, false).unwrap();
    assert!(!explicit.contains_key(&durability));
    assert_eq!(
        derived.get(&durability),
        Some(&OptionValue::derived(
            Durability::Persisted.encode().unwrap()
        ))
    );

    // A shadow adoption is stored as a normal topic and keeps its block.
    let mut adopted = create(&TopicCreateOptions::default());
    metadata.assign_shadow_partitions(&mut adopted).unwrap();
    let explicit = resource_options_from_wire(&adopted.request.options, true).unwrap();
    assert_eq!(explicit.get(&durability), Some(&replicated));
}

#[test]
fn given_unrelated_metadata_inflight_when_over_cap_create_is_invalid_should_defer_it() {
    let mut metadata = placement_metadata();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    metadata.set_partitions_max(1);
    assert!(matches!(
        metadata.admit_partitions(&placement_request(1, "over-cap", 1)),
        Err(IggyError::PartitionsLimitReached)
    ));
    let mut invalid = placement_request(1, "over-cap", 1);
    let mut request = WireCreateTopicRequest::decode_from(invalid.body()).unwrap();
    request.stream_id = WireIdentifier::numeric(99);
    let body = request.to_bytes();
    invalid.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
    assert!(metadata.admit_partitions(&invalid).is_ok());
    let consensus = metadata.consensus.as_ref().unwrap();
    let body = CreateStreamRequest {
        name: WireName::new("unrelated").unwrap(),
        options: WireOptions::empty(),
    }
    .to_bytes();
    let mut pending = Message::<PrepareHeader>::new(size_of::<PrepareHeader>() + body.len());
    pending.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
    let pending = pending.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::CreateStream;
        header.op = 1;
        header.group = server_common::sharding::METADATA_GROUP;
        header.size = u32::try_from(size_of::<PrepareHeader>() + body.len()).unwrap();
    });
    consensus.pipeline_message(PlaneKind::Metadata, &pending);
    consensus.sequencer().set_sequence(pending.header().op);
    assert!(matches!(
        metadata.admit_partitions(&placement_request(1, "over-cap", 1)),
        Err(IggyError::PartitionsLimitReached)
    ));
    assert!(
        matches!(
            metadata.admit_partitions(&invalid),
            Err(IggyError::TransientNotAccepted)
        ),
        "an unrelated prepared mutation currently defers the missing-stream result"
    );
}

#[test]
fn given_missing_pipeline_entry_when_creating_should_refuse_without_allocating() {
    let metadata = placement_metadata();
    let consensus = metadata.consensus.as_ref().unwrap();
    consensus.sequencer().set_sequence(1);
    assert!(matches!(
        metadata.prepare_request(placement_request(1, "missing", 2)),
        Err(IggyError::TransientNotAccepted)
    ));
    consensus.sequencer().set_sequence(0);
    let first = metadata
        .prepare_request(placement_request(1, "ready", 1))
        .unwrap();
    let assignments = PersistedCreateTopicRequest::decode_from(first.body())
        .unwrap()
        .partitions;
    assert_eq!(
        assignments[0].consensus_group_id, 1,
        "refusal cannot consume group ids"
    );
    assert_eq!(assignments[0].initial_view, 0);
}

#[test]
fn given_unknown_session_when_creating_should_not_allocate_groups() {
    let metadata = placement_metadata();
    assert!(matches!(
        metadata.prepare_request(placement_request(99, "unauthorized", 2)),
        Err(IggyError::Unauthenticated)
    ));
    let first = metadata
        .prepare_request(placement_request(1, "authorized", 1))
        .unwrap();
    assert_eq!(
        PersistedCreateTopicRequest::decode_from(first.body())
            .unwrap()
            .partitions[0]
            .consensus_group_id,
        1
    );
}

#[test]
fn given_partition_observations_when_creating_should_keep_highest_matching_incarnation() {
    let mut metadata = placement_metadata();
    let streams = stream_with_one_partition_in_view(2);
    let revision = streams
        .items
        .get(0)
        .unwrap()
        .topics
        .get(0)
        .unwrap()
        .partitions[0]
        .created_revision;
    let mut recreated = streams.clone();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (streams.into(), ()),
    )));
    let namespace = IggyNamespace::new(0, 0, 0);
    metadata.observe_partition_views(&[
        PartitionPlacementView {
            namespace,
            created_revision: revision,
            view: 6,
        },
        PartitionPlacementView {
            namespace,
            created_revision: revision,
            view: 4,
        },
        PartitionPlacementView {
            namespace,
            created_revision: revision - 1,
            view: 100,
        },
        PartitionPlacementView {
            namespace,
            created_revision: revision + 1,
            view: 101,
        },
    ]);
    let first = metadata
        .prepare_request(placement_request(1, "observed", 1))
        .unwrap();
    assert_eq!(
        assigned_views(&first),
        [1],
        "view 6 keeps load on replica 0 despite older reports"
    );
    metadata.observe_partition_views(&[]);
    let unchanged = metadata
        .prepare_request(placement_request(2, "missing-report", 1))
        .unwrap();
    assert_eq!(assigned_views(&unchanged), [1]);

    assert_eq!(
        apply_with_lifecycle_completion(
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
            &mut recreated,
            IggyTimestamp::now(),
        )
        .code,
        0
    );
    create_topic_in_view(&mut recreated, 2);
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (recreated.into(), ()),
    )));
    metadata.observe_partition_views(&[PartitionPlacementView {
        namespace,
        created_revision: revision,
        view: 102,
    }]);
    let next = metadata
        .prepare_request(placement_request(3, "recreated", 1))
        .unwrap();
    assert_eq!(
        assigned_views(&next),
        [0],
        "recreation falls back to its own committed view 2"
    );
    metadata.mux_stm = Rc::new(TestMux::default());
    let empty = metadata
        .prepare_request(placement_request(3, "deleted", 1))
        .unwrap();
    assert_eq!(assigned_views(&empty), [0]);
}

#[compio::test]
async fn given_inherited_create_when_summary_missing_should_refuse_then_restore_from_wal() {
    let directory = tempfile::tempdir().unwrap();
    let journal = PrepareJournal::open(&directory.path().join("journal.wal"), 0)
        .await
        .unwrap();
    let old_primary = placement_metadata();
    let inherited = old_primary
        .prepare_request(placement_request(1, "inherited", 2))
        .unwrap();
    journal.handle().append(inherited.clone()).await.unwrap();
    let mut metadata = placement_metadata();
    metadata.journal = Some(journal);
    let consensus = metadata.consensus.as_ref().unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &inherited);
    assert!(matches!(
        metadata.prepare_request(placement_request(2, "waiting", 1)),
        Err(IggyError::TransientNotAccepted)
    ));
    let reply = metadata
        .submit_request_in_process(placement_request(2, "waiting", 1))
        .await
        .unwrap()
        .try_into_typed::<ReplyHeader>()
        .unwrap();
    assert_eq!(
        iggy_binary_protocol::result_code(reply.body()),
        Some(IggyError::TransientNotAccepted.as_code())
    );
    assert!(!reply.header().has_committed_result());
    metadata.refresh_placement().await;
    let next = metadata
        .prepare_request(placement_request(2, "ready", 1))
        .unwrap();
    let assignments = PersistedCreateTopicRequest::decode_from(next.body())
        .unwrap()
        .partitions;
    assert_eq!(assignments[0].initial_view, 2);
    assert_eq!(
        assignments[0].consensus_group_id, 3,
        "inherited ids must seed the allocator before admission"
    );
}

#[compio::test]
async fn given_missing_placement_summary_when_wire_or_queue_submits_should_reply_58_without_eviction()
 {
    let old_primary = placement_metadata();
    let inherited = old_primary
        .prepare_request(placement_request(1, "inherited", 2))
        .unwrap();
    let metadata = placement_metadata_with_bus(ClientSendSpyBus::default());
    let consensus = metadata.consensus.as_ref().unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &inherited);
    metadata.on_request(placement_request(2, "wire", 1)).await;
    {
        let replies = consensus.message_bus().client_replies.borrow();
        assert_eq!(replies.len(), 1);
        let frame = replies[0].first();
        let header =
            bytemuck::checked::from_bytes::<ReplyHeader>(&frame[..size_of::<ReplyHeader>()]);
        assert_eq!(header.command, Command::Reply);
        assert!(!header.has_committed_result());
        assert_eq!(
            iggy_binary_protocol::result_code(&frame[size_of::<ReplyHeader>()..]),
            Some(IggyError::TransientNotAccepted.as_code())
        );
    }
    let (queued, receiver) =
        consensus::RequestEntry::with_subscriber(placement_request(3, "queued", 1));
    assert!(consensus.push_queued_request(queued).is_ok());
    metadata.drain_request_queue_into_prepares().await;
    let reply = receiver.await.unwrap();
    assert_eq!(
        iggy_binary_protocol::result_code(reply.body()),
        Some(IggyError::TransientNotAccepted.as_code())
    );
    assert!(!reply.header().has_committed_result());
    assert_eq!(metadata.allocator.highest(), 0);
    assert_eq!(
        resolve_acting_user_id(Operation::CreateTopic, 2, &metadata.client_table).unwrap(),
        Some(0)
    );
    assert_eq!(
        resolve_acting_user_id(Operation::CreateTopic, 3, &metadata.client_table).unwrap(),
        Some(0)
    );
}

#[compio::test]
async fn given_wrong_canonical_wal_body_when_refreshing_placement_should_keep_admission_closed() {
    let directory = tempfile::tempdir().unwrap();
    let journal = PrepareJournal::open(&directory.path().join("journal.wal"), 0)
        .await
        .unwrap();
    let source = placement_metadata();
    let expected = source
        .prepare_request(placement_request(1, "expected", 2))
        .unwrap();
    let different = source
        .prepare_request(placement_request(2, "different", 1))
        .unwrap();
    assert_eq!(expected.header().op, different.header().op);
    assert_ne!(expected.header().checksum, different.header().checksum);
    journal.handle().append(different).await.unwrap();
    let mut metadata = placement_metadata();
    metadata.journal = Some(journal);
    metadata
        .consensus
        .as_ref()
        .unwrap()
        .pipeline_message(PlaneKind::Metadata, &expected);
    metadata.refresh_placement().await;
    assert!(matches!(
        metadata.prepare_request(placement_request(3, "blocked", 1)),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(
        metadata.allocator.highest(),
        0,
        "an op-only WAL match must not supply assignment ids"
    );
}

#[compio::test]
async fn given_replaced_pipeline_create_when_refreshing_should_discard_old_summary() {
    let directory = tempfile::tempdir().unwrap();
    let mut metadata = placement_metadata();
    metadata.journal = Some(
        PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap(),
    );
    let consensus = metadata.consensus.as_ref().unwrap();
    let old = metadata
        .prepare_request(placement_request(1, "old", 2))
        .unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &old);
    assert_eq!(
        consensus.rollback_pipelined_prepare(old.header()),
        PrepareRollback::Unwound
    );
    let source = placement_metadata();
    let replacement = source
        .prepare_request(placement_request(2, "replacement", 1))
        .unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &replacement);
    metadata
        .journal
        .as_ref()
        .unwrap()
        .handle()
        .append(replacement)
        .await
        .unwrap();
    assert!(matches!(
        metadata.prepare_request(placement_request(3, "blocked", 1)),
        Err(IggyError::TransientNotAccepted)
    ));
    metadata.refresh_placement().await;
    let next = metadata
        .prepare_request(placement_request(3, "ready", 1))
        .unwrap();
    assert_eq!(
        assigned_views(&next),
        [1],
        "only the replacement's one assignment remains reserved"
    );
}
