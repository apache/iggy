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

#[compio::test]
async fn given_transfer_above_cap_when_file_write_fails_should_refuse_until_retry_succeeds() {
    let dir = tempfile::tempdir().expect("transfer directory");
    let consumers = dir.path().join("consumers");
    let groups = dir.path().join("groups");
    std::fs::File::create(&groups).expect("block group-file creation");
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(consumers.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(groups.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        dir.path()
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 10,
        emptied_at: 0,
        consumers: vec![(7, 1), (8, 2)],
        groups: vec![(9, 3)],
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &wire.encode())
        .await
        .expect_err("incomplete offset state must not advance the commit floor");
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(
        partition.log.segments().len(),
        1,
        "offset staging failure must preserve the existing segment chain"
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    std::fs::remove_file(&groups).expect("remove file-write fault");
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &wire.encode())
        .await
        .expect("retry must install accepted state above the local cap");
    assert_eq!(partition.consensus.commit_min(), 12);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        2
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 10)
            .is_err()
    );
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::ConsumerGroup, 9, 3)
    );
    assert_eq!(
        partition
            .offsets_wire_snapshot_for_test()
            .expect("durable snapshot"),
        vec![(7, 1), (8, 2)]
    );
}

#[compio::test]
async fn durability_combinations_share_body_ownership_and_keep_materialization_thresholds() {
    const SEGMENT_BYTES: u64 = 1024;
    for replicas in [1, 3] {
        for durability in [
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Persisted,
        ] {
            for offset_durability in [
                iggy_common::Durability::Replicated,
                iggy_common::Durability::Persisted,
            ] {
                for byte_threshold in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let (mut partition, _) = recording_partition_at(0, replicas);
                    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
                    partition.runtime_options.durability = durability;
                    partition.runtime_options.consumer_offset_durability = offset_durability;
                    partition.runtime_options.preallocate_segments = Some(false);
                    partition.runtime_options.segment_size =
                        Some(IggyByteSize::from(SEGMENT_BYTES));
                    partition.log.segments_mut()[0].max_size = IggyByteSize::from(SEGMENT_BYTES);
                    partition.open_persistence().await.unwrap();
                    let persistence = partition.persistence.clone();
                    let owned = true;
                    assert_eq!(persistence.is_some(), owned);
                    let mut config = repair_config();
                    config.messages_required_to_save = if byte_threshold { u32::MAX } else { 2 };
                    partition.log.retire_front().unwrap();
                    partition.install_empty_segment(&config, 0).await.unwrap();
                    assert_eq!(partition.log.messages_writers()[0].is_none(), owned);
                    let mut expected = Vec::new();
                    let mut parent = 0;
                    for op in 1..=3 {
                        let namespace = partition.namespace();
                        let request = checksumless_send_request(namespace, 1);
                        let prepare =
                            request.transmute_header(|old, header: &mut PrepareHeader| {
                                header.command = Command::Prepare;
                                header.operation = Operation::SendMessages;
                                header.cluster = TEST_CLUSTER;
                                header.group = namespace.inner();
                                header.op = op;
                                header.parent = parent;
                                header.timestamp = op;
                                header.size = old.size;
                                header.retry_capacity =
                                    u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
                                header.client = AUTO_COMMIT_CLIENT_ID;
                                header.session = 1;
                                header.request = op;
                            });
                        let prepare = partition
                            .stamp_and_append_messages(prepare)
                            .await
                            .unwrap()
                            .prepare;
                        let header = *bytemuck::checked::from_bytes::<PrepareHeader>(
                            &prepare.as_slice()[..size_of::<PrepareHeader>()],
                        );
                        parent = header.checksum;
                        expected
                            .extend_from_slice(&prepare.as_slice()[size_of::<PrepareHeader>()..]);
                        partition.consensus().sequencer().set_sequence(op);
                        assert!(
                            partition.submit_prepare_persistence(prepare, Operation::SendMessages)
                        );
                        if replicas > 1 {
                            assert_eq!(
                                partition.send_prepare_ok(&header).await,
                                !durability.is_persisted()
                            );
                        }
                        if let Some(persistence) = &persistence {
                            persistence
                                .drain_with_timeout(compio::runtime::time::sleep)
                                .await
                                .unwrap();
                            assert!(partition.send_prepare_ok(&header).await);
                        }
                        partition.consensus().advance_commit_max(op);
                        if op == 1 && byte_threshold {
                            config.size_of_messages_required_to_save = IggyByteSize::from(
                                2 * partition.log.journal().info.size.as_bytes_u64(),
                            );
                        }
                        partition.commit_messages(&config, op).await.unwrap();
                        if op == 1 {
                            assert_eq!(
                                partition
                                    .log
                                    .segments()
                                    .iter()
                                    .any(|segment| segment.size.as_bytes_u64() > 0),
                                replicas == 1 && durability.is_persisted(),
                                "replicas={replicas} durability={durability:?} offsets={offset_durability:?} byte_threshold={byte_threshold}",
                            );
                        } else if op == 2 {
                            assert_eq!(partition.log.journal().info.messages_count, 0);
                        }
                    }
                    if let Some(persistence) = &persistence {
                        assert_eq!(
                            persistence.durable_op(),
                            if durability.is_persisted() { 3 } else { 0 }
                        );
                    }
                    partition.flush_committed_messages(&config).await.unwrap();
                    let mut actual = Vec::new();
                    for segment in partition.log.segments() {
                        let public = directory
                            .path()
                            .join(format!("{:020}.log", segment.start_offset));
                        actual.extend(std::fs::read(&public).unwrap());
                        let metadata = std::fs::metadata(public).unwrap();
                        assert_eq!(metadata.len(), segment.size.as_bytes_u64());
                        #[cfg(target_os = "linux")]
                        if owned && metadata.len() > 0 {
                            let wal = directory
                                .path()
                                .join(format!("prepares-{}", partition.created_revision()));
                            assert!(
                                std::fs::read_dir(wal).unwrap().any(|entry| entry
                                    .unwrap()
                                    .metadata()
                                    .unwrap()
                                    .ino()
                                    == metadata.ino())
                            );
                        }
                    }
                    assert_eq!(
                        actual, expected,
                        "replicas={replicas} durability={durability:?} offsets={offset_durability:?} byte_threshold={byte_threshold}"
                    );
                    assert_eq!(partition.log.journal().info.messages_count, 0);
                    if let Some(persistence) = persistence {
                        assert!(persistence.segment_checkpoint().is_some());
                        assert_eq!(persistence.durable_op(), 3);
                    }
                }
            }
        }
    }
}

#[compio::test]
async fn wal_backpressure_preserves_replay_outcomes() {
    for expected_status in [
        0,
        IggyError::TransientNotCommitted.as_code(),
        IggyError::TransientNotAccepted.as_code(),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.open_persistence().await.unwrap();
        let mut request = checksumless_send_request(partition.namespace(), 1);
        request.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(partition.namespace(), 0));
        let header = *request.header();
        let mut cached_reply = None;
        if expected_status == 0 {
            let prepare = PrepareHeader {
                client: header.client,
                session: header.session,
                request: header.request,
                request_checksum: header.request_checksum,
                operation: header.operation,
                op: 1,
                group: header.group,
                ..Default::default()
            };
            let reply = build_reply_message(
                &prepare,
                &send_messages_reply_body(
                    header.group,
                    Some(CommittedBatchStats {
                        producer: None,
                        timestamp: 0,
                        base_offset: 0,
                        message_count: 1,
                        size_bytes: 0,
                    }),
                ),
            );
            cached_reply = Some(
                partition
                    .dedup
                    .commit_partition_reply(header.user_id, header.session, reply)
                    .unwrap()
                    .into_wire_bytes(),
            );
        } else if expected_status == IggyError::TransientNotCommitted.as_code() {
            let prepare =
                request
                    .clone()
                    .transmute_header(|request, prepare: &mut PrepareHeader| {
                        prepare.command = Command::Prepare;
                        prepare.operation = request.operation;
                        prepare.client = request.client;
                        prepare.user_id = request.user_id;
                        prepare.request = request.request;
                        prepare.session = request.session;
                        prepare.request_checksum = request.request_checksum;
                        prepare.retry_capacity =
                            u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
                        prepare.op = 1;
                        prepare.size = request.size;
                    });
            partition
                .consensus()
                .pipeline_message(PlaneKind::Partitions, &prepare);
        }
        let pipeline_len = partition.consensus().pipeline_len();
        partition
            .persistence
            .as_ref()
            .unwrap()
            .exhaust_capacity_for_test();

        partition.on_request(request, None).await;

        let replies = replies.borrow();
        assert_eq!(
            replies.len(),
            1,
            "every retry must receive its known outcome"
        );
        let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_eq!(reply.status, expected_status);
        if let Some(cached) = cached_reply {
            assert!(replies[0].1.shares_allocation(&cached));
        }
        assert_eq!(partition.consensus().pipeline_len(), pipeline_len);
        assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    }
}

#[compio::test]
async fn wal_backpressure_on_one_backup_does_not_stop_the_other_backup() {
    let (mut primary, _) = recording_partition_at(0, 3);
    let namespace = primary.namespace();
    let request = checksumless_send_request(namespace, 1);
    let prepare = request.transmute_header(|old, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = 1;
        header.timestamp = 1;
        header.size = old.size;
    });
    let forwarded = primary
        .stamp_and_append_messages(prepare)
        .await
        .unwrap()
        .prepare;
    let message = Message::<PrepareHeader>::try_from(server_common::iobuf::Owned::copy_from_slice(
        forwarded.as_slice(),
    ))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.open_persistence().await.unwrap();
    backup
        .persistence
        .as_ref()
        .unwrap()
        .exhaust_capacity_for_test();
    let stalled_header = *message.header();
    backup.on_replicate(message).await;
    assert_eq!(backup.consensus().sequencer().current_sequence(), 1);
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 0);
    assert!(
        backup
            .consensus()
            .message_bus()
            .sent_to_replicas
            .borrow()
            .is_empty()
    );
    let message =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(forwarded.as_slice())).unwrap();
    primary.on_replicate(message).await;
    let healthy_copy = {
        let sent = primary.consensus().message_bus().sent_to_replicas.borrow();
        let (_, frame) = sent
            .iter()
            .find(|(target, _)| *target == 2)
            .expect("the primary sends directly to the other backup");
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(frame.as_slice())).unwrap()
    };
    let (mut healthy, _) = recording_partition_at(2, 3);
    healthy.on_replicate(healthy_copy).await;
    assert!(healthy.log.journal().inner.holds_op(1));

    assert!(!backup.register_rebuilt_ack(&stalled_header));
    assert!(backup.pending_persisted_acks.borrow().contains_key(&1));
    backup.drive_persistence().await;
    assert!(
        backup.pending_persisted_acks.borrow().contains_key(&1),
        "the WAL has not accepted the parked ack's prepare yet"
    );
    backup
        .persistence
        .as_ref()
        .unwrap()
        .release_capacity_for_test();
    let request = checksumless_send_request(namespace, 2);
    let prepare = request.transmute_header(|old, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.cluster = TEST_CLUSTER;
        header.group = namespace.inner();
        header.op = 2;
        header.timestamp = 2;
        header.size = old.size;
    });
    let forwarded = primary
        .stamp_and_append_messages(prepare)
        .await
        .unwrap()
        .prepare;
    let message =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(forwarded.as_slice())).unwrap();
    backup.on_replicate(message).await;
    let persistence = backup.persistence.as_ref().unwrap();
    assert_eq!(persistence.head(), 2);
    assert_eq!(backup.consensus().sequencer().current_sequence(), 2);
    assert!(persistence.failure().is_none());
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(persistence.is_durable_through(2));
    backup.drive_persistence().await;
    assert!(
        backup
            .consensus()
            .message_bus()
            .sent_to_replicas
            .borrow()
            .iter()
            .any(|(target, message)| {
                *target == 0
                    && message.len() == size_of::<PrepareOkHeader>()
                    && bytemuck::checked::from_bytes::<PrepareOkHeader>(message.as_slice()).op == 1
            }),
        "the parked ack must be delivered without a prepare retransmit"
    );
}

/// The primary sends prepares before it commits any, so this backup's WAL
/// fills and refuses the last two, which stay journaled. The cluster then
/// commits past the WAL head. The backup must not commit past its WAL
/// head, and its own maintenance must reclaim the WAL so the refused
/// prepares persist and commit, with no capacity released from outside.
#[compio::test]
async fn given_full_backup_wal_when_cluster_commits_past_its_head_should_reclaim_and_catch_up() {
    // Four bodies of this size fit the smallest WAL, and a fifth does not.
    const PAYLOAD_BYTES: usize = 30 * 1024 * 1024;
    const WAL_HEAD: u64 = 4;
    const OPS: u64 = WAL_HEAD + 2;
    const ROUNDS_MAX: usize = 16;
    let payload = vec![0; PAYLOAD_BYTES];
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        // Checkpoints sync the segment paths the layout names.
        let mut config = repair_config();
        config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
        let partition_dir = config.get_partition_path(1, 1, 0);
        std::fs::create_dir_all(&partition_dir).unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        set_offset_dirs_under(&mut backup, std::path::Path::new(&partition_dir));
        backup.set_partition_dir(partition_dir);
        backup.runtime_options.durability = durability;
        backup.log.retire_front().unwrap();
        backup.install_empty_segment(&config, 0).await.unwrap();
        backup
            .open_persistence_with_capacity(
                journal::partition_journal::PARTITION_WAL_CAPACITY_MIN,
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();
        backup.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let completions = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&completions);
        backup.set_persistence_notifier(Rc::new(move |completion| {
            captured.borrow_mut().push(completion);
        }));
        let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

        let mut parent = 0;
        let mut frame_bytes = 0;
        for op in 1..=OPS {
            let prepare = checksummed_segment_prepare(op, parent, op - 1, &payload);
            parent = prepare.header().checksum;
            frame_bytes = prepare.as_slice().len();
            backup.on_replicate(prepare).await;
            backup.commit_journal(&config).await;
            run_shard_round(&mut backup, &completions, &config).await;
        }
        assert_eq!(
            backup.consensus().sequencer().current_sequence(),
            OPS,
            "{durability:?}: the backup journals every prepare"
        );
        assert_eq!(
            persistence.head(),
            WAL_HEAD,
            "{durability:?}: the WAL refuses prepares past its capacity"
        );
        assert!(!persistence.has_capacity(frame_bytes), "{durability:?}");

        // A commit heartbeat: the other backup's acks committed every op.
        backup.consensus().advance_commit_max(OPS);
        backup.commit_journal(&config).await;

        for _ in 0..ROUNDS_MAX {
            assert!(
                backup.consensus().commit_min() <= persistence.head(),
                "{durability:?}: commit {} passed the WAL head {}",
                backup.consensus().commit_min(),
                persistence.head()
            );
            if backup.consensus().commit_min() == OPS {
                break;
            }
            run_shard_round(&mut backup, &completions, &config).await;
            assert!(
                backup.fatal().is_none(),
                "{durability:?}: {:?}",
                backup.fatal()
            );
        }
        assert_eq!(
            (backup.consensus().commit_min(), persistence.head()),
            (OPS, OPS),
            "{durability:?}: wedged with checkpoint {}, WAL capacity {}, checkpoint due {}",
            persistence.checkpoint_op(),
            persistence.has_capacity(frame_bytes),
            backup.needs_persistence_checkpoint()
        );
        assert!(
            persistence.checkpoint_op() >= WAL_HEAD,
            "{durability:?}: only a checkpoint can free the full WAL"
        );
    }
}
