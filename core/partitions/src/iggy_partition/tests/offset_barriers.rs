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
#[allow(clippy::too_many_lines)]
async fn queued_delete_observes_held_no_ack_store_success_and_failures() {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    const MAX_STEPS: usize = 256;
    for delete_ack in [AckLevel::NoAck, AckLevel::Quorum] {
        for (fail_write, fail_barrier) in [(false, false), (true, false), (false, true)] {
            let directory = tempfile::tempdir().unwrap();
            let offset_directory = directory.path().join("consumers");
            let (mut partition, replies) = recording_partition();
            partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
                Some(offset_directory.to_string_lossy().into_owned());
            partition.runtime_options.consumer_offset_durability =
                iggy_common::Durability::Persisted;
            partition.stats.increment_messages_count(1);
            partition.set_io_notifier(
                Rc::new(|_, _| {}),
                crate::largest_legal_job_charge().unwrap(),
            );
            let config = repair_config();
            partition
                .on_request(
                    store_offset_request(
                        CLIENT,
                        1,
                        ConsumerKind::Consumer,
                        CONSUMER,
                        0,
                        AckLevel::NoAck,
                    ),
                    None,
                )
                .await;
            let first = loop {
                match partition.resume_io(&config).await {
                    crate::PartitionIoStep::Progress => {}
                    crate::PartitionIoStep::Ready(plan) => {
                        break partition.capture_io(plan, &config).unwrap().unwrap();
                    }
                    _ => panic!("admitted store did not reach file execution"),
                }
            };
            assert_eq!(
                first.identity.continuation,
                crate::PartitionIoContinuation::NoAck
            );
            partition
                .on_request(
                    delete_offset_request_with_ack(
                        CLIENT,
                        2,
                        ConsumerKind::Consumer,
                        CONSUMER,
                        delete_ack,
                    ),
                    None,
                )
                .await;
            assert!(
                replies.borrow().is_empty(),
                "delete must wait for the new key's predecessor"
            );
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                1
            );
            assert!(
                !partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize))
            );
            assert!(matches!(
                partition.resume_io(&config).await,
                crate::PartitionIoStep::Pending
            ));
            if fail_write {
                std::fs::write(&offset_directory, b"not a directory").unwrap();
            }
            if fail_barrier {
                partition.consumer_offset_dir_sync_fault.set(Some(0));
            }
            let result = first.job.execute().await;
            partition.accept_io(first.identity, result).unwrap();
            assert!(replies.borrow().is_empty());
            assert_eq!(
                partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize)),
                !fail_write
            );
            if fail_write {
                assert!(
                    partition
                        .consumer_offset_capacity_for(ConsumerKind::Consumer)
                        .is_stranded(CONSUMER)
                );
                std::fs::remove_file(&offset_directory).unwrap();
                std::fs::create_dir(&offset_directory).unwrap();
            }
            for _ in 0..MAX_STEPS {
                let mut loopbacks = Vec::new();
                partition.consensus.drain_loopback_into(&mut loopbacks);
                for ack in loopbacks {
                    partition
                        .on_ack(ack.try_into_typed().unwrap(), &config)
                        .await;
                }
                match partition.resume_io(&config).await {
                    crate::PartitionIoStep::Progress => {}
                    crate::PartitionIoStep::Ready(plan) => {
                        let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                        let directory_barrier =
                            matches!(&captured.job, crate::PartitionIoJob::OffsetDirectories(_));
                        let result = captured.job.execute().await;
                        partition.accept_io(captured.identity, result).unwrap();
                        if directory_barrier {
                            partition.consumer_offset_dir_sync_fault.set(None);
                        }
                    }
                    crate::PartitionIoStep::Pending if replies.borrow().len() == 2 => break,
                    _ => panic!("ordered offset work stalled before both replies"),
                }
            }
            assert_eq!(replies.borrow().len(), 2);
            let statuses: Vec<_> = replies
                .borrow()
                .iter()
                .map(|(_, reply)| {
                    bytemuck::checked::from_bytes::<ReplyHeader>(
                        &reply.as_slice()[..size_of::<ReplyHeader>()],
                    )
                    .status
                })
                .collect();
            assert_eq!(statuses[0] == 0, !fail_write && !fail_barrier);
            assert_eq!(
                statuses[1], 0,
                "delete must clean visible or stranded predecessor state"
            );
            assert!(!offset_directory.join(CONSUMER.to_string()).exists());
            assert!(
                !partition
                    .consumer_offsets
                    .pin()
                    .contains_key(&(CONSUMER as usize))
            );
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                0
            );
            assert!(
                partition.fatal().is_none(),
                "NoAck failure remains request-scoped"
            );
        }
    }
}

#[compio::test]
async fn given_offset_phases_when_barrier_settles_should_publish_ordered_effects_and_release_replies()
 {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    const INITIAL_OFFSET: u64 = 9;
    let kind = ConsumerKind::Consumer;
    'cases: for (fail_barrier, fail_write) in [(false, false), (true, false), (false, true)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(CONSUMER.to_string());
        let (mut partition, bus_replies) = recording_partition_at(0, 3);
        partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
            Some(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        let initial = PendingConsumerOffsetCommit::upsert(kind, CONSUMER, INITIAL_OFFSET);
        partition
            .persist_consumer_offset_commit(initial)
            .await
            .unwrap();
        partition.apply_consumer_offset_commit(initial);
        let mutations = [
            PendingConsumerOffsetCommit::upsert(kind, CONSUMER, 3),
            PendingConsumerOffsetCommit::upsert(kind, CONSUMER + 1, 4),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER + 2, 5),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER, 2),
            PendingConsumerOffsetCommit::delete(kind, CONSUMER),
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, CONSUMER, 1),
        ];
        let mut receivers = Vec::new();
        let mut parent = 0;
        for (index, pending) in mutations.into_iter().enumerate() {
            let op = u64::try_from(index + 1).unwrap();
            let client = if pending.auto_commit {
                AUTO_COMMIT_CLIENT_ID
            } else {
                CLIENT
            };
            let request = match pending.mutation {
                PendingConsumerOffsetMutation::Upsert(offset) => store_offset_request(
                    client,
                    op,
                    kind,
                    pending.consumer_id,
                    offset,
                    AckLevel::Quorum,
                ),
                PendingConsumerOffsetMutation::Delete => {
                    delete_offset_request(client, op, CONSUMER)
                }
            };
            let prepare = request.transmute_header(|request, header: &mut PrepareHeader| {
                *header = PrepareHeader {
                    command: Command::Prepare,
                    operation: request.operation,
                    cluster: TEST_CLUSTER,
                    group: request.group,
                    size: request.size,
                    client,
                    user_id: request.user_id,
                    session: request.session,
                    request: op,
                    retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
                    op,
                    parent,
                    ..PrepareHeader::default()
                };
                header.checksum = header.identity_checksum();
            });
            parent = prepare.header().checksum;
            let (entry, receiver) = PipelineEntry::with_subscriber(*prepare.header());
            receivers.push(receiver);
            partition
                .consensus
                .with_pipeline_mut(|pipeline| pipeline.push(entry));
            partition
                .log
                .journal()
                .inner
                .append(prepare.into_frozen())
                .await
                .unwrap();
            match pending.mutation {
                PendingConsumerOffsetMutation::Upsert(offset) => {
                    partition.stage_consumer_offset_upsert(
                        op,
                        kind,
                        pending.consumer_id,
                        offset,
                        pending.auto_commit,
                    );
                }
                PendingConsumerOffsetMutation::Delete => {
                    partition.stage_consumer_offset_delete(op, kind, CONSUMER);
                }
            }
        }
        let through_op = u64::try_from(mutations.len()).unwrap();
        partition.consensus.restore_commit_state(0, through_op);
        let config = repair_config();
        let selected = partition.select_persistable_commits(&config);
        assert_eq!(selected.len(), mutations.len());
        assert!(partition.select_commit(selected, &config, true));
        if fail_barrier {
            partition
                .consumer_offset_dir_sync_fault
                .set(Some(kind.index()));
        }
        let mut written_offsets = Vec::new();
        let mut deletes = 0;
        let mut barriers = 0;
        let mut batches = 0;
        loop {
            match partition.drive_commit().unwrap() {
                CommitStep::Progress => {}
                CommitStep::Pending => panic!("the test owns every submitted job"),
                CommitStep::Publish => break,
                CommitStep::Ready => {
                    assert_eq!(partition.consensus.pipeline_len(), mutations.len());
                    assert!(partition.consensus.with_pipeline(|pipeline| {
                        (1..=through_op)
                            .all(|op| pipeline.entry_by_op(op).unwrap().has_reply_sender())
                    }));
                    let job = partition.capture_commit_phase(&config).unwrap().unwrap();
                    let writes_offset = matches!(
                        &job,
                        crate::io::PartitionIoJob::OffsetWrite(_)
                            | crate::io::PartitionIoJob::OffsetBatch(_)
                    );
                    if let crate::io::PartitionIoJob::OffsetBatch(writes) = &job {
                        assert_eq!(writes.len(), 3);
                        batches += 1;
                    }
                    let deletes_offset = matches!(&job, crate::io::PartitionIoJob::OffsetDelete(_));
                    let syncs_directory =
                        matches!(&job, crate::io::PartitionIoJob::OffsetDirectories(_));
                    let mut result = job.execute().await;
                    if fail_write {
                        let crate::PartitionIoResult::OffsetBatch(results) = &mut result else {
                            panic!("the first file phase must batch independent offsets");
                        };
                        results[0].outcome = Err(IggyError::CannotWriteToFile);
                        results[0].write_error =
                            Some(std::io::Error::other("injected first write failure"));
                    }
                    assert_eq!(partition.consensus.commit_min(), 0);
                    assert_eq!(
                        partition
                            .durable_consumer_offsets
                            .get(kind, CONSUMER)
                            .unwrap()
                            .committed_offset,
                        INITIAL_OFFSET
                    );
                    for receiver in &mut receivers {
                        assert!(futures::poll!(&mut *receiver).is_pending());
                    }
                    if writes_offset {
                        written_offsets.push(
                            crate::offset_storage::read_offset_max(
                                path.to_str().unwrap(),
                                u64::MIN,
                            )
                            .await
                            .unwrap()
                            .offset,
                        );
                    }
                    if deletes_offset {
                        deletes += 1;
                        assert!(!path.exists());
                    }
                    if syncs_directory {
                        barriers += 1;
                    }
                    if let Err(error) = partition.accept_commit_phase(result) {
                        if fail_write {
                            assert!(matches!(error, IggyError::CannotWriteToFile));
                            for consumer in [CONSUMER + 1, CONSUMER + 2] {
                                let path = directory.path().join(consumer.to_string());
                                assert!(
                                    path.is_file(),
                                    "successful sibling offset {consumer} must remain on disk"
                                );
                            }
                            let pending = partition.pending_commit.as_ref().unwrap();
                            assert_eq!(pending.offset_effects.len(), 2);
                            assert_eq!(
                                pending.next_entry, 0,
                                "the fence must name the first failed op"
                            );
                            assert_eq!(
                                partition.consumer_offset_dirs_touched[kind.index()]
                                    .get()
                                    .unwrap()
                                    .0,
                                3
                            );
                            assert_eq!(partition.consensus.commit_min(), 0);
                            assert!(bus_replies.borrow().is_empty());
                            for receiver in &mut receivers {
                                assert!(futures::poll!(&mut *receiver).is_pending());
                            }
                            continue 'cases;
                        }
                        assert!(fail_barrier && syncs_directory, "{error}");
                        break;
                    }
                }
            }
        }
        assert_eq!(
            written_offsets,
            vec![3, 1],
            "coverage must follow the reset and deletion"
        );
        assert_eq!(batches, 1, "independent keys must share one file job");
        assert_eq!(deletes, 1);
        assert_eq!(barriers, 1);
        assert!(bus_replies.borrow().is_empty());
        if fail_barrier {
            assert!(partition.fatal().is_some());
            assert_eq!(partition.consensus.commit_min(), 0);
            assert_eq!(partition.consensus.pipeline_len(), mutations.len());
            assert_eq!(
                partition.log.journal().inner.resident_count(),
                mutations.len()
            );
            assert_eq!(
                partition
                    .durable_consumer_offsets
                    .get(kind, CONSUMER)
                    .unwrap()
                    .committed_offset,
                INITIAL_OFFSET
            );
            for receiver in &mut receivers {
                assert!(futures::poll!(&mut *receiver).is_pending());
            }
        } else {
            assert!(matches!(
                partition.resume_io(&config).await,
                crate::PartitionIoStep::Progress
            ));
            assert_eq!(partition.consensus.commit_min(), through_op);
            assert_eq!(partition.consensus.pipeline_len(), 0);
            assert!(partition.pending_consumer_offset_commits.is_empty());
            assert_eq!(
                partition
                    .durable_consumer_offsets
                    .get(kind, CONSUMER)
                    .unwrap()
                    .committed_offset,
                1
            );
            for mut receiver in receivers {
                let std::task::Poll::Ready(Ok(reply)) = futures::poll!(&mut receiver) else {
                    panic!("one continuation must deliver the whole committed reply batch");
                };
                assert_eq!(reply.header().status, 0);
            }
        }
    }
}

/// `AckLevel::NoAck` stores apply on the primary only and never replicate, so
/// which replicas hold an offset is not agreed and a committed delete can
/// legitimately find nothing. Erroring on that fails the committed apply,
/// fences the partition, then crash-loops on every replay of the op.
///
/// Both kinds, because they are separate maps with separate directories.
#[compio::test]
async fn given_absent_offset_file_when_delete_commits_should_skip_directory_sync() {
    for (op, kind) in [
        (1, ConsumerKind::Consumer),
        (2, ConsumerKind::ConsumerGroup),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut partition, sent) = recording_partition_at(0, 3);
        let missing = Some(dir.path().join("missing").to_string_lossy().into_owned());
        partition.consumer_offset_dirs[ConsumerKind::Consumer.index()].clone_from(&missing);
        partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = missing;
        if kind == ConsumerKind::ConsumerGroup {
            install_test_owner(&mut partition, 7, 42).await;
        }
        partition.stage_consumer_offset_delete(op, kind, 7);
        if kind == ConsumerKind::ConsumerGroup {
            partition
                .pending_consumer_offset_commits
                .get_mut(&op)
                .unwrap()
                .owner_generation = 1;
        }
        partition.consensus.sequencer().set_sequence(op);
        partition.consensus.advance_commit_max(op);
        let header = PrepareHeader {
            retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
            session: 1,
            op,
            operation: Operation::DeleteConsumerOffset,
            client: 42,
            request: 1,
            ..Default::default()
        };
        partition
            .handle_committed_entries(vec![header], &repair_config(), true)
            .await;
        assert!(partition.fatal.is_none(), "{kind:?} delete must not fence");
        assert_eq!(partition.consensus.commit_min(), op);
        assert_eq!(partition.offset_dir_sync_count.get(), 0);
        assert_eq!(sent.borrow().len(), 1);
    }
}

#[compio::test]
async fn given_one_offset_directory_sync_failure_when_flushing_should_sync_the_other_directory() {
    let dir = tempfile::tempdir().unwrap();
    let invalid = dir.path().join("file");
    std::fs::write(&invalid, b"not a directory").unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(invalid.join("child").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    for kind in ConsumerKind::ALL {
        partition.mark_consumer_offset_dir_dirty(kind);
    }
    assert_eq!(
        partition
            .flush_consumer_offset_directories_for([true; ConsumerKind::COUNT])
            .await,
        [true, false, false]
    );
    assert!(partition.consumer_offset_dirs_dirty[0].get());
    assert!(!partition.consumer_offset_dirs_dirty[1].get());
    assert!(!partition.consumer_offset_dirs_dirty[2].get());
    assert_eq!(partition.offset_dir_sync_count.get(), 2);
}

#[compio::test]
async fn given_covered_auto_commit_when_persisting_should_not_sync_directory() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let path = dir.path().join("7");
    persist_offset(path.to_str().unwrap(), 10, true)
        .await
        .unwrap();
    for offset in [5, 7] {
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                7,
                offset,
            ))
            .await
            .unwrap();
        assert!(!partition.consumer_offset_dirs_dirty[0].get());
    }
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            7,
            11,
        ))
        .await
        .unwrap();
    assert!(partition.consumer_offset_dirs_dirty[0].get());
}
