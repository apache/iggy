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

/// A backup's `PrepareOk` for `header`, in the primary's current view.
fn backup_prepare_ok(
    partition: &IggyPartition<RecordingBus>,
    replica: u8,
    header: &PrepareHeader,
) -> Message<PrepareOkHeader> {
    let consensus = partition.consensus();
    Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>()).transmute_header(
        |_, ack: &mut PrepareOkHeader| {
            ack.command = Command::PrepareOk;
            ack.cluster = consensus.cluster();
            ack.replica = replica;
            ack.view = consensus.view();
            ack.op = header.op;
            ack.prepare_checksum = header.checksum;
            ack.operation = header.operation;
            ack.group = header.group;
            ack.size = u32::try_from(size_of::<PrepareOkHeader>()).unwrap();
            ack.seal();
        },
    )
}

/// A two-replica primary whose WAL refused its own prepare for op 1, which
/// is journaled and pipelined with no WAL entry and no armed self-ack.
#[allow(clippy::future_not_send)]
async fn primary_with_refused_offset_prepare(
    directory: &Path,
) -> (IggyPartition<RecordingBus>, SentFrames, PrepareHeader) {
    let (mut partition, replies) = recording_partition_at(0, 2);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.join("consumers").to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    // Exhaust WAL capacity between admission and the submit under
    // `write_lock`, so the WAL refuses the prepare.
    let write_lock = Arc::clone(&partition.write_lock);
    let parked = write_lock.lock().await;
    let request = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    let mut admission = Box::pin(partition.on_request(request, None));
    assert!(futures::poll!(&mut admission).is_pending());
    persistence.exhaust_capacity_for_test();
    drop(parked);
    admission.await;
    persistence.release_capacity_for_test();
    let header = partition
        .consensus()
        .pipeline_head_header()
        .expect("op 1 was admitted");
    assert_eq!(
        persistence.head(),
        0,
        "the WAL refused the primary's own prepare"
    );
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    (partition, replies, header)
}

fn answer_partition_heartbeat(
    partition: &IggyPartition<RecordingBus>,
    action: &consensus::VsrAction,
) {
    if let consensus::VsrAction::SendCommit {
        view,
        group,
        timestamp_monotonic,
        ..
    } = action
    {
        let reply = Message::<CommitReplyHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut CommitReplyHeader| {
                header.command = Command::CommitReply;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.cluster = partition.consensus().cluster();
                header.view = *view;
                header.replica = 1;
                header.group = *group;
                header.partition_incarnation = partition.consensus().partition_incarnation();
                header.timestamp_monotonic = *timestamp_monotonic;
                header.seal();
            },
        );
        partition.consensus().handle_commit_reply(reply.header());
    }
}

#[compio::test]
async fn given_an_armed_self_ack_when_the_local_write_is_retried_should_not_send_a_second_prepare_ok()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .pending_persisted_acks
        .borrow_mut()
        .insert(header.op, header);
    partition.persist_repaired_prefix();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();

    partition.retry_local_write(header.op).await;
    partition.drive_persistence().await;
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert_eq!(
        acknowledgments.len(),
        1,
        "the armed self-ack fires once, so the retry must not ack on its own"
    );
}

#[compio::test]
async fn given_a_wal_still_full_at_the_first_retry_when_capacity_frees_should_resubmit_and_commit()
{
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    persistence.exhaust_capacity_for_test();

    let mut retries = 0;
    for _ in 0..consensus::TimeoutManager::PREPARE_TICKS * 200 {
        if retries == 1 {
            persistence.release_capacity_for_test();
        }
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            answer_partition_heartbeat(&partition, &action);
            if let consensus::VsrAction::RetryLocalWrite { op } = action {
                partition.retry_local_write(op).await;
                retries += 1;
            }
        }
        commit_recorded_loopback(&mut partition).await;
    }
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    commit_recorded_loopback(&mut partition).await;

    assert!(
        retries > 1,
        "the timer must keep retrying, retries={retries}"
    );
    assert_eq!(
        persistence.head(),
        header.op,
        "a retry after capacity frees must re-submit the op the full WAL refused"
    );
    assert_eq!(partition.consensus().commit_min(), header.op);
}

#[compio::test]
async fn given_a_self_ack_queued_in_loopback_when_the_prepare_timeout_keeps_firing_should_not_queue_another()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    partition.retry_local_write(header.op).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    partition.drive_persistence().await;

    for _ in 0..consensus::TimeoutManager::PREPARE_TICKS * 40 {
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            answer_partition_heartbeat(&partition, &action);
            if let consensus::VsrAction::RetryLocalWrite { op } = action {
                partition.retry_local_write(op).await;
            }
        }
    }
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert_eq!(
        acknowledgments.len(),
        1,
        "an undrained self-ack already carries the primary's vote"
    );
}

#[compio::test]
async fn given_a_replica_that_lost_primacy_when_the_local_write_is_retried_should_neither_submit_nor_ack()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    partition.consensus.set_view(1);
    assert!(!partition.consensus().is_primary());

    partition.retry_local_write(header.op).await;
    assert_eq!(persistence.head(), 0, "a non-primary must not re-submit");
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(acknowledgments.is_empty());
}

#[compio::test]
async fn given_a_wal_that_refused_the_primary_prepare_when_the_prepare_timeout_fires_should_retry_locally_and_commit()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies, header) =
        Box::pin(primary_with_refused_offset_prepare(directory.path())).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    // Two replicas need both votes, so the backup's ack alone commits nothing.
    partition
        .on_ack(backup_prepare_ok(&partition, 1, &header), &repair_config())
        .await;
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(partition.consensus().commit_min(), 0);

    let mut retried = false;
    for _ in 0..=consensus::TimeoutManager::PREPARE_TICKS {
        for action in partition.consensus().tick(PlaneKind::Partitions) {
            match action {
                consensus::VsrAction::RetryLocalWrite { op } => {
                    partition.retry_local_write(op).await;
                    retried = true;
                }
                consensus::VsrAction::RetransmitPrepares { targets } => {
                    assert!(
                        targets.iter().all(|(_, replicas)| !replicas.contains(&0)),
                        "a send to self is a no-op, so it cannot be the remedy: {targets:?}"
                    );
                }
                _ => {}
            }
        }
    }
    assert!(
        retried,
        "the prepare timeout must retry the primary's own write"
    );
    assert_eq!(
        persistence.head(),
        header.op,
        "the retry re-submitted op 1 to the WAL"
    );

    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(partition.consensus().commit_min(), header.op);
    assert_eq!(
        replies.borrow().len(),
        1,
        "the client learns the outcome from a reply"
    );
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
}

#[compio::test]
async fn given_replicated_partition_when_quorum_offset_is_stored_should_enter_vsr() {
    let (mut partition, sent_to_clients) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;

    assert_eq!(partition.consensus().pipeline_len(), 1);
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0,
        "durable membership begins at commit"
    );
    assert!(sent_to_clients.borrow().is_empty());
}

#[compio::test]
async fn given_pending_offset_prepare_when_view_changes_should_rebuild_capacity_from_journal() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);

    partition.pending_consumer_offset_commits.clear();
    partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .set_pending_count(7, 0);
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();

    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 8)
            .is_err(),
        "the retained prepare must keep the only slot reserved"
    );
}

#[test]
fn given_phantom_and_eager_offsets_when_snapshotting_should_export_committed_durable_value_only() {
    let (partition, _) = recording_partition();
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 9, String::new()),
    );
    partition.consumer_offsets.pin().insert(
        8,
        ConsumerOffset::new(ConsumerKind::Consumer, 8, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 5, 5);

    assert_eq!(
        partition
            .offsets_wire_snapshot_for_test()
            .expect("durable key exists in live map"),
        vec![(7, 5)],
        "the eager value and the follower-local phantom must not enter the artifact"
    );
}

#[test]
fn given_durable_offset_missing_from_live_map_when_snapshotting_should_refuse() {
    let (partition, _) = recording_partition();
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 5, 5);
    assert!(matches!(
        partition.offsets_wire_snapshot_for_test(),
        Err(
            crate::state_transfer::PartitionTransferUnavailable::ConsumerOffsetStateInconsistent {
                kind: ConsumerKind::Consumer,
                consumer_id: 7,
            }
        )
    ));
}

#[compio::test]
async fn given_no_reply_owner_when_partition_commits_should_not_fire_http_slot() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        Rc::clone(&bus),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition: IggyPartition<Rc<IggyMessageBus>> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    let mut request = checksumless_send_request(namespace, 1);
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, 0));
    let header = *request.header();
    bus.clients().insert_in_process(header.client).unwrap();
    let (_slot, mut receiver) = bus
        .clients()
        .install_reply_slot(header.client, header.request)
        .unwrap();
    partition.on_request(request.clone(), None).await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(partition.consensus.commit_min(), 1);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "a committed reply without its owner must not fill an HTTP slot"
    );
    let (reply, answered) = consensus::oneshot_channel();
    partition.on_request(request, Some(reply)).await;
    let replay = answered.await.unwrap();
    assert_eq!(replay.header().status, 0);
    assert_eq!(replay.header().op, 1);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "the legitimate replay answers its carried receiver"
    );
}

#[compio::test]
async fn given_no_reply_owner_when_partition_denies_should_not_fire_http_slot() {
    const CLIENT: u128 = 42;
    const REQUEST: u64 = 7;
    let bus = Rc::new(IggyMessageBus::new(0));
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        1,
        namespace.inner(),
        Rc::clone(&bus),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition: IggyPartition<Rc<IggyMessageBus>> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    bus.clients().insert_in_process(CLIENT).unwrap();
    let (_slot, mut receiver) = bus.clients().install_reply_slot(CLIENT, REQUEST).unwrap();
    partition
        .on_request(delete_offset_request(CLIENT, REQUEST, 5), None)
        .await;
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert!(
        matches!(receiver.try_recv(), Ok(None)),
        "an originless denial must not take another request's slot"
    );
    let (reply, answered) = consensus::oneshot_channel();
    partition
        .on_request(delete_offset_request(CLIENT, REQUEST, 5), Some(reply))
        .await;
    assert_eq!(
        answered.await.unwrap().header().status,
        IggyError::ConsumerOffsetNotFound(0).as_code()
    );
    assert!(matches!(receiver.try_recv(), Ok(None)));
}

/// Deleting a consumer offset that was never stored must answer with a
/// typed deny reply (empty body, `status` = `ConsumerOffsetNotFound`,
/// `op` 0) before consensus: nothing may enter the pipeline, and an
/// awaited client write must fail fast instead of waiting out its reply
/// timeout. Once the offset exists, the same request must pass the gate
/// into the pipeline without a deny.
#[compio::test]
async fn on_request_delete_of_missing_offset_replies_typed_deny() {
    let (mut partition, sent_to_clients) = recording_partition();
    let client_id: u128 = 42;
    let consumer_id: u32 = 5;

    partition
        .on_request(delete_offset_request(client_id, 7, consumer_id), None)
        .await;

    {
        let sent = sent_to_clients.borrow();
        assert_eq!(sent.len(), 1, "exactly one deny reply");
        let (reply_client, frame) = &sent[0];
        assert_eq!(*reply_client, client_id);
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("deny frame starts with a valid reply header");
        assert_eq!(header.command, Command::Reply);
        assert_eq!(
            header.status,
            IggyError::ConsumerOffsetNotFound(0).as_code()
        );
        assert_eq!(header.op, 0, "a deny commits nothing");
        assert_eq!(header.request, 7);
        assert_eq!(
            header.size as usize,
            std::mem::size_of::<ReplyHeader>(),
            "deny reply body must be empty"
        );
    }
    assert_eq!(
        partition.consensus().pipeline_len(),
        0,
        "denied delete must not replicate"
    );
    assert!(partition.pending_consumer_offset_commits.is_empty());

    // Existing offset: the gate passes and the delete enters the pipeline.
    partition.consumer_offsets.pin().insert(
        consumer_id as usize,
        ConsumerOffset::new(ConsumerKind::Consumer, consumer_id, 3, String::new()),
    );
    partition
        .on_request(delete_offset_request(client_id, 8, consumer_id), None)
        .await;
    assert_eq!(
        partition.consensus().pipeline_len(),
        1,
        "existing offset delete must replicate"
    );
}

#[compio::test]
async fn on_request_delete_on_stale_backup_replies_routing_denial_before_not_found() {
    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        let (mut partition, sent_to_clients) = recording_partition_at(1, 3);
        if transferring {
            partition.consensus.begin_state_transfer_await();
        }
        let client_id = 42;

        partition
            .on_request(delete_offset_request(client_id, 7, 5), None)
            .await;

        let sent = sent_to_clients.borrow();
        assert_eq!(sent.len(), 1, "exactly one routing denial");
        let (reply_client, frame) = &sent[0];
        assert_eq!(*reply_client, client_id);
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("deny frame starts with a valid reply header");
        assert_eq!(header.status, expected.as_code());
        assert_eq!(header.op, 0, "the backup admitted nothing");
        assert_eq!(
            partition.consensus().pipeline_len(),
            0,
            "a backup must not replicate the delete"
        );
    }
}
