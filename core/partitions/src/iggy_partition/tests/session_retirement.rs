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
use std::rc::Rc;

use consensus::LocalPipeline;
use iggy_binary_protocol::{
    ConsumerGroupOwner, Operation, PrepareHeader, ReplyHeader, RoutedRequestHeader,
    requests::{
        partitions::{
            InstallConsumerGroupOwnerRequest, RetireConsumerGroupOwnersRequest,
            TransitionPartitionHistoryRequest,
        },
        system::SessionIdentity,
    },
};
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyError};
use server_common::Message;

use crate::{PollingConsumer, RetirementDebtStatus, iggy_partition::PendingCheckpoint};

/// Pins the barrier count of a burst of ended sessions: the sweep sends the
/// oldest `MAX_SESSIONS_PER_RETIREMENT` identities in one barrier, so one
/// partition commits `ceil(K / MAX_SESSIONS_PER_RETIREMENT)` barriers for K
/// sessions, and each commit releases the dedup slots of its whole batch.
#[compio::test]
async fn given_more_ended_sessions_than_one_batch_when_retiring_should_commit_one_barrier_per_batch()
 {
    const BATCH: usize = iggy_binary_protocol::requests::system::MAX_SESSIONS_PER_RETIREMENT;
    const SESSIONS: usize = BATCH + 1;
    const ENDING_FRONTIER: u64 = 10;
    let (mut partition, _replies) = recording_partition_at(0, 3);
    partition.set_dedup_clients_max(SESSIONS);
    let mut op = 0;
    let identities: Vec<SessionIdentity> = (1..=SESSIONS)
        .map(|index| SessionIdentity {
            client_id: u128::try_from(index).unwrap(),
            session: 1,
            metadata_watermark: ENDING_FRONTIER + u64::try_from(index).unwrap(),
        })
        .collect();
    for identity in &identities {
        let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.client = identity.client_id;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(partition.namespace(), 0));
        partition.on_request(send, None).await;
        op += 1;
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&repair_config()).await;
    }
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(partition.dedup.count(), SESSIONS);

    let mut barriers = 0;
    for batch in identities.chunks(BATCH) {
        let held = partition.dedup.count();
        partition
            .on_request(retire_sessions_request(partition.namespace(), batch), None)
            .await;
        assert_eq!(
            partition.dedup.count(),
            held,
            "a prepared barrier must not release capacity"
        );
        assert!(
            batch
                .iter()
                .all(|identity| !partition.session_retired(*identity))
        );
        op += 1;
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&repair_config()).await;
        barriers += 1;
        assert_eq!(partition.consensus.commit_min(), op);
        assert_eq!(partition.dedup.count(), held - batch.len());
        assert!(
            batch
                .iter()
                .all(|identity| partition.session_retired(*identity))
        );
        assert!(
            identities[(barriers * BATCH).min(SESSIONS)..]
                .iter()
                .all(|identity| !partition.session_retired(*identity)),
            "a barrier must not retire a session outside its batch"
        );
    }
    assert_eq!(barriers, SESSIONS.div_ceil(BATCH));
    assert_eq!(partition.dedup.count(), 0);
}

#[compio::test]
async fn given_ended_sessions_when_retiring_batch_should_use_one_partition_commit() {
    const LIVE_CLIENT: u128 = 3;
    let (mut partition, _) = recording_partition_at(0, 3);
    for client_id in 1..=LIVE_CLIENT {
        let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.client = client_id;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(partition.namespace(), 0));
        partition.on_request(send, None).await;
    }
    partition.consensus.advance_commit_max(3);
    partition.commit_journal(&repair_config()).await;
    let identities = [
        SessionIdentity {
            client_id: 1,
            session: 1,
            metadata_watermark: 10,
        },
        SessionIdentity {
            client_id: 2,
            session: 1,
            metadata_watermark: 11,
        },
    ];
    partition
        .on_request(
            retire_sessions_request(partition.namespace(), &identities),
            None,
        )
        .await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 4);
    partition.consensus.advance_commit_max(4);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.dedup.count(),
        1,
        "one barrier must retire both ended sessions"
    );
    assert!(
        partition.dedup.contains(LIVE_CLIENT),
        "active sessions keep their retry receipts"
    );
    for identity in identities {
        assert!(partition.session_retired(identity));
    }
}

#[compio::test]
async fn retire_session_fence_is_durable_before_metadata_can_finalize() {
    for (durability, consumer_offset_durability) in [
        (
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Replicated,
        ),
        (
            iggy_common::Durability::Persisted,
            iggy_common::Durability::Replicated,
        ),
        (
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Persisted,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(0, 1);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.runtime_options.consumer_offset_durability = consumer_offset_durability;
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        // Hold the writer so that no fence can become durable.
        persistence.sync();
        assert!(persistence.start());
        let identity = SessionIdentity {
            client_id: 1,
            session: 1,
            metadata_watermark: 1,
        };
        partition
            .on_request(
                retire_session_request(partition.namespace(), identity),
                None,
            )
            .await;
        let header = partition.consensus.pipeline_head_header().unwrap();
        assert_eq!(header.operation, Operation::RetireSession);
        commit_recorded_loopback(&mut partition).await;
        assert!(
            partition
                .pending_persisted_acks
                .borrow()
                .contains_key(&header.op),
            "{durability:?} messages, {consumer_offset_durability:?} offsets"
        );
        assert_eq!(partition.consensus.commit_min(), 0);
        assert!(!partition.session_retired(identity));
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_durable(&header));
        partition.drive_persistence().await;
        commit_recorded_loopback(&mut partition).await;
        assert!(partition.session_retired(identity));
        let namespace = partition.namespace();
        drop(partition);
        drop(persistence);
        let (recovered, prepares) = crate::persistence::PartitionPersistence::open_with_storage(
            &directory.path().join("prepares-0"),
            namespace.inner(),
            0,
            journal::durable_storage::DiskStorage,
        )
        .await
        .unwrap();
        assert!(recovered.is_durable(&header));
        assert!(
            prepares.iter().any(|prepare| *prepare.header() == header),
            "recovery must retain the barrier metadata finalization relies on"
        );
    }
}

#[compio::test]
async fn given_interleaved_sends_and_retirements_when_committing_should_preserve_live_receipts() {
    const CLIENTS: u128 = 3;
    const ENDING_FRONTIER: u64 = 10;
    let (mut partition, replies) = recording_partition_at(0, 3);
    for client_id in 1..=CLIENTS {
        let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.client = client_id;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(partition.namespace(), 0));
        partition.on_request(send, None).await;
    }
    partition
        .consensus
        .advance_commit_max(u64::try_from(CLIENTS).unwrap());
    partition.commit_journal(&repair_config()).await;
    for client_id in 1..CLIENTS {
        let identity = SessionIdentity {
            client_id,
            session: 1,
            metadata_watermark: ENDING_FRONTIER + u64::try_from(client_id).unwrap(),
        };
        partition
            .on_request(
                retire_session_request(partition.namespace(), identity),
                None,
            )
            .await;
        if client_id == 1 {
            let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
                |header, next: &mut RoutedRequestHeader| {
                    *next = header;
                    next.client = CLIENTS;
                    next.request = 2;
                },
            );
            send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
                .copy_from_slice(&build_segment_record(partition.namespace(), 0));
            partition.on_request(send, None).await;
        }
    }
    let committed_op = u64::try_from(CLIENTS * 2).unwrap();
    partition.consensus.advance_commit_max(committed_op);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(partition.consensus.commit_min(), committed_op);
    assert_eq!(partition.dedup.count(), 1);
    assert!(partition.dedup.contains(CLIENTS));
    assert_eq!(
        replies.borrow().len(),
        usize::try_from(CLIENTS + 1).unwrap()
    );
    for client_id in 1..CLIENTS {
        assert!(partition.session_retired(SessionIdentity {
            client_id,
            session: 1,
            metadata_watermark: ENDING_FRONTIER + u64::try_from(client_id).unwrap(),
        }));
    }
}

#[compio::test]
async fn given_retirement_debt_when_session_requests_arrive_should_refuse_them_until_the_payment_commits()
 {
    const DEBT_OP: u64 = 40;
    const FINALIZED_CLIENT: u128 = 1;
    const NEW_CLIENT: u128 = 2;
    let (mut partition, _replies) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    let send = |client_id: u128, metadata_watermark: u64| {
        let mut send = checksumless_send_request(namespace, 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.client = client_id;
                next.metadata_watermark = metadata_watermark;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(namespace, 0));
        send
    };
    partition.on_request(send(FINALIZED_CLIENT, 0), None).await;
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;

    assert_eq!(
        partition.observe_retirement_debt(DEBT_OP),
        RetirementDebtStatus::Unpaid(Some(vec![(FINALIZED_CLIENT, 1)]))
    );
    // The first request is the committed one's retry, a cached hit.
    for request in [send(FINALIZED_CLIENT, 0), send(NEW_CLIENT, DEBT_OP)] {
        let (sender, receiver) = consensus::oneshot_channel();
        partition.on_request(request, Some(sender)).await;
        assert_eq!(
            receiver.await.unwrap().header().status,
            IggyError::TransientNotAccepted.as_code()
        );
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);

    partition
        .on_request(
            retirement_payment_request(
                namespace,
                vec![SessionIdentity {
                    client_id: FINALIZED_CLIENT,
                    session: 1,
                    metadata_watermark: DEBT_OP,
                }],
                DEBT_OP,
            ),
            None,
        )
        .await;
    assert!(partition.owes_session_retirement(), "prepared is not paid");
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert!(!partition.owes_session_retirement());
    assert_eq!(
        partition.observe_retirement_debt(DEBT_OP),
        RetirementDebtStatus::Paid
    );
    assert!(!partition.dedup.contains(FINALIZED_CLIENT));
    assert_eq!(partition.required_metadata_frontier, DEBT_OP);

    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(send(FINALIZED_CLIENT, 0), Some(sender))
        .await;
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code(),
        "the paid frontier keeps a finalized session's stale request out"
    );
    partition.on_request(send(NEW_CLIENT, DEBT_OP), None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 3);

    let wire = crate::state_transfer::ConsumerOffsetsWire::decode(
        &crate::state_transfer::ConsumerOffsetsWire {
            retirement_paid_through: partition.retirement_paid_through,
            ..Default::default()
        }
        .encode(),
    )
    .unwrap();
    assert_eq!(wire.retirement_paid_through, DEBT_OP);
}

#[test]
fn given_new_partition_when_created_revision_exceeds_session_end_should_require_retirement() {
    let mut partition = test_partition();
    let ended = SessionIdentity {
        client_id: 1,
        session: 2,
        metadata_watermark: 3,
    };
    partition.set_created_revision(7);

    assert!(
        !partition.session_retired(ended),
        "a Streams revision cannot certify an applied metadata retirement barrier"
    );
}

#[compio::test]
async fn retirement_excludes_pending_target_writes_and_reports_during_unrelated_commits() {
    const ENDING_FRONTIER: u64 = 10;
    let (mut partition, replies) = recording_partition_at(0, 3);
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: ENDING_FRONTIER,
    };
    let mut send = checksumless_send_request(partition.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(send.clone(), None).await;
    partition
        .on_request(
            retire_session_request(partition.namespace(), identity),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        1,
        "retirement must wait for the target write"
    );
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&repair_config()).await;
    partition
        .on_request(
            retire_session_request(partition.namespace(), identity),
            None,
        )
        .await;
    let retry = send
        .clone()
        .transmute_header(|header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.request = 2;
        });
    partition.on_request(retry, None).await;
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        2,
        "target write must wait for its retirement"
    );
    let rejected = *bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies.borrow().last().unwrap().1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(rejected.status, IggyError::TransientNotAccepted.as_code());
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.dedup.count(), 0);
    let unrelated = send.transmute_header(|header, next: &mut RoutedRequestHeader| {
        *next = header;
        next.client = 2;
        next.metadata_watermark = ENDING_FRONTIER;
    });
    partition.on_request(unrelated, None).await;
    partition.consensus.advance_commit_max(3);
    assert_eq!(partition.consensus.commit_min(), 2);
    assert!(
        partition.session_retired(identity),
        "unrelated commits must not block the completed retirement"
    );
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.dedup.count(), 1);
    assert!(!partition.dedup.contains(identity.client_id));
}

#[compio::test]
async fn queued_retirement_excludes_a_later_target_write() {
    let (mut partition, replies) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 4));
    let mut unrelated = checksumless_send_request(partition.namespace(), 1).transmute_header(
        |header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.client = 2;
        },
    );
    unrelated.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(unrelated, None).await;
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 10,
    };
    let second_identity = SessionIdentity {
        client_id: 3,
        ..identity
    };
    partition
        .on_request(
            retire_sessions_request(partition.namespace(), &[identity, second_identity]),
            None,
        )
        .await;
    assert_eq!(
        partition
            .consensus
            .with_pipeline(LocalPipeline::request_queue_len),
        1
    );
    let mut target = checksumless_send_request(partition.namespace(), 1).transmute_header(
        |old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.client = second_identity.client_id;
        },
    );
    target.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(target, None).await;
    assert_eq!(
        partition
            .consensus
            .with_pipeline(LocalPipeline::request_queue_len),
        1,
        "target cannot queue behind its retirement"
    );
    let rejected = *bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(rejected.status, IggyError::TransientNotAccepted.as_code());
}

#[compio::test]
async fn given_foreign_session_retirement_when_replicated_or_repaired_should_preserve_current_history()
 {
    const INCARNATION: u64 = 7;
    const COMMITTED_ELSEWHERE: u64 = 14;
    const NEXT_OP: u64 = 1;
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.set_created_revision(INCARNATION);
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 1,
    };
    primary
        .on_request(retire_session_request(primary.namespace(), identity), None)
        .await;
    let bytes = primary.log.journal().inner.repair_entry(NEXT_OP).unwrap();
    let mut valid = Message::<PrepareHeader>::new(bytes.len());
    valid.as_mut_slice().copy_from_slice(bytes.as_slice());
    assert_eq!(valid.header().partition_incarnation, INCARNATION);

    for repaired in [false, true] {
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_created_revision(INCARNATION);
        if repaired {
            backup.repair = Some(armed_session(NEXT_OP, 0));
        }
        for foreign in [0, INCARNATION - 1, INCARNATION + 1] {
            for commit in [0, COMMITTED_ELSEWHERE] {
                let message = consensus::seal_prepare_checksum(valid.clone().transmute_header(
                    |mut original, header: &mut PrepareHeader| {
                        original.partition_incarnation = foreign;
                        original.commit = commit;
                        original.op = commit + NEXT_OP;
                        *header = original;
                    },
                ));
                if repaired {
                    backup.apply_repaired_prepare(message).await;
                } else {
                    backup.on_replicate(message).await;
                }
                assert_eq!(backup.consensus().commit_max(), 0);
                assert_eq!(backup.consensus().sequencer().current_sequence(), 0);
                assert!(
                    backup
                        .log
                        .journal()
                        .inner
                        .header_by_op(commit + NEXT_OP)
                        .is_none()
                );
                assert!(!backup.session_retired(identity));
            }
        }
        if repaired {
            backup.apply_repaired_prepare(valid.clone()).await;
        } else {
            backup.on_replicate(valid.clone()).await;
        }
        assert!(
            backup.log.journal().inner.header_by_op(NEXT_OP).is_some(),
            "a retirement of the hosted incarnation must still enter the journal"
        );
    }
}

#[compio::test]
async fn given_unpublished_delete_fence_when_checking_retirement_should_wait_for_commit() {
    let mut partition = test_partition();
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 20,
    };
    partition
        .apply_history_transition(
            TransitionPartitionHistoryRequest {
                incarnation: partition.created_revision(),
                metadata_op: 10,
            },
            1,
        )
        .unwrap();
    assert!(!partition.session_retired(identity));
    partition.consensus.advance_commit_max(1);
    partition.consensus.advance_commit_min(1);
    assert!(partition.session_retired(identity));
}

#[compio::test]
async fn given_queued_session_retirement_when_promoted_at_a_live_incarnation_should_prepare_it() {
    const INCARNATION: u64 = 7;
    let (mut partition, replies) =
        recording_partition_with_pipeline(0, 1, LocalPipeline::with_capacities(1, 1));
    partition.set_created_revision(INCARNATION);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    // The send holds the only prepare slot, so the retirement has to queue.
    let mut send = checksumless_send_request(partition.namespace(), 1).transmute_header(
        |header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.client = 2;
            next.partition_incarnation = INCARNATION;
        },
    );
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(send, None).await;
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 1,
    };
    partition
        .on_request(
            retire_session_request(partition.namespace(), identity),
            None,
        )
        .await;
    assert_eq!(partition.consensus().request_queue_len(), 1);

    commit_recorded_loopback(&mut partition).await;

    assert_eq!(partition.consensus().request_queue_len(), 0);
    let promoted = partition.consensus.pipeline_head_header().unwrap();
    assert_eq!(promoted.operation, Operation::RetireSession);
    assert_eq!(promoted.partition_incarnation, INCARNATION);
    for (_, reply) in replies.borrow().iter() {
        let header = bytemuck::checked::from_bytes::<ReplyHeader>(
            &reply.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_ne!(header.status, IggyError::HistoryUnavailable.as_code());
    }
}

#[test]
fn given_recorded_last_polled_when_retirement_prunes_the_group_should_clear_it() {
    const GROUP_ID: u32 = 7;
    const POLLED_OFFSET: u64 = 3;
    let mut partition = test_partition();
    let fence = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: u64::from(GROUP_ID),
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 1,
        },
        metadata_op: 1,
    };
    partition.apply_consumer_group_owner(fence, 1).unwrap();
    // The fence install clears the entry, so the test seeds it after.
    partition.last_polled_offsets.pin().insert(
        ConsumerGroupId(GROUP_ID as usize),
        ConsumerOffset::new(
            ConsumerKind::ConsumerGroup,
            GROUP_ID,
            POLLED_OFFSET,
            String::new(),
        ),
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: fence.incarnation,
        metadata_op: fence.metadata_op + 1,
        next_group_id: fence.group_id + 1,
        live_group_ids: Vec::new(),
    };

    partition
        .apply_consumer_group_owner_retirement(catalog)
        .unwrap();

    assert!(partition.consumer_group_owners.is_empty());
    assert_eq!(
        partition.group_offset_state(fence.group_id).0,
        None,
        "a retired group must not keep its last polled offset"
    );
}

/// A replay starts from the owners of an older checkpoint, and the offset
/// files can already lack the offset that a later delete removed. A catalog
/// committed ahead of that delete would then prune the owner it needs.
#[compio::test]
async fn given_retirement_racing_offset_cleanup_when_replayed_from_older_checkpoint_should_not_fence_partition()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const STORED_OFFSET: u64 = 7;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let fence = install_unassigned_test_owner(&mut partition, GROUP_ID).await;
    let checkpoint = partition.consensus.commit_min();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    partition.seed_recovered_consumer_offset(
        ConsumerKind::ConsumerGroup,
        u32::try_from(GROUP_ID).unwrap(),
        STORED_OFFSET,
        STORED_OFFSET,
    );
    let catalog = RetireConsumerGroupOwnersRequest {
        incarnation: partition.created_revision(),
        metadata_op: partition.required_metadata_frontier + 1,
        next_group_id: GROUP_ID + 1,
        live_group_ids: Vec::new(),
    };
    // The reconciler can send the catalog in the pass that deletes the
    // offset, and again in a later pass.
    let passes = [
        vec![
            owner_retirement_request(&partition, &catalog),
            fenced_group_offset_delete(&fence),
        ],
        vec![owner_retirement_request(&partition, &catalog)],
    ];
    let mut tail = Vec::new();
    for pass in passes {
        for request in pass {
            partition.on_request(request, None).await;
        }
        // The commit walk evicts what it applied, so take the tail first.
        let first = checkpoint + 1 + tail.len() as u64;
        for op in first..=partition.consensus.sequencer().current_sequence() {
            let journaled = partition.log.journal().inner.repair_entry(op).unwrap();
            tail.push(
                Message::<PrepareHeader>::try_from(Owned::copy_from_slice(journaled.as_slice()))
                    .unwrap(),
            );
        }
        commit_log(&mut partition).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    }
    let head = partition.consensus.commit_min();
    assert_eq!(head, checkpoint + tail.len() as u64);
    drop(partition);

    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    for prepare in tail {
        recovered.apply_replicated_operation(prepare).await.unwrap();
    }
    recovered.consensus.sequencer().set_sequence(head);
    recovered.consensus.restore_commit_state(checkpoint, head);
    commit_log(&mut recovered).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert_eq!(recovered.consensus.commit_min(), head);
    assert_eq!(recovered.consumer_group_owner(GROUP_ID), None);
    assert_eq!(recovered.owner_retirement, Some(catalog));
}

/// A retirement debt is paid only from an empty pipeline, so an automatic
/// commit waits for the payment like a session request does.
#[test]
fn given_retirement_debt_when_auto_commit_poll_completes_should_reject_without_progress() {
    const DEBT_OP: u64 = 40;
    for owed in [false, true] {
        let (mut partition, _) = recording_partition_at(0, 3);
        let consumer = PollingConsumer::Consumer(7, 0);
        if owed {
            assert!(matches!(
                partition.observe_retirement_debt(DEBT_OP),
                RetirementDebtStatus::Unpaid(Some(_))
            ));
        }
        let read_result = poll_read_result(&partition, consumer, true, Some(9));
        let completion = partition.complete_poll(read_result);
        if owed {
            assert!(matches!(completion, Err(IggyError::TransientNotAccepted)));
            assert_eq!(partition.get_consumer_offset(consumer), None);
            assert_eq!(partition.consensus.pipeline_len(), 0);
        } else {
            assert!(completion.unwrap().replication.is_some());
        }
    }
}

#[compio::test]
async fn checkpoint_receipts_survive_deferred_completion_and_state_transfer() {
    for install_before_completion in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut config = repair_config();
        config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
        let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
        std::fs::create_dir_all(&directory).unwrap();
        let mut partition = partition_at_view(0, 0);
        partition.set_partition_dir(directory.to_string_lossy().into_owned());
        set_offset_dirs_under(&mut partition, &directory);
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.open_persistence().await.unwrap();
        partition.log.retire_front().unwrap();
        partition.install_empty_segment(&config, 0).await.unwrap();
        let prepare = checksummed_segment_prepare(1, 0, 0, b"checkpoint");
        let header = *prepare.header();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        persistence
            .append(prepare.clone().into_frozen(), true)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition
            .append_repaired_send_messages(prepare.clone())
            .await
            .unwrap();
        partition.consensus.restore_commit_state(0, header.op);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), header.op);
        let completion = Rc::new(std::cell::Cell::new(None));
        let captured = Rc::clone(&completion);
        persistence.set_notifier(Rc::new(move |value| captured.set(Some(value))));
        let previous = directory.join("prepares-0/receipts-0.checkpoint");
        std::fs::write(&previous, b"obsolete receipt").unwrap();
        persistence.sync();
        assert!(persistence.start());
        persistence.request_checkpoint();

        partition.checkpoint_persistence(&config).await;

        assert!(partition.fatal().is_none());
        assert_eq!(persistence.checkpoint_op(), 0);
        assert!(
            previous.exists(),
            "unpublished checkpoints cannot reclaim receipts"
        );
        assert!(directory.join("prepares-0/receipts-1.checkpoint").exists());
        Rc::clone(&persistence).run().await;
        assert!(
            persistence.failure().is_none(),
            "{:?}",
            persistence.failure()
        );
        assert_eq!(persistence.checkpoint_op(), header.op);
        let checkpoint_op = if install_before_completion {
            partition.maintenance.checkpoint = Some(PendingCheckpoint::Wal(header.op));
            assert!(matches!(
                partition.drive_checkpoint_io(&config).unwrap(),
                Some(crate::PartitionIoStep::Progress)
            ));
            assert!(matches!(
                partition.maintenance.checkpoint,
                Some(PendingCheckpoint::Reclaim(1))
            ));
            let transferred = checksummed_segment_prepare(2, header.checksum, 1, b"transferred");
            let mut segment_body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
            segment_body.extend_from_slice(&transferred.as_slice()[size_of::<PrepareHeader>()..]);
            let artifact = consensus::StateArtifact::for_bytes(
                consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                0,
                &segment_body,
            );
            let staged = partition
                .spill_transfer_segment(&artifact, segment_body, None)
                .await
                .unwrap();
            let offsets = crate::state_transfer::ConsumerOffsetsWire {
                producers: Vec::new(),
                owner_retirement: None,
                dedup_capacity: partition.dedup.capacity(),
                required_metadata_frontier: 0,
                prepare_checksum: Some(transferred.header().checksum),
                checkpoint_prepare: transferred.as_slice().to_vec(),
                next_offset: 2,
                emptied_at: 0,
                consumers: Vec::new(),
                groups: Vec::new(),
                external_groups: Vec::new(),
                dedup: partition.dedup.watermarks_sorted(),
                owners: Vec::new(),
                history_transition: None,
                retirement_paid_through: 0,
                machines: None,
            };
            partition
                .install_state_transfer(&config, 2, vec![staged], &offsets.encode())
                .await
                .unwrap();
            settle_partition_io(&mut partition, &config).await;
            2
        } else {
            header.op
        };
        let current = directory.join(format!("prepares-0/receipts-{checkpoint_op}.checkpoint"));
        assert!(current.exists());
        let completion = completion.get().unwrap();
        assert!(persistence.accepts_completion(completion));
        partition
            .on_persistence_completed(completion, &config)
            .await;
        assert!(!previous.exists());
        assert!(
            current.exists(),
            "completion must retain the published checkpoint"
        );
        drop(partition);
        drop(persistence);
        let mut reopened = partition_at_view(0, 0);
        reopened.set_partition_dir(directory.to_string_lossy().into_owned());
        let (recovered, _) = crate::persistence::PartitionPersistence::open_with_storage(
            &directory.join("prepares-0"),
            reopened.namespace().inner(),
            reopened.created_revision(),
            journal::durable_storage::DiskStorage,
        )
        .await
        .unwrap();
        assert_eq!(recovered.checkpoint_op(), checkpoint_op);
        reopened
            .restore_retry_checkpoint(recovered.checkpoint_op())
            .await
            .unwrap();
        let consensus::client_table::RequestStatus::Duplicate(reply) = reopened
            .dedup
            .check_request(1, 1, 1, Operation::SendMessages)
        else {
            panic!("checkpoint must retain the committed result for a retry");
        };
        assert!(reply.header().has_committed_result());
    }
}

#[compio::test]
async fn receipt_checkpoint_recovery_refuses_missing_corrupt_and_foreign_artifacts() {
    const CHECKPOINT_OP: u64 = 7;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.set_dedup_clients_max(3);
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let path = directory.path().join("prepares-0/receipts-7.checkpoint");
    assert_eq!(
        partition
            .restore_retry_checkpoint(CHECKPOINT_OP)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
    publish_empty_retry_checkpoint(&partition, CHECKPOINT_OP).await;
    let valid = std::fs::read(&path).unwrap();
    for offset in [0, 4, 12, 20] {
        let mut foreign = valid.clone();
        foreign[offset] ^= 1;
        let content_len = foreign.len() - size_of::<u64>();
        let checksum = consensus::state_artifact_checksum(&foreign[..content_len]);
        foreign[content_len..].copy_from_slice(&checksum.to_le_bytes());
        std::fs::write(&path, &foreign).unwrap();
        assert_eq!(
            partition
                .restore_retry_checkpoint(CHECKPOINT_OP)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::read(&path).unwrap(), foreign);
        assert_eq!(
            partition.dedup().capacity(),
            3,
            "refusal must preserve the live protection table"
        );
    }
    for end in 0..valid.len() {
        std::fs::write(&path, &valid[..end]).unwrap();
        assert!(
            partition
                .restore_retry_checkpoint(CHECKPOINT_OP)
                .await
                .is_err(),
            "accepted truncated receipt artifact at {end}"
        );
    }
    std::fs::write(&path, &valid).unwrap();
    partition
        .restore_retry_checkpoint(CHECKPOINT_OP)
        .await
        .unwrap();
    assert_eq!(
        partition.dedup().capacity(),
        consensus::PARTITION_DEDUP_CLIENTS_MAX
    );
}
