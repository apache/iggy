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
async fn logout_waits_for_inherited_committed_history_before_checking_the_session() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const USER: u32 = 7;
    let metadata = metadata_plane();
    let consensus = metadata.consensus.as_ref().unwrap();
    metadata
        .client_table
        .borrow_mut()
        .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, SESSION))
        .unwrap();
    consensus.advance_commit_max(1);

    assert_eq!(
        metadata.submit_logout_in_process(CLIENT, SESSION, 2).await,
        Err(MetadataSubmitError::NotCaughtUp)
    );
    assert_eq!(
        metadata
            .submit_logout_in_process(CLIENT + 1, SESSION, 2)
            .await,
        Err(MetadataSubmitError::NotCaughtUp)
    );
    assert_eq!(
        metadata.client_table.borrow().get_epoch(CLIENT),
        Some(SESSION)
    );
    assert!(consensus.pipeline_is_empty());
}

/// The exact window behind the historical "logout/unregister failed
/// ... primary not yet caught up on `commit_journal`".
/// ANOTHER client's op sits between quorum-ack (`commit_max` advanced
/// inside `on_ack`) and apply (`commit_min` behind, driver parked at
/// the journal read).
///
/// New contract (queue absorption): a logout landing in
/// that window is NOT bounced with `NotCaughtUp` — non-register ops
/// carry no catch-up gate. It pipelines behind the in-flight batch,
/// and its submit's inline loopback pump commits both ops in order.
/// The parked sibling driver then resumes onto an already-drained
/// pipeline and exits via head-revalidation, exercising the
/// concurrent-driver safety of the commit loop.
#[compio::test]
async fn logout_in_mid_commit_window_commits_instead_of_rejecting() {
    use std::future::Future;

    /// The client logging out; its session is already committed.
    const CLIENT_A: u128 = 1;
    /// The client whose in-flight commit closes the gate.
    const CLIENT_B: u128 = 2;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;

    let dir = tempfile::tempdir().unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
        );
    let consensus = md.consensus.as_ref().unwrap();
    for client in [CLIENT_A, CLIENT_B] {
        md.client_table
            .borrow_mut()
            .commit_register(
                client,
                ACTING_USER,
                [0x5a; 32],
                register_reply(client, SESSION),
            )
            .unwrap();
    }

    // B's op: prepared, journaled, self-acked onto the loopback queue.
    let prepare = md
        .prepare_request(create_stream_request(CLIENT_B, 1, "s1"))
        .expect("CreateStream is client-allowed");
    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
    md.on_replicate(prepare).await;
    let mut loopback = Vec::new();
    consensus.drain_loopback_into(&mut loopback);
    let ack = loopback
        .pop()
        .expect("one self-ack per prepare")
        .try_into_typed::<PrepareOkHeader>()
        .expect("loopback holds self PrepareOks");

    // Open the window: the first poll of `on_ack` reaches quorum and
    // advances commit_max synchronously, then parks at the journal
    // read — commit_min has not moved. This is the exact server state
    // every production NotCaughtUp line was emitted from.
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut driver = Box::pin(md.on_ack(ack));
    assert!(
        driver.as_mut().poll(&mut cx).is_pending(),
        "driver must park at the journal read inside the commit"
    );
    assert_eq!(consensus.commit_max(), 1, "quorum advanced commit_max");
    assert_eq!(consensus.commit_min(), 0, "apply has not landed yet");
    assert!(
        !is_caught_up_primary(consensus),
        "gate must be closed mid-commit"
    );

    // A's logout lands in the window. No gate for non-register ops: it
    // pipelines behind B's committing op, and its inline loopback pump
    // drives BOTH commits (B's op 1 via head-revalidated takeover, then
    // its own op 2) before resolving.
    let outcome = md.submit_logout_in_process(CLIENT_A, SESSION, 2).await;
    assert_eq!(
        outcome,
        Ok(2),
        "mid-window logout must commit and reply, never bounce NotCaughtUp"
    );
    assert_eq!(consensus.commit_min(), 2, "both ops committed in order");
    assert!(
        is_caught_up_primary(consensus),
        "gate reopens once the batch drains"
    );

    // The parked sibling driver resumes onto a drained pipeline: the
    // head it peeked is gone, revalidation sends it out without
    // touching commit state. Drive it to completion to prove it.
    let mut resumed = false;
    for _ in 0..1_000 {
        if driver.as_mut().poll(&mut cx).is_ready() {
            resumed = true;
            break;
        }
        // Let the runtime deliver the journal-read completion.
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(resumed, "parked driver must exit via head-revalidation");
    assert_eq!(
        consensus.commit_min(),
        2,
        "resumed driver commits nothing new"
    );
    assert_eq!(
        md.client_table.borrow().get_epoch(CLIENT_A),
        None,
        "session removed by the committed logout"
    );
}

#[compio::test]
async fn expired_logout_waits_for_catchup_and_preserves_registered_members() {
    const CLIENT: u128 = 1;
    const USER: u32 = 7;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata
        .submit_register_in_process(
            CLIENT,
            USER,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32],
            },
        )
        .await
        .unwrap()
        .epoch;
    let consensus = metadata.consensus.as_ref().unwrap();
    consensus.advance_commit_max(consensus.commit_min() + 1);
    assert_eq!(
        metadata
            .submit_expired_logout_in_process(CLIENT, Some(session))
            .await,
        Err(MetadataSubmitError::NotCaughtUp)
    );
    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(CLIENT)
            .len(),
        1
    );
}

#[compio::test]
async fn expired_logout_removes_only_the_observed_session() {
    const CLIENT: u128 = 1;
    const USER: u32 = 7;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata
        .submit_register_in_process(
            CLIENT,
            USER,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32],
            },
        )
        .await
        .unwrap()
        .epoch;
    let logout_op = metadata.consensus.as_ref().unwrap().commit_min() + 1;
    assert_eq!(
        metadata
            .submit_expired_logout_in_process(CLIENT, Some(session))
            .await
            .unwrap(),
        Some(logout_op)
    );
    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(CLIENT),
        []
    );
    assert_eq!(metadata.client_table.borrow().get_epoch(CLIENT), None);
    assert_eq!(metadata.mux_stm.streams().consumer_group_count(), 1);
}

#[compio::test]
async fn expired_logout_removes_a_member_without_a_client_table_entry() {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    let logout_op = metadata.consensus.as_ref().unwrap().commit_min() + 1;

    assert_eq!(
        metadata
            .submit_expired_logout_in_process(CLIENT, Some(session))
            .await
            .unwrap(),
        Some(logout_op)
    );
    assert_eq!(
        metadata
            .submit_expired_logout_in_process(CLIENT, None)
            .await
            .unwrap(),
        None
    );

    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(CLIENT),
        []
    );
    assert_eq!(metadata.mux_stm.streams().consumer_group_count(), 1);
    assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), logout_op);
}

#[compio::test]
async fn stale_logout_request_preserves_the_latest_receipt_and_group_membership() {
    const CLIENT: u128 = 1;
    const WATERMARK: u64 = 5;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    let committed_op = metadata.consensus.as_ref().unwrap().commit_min();
    let reply = committed_reply(CLIENT, WATERMARK, Operation::CreateStream, 0);
    assert_eq!(
        metadata
            .client_table
            .borrow_mut()
            .commit_reply(CLIENT, 7, reply),
        consensus::client_table::CommitReply::Cached
    );
    for (request, expected) in [
        (1, MetadataSubmitError::RequestTooOld),
        (WATERMARK, MetadataSubmitError::OperationMismatch),
    ] {
        assert_eq!(
            metadata
                .submit_logout_in_process(CLIENT, session, request)
                .await,
            Err(expected)
        );
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(session)
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT)
                .len(),
            1
        );
        assert_eq!(
            metadata.consensus.as_ref().unwrap().commit_min(),
            committed_op,
            "a refused logout must not commit a metadata operation"
        );
    }
}

#[compio::test]
async fn recovery_frontier_records_partition_revisions_separately_from_metadata_ops() {
    let (_dir, metadata) = metadata_with_group_member(1).await;
    for client in 2..=8 {
        metadata
            .submit_register_in_process(
                client,
                7,
                RegisterRequest {
                    user_created_at: 1,
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap();
    }
    let revision = metadata.mux_stm.streams().read(|inner| inner.revision);
    let commit = metadata.consensus.as_ref().unwrap().commit_min();
    assert!(
        commit > revision,
        "registration churn must exceed the partition revision in this fixture"
    );
    metadata.seed_applied_frontier_from_consensus();
    assert_eq!(metadata.applied_frontier.get(), commit);
    assert_eq!(metadata.applied_frontier.recovered_revision(), revision);
}

#[compio::test]
async fn given_group_logout_when_finalizing_through_its_end_should_release_the_session() {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    metadata
        .submit_logout_in_process(CLIENT, session, 1)
        .await
        .unwrap();
    let identity = metadata
        .client_table
        .borrow()
        .ended_sessions()
        .next()
        .unwrap();

    assert_eq!(
        metadata
            .submit_session_finalization(identity.metadata_watermark - 1, Vec::new())
            .await
            .unwrap(),
        None,
        "a watermark below every session end has nothing to finalize"
    );
    assert!(
        metadata
            .submit_session_finalization(identity.metadata_watermark, Vec::new())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        metadata.client_table.borrow().ended_sessions().next(),
        None,
        "successful finalization must release the ended session's slot"
    );
}

#[compio::test]
async fn given_a_partition_created_after_a_session_ended_when_finalizing_should_release_it() {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    metadata
        .submit_logout_in_process(CLIENT, session, 1)
        .await
        .unwrap();
    let identity = metadata
        .client_table
        .borrow()
        .ended_sessions()
        .next()
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    // A partition created after the end starts its frontier above it, so
    // the finalization does not depend on it.
    let create = build_prepare_message(
        consensus,
        &RoutedRequestHeader::default(),
        Operation::CreatePartitionsWithAssignments,
        &PersistedCreatePartitionsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 1,
                consensus_group_id: 2,
                initial_view: 0,
                members: Vec::new(),
            }],
        }
        .to_bytes(),
    );
    assert_eq!(
        gated_apply(metadata.mux_stm.as_ref(), create).unwrap().code,
        0
    );

    for (retired_through, expected_session) in [
        (identity.metadata_watermark - 1, Some(identity)),
        (identity.metadata_watermark, None),
    ] {
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &RoutedRequestHeader::default(),
                Operation::FinalizeSession,
                &FinalizeSessionRequest {
                    retired_through,
                    debt: Vec::new(),
                }
                .to_bytes(),
            ),
            metadata.client_table_capacity(),
        );
        apply_committed_prepare(
            metadata.mux_stm.as_ref(),
            &metadata.client_table,
            true,
            |_| {},
            prepare,
        );
        assert_eq!(
            metadata.client_table.borrow().ended_sessions().next(),
            expected_session,
            "finalization releases exactly the sessions that ended at or below its watermark"
        );
    }
}

#[compio::test]
async fn given_finalization_with_debt_when_applied_should_record_it_for_the_named_incarnation_only()
{
    const CLIENT: u128 = 1;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    metadata
        .submit_logout_in_process(CLIENT, session, 1)
        .await
        .unwrap();
    let identity = metadata
        .client_table
        .borrow()
        .ended_sessions()
        .next()
        .unwrap();
    let namespace = IggyNamespace::new(0, 0, 0);
    let created_revision = metadata
        .mux_stm
        .streams()
        .created_revision_for_namespace(namespace)
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    let prepare = consensus::seal_prepare_capacity(
        build_prepare_message(
            consensus,
            &RoutedRequestHeader::default(),
            Operation::FinalizeSession,
            &FinalizeSessionRequest {
                retired_through: identity.metadata_watermark,
                debt: vec![
                    RetirementDebt {
                        namespace: namespace.inner(),
                        created_revision,
                    },
                    RetirementDebt {
                        namespace: namespace.inner(),
                        created_revision: created_revision + 1,
                    },
                ],
            }
            .to_bytes(),
        ),
        metadata.client_table_capacity(),
    );
    let finalize_op = prepare.header().op;
    apply_committed_prepare(
        metadata.mux_stm.as_ref(),
        &metadata.client_table,
        true,
        |_| {},
        prepare,
    );
    assert_eq!(metadata.client_table.borrow().ended_sessions().next(), None);
    let streams = metadata.mux_stm.streams();
    assert_eq!(
        streams.retirement_debt_for(namespace, created_revision),
        Some(finalize_op)
    );
    assert_eq!(
        streams.retirement_debt_for(namespace, created_revision + 1),
        None
    );
    assert_eq!(
        streams.retirement_debts_stored_by(0, 1),
        (
            vec![(namespace, created_revision, finalize_op)],
            finalize_op
        )
    );
    assert_eq!(
        streams.retirement_debts_since(0, 0, 1),
        Some((
            vec![(namespace, created_revision, finalize_op)],
            finalize_op
        ))
    );
    assert_eq!(
        streams.retirement_debts_since(finalize_op, 0, 1),
        Some((Vec::new(), finalize_op))
    );
    let restored = <Streams as crate::stm::snapshot::Snapshotable>::from_snapshot(
        crate::stm::snapshot::Snapshotable::to_snapshot(streams),
    )
    .expect("snapshot restores");
    assert_eq!(
        restored.retirement_debt_for(namespace, created_revision),
        Some(finalize_op)
    );
    assert_eq!(
        restored.read(|inner| inner.retirement_debt_revision),
        finalize_op
    );
    assert_eq!(
        restored.retirement_debts_since(0, 0, 1),
        None,
        "a restored reader must rescan"
    );
}

#[compio::test]
async fn stale_logout_preserves_a_newer_session_and_its_membership() {
    const CLIENT: u128 = 1;
    const OLD_SESSION: u64 = 0;
    const USER: u32 = 7;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let new_session = metadata
        .submit_register_in_process(
            CLIENT,
            USER,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32],
            },
        )
        .await
        .unwrap()
        .epoch;
    let committed_op = metadata.consensus.as_ref().unwrap().commit_min();

    metadata
        .submit_logout_in_process(CLIENT, OLD_SESSION, EXPIRED_SESSION_REQUEST_ID)
        .await
        .unwrap();
    for expired_session in [None, Some(OLD_SESSION)] {
        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, expired_session)
                .await
                .unwrap(),
            None
        );
    }

    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(CLIENT)
            .len(),
        1
    );
    assert_eq!(
        metadata.client_table.borrow().get_epoch(CLIENT),
        Some(new_session)
    );
    assert_eq!(
        metadata.consensus.as_ref().unwrap().commit_min(),
        committed_op,
        "an old session must not advance the committed frontier"
    );
}

#[compio::test]
async fn capacity_pressure_preserves_membership_until_session_expiry() {
    const CLIENT: u128 = 1;
    const USER: u32 = 7;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    let capacity = metadata.client_table.borrow().capacity();
    {
        let mut table = metadata.client_table.borrow_mut();
        for client in 2..=capacity as u128 {
            table
                .commit_register(
                    client,
                    USER,
                    [0x5a; 32],
                    register_reply(client, u64::try_from(client).unwrap() + 1),
                )
                .unwrap();
        }
        assert!(
            table
                .commit_register(
                    capacity as u128 + 1,
                    USER,
                    [0x5a; 32],
                    register_reply(capacity as u128 + 1, capacity as u64 + 2)
                )
                .is_err()
        );
        assert_eq!(table.get_epoch(CLIENT), Some(session));
    }
    assert_eq!(
        metadata.mux_stm.streams().consumer_group_session(CLIENT),
        Some(session)
    );
    assert_eq!(
        metadata
            .submit_expired_logout_in_process(CLIENT, Some(session - 1))
            .await
            .unwrap(),
        None
    );
    assert!(
        metadata
            .submit_expired_logout_in_process(CLIENT, Some(session))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        metadata
            .mux_stm
            .streams()
            .consumer_group_memberships(CLIENT),
        []
    );
    assert_eq!(metadata.client_table.borrow().count(), capacity);
}

#[compio::test]
async fn replayed_register_preserves_the_original_epoch_below_the_table_frontier() {
    const CLIENT: u128 = 1;
    let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
    let epoch = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
    let request = RoutedRequestHeader {
        client: CLIENT,
        user_id: GROUP_MEMBER_USER,
        ..Default::default()
    };
    let prepare = build_prepare_message(
        metadata.consensus.as_ref().unwrap(),
        &request,
        Operation::Register,
        &RegisterRequest {
            user_created_at: 1,
            bind_verifier: [0x5a; 32],
        }
        .to_bytes(),
    );
    apply_committed_prepare(
        &*metadata.mux_stm,
        &metadata.client_table,
        false,
        |_| {},
        prepare,
    );
    assert_eq!(
        metadata.client_table.borrow().get_epoch(CLIENT),
        Some(epoch)
    );
    assert_eq!(
        metadata.mux_stm.streams().consumer_group_session(CLIENT),
        Some(epoch)
    );
}

#[compio::test]
async fn given_user_revocation_when_committed_should_release_group_memberships() {
    const CLIENT: u128 = 1;
    const USER: u32 = 7;
    for (transferred, finalized, primary, snapshot_ahead) in [
        (false, false, false, false),
        (true, false, false, false),
        (true, false, true, false),
        (true, true, false, false),
        (true, true, true, false),
        (true, true, false, true),
        (true, true, true, true),
    ] {
        let (_directory, mut metadata) = metadata_with_group_member(CLIENT).await;
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let consensus = metadata.consensus.as_ref().unwrap();
        let request = RoutedRequestHeader::default();
        for user_id in 1..=USER {
            let create = CreateUserRequest {
                username: WireName::new(format!("user-{user_id}")).unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            };
            metadata
                .mux_stm
                .update(build_prepare_message(
                    consensus,
                    &request,
                    Operation::CreateUser,
                    &create.to_bytes(),
                ))
                .unwrap();
        }
        let mut snapshot =
            <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, consensus.commit_min(), 1)
                .unwrap();
        snapshot.snapshot_mut().client_table = Some(metadata.client_table.borrow().to_snapshot());
        let deletion = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &request,
                Operation::DeleteUser,
                &DeleteUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                }
                .to_bytes(),
            ),
            metadata.client_table_capacity(),
        );
        let mut tail = vec![deletion];
        if transferred {
            let mut table = ClientTable::decode(&metadata.client_table.borrow().encode()).unwrap();
            let ended = table.end_user_sessions(USER, tail[0].header().op);
            if finalized {
                assert_eq!(
                    table.finalize_sessions_through(ended[0].metadata_watermark),
                    1
                );
                consensus.sequencer().set_sequence(tail[0].header().op);
                tail.push(consensus::seal_prepare_capacity(
                    build_prepare_message(
                        consensus,
                        &request,
                        Operation::FinalizeSession,
                        &FinalizeSessionRequest {
                            retired_through: ended[0].metadata_watermark,
                            debt: Vec::new(),
                        }
                        .to_bytes(),
                    ),
                    metadata.client_table_capacity(),
                ));
            }
            let frontier = tail.last().unwrap().header().op;
            if snapshot_ahead {
                let journal = metadata.journal.take();
                metadata = metadata_plane();
                metadata.journal = journal;
            }
            metadata
                .install_state_transfer(&snapshot.encode().unwrap(), table, frontier, frontier)
                .await
                .unwrap();
        }
        let transferred_bytes = transferred.then(|| metadata.client_table.borrow().encode());
        let consensus = metadata.consensus.as_ref().unwrap();
        for prepare in tail {
            let op = prepare.header().op;
            if primary {
                consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                metadata
                    .journal
                    .as_ref()
                    .unwrap()
                    .handle()
                    .append(prepare)
                    .await
                    .unwrap();
                metadata.resume_stranded_commits().await;
            } else {
                metadata
                    .journal
                    .as_ref()
                    .unwrap()
                    .handle()
                    .append(prepare)
                    .await
                    .unwrap();
                consensus.advance_commit_max(op);
                metadata.commit_journal().await;
            }
        }
        if let Some(expected) = transferred_bytes {
            assert_eq!(
                metadata.client_table.borrow().encode(),
                expected,
                "tail replay must preserve transferred retry protection"
            );
        }
        assert_eq!(
            metadata.client_table.borrow().ended_sessions().count(),
            usize::from(!finalized)
        );
        assert!(
            metadata.replay_client_table.borrow().is_none(),
            "replay state must be released after catch-up"
        );
        assert!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT)
                .is_empty(),
            "revoked sessions must release their consumer assignments: transferred={transferred} finalized={finalized} primary={primary} snapshot_ahead={snapshot_ahead} committed={}",
            consensus.commit_min()
        );
    }
}

/// The primary admits a request of a live session while a revocation of
/// its user is still uncommitted, so the request commits right behind it.
/// A checkpoint of the resulting table must restart, and a state transfer
/// of it must install on a fresh replica.
#[compio::test]
async fn given_request_pipelined_behind_user_revocation_when_checkpointed_should_restart_and_transfer()
 {
    const ADMIN: u128 = 1;
    const VICTIM: u128 = 2;
    const ROOT_USER: u32 = 0;
    const VICTIM_USER: u32 = 7;
    const REQUEST: u64 = 1;
    let registration = || RegisterRequest {
        user_created_at: 1,
        bind_verifier: [0x5a; 32],
    };
    let with_session = |request: Message<RoutedRequestHeader>, session: u64| {
        request.transmute_header(|old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.session = session;
        })
    };
    for revocation in [Operation::DeleteUser, Operation::UpdateResource] {
        let directory = tempfile::tempdir().unwrap();
        let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_directory).unwrap();
        let journal = PrepareJournal::open(&metadata_directory.join("journal.wal"), 0)
            .await
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::new((session_users(), (StreamsInner::new().into(), ()))),
            Some(directory.path().to_path_buf()),
        );
        let admin = metadata
            .submit_register_in_process(ADMIN, ROOT_USER, registration())
            .await
            .unwrap();
        let victim = metadata
            .submit_register_in_process(VICTIM, VICTIM_USER, registration())
            .await
            .unwrap();
        let body = if revocation == Operation::DeleteUser {
            DeleteUserRequest {
                user_id: WireIdentifier::numeric(VICTIM_USER),
            }
            .to_bytes()
        } else {
            let patch = UserUpdateOptions {
                status: Some(UserStatus::Inactive),
                ..UserUpdateOptions::default()
            }
            .to_patch()
            .unwrap();
            UpdateResourceRequest {
                target: ResourceTarget::User {
                    user_id: WireIdentifier::numeric(VICTIM_USER),
                },
                reset: patch.reset,
                options: patch.set,
            }
            .to_bytes()
        };
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        let revocation_op = consensus.commit_min() + 1;
        for request in [
            with_session(pat_request(ADMIN, REQUEST, revocation, &body), admin.epoch),
            with_session(
                create_stream_request(VICTIM, REQUEST, "pipelined"),
                victim.epoch,
            ),
        ] {
            let prepare = metadata.prepare_request(request).unwrap();
            let op = prepare.header().op;
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            journal.handle().append(prepare).await.unwrap();
            consensus.advance_commit_max(op);
        }
        metadata.resume_stranded_commits().await;
        let pipelined_op = revocation_op + 1;
        assert_eq!(consensus.commit_min(), pipelined_op);
        assert!(
            metadata
                .mux_stm
                .streams()
                .read(|streams| streams.items.is_empty()),
            "{revocation:?}: the op behind the revocation must not apply"
        );
        let ended = vec![SessionIdentity {
            client_id: VICTIM,
            session: victim.epoch,
            metadata_watermark: revocation_op,
        }];
        assert_eq!(
            metadata
                .client_table
                .borrow()
                .ended_sessions()
                .collect::<Vec<_>>(),
            ended
        );
        let table = metadata.client_table.borrow().encode();
        let transferred = ClientTable::decode(&table).expect("the transferred table must decode");

        metadata.set_checkpoint_margin(usize::MAX);
        metadata.checkpoint_if_needed(consensus, journal).await;
        let checkpoint =
            IggySnapshot::load(&metadata.coordinator.as_ref().unwrap().snapshot_path())
                .unwrap()
                .0;
        assert_eq!(checkpoint.sequence_number(), pipelined_op);
        let live_log = metadata.change_log().snapshot();
        assert_eq!(
            checkpoint.snapshot().change_log,
            live_log,
            "{revocation:?}: the checkpoint must carry the applied change log"
        );
        assert_eq!(
            live_log.activity.entries.len(),
            3,
            "{revocation:?}: both registrations and the revoked session are recorded"
        );

        let receiver = metadata_plane();
        receiver
            .install_state_transfer(
                &checkpoint.encode().unwrap(),
                transferred,
                pipelined_op,
                pipelined_op,
            )
            .await
            .expect("the transferred checkpoint must install");
        assert!(
            receiver.client_table.borrow().encode() == table,
            "{revocation:?}: the receiver must hold the transferred table"
        );
        assert_eq!(receiver.change_log().snapshot(), live_log);

        drop(metadata);
        let recovered = crate::impls::recovery::recover::<TestMux>(
            directory.path(),
            crate::impls::recovery::ReplicaIdentity {
                cluster: 1,
                replica_id: 0,
                replica_count: 1,
            },
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .expect("restart must accept the checkpointed table");
        assert_eq!(
            recovered.client_table.ended_sessions().collect::<Vec<_>>(),
            ended
        );
        assert_eq!(recovered.change_log.snapshot(), live_log);
    }
}

/// All owner decisions of one revocation share its op as generation, and a
/// reconciler on another shard installs what it reads. A reader that holds
/// a guard across the revocation stalls any second publish, so a racing
/// reader would then see one removal alone.
#[compio::test]
async fn given_two_sessions_of_one_user_when_revoked_should_publish_both_removals_at_once() {
    const FIRST: u128 = 1;
    const SECOND: u128 = 2;
    const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
    let (_directory, metadata) = metadata_with_group_member(FIRST).await;
    join_group_member(&metadata, SECOND).await;
    let header = PrepareHeader {
        op: metadata.consensus.as_ref().unwrap().commit_min() + 1,
        operation: Operation::DeleteUser,
        ..Default::default()
    };
    let factory = metadata.mux_stm.streams().factory();
    // One guard for both members: two reads could straddle a publish.
    let memberships = |streams: &Streams| {
        streams.read(|inner| {
            [FIRST, SECOND].map(|client_id| {
                inner
                    .items
                    .iter()
                    .flat_map(|(_, stream)| stream.topics.iter())
                    .flat_map(|(_, topic)| topic.consumer_groups.values())
                    .filter(|group| {
                        group
                            .members
                            .iter()
                            .any(|(_, member)| member.client_id == client_id)
                    })
                    .count()
            })
        })
    };
    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();

    let seen = thread::scope(|scope| {
        let holder_factory = factory.clone();
        let holder = scope.spawn(move || {
            Streams::from_factory(holder_factory)
                .read(|_| {
                    held_sender.send(()).unwrap();
                    release_receiver.recv()
                })
                .unwrap();
        });
        held_receiver.recv().unwrap();
        let observer = scope.spawn(move || {
            let streams = Streams::from_factory(factory);
            let deadline = Instant::now() + PUBLISH_TIMEOUT;
            let seen = loop {
                let seen = memberships(&streams);
                if seen != [1, 1] {
                    break seen;
                }
                assert!(
                    Instant::now() < deadline,
                    "the revocation never reached readers"
                );
                thread::yield_now();
            };
            release_sender.send(()).unwrap();
            seen
        });
        apply_user_revocation(
            &*metadata.mux_stm,
            &metadata.client_table,
            true,
            WireUserKey {
                user_id: GROUP_MEMBER_USER,
                user_created_at: 0,
            },
            &header,
        );
        let seen = observer.join().unwrap();
        holder.join().unwrap();
        seen
    });

    assert_eq!(
        seen,
        [0, 0],
        "a reader saw one member removal of the revocation without the other"
    );
}

#[test]
fn given_rejected_or_older_user_deletion_when_applying_should_preserve_newer_sessions() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 100;
    const USER: u32 = 1;
    for (frontier, acting_user) in [(SESSION, 0), (0, u32::MAX)] {
        let metadata = metadata_plane();
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let consensus = metadata.consensus.as_ref().unwrap();
        let request = RoutedRequestHeader::default();
        let create = CreateUserRequest {
            username: WireName::new("original").unwrap(),
            password: "hash".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        };
        metadata
            .mux_stm
            .update(build_prepare_message(
                consensus,
                &request,
                Operation::CreateUser,
                &create.to_bytes(),
            ))
            .unwrap();
        metadata
            .client_table
            .borrow_mut()
            .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, SESSION))
            .unwrap();
        metadata.client_table_frontier.set(frontier);
        let request = RoutedRequestHeader {
            user_id: acting_user,
            ..request
        };
        let deletion = build_prepare_message(
            consensus,
            &request,
            Operation::DeleteUser,
            &DeleteUserRequest {
                user_id: WireIdentifier::numeric(USER),
            }
            .to_bytes(),
        )
        .transmute_header(|old, header: &mut PrepareHeader| {
            *header = old;
            header.retry_capacity = u32::try_from(metadata.client_table_capacity()).unwrap();
        });
        apply_committed_prepare(
            &*metadata.mux_stm,
            &metadata.client_table,
            metadata.client_table_mutation_allowed(deletion.header().op),
            |_| {},
            consensus::seal_prepare_checksum(deletion),
        );
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(SESSION)
        );
        assert_eq!(metadata.client_table.borrow().ended_sessions().count(), 0);
        assert_eq!(
            metadata
                .mux_stm
                .users()
                .read(|users| users.items.get(USER as usize).is_some()),
            acting_user != 0,
            "only the authorized deletion updates the state machine"
        );
    }
}

/// Register is the one op that still honors the catch-up gate (its
/// admission races a committed-but-unapplied register; a double commit
/// bumps the epoch past the first reply's and fences a live client).
/// New contract: a register arriving in the mid-commit window is
/// ABSORBED into the pipeline's request queue with its reply subscriber
/// attached, promoted by the commit path once the batch drains, and the
/// caller's await resolves with the committed epoch — instead of the
/// historical `NotCaughtUp` bounce that one-shot CLI clients surfaced as
/// "Disconnected" login failures.
#[compio::test]
async fn register_in_mid_commit_window_is_queued_then_committed() {
    use std::future::Future;

    /// The client whose in-flight commit closes the gate.
    const CLIENT_B: u128 = 2;
    /// The client registering mid-window.
    const CLIENT_C: u128 = 3;
    const SESSION: u64 = 1;
    const ACTING_USER: u32 = 7;

    let dir = tempfile::tempdir().unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::new((session_users(), (Streams::default(), ()))),
            None,
        );
    let consensus = md.consensus.as_ref().unwrap();
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT_B,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT_B, SESSION),
        )
        .unwrap();

    // B's op journaled + self-acked; park its commit mid-window.
    let prepare = md
        .prepare_request(create_stream_request(CLIENT_B, 1, "s1"))
        .expect("CreateStream is client-allowed");
    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
    md.on_replicate(prepare).await;
    let mut loopback = Vec::new();
    consensus.drain_loopback_into(&mut loopback);
    let ack = loopback
        .pop()
        .expect("one self-ack per prepare")
        .try_into_typed::<PrepareOkHeader>()
        .expect("loopback holds self PrepareOks");
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut driver = Box::pin(md.on_ack(ack));
    assert!(driver.as_mut().poll(&mut cx).is_pending());
    assert_eq!(consensus.commit_max(), 1);
    assert_eq!(consensus.commit_min(), 0);

    // C's register lands in the window: absorbed, not bounced.
    let mut register = Box::pin(md.submit_register_in_process(
        CLIENT_C,
        ACTING_USER,
        RegisterRequest {
            user_created_at: 1,
            bind_verifier: [0x5a; 32],
        },
    ));
    assert!(
        register.as_mut().poll(&mut cx).is_pending(),
        "mid-window register must park in the request queue, not error"
    );
    assert_eq!(
        consensus.request_queue_len(),
        1,
        "register buffered in the request queue"
    );

    // The committing driver drains its batch, then promotes the queued
    // register into a prepare (its self-ack lands on the loopback).
    let mut resumed = false;
    for _ in 0..1_000 {
        if driver.as_mut().poll(&mut cx).is_ready() {
            resumed = true;
            break;
        }
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(resumed, "B's commit must complete and promote the register");
    assert_eq!(consensus.commit_min(), 1, "B's op committed");
    assert_eq!(
        consensus.request_queue_len(),
        0,
        "promotion emptied the request queue"
    );

    // Commit the promoted register (production: the shard pump or any
    // sibling submit drains this ack) and the parked caller resolves.
    loopback.clear();
    consensus.drain_loopback_into(&mut loopback);
    let ack = loopback
        .pop()
        .expect("promoted register must self-ack")
        .try_into_typed::<PrepareOkHeader>()
        .expect("loopback holds self PrepareOks");
    md.on_ack(ack).await;

    let mut outcome = None;
    for _ in 0..1_000 {
        if let std::task::Poll::Ready(result) = register.as_mut().poll(&mut cx) {
            outcome = Some(result);
            break;
        }
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(
        outcome.expect("absorbed register must resolve"),
        Ok(BoundSession {
            epoch: 2,
            watermark: 0
        }),
        "queued register commits with the next batch; the bind fences at its commit op"
    );
    assert_eq!(
        md.client_table.borrow().get_epoch(CLIENT_C),
        Some(2),
        "entry created by the promoted register"
    );
}
