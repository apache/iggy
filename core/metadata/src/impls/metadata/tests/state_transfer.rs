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
async fn given_retried_keyless_create_when_committed_should_store_one_envelope() {
    const CLIENT: u128 = 9;
    const USER: u32 = 0;
    let directory = tempfile::tempdir().unwrap();
    let context = Arc::new(
        ServerKeyContext::from_config("/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=", &[], true)
            .unwrap(),
    );
    let mut metadata = metadata_plane().with_stream_key_context(Arc::clone(&context));
    metadata.journal = Some(
        PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap(),
    );
    metadata.mux_stm.users().ensure_root_user("root", "hash");
    metadata
        .submit_register_in_process(
            CLIENT,
            USER,
            RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: [0x5a; 32],
            },
        )
        .await
        .unwrap();
    // Two edge rewrites of the same keyless create generate two keys.
    let envelope_block = |seed: u8| {
        let set = WrapSet::from(
            context
                .wrap(
                    &StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap(),
                    StreamKeyOrigin::Server,
                )
                .unwrap(),
        );
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
            ),
        );
        (
            set,
            resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
        )
    };
    let (first_set, first) = envelope_block(1);
    let (_, retry) = envelope_block(2);

    let committed = metadata
        .submit_request_in_process(create_stream_request_with(CLIENT, 1, "orders", first))
        .await
        .unwrap();
    let replayed = metadata
        .submit_request_in_process(create_stream_request_with(CLIENT, 1, "orders", retry))
        .await
        .unwrap();

    assert_eq!(replayed.as_slice(), committed.as_slice());
    let stored = metadata.mux_stm.streams().read(|inner| {
        let (_, stream) = inner.items.iter().next().expect("one stream");
        stream_wrap_set(&stream.options).unwrap()
    });
    assert_eq!(stored, Some(first_set));
    assert_eq!(
        metadata.mux_stm.streams().read(|inner| inner.items.len()),
        1
    );
}

#[compio::test]
async fn given_newer_local_state_when_installing_stale_transfer_should_preserve_sessions() {
    const CLIENT: u128 = 9;
    const NEW_CLIENT: u128 = 10;
    const USER: u32 = 0;
    const BIND_VERIFIER: [u8; 32] = [0x5a; 32];
    let directory = tempfile::tempdir().unwrap();
    let mut metadata = metadata_plane();
    metadata.journal = Some(
        PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap(),
    );
    metadata.mux_stm.users().ensure_root_user("root", "hash");
    let original_session = metadata
        .submit_register_in_process(
            CLIENT,
            USER,
            RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: BIND_VERIFIER,
            },
        )
        .await
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    let transfer_op = consensus.commit_min();
    let snapshot = <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, transfer_op, 1)
        .unwrap()
        .encode()
        .unwrap();
    let transferred_table = ClientTable::decode(&metadata.client_table.borrow().encode()).unwrap();

    let original_reply = metadata
        .submit_request_in_process(create_stream_request(CLIENT, 1, "original"))
        .await
        .unwrap();
    assert_eq!(
        iggy_binary_protocol::result_code(original_reply.body()),
        Some(0),
    );
    let new_session = metadata
        .submit_register_in_process(
            NEW_CLIENT,
            USER,
            RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: BIND_VERIFIER,
            },
        )
        .await
        .unwrap();
    let local_applied = consensus.commit_min();
    let local_table = metadata.client_table.borrow().encode();
    let local_snapshot = <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, local_applied, 1)
        .unwrap()
        .encode()
        .unwrap();

    let outcome = metadata
        .install_state_transfer(&snapshot, transferred_table, transfer_op, transfer_op)
        .await
        .unwrap();

    assert_eq!(outcome.installed_frontier, local_applied);
    assert_eq!(consensus.commit_min(), local_applied);
    assert_eq!(consensus.commit_max(), local_applied);
    assert_eq!(
        metadata.client_table.borrow().get_epoch(NEW_CLIENT),
        Some(new_session.epoch),
        "a stale transfer must preserve registrations above its frontier",
    );
    assert_eq!(
        metadata.client_table.borrow().encode(),
        local_table,
        "a stale transfer must preserve committed registrations and receipts",
    );
    assert_eq!(
        <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, local_applied, 1)
            .unwrap()
            .encode()
            .unwrap(),
        local_snapshot,
    );
    assert_eq!(
        metadata
            .bound_session(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: BIND_VERIFIER
                },
                consensus.clock_realtime_micros()
            )
            .unwrap()
            .epoch,
        original_session.epoch,
    );
    assert_eq!(
        metadata
            .bound_session(
                NEW_CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: BIND_VERIFIER
                },
                consensus.clock_realtime_micros()
            )
            .unwrap()
            .epoch,
        new_session.epoch,
    );
    let replay = metadata
        .submit_request_in_process(create_stream_request(CLIENT, 1, "original"))
        .await
        .unwrap();
    assert_eq!(replay.as_slice(), original_reply.as_slice());
    assert_eq!(consensus.commit_min(), local_applied);

    let mut request = create_stream_request(NEW_CLIENT, 1, "after-transfer");
    bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
        &mut request.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
    )
    .session = new_session.epoch;
    let prepare = metadata.prepare_request(request).unwrap();
    let op = prepare.header().op;
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
    assert_eq!(consensus.commit_min(), op);
    assert_eq!(
        metadata.client_table.borrow().get_watermark(NEW_CLIENT),
        Some(1)
    );
}

#[compio::test]
async fn given_ahead_client_table_when_installing_older_transfer_should_preserve_its_frontier() {
    const CLIENT: u128 = 9;
    const OTHER_CLIENT: u128 = 10;
    const NEW_CLIENT: u128 = 11;
    const USER: u32 = 7;
    const SESSION: u64 = 1;
    const TABLE_FRONTIER: u64 = 3;
    const OLDER_FRONTIER: u64 = 2;
    let metadata = metadata_plane();
    let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
    table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
    let mut snapshot = <IggySnapshot as Snapshot>::create(&TestMux::default(), SESSION, 1).unwrap();
    let mut older_table = None;
    for (client, epoch) in [
        (CLIENT, SESSION),
        (OTHER_CLIENT, OLDER_FRONTIER),
        (NEW_CLIENT, TABLE_FRONTIER),
    ] {
        let reply = build_reply_message(
            &PrepareHeader {
                client,
                user_id: USER,
                op: epoch,
                operation: Operation::Register,
                ..Default::default()
            },
            &bytes::Bytes::new(),
        );
        table
            .commit_register(client, USER, [0x5a; 32], reply)
            .unwrap();
        if epoch == SESSION {
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        }
        if epoch == OLDER_FRONTIER {
            older_table = Some(ClientTable::decode(&table.encode()).unwrap());
        }
    }
    let snapshot = snapshot.encode().unwrap();
    let expected_table = table.encode();
    metadata
        .install_state_transfer(&snapshot, table, TABLE_FRONTIER, TABLE_FRONTIER)
        .await
        .unwrap();
    metadata
        .install_state_transfer(
            &snapshot,
            older_table.unwrap(),
            OLDER_FRONTIER,
            OLDER_FRONTIER,
        )
        .await
        .unwrap();

    assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), SESSION);
    assert_eq!(metadata.client_table.borrow().encode(), expected_table);
    assert_eq!(metadata.client_table_frontier.get(), TABLE_FRONTIER);
    assert!(!metadata.client_table_mutation_allowed(TABLE_FRONTIER));
    assert!(metadata.client_table_mutation_allowed(TABLE_FRONTIER + 1));
}

#[compio::test]
async fn given_a_durable_pairing_and_a_failed_trim_when_retrying_at_the_same_op_should_keep_the_paired_snapshot()
 {
    const COMMITTED: u64 = 3;
    let dir = tempfile::tempdir().unwrap();
    let superblock = Rc::new(CheckpointSuperblock::default());
    let (md, _jobs) = job_metadata(dir.path(), 0, 1, Some(Rc::clone(&superblock))).await;
    commit_on_primary(&md, 1..=COMMITTED).await;
    let consensus = md.consensus.as_ref().unwrap();
    let journal = md.journal.as_ref().unwrap();
    let coordinator = md.coordinator.as_ref().unwrap();
    // The journal refuses a trim while another drain is in flight.
    let mut other_drain = std::pin::pin!(journal.handle().drain(1..=1));
    assert!(futures::poll!(other_drain.as_mut()).is_pending());
    md.set_checkpoint_margin(usize::MAX);
    md.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(coordinator.superblock_checkpoint_op.get(), COMMITTED);
    assert!(coordinator.checkpoint_pending.get(), "the trim failed");
    assert_eq!(journal_floor(&md), 0);
    let paired = std::fs::read(coordinator.snapshot_path()).unwrap();

    md.checkpoint_if_needed(consensus, journal).await;

    assert_eq!(
        superblock.writes.get(),
        1,
        "a second capture of the paired op must not start"
    );
    assert_eq!(
        std::fs::read(coordinator.snapshot_path()).unwrap(),
        paired,
        "the superblock pairs the checksum of the first snapshot of this op"
    );

    other_drain.await.unwrap();
    md.checkpoint_if_needed(consensus, journal).await;
    assert!(!coordinator.checkpoint_pending.get());
    assert_eq!(journal_floor(&md), COMMITTED - 1);
    assert_eq!(superblock.writes.get(), 1);
}

#[compio::test]
async fn given_a_saved_snapshot_and_failed_superblock_when_retrying_should_finish_the_same_checkpoint()
 {
    let directory = tempfile::tempdir().unwrap();
    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir).unwrap();
    let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
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
    let superblock = Rc::new(CheckpointSuperblock::default());
    superblock.fail.set(true);
    let metadata: IggyMetadata<_, PrepareJournal, (), TestMux, CheckpointSuperblock> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            Some(Rc::clone(&superblock)),
            TestMux::default(),
            Some(directory.path().to_path_buf()),
        );
    let consensus = metadata.consensus.as_ref().unwrap();
    let journal = metadata.journal.as_ref().unwrap();
    let request = CreateUserRequest {
        username: WireName::new("root").unwrap(),
        password: "hash".to_owned(),
        status: UserStatus::Active.as_code(),
        permissions: None,
        options: WireOptions::empty(),
    };
    let prepare = consensus::seal_prepare_capacity(
        build_prepare_message(
            consensus,
            &RoutedRequestHeader::default(),
            Operation::CreateUser,
            &request.to_bytes(),
        ),
        consensus.retry_capacity(&metadata.client_table.borrow()),
    );
    assert_eq!(
        metadata
            .mux_stm
            .gated_update(prepare.deep_copy())
            .unwrap()
            .code,
        0
    );
    journal.append(prepare).await.unwrap();
    consensus.sequencer().set_sequence(1);
    consensus.restore_commit_state(1, 1);
    metadata.set_checkpoint_margin(usize::MAX);
    metadata.checkpoint_if_needed(consensus, journal).await;
    let coordinator = metadata.coordinator.as_ref().unwrap();
    assert_eq!(coordinator.last_checkpoint().0, 1);
    assert!(coordinator.snapshot_path().exists());
    assert!(coordinator.checkpoint_pending.get());
    assert_eq!(superblock.writes.get(), 1);
    // A failed write may have reached the disk, so each retry must keep
    // the snapshot that it paired: a capture here would trip the boundary.
    STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::TableCaptured));
    metadata.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(
        superblock.writes.get(),
        2,
        "the retry writes the superblock"
    );
    assert!(coordinator.checkpoint_pending.get());
    superblock.fail.set(false);
    metadata.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(
        STOP_CHECKPOINT_AFTER.take(),
        Some(CheckpointBoundary::TableCaptured),
        "no retry may capture the op again"
    );
    assert!(!coordinator.checkpoint_pending.get());
    assert_eq!(coordinator.superblock_checkpoint_op.get(), 1);
    assert_eq!(superblock.writes.get(), 3);
    metadata.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(superblock.writes.get(), 3);
}

#[compio::test]
async fn given_no_applied_progress_when_forcing_checkpoints_should_not_persist_empty_state() {
    let directory = tempfile::tempdir().unwrap();
    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir).unwrap();
    let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
        .await
        .unwrap();
    let consensus = VsrConsensus::new(
        1,
        1,
        3,
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
        TestMux::default(),
        Some(directory.path().to_path_buf()),
    );
    metadata.set_checkpoint_margin(usize::MAX);
    let consensus = metadata.consensus.as_ref().unwrap();
    let journal = metadata.journal.as_ref().unwrap();
    for _ in 0..3 {
        metadata.checkpoint_if_needed(consensus, journal).await;
    }
    let coordinator = metadata.coordinator.as_ref().unwrap();
    assert_eq!(coordinator.last_checkpoint(), (0, 0));
    assert!(!coordinator.snapshot_path().exists());
    assert!(!coordinator.checkpoint_pending.get());
}

#[compio::test]
async fn transferred_client_table_waits_for_applied_frontier_before_checkpoint() {
    const CLIENT: u128 = 9;
    const USER: u32 = 7;
    const SNAPSHOT_OP: u64 = 1;
    const TABLE_FRONTIER: u64 = 3;
    const JOURNAL_SLOTS: usize = 16;
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
        TestMux::default(),
        Some(directory.path().to_path_buf()),
    );
    let mut transferred_table = ClientTable::new(CLIENTS_TABLE_MAX);
    transferred_table
        .commit_capacity(CLIENTS_TABLE_MAX)
        .unwrap();
    transferred_table
        .commit_register(
            CLIENT,
            USER,
            [0x5a; 32],
            register_reply(CLIENT, SNAPSHOT_OP),
        )
        .unwrap();
    let mut snapshot =
        <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_OP, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(transferred_table.to_snapshot());
    for request in 1..TABLE_FRONTIER {
        let reply = build_reply_message(
            &PrepareHeader {
                client: CLIENT,
                user_id: USER,
                session: SNAPSHOT_OP,
                request,
                op: request + SNAPSHOT_OP,
                operation: Operation::CreateStream,
                ..Default::default()
            },
            &bytes::Bytes::new(),
        );
        assert_eq!(
            transferred_table.commit_reply(CLIENT, USER, reply),
            CommitReply::Cached,
        );
    }
    metadata
        .install_state_transfer(
            &snapshot.encode().unwrap(),
            transferred_table,
            TABLE_FRONTIER,
            TABLE_FRONTIER,
        )
        .await
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    let journal = metadata.journal.as_ref().unwrap();
    let coordinator = metadata.coordinator.as_ref().unwrap();
    metadata.set_checkpoint_margin(usize::MAX);
    metadata.checkpoint_if_needed(consensus, journal).await;
    let persisted = IggySnapshot::load(&coordinator.snapshot_path()).unwrap().0;
    assert_eq!(persisted.sequence_number(), SNAPSHOT_OP);
    assert_eq!(
        persisted.snapshot().client_table.as_ref().unwrap().slots[0]
            .1
            .watermark,
        0,
        "ahead retry protection cannot be persisted at an earlier state-machine floor",
    );
    drop(persisted);
    for (request, name) in [(1, "s1"), (2, "s2")] {
        let prepare = metadata
            .prepare_request(create_stream_request(CLIENT, request, name))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        journal.handle().append(prepare).await.unwrap();
    }
    metadata.resume_stranded_commits().await;
    assert_eq!(consensus.commit_min(), TABLE_FRONTIER);
    metadata.checkpoint_if_needed(consensus, journal).await;
    let persisted = IggySnapshot::load(&coordinator.snapshot_path()).unwrap().0;
    assert_eq!(persisted.sequence_number(), TABLE_FRONTIER);
    drop(persisted);
    drop(metadata);
    let recovered = crate::impls::recovery::recover::<TestMux>(
        directory.path(),
        crate::impls::recovery::ReplicaIdentity {
            cluster: 1,
            replica_id: 0,
            replica_count: 1,
        },
        JOURNAL_SLOTS,
        CLIENTS_TABLE_MAX,
        |_| {},
        ChangeLogCapacities::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        recovered.client_table.get_watermark(CLIENT),
        Some(TABLE_FRONTIER - 1)
    );
}

/// No checkpoint runs below the frontier of a transferred client table, so
/// the journal must hold every op from the floor of the install up to it.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_a_transfer_tail_when_it_exceeds_the_journal_slots_should_refuse_the_install() {
    const SLOTS: usize = 4;
    const SNAPSHOT_OP: u64 = 1;
    const FRONTIER: u64 = SNAPSHOT_OP + SLOTS as u64;
    const NEXT_SNAPSHOT_OP: u64 = FRONTIER + 2;
    let directory = tempfile::tempdir().unwrap();
    let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
    std::fs::create_dir_all(&metadata_directory).unwrap();
    let journal =
        PrepareJournal::open_with_slots(&metadata_directory.join("journal.wal"), 0, SLOTS)
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
        TestMux::default(),
        Some(directory.path().to_path_buf()),
    );
    let consensus = metadata.consensus.as_ref().unwrap();
    let journal = metadata.journal.as_ref().unwrap();
    let coordinator = metadata.coordinator.as_ref().unwrap();
    metadata.set_checkpoint_margin(usize::MAX);

    // One op per slot is the largest tail that fits.
    let (snapshot, table) = transfer_with_tail(SNAPSHOT_OP, SLOTS as u64);
    metadata
        .install_state_transfer(&snapshot, table, FRONTIER, FRONTIER)
        .await
        .unwrap();
    for request in 1..=SLOTS as u64 {
        let prepare = metadata
            .prepare_request(create_stream_request(
                TAIL_CLIENT,
                request,
                &format!("s{request}"),
            ))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        journal.handle().append(prepare).await.unwrap();
    }
    metadata.resume_stranded_commits().await;
    assert_eq!(consensus.commit_min(), FRONTIER);
    let above = metadata
        .prepare_request(create_stream_request(
            TAIL_CLIENT,
            SLOTS as u64 + 1,
            "above",
        ))
        .unwrap();
    assert!(
        journal.handle().append(above.clone()).await.is_err(),
        "the tail fills the journal, so one more op needs a checkpoint"
    );
    metadata.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(coordinator.last_checkpoint().0, FRONTIER);
    journal.handle().append(above).await.unwrap();

    let durable_state = || {
        (
            std::fs::read(coordinator.snapshot_path()).unwrap(),
            coordinator.last_checkpoint(),
            coordinator.superblock_checkpoint_op.get(),
            journal.handle().snapshot_op(),
            journal.handle().last_op(),
            metadata.client_table_frontier.get(),
            consensus.commit_min(),
            consensus.commit_max(),
        )
    };
    let before = durable_state();

    // One more op than slots above a snapshot that is ahead.
    let tail = SLOTS as u64 + 1;
    let frontier = NEXT_SNAPSHOT_OP + tail;
    let (snapshot, table) = transfer_with_tail(NEXT_SNAPSHOT_OP, tail);
    let error = metadata
        .install_state_transfer(&snapshot, table, frontier, frontier)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            SnapshotError::TransferTailExceedsJournal {
                tail: refused,
                journal_slots: SLOTS,
            } if refused == tail
        ),
        "{error}"
    );
    assert!(error.to_string().contains("`metadata.journal_slots`"));
    assert_eq!(
        durable_state(),
        before,
        "a refused install must leave the replica as it was"
    );

    // A snapshot at the commit point is not ahead and installs its table
    // only, so the floor stays below the entry of the last checkpoint.
    let floor = journal.handle().snapshot_op();
    assert_eq!(floor, FRONTIER - 1);
    let fitting = floor + SLOTS as u64;
    let (snapshot, table) = transfer_with_tail(FRONTIER, fitting + 1 - FRONTIER);
    assert!(matches!(
        metadata
            .install_state_transfer(&snapshot, table, fitting + 1, fitting + 1)
            .await,
        Err(SnapshotError::TransferTailExceedsJournal { .. })
    ));
    assert_eq!(durable_state(), before);
    let (snapshot, table) = transfer_with_tail(FRONTIER, fitting - FRONTIER);
    metadata
        .install_state_transfer(&snapshot, table, fitting, fitting)
        .await
        .unwrap();
    assert_eq!(metadata.client_table_frontier.get(), fitting);
}

/// The commit loop and the promotion of queued requests run at the tail
/// of `on_ack`, inside whichever future delivered the quorum ack. Drop
/// that future mid-commit and — on an idle server — nothing re-drives
/// the work: duplicate/repair acks do not re-open the commit path
/// (quorum already recorded, `commit_max` does not advance), so the
/// committed-but-unapplied op pins the catch-up gate closed and an
/// absorbed register parks in the request queue indefinitely.
///
/// `resume_stranded_commits` (wired into the shard pump tick) is the
/// backstop: it re-enters the commit path, applies the stranded prefix,
/// and promotes the queued register, whose awaiter then resolves.
/// A state-transfer receiver's WAL can hold a pre-crash suffix above the
/// incoming floor. The commit walk matches entries by op number alone, so
/// installing without discarding that suffix would later apply its stale
/// bodies as committed. The install discards that suffix and keeps the
/// boundary header only when the adopted peer log confirms its checksum.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_a_state_transfer_when_installing_should_keep_only_a_verified_boundary() {
    for peer_matches in [None, Some(false), Some(true)] {
        const CLIENT: u128 = 9;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const SNAPSHOT_SEQ: u64 = 2;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

        // Journal ops 1..=3 directly (no acks, so `commit_min` stays 0 and the
        // transfer is "ahead"): 1 and 2 sit at or below the incoming floor, 3 is
        // the relic suffix a view change has since reassigned.
        for (request_id, name) in [(1, "s1"), (2, "s2"), (3, "s3")] {
            let projected = md
                .prepare_request(create_stream_request(CLIENT, request_id, name))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &projected);
            md.journal
                .as_ref()
                .unwrap()
                .handle()
                .append(projected)
                .await
                .expect("seeding the WAL succeeds");
        }
        let journal_handle = md.journal.as_ref().unwrap().handle();
        assert_eq!(journal_handle.last_op(), Some(3));
        consensus.fence_view_log(SNAPSHOT_SEQ + 1, u64::MAX);

        // A donor mux fills the snapshot the way a serving primary would, so
        // the restore path sees populated sections rather than a bare envelope.
        for missing in [true, false] {
            for frontier in [SNAPSHOT_SEQ, SNAPSHOT_SEQ + 1] {
                let mut invalid_snapshot =
                    <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_SEQ, 1)
                        .unwrap();
                if !missing {
                    let mut table = ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot();
                    table.capacity = 0;
                    invalid_snapshot.snapshot_mut().client_table = Some(table);
                }
                assert!(matches!(
                    md.install_state_transfer(
                        &invalid_snapshot.encode().unwrap(),
                        ClientTable::new(CLIENTS_TABLE_MAX),
                        frontier,
                        frontier
                    )
                    .await,
                    Err(SnapshotError::MissingClientTable | SnapshotError::ClientTable(_))
                ));
                assert_eq!(
                    journal_handle.last_op(),
                    Some(3),
                    "invalid protection must not truncate the WAL"
                );
                assert_eq!(consensus.commit_min(), 0);
                assert_eq!(md.client_table.borrow().get_epoch(CLIENT), Some(SESSION));
            }
        }
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_SEQ, 1)
            .expect("donor snapshot builds");
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let snapshot_bytes = snapshot.encode().expect("donor snapshot encodes");
        if let Some(matches) = peer_matches {
            let mut peer = *journal_handle
                .header(usize::try_from(SNAPSHOT_SEQ).unwrap())
                .unwrap();
            if !matches {
                peer.timestamp += 1;
                peer.checksum = peer.identity_checksum();
            }
            let header = StartViewHeader {
                checksum: 0,
                checksum_body: 0,
                cluster: 1,
                size: u32::try_from(size_of::<StartViewHeader>() + size_of::<PrepareHeader>())
                    .unwrap(),
                view: 1,
                release: 0,
                command: Command::StartView,
                replica: 1,
                reserved_frame: [0; 66],
                op: SNAPSHOT_SEQ,
                commit: SNAPSHOT_SEQ,
                group: server_common::sharding::METADATA_GROUP,
                partition_incarnation: 0,
                reserved: [0; 80],
                incarnation: 0,
            };
            let _ = consensus.handle_start_view(
                PlaneKind::Metadata,
                &header,
                bytemuck::bytes_of(&peer),
            );
            assert!(consensus.view_log_is_pending());
        }
        md.install_state_transfer(&snapshot_bytes, table, SNAPSHOT_SEQ, SNAPSHOT_SEQ)
            .await
            .expect("install succeeds");

        assert_eq!(
            journal_handle.last_op(),
            Some(if peer_matches == Some(true) {
                SNAPSHOT_SEQ
            } else {
                SNAPSHOT_SEQ - 1
            }),
            "the unverified boundary and relic above it must be gone"
        );
        assert!(
            journal_handle.header(3).is_none(),
            "op 3 was above the floor; the commit walk must never see it again"
        );
        assert!(
            journal_handle.header(1).is_some()
                && journal_handle.header(2).is_some() == (peer_matches == Some(true)),
            "the snapshot replaces an unverified boundary header too"
        );
        assert_eq!(
            md.applied_frontier().get(),
            SNAPSHOT_SEQ,
            "the snapshot IS ops up to its sequence applied, so the read gate has \
         to admit reads at the floor the install jumped to"
        );
    }
}

#[compio::test]
async fn given_an_adopted_log_when_persistence_fails_or_completes_should_reopen_one_complete_identity()
 {
    const ANNOUNCED_VIEW: u32 = ADOPTED_VIEW + 3;
    const ANNOUNCED_HEAD: u64 = TRANSFER_RECEIVER_HEAD + consensus::DVC_HEADERS_MAX as u64;
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let consensus = md.consensus.as_ref().unwrap();
    let journal = md.journal.as_ref().unwrap().handle();
    let original: Vec<_> = (1..=TRANSFER_RECEIVER_HEAD)
        .map(|op| *journal.header(usize::try_from(op).unwrap()).unwrap())
        .collect();
    let mut headers = Vec::new();
    let mut parent = *original.last().unwrap();
    for op in TRANSFER_RECEIVER_HEAD + 1..=ANNOUNCED_HEAD {
        let mut header = PrepareHeader {
            op,
            view: ANNOUNCED_VIEW,
            timestamp: parent.timestamp + 1,
            request: op,
            parent: parent.checksum,
            ..parent
        };
        header.checksum = header.identity_checksum();
        headers.push(header);
        parent = header;
    }
    headers.reverse();
    let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
        .transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = TRANSFER_RECEIVER.cluster;
            header.group = server_common::sharding::METADATA_GROUP;
            header.view = ANNOUNCED_VIEW;
            header.replica = consensus.primary_index(ANNOUNCED_VIEW);
            header.op = ANNOUNCED_HEAD;
            header.commit = ANNOUNCED_HEAD - 1;
            header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
        });
    consensus.handle_start_view(
        PlaneKind::Metadata,
        start_view.header(),
        bytemuck::cast_slice(&headers),
    );
    assert_eq!(consensus.log_view_claim(), ANNOUNCED_VIEW);
    assert!(!consensus.history_certified());
    assert!(consensus.needs_superblock_persist());
    let before = crash_image(directory.path());
    {
        let mut write = Box::pin(md.persist_superblock_if_needed(consensus));
        assert!(futures::poll!(&mut write).is_pending());
    }
    let cancelled = crash_image(directory.path());

    // The first write used A. Refuse B's atomic rename without touching A.
    let blocked_slot = directory
        .path()
        .join(crate::impls::METADATA_DIR)
        .join(journal::superblock::SLOT_FILE_NAMES[1]);
    std::fs::create_dir(&blocked_slot).unwrap();
    assert!(!md.persist_superblock_if_needed(consensus).await);
    assert!(consensus.needs_superblock_persist());
    std::fs::remove_dir(&blocked_slot).unwrap();
    let failed = crash_image(directory.path());
    md.superblock_retry_after_micros.set(0);
    assert!(md.persist_superblock_if_needed(consensus).await);
    assert!(!consensus.needs_superblock_persist());
    let completed = crash_image(directory.path());

    for (image, persisted) in [
        (before, false),
        (cancelled, false),
        (failed, false),
        (completed, true),
    ] {
        let recovered = recover::<TestMux>(
            image.path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        let state = recovered.recovered_state.as_ref().unwrap();
        assert_eq!(state.adopted_view.is_some(), persisted);
        assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
        for header in &original {
            assert_eq!(
                recovered
                    .journal
                    .header(usize::try_from(header.op).unwrap())
                    .unwrap()
                    .checksum,
                header.checksum
            );
        }
        let mut restarted = VsrConsensus::new(
            TRANSFER_RECEIVER.cluster,
            TRANSFER_RECEIVER.replica_id,
            TRANSFER_RECEIVER.replica_count,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        restarted.set_view(state.view);
        restarted.set_log_view(recovered.history.log_view());
        restarted.sequencer().set_sequence(TRANSFER_RECEIVER_HEAD);
        restarted.restore_history(
            recovered.history,
            (TRANSFER_RECEIVER_HEAD, original.last().unwrap().checksum),
            Some(state),
        );
        restarted.restore_adopted_view(state.adopted_view.as_ref());
        restarted.resume_adopted_view();
        if persisted {
            assert_eq!(
                restarted.adopted_view_head(),
                Some((ANNOUNCED_VIEW, ANNOUNCED_HEAD))
            );
            assert_eq!(restarted.log_view_claim(), ANNOUNCED_VIEW);
            assert_eq!(restarted.sequencer().current_sequence(), ANNOUNCED_HEAD);
            assert_eq!(restarted.adopted_view_log().unwrap().headers, headers);
            assert!(restarted.view_log_unproven());
            assert!(!restarted.history_certified());
            assert!(
                recovered
                    .journal
                    .header(usize::try_from(ANNOUNCED_HEAD).unwrap())
                    .is_none()
            );
        } else {
            assert_eq!(restarted.log_view_claim(), CERTIFIED_VIEW);
            assert_eq!(
                restarted.sequencer().current_sequence(),
                TRANSFER_RECEIVER_HEAD
            );
        }
    }
}

/// A crash right after the install truncates the WAL above the incoming floor
/// must restart certified. The truncate removes the entry the certificate was
/// bound to, so the superblock must already hold a binding the restart can
/// prove. One naming a removed entry restarts `Unproven`, which keeps the
/// replica out of every election until a `StartView` proves its log.
#[compio::test]
async fn given_a_crash_after_the_transfer_truncate_when_restarting_should_stay_certified() {
    for coordinated in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let md = transfer_receiver(dir.path(), coordinated).await;
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let snapshot_bytes = snapshot.encode().unwrap();
        let journal = md.journal.as_ref().unwrap().handle();
        let mut install = Box::pin(md.install_state_transfer(
            &snapshot_bytes,
            table,
            TRANSFER_FLOOR,
            TRANSFER_FLOOR,
        ));
        let mut image = None;
        let mut outcome = None;
        for _ in 0..INSTALL_POLL_BUDGET {
            let polled = futures::poll!(&mut install);
            if image.is_none() && journal.last_op() == Some(TRANSFER_FLOOR - 1) {
                image = Some(crash_image(dir.path()));
            }
            if let std::task::Poll::Ready(result) = polled {
                outcome = Some(result);
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            matches!(outcome, Some(Ok(_))),
            "coordinated {coordinated}: the install did not complete: {outcome:?}"
        );
        let image = image.expect("the install truncates the WAL above its floor");
        assert_eq!(
            restarted_history(image.path()).await,
            transfer_restart(coordinated),
            "coordinated {coordinated}"
        );
    }
}

#[compio::test]
async fn given_a_vouched_transfer_suffix_when_installing_and_restarting_should_keep_its_prefix() {
    const FIRST_TAIL: u64 = TRANSFER_FLOOR + 1;
    for coordinated in [true, false] {
        for (retained, boundary_matches, bound_at_boundary) in [
            (TRANSFER_FLOOR, true, false),
            (FIRST_TAIL, true, false),
            (TRANSFER_RECEIVER_HEAD, true, false),
            (FIRST_TAIL, false, false),
            (TRANSFER_RECEIVER_HEAD, false, false),
            (FIRST_TAIL, false, true),
            (TRANSFER_RECEIVER_HEAD, false, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let md = transfer_receiver(directory.path(), coordinated).await;
            let consensus = md.consensus.as_ref().unwrap();
            consensus.set_log_view(CERTIFIED_VIEW);
            let journal = md.journal.as_ref().unwrap().handle();
            let mut headers: Vec<_> = (TRANSFER_FLOOR..=TRANSFER_RECEIVER_HEAD)
                .map(|op| *journal.header(usize::try_from(op).unwrap()).unwrap())
                .collect();
            for index in 1..headers.len() {
                if headers[index].op > retained {
                    headers[index].timestamp += 1;
                    headers[index].parent = headers[index - 1].checksum;
                    headers[index].checksum = headers[index].identity_checksum();
                }
            }
            headers.reverse();
            let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
                .transmute_header(|_, header: &mut StartViewHeader| {
                    header.command = Command::StartView;
                    header.cluster = TRANSFER_RECEIVER.cluster;
                    header.replica = consensus.primary_index(ADOPTED_VIEW);
                    header.view = ADOPTED_VIEW;
                    header.op = TRANSFER_RECEIVER_HEAD;
                    header.commit = TRANSFER_FLOOR;
                    header.group = server_common::sharding::METADATA_GROUP;
                    header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
                });
            consensus.handle_start_view(
                PlaneKind::Metadata,
                start_view.header(),
                bytemuck::cast_slice(&headers),
            );
            assert!(consensus.view_log_unproven());
            if !boundary_matches {
                let mut boundary = *journal
                    .header(usize::try_from(TRANSFER_FLOOR).unwrap())
                    .unwrap();
                let entry = journal.entry_at(&boundary).await.unwrap().unwrap();
                if bound_at_boundary {
                    consensus.lower_history_certificate(
                        TRANSFER_FLOOR,
                        TRANSFER_FLOOR,
                        boundary.checksum,
                    );
                }
                journal
                    .remove_range(TRANSFER_FLOOR..=TRANSFER_FLOOR)
                    .await
                    .unwrap();
                boundary.timestamp += 1;
                boundary.checksum = boundary.identity_checksum();
                let stale =
                    entry.transmute_header(|_, header: &mut PrepareHeader| *header = boundary);
                let original = *journal
                    .header(usize::try_from(FIRST_TAIL).unwrap())
                    .unwrap();
                assert_ne!(original.parent, boundary.checksum);
                journal.append(stale).await.unwrap();
            }
            let expected: Vec<_> = (FIRST_TAIL..=retained)
                .map(|op| {
                    (
                        op,
                        journal
                            .header(usize::try_from(op).unwrap())
                            .unwrap()
                            .checksum,
                    )
                })
                .collect();
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut snapshot =
                IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            md.install_state_transfer(
                &snapshot.encode().unwrap(),
                table,
                TRANSFER_FLOOR,
                TRANSFER_FLOOR,
            )
            .await
            .unwrap();
            assert_eq!(
                journal.last_op(),
                Some(retained),
                "coordinated {coordinated}"
            );
            assert_eq!(
                journal
                    .header(usize::try_from(TRANSFER_FLOOR).unwrap())
                    .is_some(),
                boundary_matches
            );
            if bound_at_boundary {
                assert!(consensus.history_certificate().unwrap().op < TRANSFER_FLOOR);
            }
            for (op, checksum) in &expected {
                assert_eq!(
                    journal
                        .header(usize::try_from(*op).unwrap())
                        .unwrap()
                        .checksum,
                    *checksum
                );
            }
            let image = crash_image(directory.path());
            assert_eq!(
                restarted_history(image.path()).await,
                (
                    HistoryRecovery::Certified {
                        view: CERTIFIED_VIEW,
                        truncate_from: Some(retained + 1)
                    },
                    Some(retained)
                ),
                "coordinated {coordinated}, retained {retained}"
            );
        }
    }
}

#[compio::test]
async fn given_a_proven_log_with_a_gap_below_the_snapshot_when_installing_should_keep_the_suffix_across_crashes()
 {
    const MISSING: u64 = TRANSFER_FLOOR;
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let consensus = md.consensus.as_ref().unwrap();
    let journal = md.journal.as_ref().unwrap().handle();
    consensus.certify_view_log(
        TRANSFER_RECEIVER_HEAD,
        journal
            .header(usize::try_from(TRANSFER_RECEIVER_HEAD).unwrap())
            .unwrap()
            .checksum,
    );
    assert!(consensus.history_certified());
    journal.remove_range(MISSING..=MISSING).await.unwrap();
    let expected: Vec<_> = (TRANSFER_FLOOR + 1..=TRANSFER_RECEIVER_HEAD)
        .map(|op| {
            (
                op,
                journal
                    .header(usize::try_from(op).unwrap())
                    .unwrap()
                    .checksum,
            )
        })
        .collect();
    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    let mut snapshot = IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
    let bytes = snapshot.encode().unwrap();
    let mut install =
        Box::pin(md.install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR));
    let mut image = None;
    let mut outcome = None;
    for _ in 0..INSTALL_POLL_BUDGET {
        let polled = futures::poll!(&mut install);
        if image.is_none()
            && let journal::superblock::SuperblockContents::Present(record) =
                md.superblock.as_ref().unwrap().read_latest().await.unwrap()
            && consensus::VsrState::try_from(record.as_slice())
                .unwrap()
                .checkpoint_op
                == TRANSFER_FLOOR
        {
            image = Some(crash_image(directory.path()));
        }
        if let std::task::Poll::Ready(result) = polled {
            outcome = Some(result);
            break;
        }
        compio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(matches!(outcome, Some(Ok(_))), "{outcome:?}");
    assert_eq!(journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
    let image = image.expect("the snapshot pairing reached the superblock");
    for image in [image, crash_image(directory.path())] {
        let recovered = recover::<crate::MuxStateMachine<()>>(
            image.path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
        for (op, checksum) in &expected {
            assert_eq!(
                recovered
                    .journal
                    .header(usize::try_from(*op).unwrap())
                    .unwrap()
                    .checksum,
                *checksum
            );
        }
        assert_eq!(recovered.chain_break_op, None);
    }
}

/// An install that fails once the binding moved must also restart certified,
/// at once and after the next tick persists what the failed install left in
/// memory. A snapshot with no state machine sections fails the in-place
/// restore, the last step that can fail.
#[compio::test]
async fn given_an_install_error_after_the_binding_moved_when_restarting_should_stay_certified() {
    for coordinated in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let md = transfer_receiver(dir.path(), coordinated).await;
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::new(TRANSFER_FLOOR);
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let installed = md
            .install_state_transfer(
                &snapshot.encode().unwrap(),
                table,
                TRANSFER_FLOOR,
                TRANSFER_FLOOR,
            )
            .await;
        assert!(
            matches!(installed, Err(SnapshotError::Deserialize(_))),
            "coordinated {coordinated}: {installed:?}"
        );
        let failed = crash_image(dir.path());
        assert!(
            md.persist_superblock_if_needed(md.consensus.as_ref().unwrap())
                .await
        );
        let ticked = crash_image(dir.path());
        for (image, moment) in [(failed, "the failed install"), (ticked, "the next tick")] {
            assert_eq!(
                restarted_history(image.path()).await,
                transfer_restart(coordinated),
                "coordinated {coordinated}, crash after {moment}"
            );
        }
    }
}

#[compio::test]
async fn given_a_failed_install_when_an_older_offer_arrives_should_keep_the_durable_checkpoint() {
    const OLDER: u64 = TRANSFER_FLOOR - 1;
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let consensus = md.consensus.as_ref().unwrap();
    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    let wal_path = metadata_dir.join("journal.wal");
    let saved_wal = metadata_dir.join("journal.saved");
    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    let incoming_state = TestMux::default();
    incoming_state.users().ensure_root_user("restored", "hash");
    let mut snapshot = IggySnapshot::create(&incoming_state, TRANSFER_FLOOR, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
    let bytes = snapshot.encode().unwrap();

    // Keep the open WAL readable but make the truncate's rename fail, after
    // the new snapshot and its superblock pairing have reached disk.
    std::fs::rename(&wal_path, &saved_wal).unwrap();
    std::fs::create_dir(&wal_path).unwrap();
    let failed = md
        .install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
        .await;
    std::fs::remove_dir(&wal_path).unwrap();
    std::fs::rename(&saved_wal, &wal_path).unwrap();
    assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
    assert_eq!(consensus.commit_min(), 0);
    assert_eq!(
        md.coordinator.as_ref().unwrap().last_checkpoint().0,
        TRANSFER_FLOOR
    );
    let before = std::fs::read(metadata_dir.join("snapshot.bin")).unwrap();
    let failed_image = crash_image(directory.path());

    let older_table = ClientTable::new(CLIENTS_TABLE_MAX);
    let mut older = IggySnapshot::create(&TestMux::default(), OLDER, 1).unwrap();
    older.snapshot_mut().client_table = Some(older_table.to_snapshot());
    let refused = md
        .install_state_transfer(&older.encode().unwrap(), older_table, OLDER, OLDER)
        .await;
    assert!(
        matches!(refused, Err(SnapshotError::InvalidState(_))),
        "an older offer must not replace a newer durable checkpoint: {refused:?}"
    );
    assert_eq!(
        std::fs::read(metadata_dir.join("snapshot.bin")).unwrap(),
        before
    );
    assert_eq!(consensus.commit_min(), 0);
    let refused_image = crash_image(directory.path());
    for image in [failed_image, refused_image] {
        let recovered = recover::<TestMux>(
            image.path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.snapshot_checkpoint.0, TRANSFER_FLOOR);
        assert_eq!(recovered.journal.last_op(), Some(TRANSFER_FLOOR));
    }

    // The durable floor must not make an equal retry skip the STM restore.
    for retry_floor in [TRANSFER_FLOOR, TRANSFER_FLOOR + 1] {
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut retry = IggySnapshot::create(&incoming_state, retry_floor, 1).unwrap();
        let expected_state = retry.encode().unwrap();
        retry.snapshot_mut().client_table = Some(table.to_snapshot());
        let retried = md
            .install_state_transfer(&retry.encode().unwrap(), table, retry_floor, retry_floor)
            .await
            .unwrap();
        assert_eq!(retried.installed_frontier, retry_floor);
        assert_eq!(consensus.commit_min(), retry_floor);
        assert_eq!(
            IggySnapshot::create(&*md.mux_stm, retry_floor, 1)
                .unwrap()
                .encode()
                .unwrap(),
            expected_state
        );
        let recovered = recover::<TestMux>(
            crash_image(directory.path()).path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.snapshot_checkpoint.0, retry_floor);
    }
}

#[compio::test]
async fn given_a_failed_install_when_an_offer_of_the_same_op_arrives_should_keep_the_durable_snapshot()
 {
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    let wal_path = metadata_dir.join("journal.wal");
    let saved_wal = metadata_dir.join("journal.saved");
    let incoming_state = TestMux::default();
    incoming_state.users().ensure_root_user("restored", "hash");
    let offer = |created_at| {
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot =
            IggySnapshot::create(&incoming_state, TRANSFER_FLOOR, created_at).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        (snapshot.encode().unwrap(), table)
    };

    // Keep the open WAL readable but make the truncate's rename fail, after
    // the snapshot and its superblock pairing have reached disk.
    let (first, table) = offer(1);
    std::fs::rename(&wal_path, &saved_wal).unwrap();
    std::fs::create_dir(&wal_path).unwrap();
    let failed = md
        .install_state_transfer(&first, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
        .await;
    std::fs::remove_dir(&wal_path).unwrap();
    std::fs::rename(&saved_wal, &wal_path).unwrap();
    assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
    let durable = std::fs::read(metadata_dir.join("snapshot.bin")).unwrap();

    // The checkpoint of another primary at the same op, stamped at another time.
    let (second, table) = offer(2);
    assert_ne!(first, second);
    // A crash once a rewritten snapshot is durable, before its superblock write.
    STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::DirectorySynced));
    let installed = md
        .install_state_transfer(&second, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
        .await;
    let image = crash_image(directory.path());
    let boundary = STOP_CHECKPOINT_AFTER.take();
    let recovered = recover::<TestMux>(
        image.path(),
        TRANSFER_RECEIVER,
        DEFAULT_SLOT_COUNT,
        CLIENTS_TABLE_MAX,
        |_| {},
        ChangeLogCapacities::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(metadata_dir.join("snapshot.bin")).unwrap(),
        durable
    );
    assert_eq!(
        recovered.snapshot_checkpoint,
        md.coordinator.as_ref().unwrap().last_checkpoint()
    );
    assert_eq!(
        boundary,
        Some(CheckpointBoundary::DirectorySynced),
        "an offer of the durable op must not rewrite its snapshot"
    );
    assert!(installed.is_ok(), "{installed:?}");
    assert_eq!(md.consensus.as_ref().unwrap().commit_min(), TRANSFER_FLOOR);
}

#[compio::test]
async fn given_a_pending_checkpoint_after_failed_transfer_when_retried_should_keep_the_newer_snapshot()
 {
    /// Two ops above the commit point, so a trim to it would drain an op
    /// above that point too.
    const INSTALLED: u64 = TRANSFER_FLOOR + 1;
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let consensus = md.consensus.as_ref().unwrap();
    let journal = md.journal.as_ref().unwrap();
    let coordinator = md.coordinator.as_ref().unwrap();
    consensus.clear_pipeline();
    consensus.advance_commit_max(TRANSFER_FLOOR - 1);
    md.commit_journal().await;
    assert_eq!(consensus.commit_min(), TRANSFER_FLOOR - 1);
    md.set_checkpoint_margin(usize::MAX);
    STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::TableCaptured));
    md.checkpoint_if_needed(consensus, journal).await;
    assert!(STOP_CHECKPOINT_AFTER.get().is_none());
    assert!(coordinator.checkpoint_pending.get());

    let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
    let wal_path = metadata_dir.join("journal.wal");
    let saved_wal = metadata_dir.join("journal.saved");
    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    let mut snapshot = IggySnapshot::create(&TestMux::default(), INSTALLED, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
    std::fs::rename(&wal_path, &saved_wal).unwrap();
    std::fs::create_dir(&wal_path).unwrap();
    let failed = md
        .install_state_transfer(&snapshot.encode().unwrap(), table, INSTALLED, INSTALLED)
        .await;
    std::fs::remove_dir(&wal_path).unwrap();
    std::fs::rename(&saved_wal, &wal_path).unwrap();
    assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
    assert_eq!(consensus.commit_min(), TRANSFER_FLOOR - 1);
    assert_eq!(coordinator.last_checkpoint().0, INSTALLED);
    assert!(coordinator.checkpoint_pending.get());
    let before = std::fs::read(coordinator.snapshot_path()).unwrap();

    md.checkpoint_if_needed(consensus, journal).await;
    for op in consensus.commit_min()..INSTALLED {
        assert!(
            journal
                .handle()
                .header(usize::try_from(op).unwrap())
                .is_some(),
            "op {op}: the pairing is ahead of the commit point, so no trim may reach it"
        );
    }
    assert_eq!(journal.handle().snapshot_op(), 0);
    assert!(coordinator.checkpoint_pending.get());
    assert_eq!(std::fs::read(coordinator.snapshot_path()).unwrap(), before);
    let recovered = recover::<TestMux>(
        crash_image(directory.path()).path(),
        TRANSFER_RECEIVER,
        DEFAULT_SLOT_COUNT,
        CLIENTS_TABLE_MAX,
        |_| {},
        ChangeLogCapacities::default(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.snapshot_checkpoint.0, INSTALLED);

    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    md.install_state_transfer(&snapshot.encode().unwrap(), table, INSTALLED, INSTALLED)
        .await
        .unwrap();
    md.checkpoint_if_needed(consensus, journal).await;
    assert_eq!(consensus.commit_min(), INSTALLED);
    assert!(!coordinator.checkpoint_pending.get());
}

#[compio::test]
async fn given_a_vouched_suffix_when_commits_walk_during_snapshot_io_should_preserve_their_receipts()
 {
    const CLIENT: u128 = 1;
    let directory = tempfile::tempdir().unwrap();
    let md = transfer_receiver(directory.path(), true).await;
    let consensus = md.consensus.as_ref().unwrap();
    let journal = md.journal.as_ref().unwrap().handle();
    consensus.clear_pipeline();
    consensus.certify_view_log(
        TRANSFER_RECEIVER_HEAD,
        journal
            .header(usize::try_from(TRANSFER_RECEIVER_HEAD).unwrap())
            .unwrap()
            .checksum,
    );
    let table = ClientTable::new(CLIENTS_TABLE_MAX);
    let mut snapshot = IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
    snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
    let bytes = snapshot.encode().unwrap();
    let held = SNAPSHOT_IO.lock().await;
    let mut install =
        Box::pin(md.install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR));
    assert!(futures::poll!(&mut install).is_pending());
    consensus.advance_commit_max(TRANSFER_RECEIVER_HEAD);
    md.commit_journal().await;
    assert_eq!(consensus.commit_min(), TRANSFER_RECEIVER_HEAD);
    assert_eq!(
        md.client_table.borrow().get_watermark(CLIENT),
        Some(TRANSFER_RECEIVER_HEAD)
    );
    let applied = IggySnapshot::create(&*md.mux_stm, TRANSFER_RECEIVER_HEAD, 1)
        .unwrap()
        .encode()
        .unwrap();
    let table_after_walk = md.client_table.borrow().encode();
    drop(held);
    let outcome = install.await.unwrap();
    assert_eq!(outcome.installed_frontier, TRANSFER_RECEIVER_HEAD);
    assert_eq!(journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
    assert_eq!(md.client_table.borrow().encode(), table_after_walk);
    assert_eq!(
        IggySnapshot::create(&*md.mux_stm, TRANSFER_RECEIVER_HEAD, 1)
            .unwrap()
            .encode()
            .unwrap(),
        applied
    );
    let recovered = recover::<TestMux>(
        crash_image(directory.path()).path(),
        TRANSFER_RECEIVER,
        DEFAULT_SLOT_COUNT,
        CLIENTS_TABLE_MAX,
        |_| {},
        ChangeLogCapacities::default(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
    assert_eq!(recovered.chain_break_op, None);
}

#[compio::test]
async fn tick_backstop_must_resume_stranded_commits_and_promotions() {
    use std::future::Future;

    /// The client whose in-flight commit is stranded by the dropped driver.
    const CLIENT_B: u128 = 2;
    /// The client whose register parks in the request queue.
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

    // B's op journaled + self-acked; park its commit driver mid-window
    // at the journal read.
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

    // C's register lands in the window: absorbed into the request queue.
    let mut register = Box::pin(md.submit_register_in_process(
        CLIENT_C,
        ACTING_USER,
        RegisterRequest {
            credential: CredentialProof::account(1, 1),
            bind_verifier: [0x5a; 32],
        },
    ));
    assert!(register.as_mut().poll(&mut cx).is_pending());
    assert_eq!(consensus.request_queue_len(), 1);

    // The committing driver dies at its await — the hyper-disconnect
    // analogue. Commit and promotion are now stranded: op 1 is quorum'd
    // (commit_max = 1) but unapplied (commit_min = 0), and no further
    // ack will arrive to re-drive either.
    drop(driver);
    assert_eq!(consensus.commit_max(), 1);
    assert_eq!(consensus.commit_min(), 0);
    assert_eq!(consensus.request_queue_len(), 1);
    assert!(
        register.as_mut().poll(&mut cx).is_pending(),
        "queued register must still be parked with no driver alive"
    );

    // The pump tick backstop re-drives: commits op 1 (reopening the
    // catch-up gate) and promotes the queued register into a prepare
    // (its self-ack lands on the loopback).
    md.resume_stranded_commits().await;
    assert_eq!(consensus.commit_min(), 1, "stranded op 1 applied");
    assert_eq!(consensus.request_queue_len(), 0, "queued register promoted");

    // Commit the promoted register (production: pump loopback drain)
    // and the parked caller resolves with its session.
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
        outcome.expect("promoted register must resolve"),
        Ok(BoundSession {
            epoch: 2,
            watermark: 0
        })
    );
    assert_eq!(md.client_table.borrow().get_epoch(CLIENT_C), Some(2));
    assert!(is_caught_up_primary(consensus));
}

#[compio::test]
async fn failed_journal_append_hands_the_op_back_instead_of_leaving_a_phantom() {
    // The primary claims its op before the append (`push_prepare_entry`), so a
    // failed append used to leave the sequencer one ahead of the WAL forever:
    // the next request projected over the hole, and no repair path refilled it.
    const CLIENT: u128 = 1;
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
    md.client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    let projected = md
        .prepare_request(create_stream_request(CLIENT, 1, "s1"))
        .expect("CreateStream is client-allowed");
    let op = projected.header().op;
    let parent = projected.header().parent;
    let sequence_before = consensus.sequencer().current_sequence();

    // Forces the append to fail deterministically, before any disk write: the
    // buffer carries eight bytes of slack past the header's `size`, which
    // `PrepareJournal::append` refuses rather than write slack that would
    // mis-frame the recovery scan. Any append failure reaches the same arm.
    let size = projected.header().size as usize;
    let mut padded = Message::<PrepareHeader>::new(size + 8);
    padded.as_mut_slice()[..size].copy_from_slice(projected.as_slice());

    consensus.pipeline_message(PlaneKind::Metadata, &padded);
    assert_eq!(consensus.sequencer().current_sequence(), op);

    md.on_replicate(padded).await;

    assert_eq!(
        consensus.sequencer().current_sequence(),
        sequence_before,
        "the claimed op must be handed back so the next request reuses it"
    );
    assert_eq!(consensus.last_prepare_checksum(), parent);
    assert!(
        consensus.pipeline_is_empty(),
        "the undurable prepare must not stay live in the pipeline"
    );
    #[allow(clippy::cast_possible_truncation)]
    let journaled = md.journal.as_ref().unwrap().handle().header(op as usize);
    assert!(
        journaled.is_none(),
        "the append failed, so the WAL must hold nothing at that op"
    );
}
