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
fn given_cancelled_snapshot_writer_when_replaced_should_keep_the_new_snapshot() {
    let runtime = server_common::executor::create_shard_executor().unwrap();
    runtime.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.bin");
        let first = IggySnapshot::create(&TestMux::default(), 1, 1).unwrap();
        let mut cancelled = Box::pin(first.persist(&path));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);

        let replacement = IggySnapshot::create(&TestMux::default(), 2, 2).unwrap();
        replacement.persist(&path).await.unwrap();
        let (loaded, checksum) = IggySnapshot::load(&path).unwrap();
        assert_eq!(loaded.sequence_number(), 2);
        assert_eq!(
            checksum,
            checkpoint_checksum(&replacement.encode().unwrap())
        );
        assert!(!path.with_extension("bin.tmp").exists());
    });
}

#[compio::test]
async fn given_cached_offer_when_installing_snapshot_should_invalidate_without_rereading() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
    let mut metadata = metadata_plane();
    metadata.coordinator = Some(SnapshotCoordinator::new(
        directory.path().to_path_buf(),
        IggySnapshot::create,
    ));
    let path = metadata.coordinator.as_ref().unwrap().snapshot_path();
    IggySnapshot::create(&TestMux::default(), 0, 1)
        .unwrap()
        .persist(&path)
        .await
        .unwrap();
    let checkpoint = metadata.checkpoint_lock.acquire().await;
    {
        let mut blocked_offer = std::pin::pin!(metadata.state_transfer_offer());
        assert!(matches!(
            futures::poll!(&mut blocked_offer),
            std::task::Poll::Ready(Err(StateTransferUnavailable::CheckpointInProgress))
        ));
    }
    drop(checkpoint);
    let first = settled_transfer_offer(&metadata).await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let cached = settled_transfer_offer(&metadata).await.unwrap();
    assert!(
        Rc::ptr_eq(&first, &cached),
        "a cache hit must need no disk read"
    );

    let mut snapshot = IggySnapshot::create(&TestMux::default(), 1, 2).unwrap();
    snapshot.snapshot_mut().client_table = Some(metadata.client_table.borrow().to_snapshot());
    let incoming = snapshot.encode().unwrap();
    metadata
        .install_state_transfer(&incoming, ClientTable::new(CLIENTS_TABLE_MAX), 1, 1)
        .await
        .unwrap();
    let replaced = settled_transfer_offer(&metadata).await.unwrap();
    assert_eq!(replaced.snapshot_seq, 1);
    assert!(!Rc::ptr_eq(&first, &replaced));

    metadata.clear_state_transfer_offer_cache();
    let mut damaged = std::fs::read(&path).unwrap();
    damaged[0] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    assert!(matches!(
        settled_transfer_offer(&metadata).await,
        Err(StateTransferUnavailable::SnapshotUnreadable(_))
    ));
}

#[test]
fn given_log_and_frontier_when_reading_changes_should_answer_only_a_coherent_image() {
    let metadata = metadata_plane();
    let latest = MetadataChangesQuery {
        start: MetadataChangeStart::Latest,
        ..MetadataChangesQuery::default()
    };
    let header = |op| PrepareHeader {
        op,
        checksum: 0x5eed,
        timestamp: op,
        ..PrepareHeader::default()
    };
    assert_eq!(
        metadata.metadata_changes(&latest).unwrap().status,
        WireMetadataChangeStatus::Behind,
        "no op 1 applied yet"
    );

    metadata
        .change_log()
        .append(
            &header(1),
            vec![AppliedChange {
                kind: WireMetadataChangeKind::UserCreated,
                actor_user_id: None,
                body: Bytes::new(),
            }],
        )
        .unwrap();
    assert_eq!(
        metadata.metadata_changes(&latest),
        Err(IggyError::TransientNotAccepted),
        "a log ahead of the frontier is not a coherent image"
    );
    metadata.advance_applied_frontier(1);
    let page = metadata.metadata_changes(&latest).unwrap();
    assert_eq!(page.status, WireMetadataChangeStatus::Ok);
    assert_eq!(page.head_op, 1);
    assert_eq!(page.next.map(|next| (next.op, next.ordinal)), Some((1, 0)));

    let current = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: vec![MetadataChangeGroup::Streams],
        ..MetadataChangesQuery::default()
    };
    assert_eq!(
        metadata.metadata_changes(&current),
        Err(IggyError::InvalidCommand)
    );
    assert_eq!(
        metadata.metadata_current_state(&current).unwrap().status,
        WireMetadataChangeStatus::Ok
    );
    assert_eq!(
        peer_metadata().metadata_current_state(&current),
        Err(IggyError::InvalidConfiguration),
        "only the metadata shard holds the client table"
    );
}

#[compio::test]
async fn given_state_transfer_when_installing_should_replace_the_change_log_only_with_the_state_machines()
 {
    const TRANSFER_OP: u64 = 2;
    let metadata = metadata_plane();
    let local = |op, checksum| PrepareHeader {
        op,
        checksum,
        timestamp: op,
        ..PrepareHeader::default()
    };
    metadata
        .change_log()
        .append(&local(1, 0xaa), Vec::new())
        .unwrap();
    let before = metadata.change_log().snapshot();
    let transferred = |sequence_number, log_id| {
        let mut snapshot = IggySnapshot::create(&TestMux::default(), sequence_number, 1).unwrap();
        snapshot.snapshot_mut().client_table =
            Some(ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot());
        let log = MetadataChangeLog::new(ChangeLogCapacities::default());
        log.append(&local(1, log_id), Vec::new()).unwrap();
        log.append(
            &local(sequence_number, log_id),
            vec![AppliedChange {
                kind: WireMetadataChangeKind::StreamCreated,
                actor_user_id: None,
                body: Bytes::from_static(b"body"),
            }],
        )
        .unwrap();
        snapshot.snapshot_mut().change_log = log.snapshot();
        snapshot
    };

    let incoherent = transferred(TRANSFER_OP, 0xbb).encode().unwrap();
    assert!(matches!(
        metadata
            .install_state_transfer(
                &incoherent,
                ClientTable::new(CLIENTS_TABLE_MAX),
                TRANSFER_OP,
                TRANSFER_OP - 1,
            )
            .await,
        Err(SnapshotError::IncoherentManifest { .. })
    ));
    assert_eq!(
        metadata.change_log().snapshot(),
        before,
        "a refused transfer leaves the log untouched"
    );

    let ahead = transferred(TRANSFER_OP, 0xbb);
    metadata
        .install_state_transfer(
            &ahead.encode().unwrap(),
            ClientTable::new(CLIENTS_TABLE_MAX),
            TRANSFER_OP,
            TRANSFER_OP,
        )
        .await
        .unwrap();
    assert_eq!(
        metadata.change_log().snapshot(),
        ahead.snapshot().change_log,
        "the transfer replaces the log with the state machines"
    );

    let behind = transferred(TRANSFER_OP, 0xcc);
    metadata
        .install_state_transfer(
            &behind.encode().unwrap(),
            ClientTable::new(CLIENTS_TABLE_MAX),
            TRANSFER_OP,
            TRANSFER_OP,
        )
        .await
        .unwrap();
    assert_eq!(
        metadata.change_log().snapshot(),
        ahead.snapshot().change_log,
        "a transfer at the applied frontier keeps the local state and log"
    );
}

/// The offer ships state at or below `commit_min`, which a recovered suffix
/// above `commit_max` cannot change, so the barrier waiting on that suffix must
/// not withhold it.
#[compio::test]
async fn given_a_primary_below_its_recovery_barrier_when_a_peer_asks_for_state_should_serve_its_checkpoint()
 {
    const SNAPSHOT_OP: u64 = 3;
    const COMMITTED: u64 = 5;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
    let mut metadata = metadata_plane();
    metadata.coordinator = Some(SnapshotCoordinator::new(
        directory.path().to_path_buf(),
        IggySnapshot::create,
    ));
    let path = metadata.coordinator.as_ref().unwrap().snapshot_path();
    IggySnapshot::create(&TestMux::default(), SNAPSHOT_OP, 1)
        .unwrap()
        .persist(&path)
        .await
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    consensus.restore_commit_state(COMMITTED, COMMITTED);
    consensus.set_recovery_barrier(COMMITTED + 1);

    let offer = settled_transfer_offer(&metadata)
        .await
        .expect("a primary below its recovery barrier must still serve committed state");
    assert_eq!(offer.snapshot_seq, SNAPSHOT_OP);
    assert_eq!(
        offer.commit_op, COMMITTED,
        "the offer must stop at the applied commit point, below the barrier"
    );
}

#[compio::test]
async fn given_commits_during_snapshot_io_when_installing_should_preserve_the_newer_frontier() {
    const SNAPSHOT_OP: u64 = 1;
    const LOCAL_OP: u64 = 2;
    const TRANSFER_OP: u64 = 3;
    const CLIENT: u128 = 9;
    const USER: u32 = 7;
    const BIND_VERIFIER: [u8; 32] = [0x5a; 32];
    for table_frontier in [SNAPSHOT_OP, TRANSFER_OP] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
        let mut metadata = metadata_plane();
        metadata.coordinator = Some(SnapshotCoordinator::new(
            directory.path().to_path_buf(),
            IggySnapshot::create,
        ));
        let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
        table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
        let mut snapshot = IggySnapshot::create(&TestMux::default(), SNAPSHOT_OP, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let incoming = snapshot.encode().unwrap();
        let held = SNAPSHOT_IO.lock().await;
        let mut install = Box::pin(metadata.install_state_transfer(
            &incoming,
            table,
            table_frontier,
            table_frontier,
        ));
        assert!(futures::poll!(&mut install).is_pending());
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.advance_commit_max(LOCAL_OP);
        consensus.set_commit_floor(LOCAL_OP);
        {
            let mut local_table = metadata.client_table.borrow_mut();
            local_table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
            local_table
                .commit_register(
                    CLIENT,
                    USER,
                    BIND_VERIFIER,
                    register_reply(CLIENT, LOCAL_OP),
                )
                .unwrap();
        }
        drop(held);
        let outcome = install.await.unwrap();
        assert_eq!(outcome.installed_frontier, LOCAL_OP);
        assert_eq!(consensus.commit_min(), LOCAL_OP);
        if table_frontier > LOCAL_OP {
            assert_eq!(metadata.client_table.borrow().get_epoch(CLIENT), None);
            let replay = metadata.replay_client_table.borrow();
            assert_eq!(
                replay.as_ref().unwrap().borrow().get_epoch(CLIENT),
                Some(LOCAL_OP),
                "tail replay must retain the session committed during snapshot I/O"
            );
        } else {
            assert_eq!(
                metadata.client_table.borrow().get_epoch(CLIENT),
                Some(LOCAL_OP)
            );
            assert!(metadata.replay_client_table.borrow().is_none());
        }
    }
}

#[compio::test]
async fn given_failed_snapshot_replacement_when_retrying_should_publish_valid_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot.bin");
    std::fs::create_dir(&path).unwrap();
    let snapshot = IggySnapshot::create(&TestMux::default(), 1, 1).unwrap();
    assert!(matches!(
        snapshot.persist(&path).await,
        Err(SnapshotError::Persist {
            stage: PersistStage::Rename,
            ..
        })
    ));
    std::fs::remove_dir(&path).unwrap();
    snapshot.persist(&path).await.unwrap();
    let (loaded, _) = IggySnapshot::load(&path).unwrap();
    assert_eq!(loaded.sequence_number(), 1);
}
