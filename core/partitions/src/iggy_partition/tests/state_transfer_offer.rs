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

/// An offer whose frontier sits below this replica's own offset counter is
/// refused: installing it would rewind the counter, and the next replicated
/// prepare is re-stamped from it, so this replica would persist different
/// bytes (and a different `batch_checksum`) than the rest of the group.
#[compio::test]
async fn given_offer_below_local_counter_when_installed_should_refuse_rewind() {
    let partition_dir = transfer_fence_dir("rewind-refused").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    partition.set_offset_space_used(true);
    partition.offset.store(99, Ordering::Release);
    // Committed and resident: the threshold-gated flush leaves exactly this
    // shape, and the fence has to count it as data.
    {
        let info = &mut partition.log.journal_mut().info;
        info.messages_count = 100;
        info.current_offset = 99;
    }

    let behind = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 50,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &behind.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 50,
                    local_next_offset: 100,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// A chain installed EMPTY at frontier N holds no sized segment and no
/// journal entry, so a fence reading only held bytes reads 0 and skips
/// itself, letting a stale offer rewind the counter under offsets this
/// replica already claimed. The committed frontier is what carries N.
#[compio::test]
async fn given_an_empty_chain_installed_at_a_frontier_when_a_stale_offer_arrives_should_refuse() {
    let partition_dir = transfer_fence_dir("empty-install-rewind").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    // What an install of an all-GC'd origin leaves: the counter at the group
    // frontier, nothing on disk, nothing resident.
    partition.set_offset_space_used(true);
    partition.offset.store(4_095, Ordering::Release);
    partition.dirty_offset.store(4_095, Ordering::Relaxed);
    assert_eq!(
        partition.held_offset_frontier(),
        4_096,
        "the committed arm has to carry a frontier no byte on disk names"
    );

    let stale = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 1_000,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &stale.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 1_000,
                    local_next_offset: 4_096,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// Primary-by-index at view 0 with nothing committed refuses to serve: an
/// empty group is trivially "caught up", so this gate is the only thing
/// separating a real primary from a phantom whose directory vanished, whose
/// zero-segment offer at frontier 0 would make a data-holding receiver
/// unlink its chain.
#[compio::test]
async fn given_nothing_committed_when_offer_requested_should_refuse() {
    let partition_dir = transfer_fence_dir("nothing-committed").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    assert_eq!(partition.consensus().commit_max(), 0);

    let refused = partition.state_transfer_offer(&repair_config()).await;
    assert!(
        matches!(
            refused,
            Err(crate::state_transfer::PartitionTransferUnavailable::NothingCommitted)
        ),
        "expected a NothingCommitted refusal, got {refused:?}"
    );
    assert!(
        refused.is_err_and(|reason| reason.transient()),
        "the refusal must be transient: the requester rotates rather than \
             charging its failure count"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// Replica 0 leads this view of a group of two. It is above 0 so that the WAL
/// certificate an install writes differs from the one of a new WAL.
const GROUP_OF_TWO_VIEW: u32 = 2;

/// A group of two commits nothing without both members, and a backup that
/// waits for a transfer acknowledges nothing. Its primary therefore holds
/// nothing committed for as long as it refuses to serve. The primary serves
/// its empty state at op 0, and the backup installs it and acknowledges again.
#[compio::test]
async fn given_empty_group_of_two_when_backup_awaits_transfer_should_install_and_acknowledge() {
    let config = repair_config();
    let (_primary_directory, mut primary) = primary_of_two();
    primary.open_persistence().await.unwrap();

    let backup_directory = tempfile::tempdir().unwrap();
    let backup_dir = backup_directory.path().to_string_lossy().into_owned();
    let (mut backup, _) = recording_partition_at(1, 2);
    let sent = backup.consensus().message_bus().sent_to_replicas.clone();
    backup.set_partition_dir(backup_dir.clone());
    set_offset_dirs_under(&mut backup, backup_directory.path());
    crate::state_transfer::mark_materialization_missing(&backup_dir, 0)
        .await
        .unwrap();
    backup.open_persistence().await.unwrap();
    assert!(backup.requires_state_transfer());
    assert!(!backup.consensus().recovery_election_allowed());
    adopt_view_of_two(&backup);

    // The primary cannot commit this op: the only other member drops it.
    let prepare = checksummed_segment_prepare(1, 0, 0, b"first").transmute_header(
        |original, header: &mut PrepareHeader| {
            *header = original;
            header.view = GROUP_OF_TWO_VIEW;
            header.checksum = header.identity_checksum();
        },
    );
    primary.consensus().sequencer().set_sequence(1);
    primary
        .consensus()
        .set_last_prepare_checksum(prepare.header().checksum);
    primary
        .log
        .journal()
        .inner
        .append(prepare.clone().into_frozen())
        .await
        .unwrap();
    backup.on_replicate(prepare.clone()).await;
    assert!(!backup.log.journal().inner.holds_op(1));
    assert_eq!(acknowledged_ops(&sent), Vec::<u64>::new());

    let offer = empty_offer_of(&mut primary, &config).await;

    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    assert_ne!(persistence.certified_log_view(), Some(GROUP_OF_TWO_VIEW));
    backup
        .install_state_transfer(&config, offer.commit_op, Vec::new(), &offer.offsets.1)
        .await
        .unwrap();
    // When an install ends, the shard returns the stage to idle and proves
    // the log of the view that the backup adopted while it waited.
    backup
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Idle);
    assert!(matches!(
        consensus::view_log_proof(
            backup.consensus().commit_min(),
            &[],
            backup.consensus().sequencer().current_sequence(),
            |op| backup.log.journal().inner.header_by_op(op),
        ),
        consensus::ViewLogProof::Proven
    ));
    backup.consensus().settle_view_log_proof();

    assert!(!backup.requires_state_transfer());
    assert!(backup.consensus().recovery_election_allowed());
    assert_eq!(
        (persistence.certified_log_view(), persistence.head()),
        (Some(GROUP_OF_TWO_VIEW), 0)
    );

    backup.on_replicate(prepare).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    assert!(backup.log.journal().inner.holds_op(1));
    assert_eq!(
        acknowledged_ops(&sent),
        vec![1],
        "the primary needs this acknowledgement to commit op 1"
    );
}

/// A backup that waits for a transfer skipped its WAL replay, so only its WAL
/// tells what it holds. The commit number of a prepare proves that every op at
/// or below it committed. An empty offer would erase those ops, as a primary
/// that lost its storage unnoticed serves it, so the backup refuses the offer
/// and keeps its WAL.
#[compio::test]
async fn given_waiting_backup_of_two_when_its_wal_names_committed_ops_should_refuse_the_empty_offer()
 {
    const WAL_COMMIT: u64 = 1;
    let config = repair_config();
    let (_primary_directory, mut primary) = primary_of_two();
    primary.open_persistence().await.unwrap();
    let offer = empty_offer_of(&mut primary, &config).await;
    let wal = wal_sends(WAL_COMMIT);
    let wal_head = u64::try_from(wal.len()).unwrap();
    let (_backup_directory, mut backup) = Box::pin(waiting_backup_with_wal(wal)).await;
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

    let refused = backup
        .install_state_transfer(&config, offer.commit_op, Vec::new(), &offer.offsets.1)
        .await;

    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                    commit_op: 0,
                    held_max: WAL_COMMIT,
                }
            )
        ),
        "expected a refusal of the empty offer, got {refused:?}"
    );
    assert!(backup.requires_state_transfer());
    assert_eq!(persistence.head(), wal_head, "a refusal changes nothing");
}

/// A stored consumer offset proves that the group committed a message. A
/// backup that waits for a transfer reads as empty, because boot skipped its
/// WAL replay. A boot clamp that removed the offset for that reason would let
/// the backup install the empty offer of a primary that lost its storage
/// unnoticed. Boot keeps the offset, and the backup refuses the offer.
#[compio::test]
async fn given_waiting_backup_of_two_when_it_holds_a_stored_consumer_offset_should_refuse_the_empty_offer()
 {
    const CONSUMER_ID: u32 = 7;
    const STORED_OFFSET: u64 = 4;
    let (_primary_directory, mut primary) = primary_of_two();
    primary.open_persistence().await.unwrap();
    let offer = empty_offer_of(&mut primary, &repair_config()).await;

    let directory = tempfile::tempdir().unwrap();
    let config = offsets_config(&directory);
    let namespace = IggyNamespace::new(1, 1, 0);
    let partition_dir = config.get_partition_path(1, 1, 0);
    let offset_file = format!(
        "{}/{CONSUMER_ID}",
        config.get_consumer_offsets_path(1, 1, 0)
    );
    persist_offset(&offset_file, STORED_OFFSET, true)
        .await
        .unwrap();
    crate::state_transfer::mark_materialization_missing(&partition_dir, 0)
        .await
        .unwrap();
    let (mut backup, _) = recording_partition_at(1, 2);
    backup.set_partition_dir(partition_dir);
    // The order of boot: recover the offsets, open the WAL, then clamp.
    crate::partition_storage::recover_consumer_offsets_with_storage(
        &DiskStorage,
        &mut backup,
        &config,
        namespace,
    )
    .await
    .unwrap();
    backup.open_persistence().await.unwrap();
    assert!(backup.requires_state_transfer());
    assert_eq!(backup.offset_frontier(), 0);
    crate::partition_storage::clamp_consumer_offsets(&backup, namespace)
        .await
        .unwrap();
    adopt_view_of_two(&backup);

    let refused = backup
        .install_state_transfer(&config, offer.commit_op, Vec::new(), &offer.offsets.1)
        .await;

    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                    local_offsets: 1,
                }
            )
        ),
        "expected a refusal of the empty offer, got {refused:?}"
    );
    assert!(backup.requires_state_transfer());
    assert!(
        std::path::Path::new(&offset_file).exists(),
        "boot removed the offset file of a replica that waits for a transfer"
    );
}

/// Nothing in the WAL proves that the group committed a prepare above its
/// commit numbers. Refusing over such prepares would leave an empty group of
/// two waiting for good, so the backup installs the empty offer and drops them.
#[compio::test]
async fn given_waiting_backup_of_two_when_its_wal_holds_only_uncommitted_ops_should_install_the_empty_offer()
 {
    let config = repair_config();
    let (_primary_directory, mut primary) = primary_of_two();
    primary.open_persistence().await.unwrap();
    let offer = empty_offer_of(&mut primary, &config).await;
    let (_backup_directory, mut backup) = Box::pin(waiting_backup_with_wal(wal_sends(0))).await;
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());

    backup
        .install_state_transfer(&config, offer.commit_op, Vec::new(), &offer.offsets.1)
        .await
        .unwrap();

    assert!(!backup.requires_state_transfer());
    assert_eq!(
        (persistence.certified_log_view(), persistence.head()),
        (Some(GROUP_OF_TWO_VIEW), 0)
    );
}

/// A commit number is a lower bound. The primary here holds op 2 but knows
/// only op 1 as committed, while the WAL of the backup names op 2: its last
/// prepare, which the primary no longer holds, carries that number. The backup
/// cannot tell this primary from one that lost op 2, and it acknowledges
/// nothing while it waits, so a refusal would never end. It installs.
#[compio::test]
async fn given_waiting_backup_of_two_when_its_wal_names_a_commit_above_the_offer_should_install() {
    const OFFER_COMMIT: u64 = 1;
    const WAL_COMMIT: u64 = 2;
    let config = repair_config();
    let (_primary_directory, mut primary) = primary_of_two();
    let store = |request: u64, offset: u64| {
        store_offset_request(
            MESSAGE_LESS_CLIENT_ID,
            request,
            ConsumerKind::ExternalGroup,
            MESSAGE_LESS_EXTERNAL_GROUP_ID,
            offset,
            AckLevel::Quorum,
        )
    };
    primary
        .on_request(store(1, MESSAGE_LESS_EXTERNAL_OFFSET), None)
        .await;
    primary.consensus.advance_commit_max(OFFER_COMMIT);
    primary.commit_journal(&config).await;
    primary
        .on_request(store(2, MESSAGE_LESS_EXTERNAL_OFFSET + 1), None)
        .await;
    assert!(primary.fatal().is_none(), "{:?}", primary.fatal());
    assert_eq!(primary.consensus().commit_min(), OFFER_COMMIT);
    let held = retained_prepare(&primary, 2);
    let last = checksummed_segment_prepare(3, held.header().checksum, 0, b"third")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.view = GROUP_OF_TWO_VIEW;
            header.commit = WAL_COMMIT;
            header.checksum = header.identity_checksum();
        });
    let offer = primary.state_transfer_offer(&config).await.unwrap();
    assert_eq!(offer.commit_op, OFFER_COMMIT);

    let wal = vec![retained_prepare(&primary, 1), held, last];
    let (_backup_directory, mut backup) = Box::pin(waiting_backup_with_wal(wal)).await;
    let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
    assert_eq!(backup.unreplayed_commit, WAL_COMMIT);

    backup
        .install_state_transfer(&config, offer.commit_op, Vec::new(), &offer.offsets.1)
        .await
        .unwrap();

    assert!(!backup.requires_state_transfer());
    assert_eq!(persistence.head(), OFFER_COMMIT);
    assert_eq!(
        backup.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
}

/// A group that acks before it persists can record a frontier over a tail that
/// a crash then takes from both members, so the group resumes below the
/// record. A backup whose chain boot refused is rebuilt and waits for a
/// transfer. A counter at the record makes it refuse every offer of that
/// group, and the primary commits nothing without this backup. The rebuild
/// takes no counter from the record, so the backup installs.
#[compio::test]
async fn given_rebuilt_backup_of_two_when_its_record_names_a_frontier_above_the_offer_should_install()
 {
    const RECORDED_FRONTIER: u64 = 13;
    // Each op of the offer is a send of one message.
    const OFFER_COMMIT: u64 = 10;
    const OFFER_FRONTIER: u64 = OFFER_COMMIT;
    let directory = tempfile::tempdir().unwrap();
    let partition_dir = directory.path().to_string_lossy().into_owned();
    let (mut backup, _) = recording_partition_at(1, 2);
    backup.runtime_options.durability = iggy_common::Durability::Replicated;
    backup.set_partition_dir(partition_dir.clone());
    set_offset_dirs_under(&mut backup, directory.path());
    crate::state_transfer::mark_materialization_missing(&partition_dir, 0)
        .await
        .unwrap();
    // The order of a rebuild: restore the counters, then open the WAL.
    backup.restore_offset_frontier(Some(&recorded_state(RECORDED_FRONTIER, 0)));
    backup.open_persistence().await.unwrap();
    assert!(backup.requires_state_transfer());

    let last = checksummed_segment_prepare(OFFER_COMMIT, 0, OFFER_FRONTIER - 1, b"last");
    let offer = crate::state_transfer::ConsumerOffsetsWire {
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        prepare_checksum: Some(last.header().checksum),
        checkpoint_prepare: last.as_slice().to_vec(),
        next_offset: OFFER_FRONTIER,
        ..Default::default()
    };
    let installed = backup
        .install_state_transfer(&repair_config(), OFFER_COMMIT, Vec::new(), &offer.encode())
        .await;

    assert!(
        installed.is_ok(),
        "the rebuilt backup refused the offer of its group: {installed:?}"
    );
    assert!(!backup.requires_state_transfer());
    assert_eq!(backup.offset_frontier(), OFFER_FRONTIER);
}

/// In a group of three the other members can commit without the requester,
/// and a primary that knows nothing would hand it an empty log, so the
/// refusal stays.
#[compio::test]
async fn given_group_of_three_with_nothing_committed_when_offer_requested_should_refuse() {
    let directory = tempfile::tempdir().unwrap();
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut primary, directory.path());

    assert_refuses_nothing_committed(&mut primary, "a group of three").await;
}

/// A restarted primary can report `commit_max == 0` while it holds applied
/// state. An offer built from it would carry that state at `commit_op = 0`,
/// so a group of two refuses as well.
#[compio::test]
async fn given_group_of_two_primary_holding_applied_state_when_offer_requested_should_refuse() {
    type Hold = fn(&mut IggyPartition<RecordingBus>);
    let cases: [(&str, Hold); 2] = [
        ("a committed offset counter", |primary| {
            primary.set_offset_space_used(true);
            primary.offset.store(9, Ordering::Release);
            primary.dirty_offset.store(9, Ordering::Relaxed);
        }),
        ("a stored consumer offset", |primary| {
            primary.durable_consumer_offsets.record_explicit(
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                MESSAGE_LESS_EXTERNAL_OFFSET,
                MESSAGE_LESS_EXTERNAL_OFFSET,
            );
        }),
    ];
    for (case, hold) in cases {
        let (_directory, mut primary) = primary_of_two();
        hold(&mut primary);
        assert_refuses_nothing_committed(&mut primary, case).await;
    }

    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("00000000000000000000.log");
    let index_path = directory.path().join("00000000000000000000.index");
    let mut fixture =
        PersistFixture::with_replicas(log_path.to_str().unwrap(), index_path.to_str().unwrap(), 2)
            .await;
    fixture.persist(FIRST_PAYLOAD, 0).await.unwrap();
    let mut primary = fixture.partition;
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    assert!(primary.held_offset_frontier() > 0);
    assert_refuses_nothing_committed(&mut primary, "a recovered chain").await;
}

/// A replica that waits for a transfer skipped its WAL replay, so it can read
/// as empty while its WAL holds prepares. It must not hand that out as the
/// group state.
#[compio::test]
async fn given_group_of_two_primary_awaiting_its_own_transfer_when_offer_requested_should_refuse() {
    let (_directory, mut primary) = primary_of_two();
    primary.materialization_missing = true;

    assert_refuses_nothing_committed(&mut primary, "a pending transfer of its own").await;
}

/// The primary of a group of two in [`GROUP_OF_TWO_VIEW`], with a partition
/// directory and nothing committed.
fn primary_of_two() -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut primary, _) = recording_partition_at(0, 2);
    primary.consensus.set_view(GROUP_OF_TWO_VIEW);
    primary.consensus.set_log_view(GROUP_OF_TWO_VIEW);
    primary.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut primary, directory.path());
    (directory, primary)
}

/// The offer of a primary of two with nothing committed: its state at op 0.
async fn empty_offer_of(
    primary: &mut IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
) -> Rc<crate::state_transfer::PartitionStateTransferOffer> {
    let offer = primary
        .state_transfer_offer(config)
        .await
        .unwrap_or_else(|reason| panic!("the primary of two refused to serve: {reason}"));
    assert_eq!((offer.commit_op, offer.segments.len()), (0, 0));
    offer
}

/// Let a waiting `backup` adopt [`GROUP_OF_TWO_VIEW`] from a `StartView` that
/// carries an empty log.
fn adopt_view_of_two(backup: &IggyPartition<RecordingBus>) {
    let group = backup.namespace().inner();
    let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
        .transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = TEST_CLUSTER;
            header.group = group;
            header.view = GROUP_OF_TWO_VIEW;
            header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
        });
    backup
        .consensus()
        .handle_start_view(PlaneKind::Partitions, start_view.header(), &[]);
    assert!(backup.consensus().is_normal());
    assert_eq!(backup.consensus().log_view(), GROUP_OF_TWO_VIEW);
    assert_eq!(
        backup.consensus().state_transfer_stage(),
        consensus::StateTransferStage::AwaitingTarget
    );
}

/// Two sends as a backup holds them in its WAL only. The last one names
/// `commit` as committed.
fn wal_sends(commit: u64) -> Vec<Message<PrepareHeader>> {
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.commit = commit;
            header.checksum = header.identity_checksum();
        });
    vec![first, second]
}

/// A backup of two that waits for a transfer in [`GROUP_OF_TWO_VIEW`] while
/// its WAL holds `prepares`. It journaled them in their view before its files
/// were marked lost.
async fn waiting_backup_with_wal(
    prepares: Vec<Message<PrepareHeader>>,
) -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let partition_dir = directory.path().to_string_lossy().into_owned();
    let open_backup = async |view: u32| {
        let (mut backup, _) = recording_partition_at(1, 2);
        backup.consensus.set_view(view);
        backup.consensus.set_log_view(view);
        backup.set_partition_dir(partition_dir.clone());
        set_offset_dirs_under(&mut backup, directory.path());
        backup.open_persistence().await.unwrap();
        backup
    };
    let wal_head = u64::try_from(prepares.len()).unwrap();
    let mut backup = open_backup(prepares[0].header().view).await;
    for prepare in prepares {
        backup.on_replicate(prepare).await;
    }
    // Replicated durability writes without a barrier, and recovery drops what
    // was never synced, so sync before the restart.
    let persistence = backup.persistence.as_ref().unwrap();
    persistence.sync();
    backup.start_persistence();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(persistence.is_durable_through(wal_head));
    drop(backup);

    crate::state_transfer::mark_materialization_missing(&partition_dir, 0)
        .await
        .unwrap();
    // These tests keep no superblock, so the restart comes back in view 0.
    let backup = open_backup(0).await;
    assert!(backup.requires_state_transfer());
    assert_eq!(
        (
            backup.persistence.as_ref().unwrap().head(),
            backup.offset_frontier()
        ),
        (wal_head, 0)
    );
    assert!(
        !backup.log.journal().inner.holds_op(1),
        "boot skips the WAL replay of a waiting replica"
    );
    adopt_view_of_two(&backup);
    (directory, backup)
}

async fn assert_refuses_nothing_committed<B: MessageBus>(
    primary: &mut IggyPartition<B>,
    case: &str,
) {
    assert!(primary.serves_state_transfer(), "{case}");
    assert_eq!(primary.consensus().commit_max(), 0, "{case}");
    let refused = primary.state_transfer_offer(&repair_config()).await;
    assert!(
        matches!(
            refused,
            Err(crate::state_transfer::PartitionTransferUnavailable::NothingCommitted)
        ),
        "{case}: expected a NothingCommitted refusal, got {refused:?}"
    );
}

/// The ops of every `PrepareOk` in `sent`, in send order.
fn acknowledged_ops(sent: &RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>) -> Vec<u64> {
    sent.borrow()
        .iter()
        .filter_map(|(_, frame)| {
            bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                .ok()
                .filter(|header| header.command == Command::PrepareOk)
                .map(|header| header.op)
        })
        .collect()
}

/// A partition that never took a message still commits owner, history and
/// retry state, so a replica behind the repair window can only recover by
/// transfer. The caught-up primary serves its empty chain at frontier 0,
/// and the receiver lands on that state without claiming an offset.
#[compio::test]
async fn given_message_less_primary_when_offer_requested_should_serve_its_committed_state() {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    assert!(offer.segments.is_empty());
    let (_receiver_directory, mut receiver) = transfer_receiver();
    let incarnation = receiver.created_revision();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
    assert_eq!(receiver.created_revision(), incarnation);
}

/// A replica that never took a message and lags past every op its peers
/// retain cannot repair, so it catches up from the primary's offer.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_fully_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // An empty serving journal reports eviction from its commit point.
    receiver.repair = Some(armed_session(3, 3));

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 3, to_op: 3 }
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
}

/// A complete window without messages says nothing about the ops below
/// its floor when the replica never took a message. Op 1 stored an
/// external offset, so jumping the floor would drop it unseen.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_partly_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // The serving peer evicted op 1 and still retains ops 2 and 3.
    receiver.repair = Some(armed_session(3, 1));
    for op in [2, 3] {
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, op))
            .await;
    }

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        (conclusion, receiver.consensus().commit_min()),
        (RepairConclusion::FloorRefused { floor: 1, to_op: 3 }, 0)
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
}

/// A backup restarts with origin op 1 replayed from its WAL but not
/// committed, and its peers retain only op 3. A floor at op 2 would skip
/// that op 1 and the op 2 nobody serves, so a replica with no committed
/// message walks op 1, refuses the floor and catches up by transfer.
#[compio::test]
async fn given_restarted_message_less_backup_when_floor_skips_unserved_op_should_refuse_and_transfer()
 {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let open_backup = async || {
            let (mut backup, _) = recording_partition_at(1, 3);
            backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
            set_offset_dirs_under(&mut backup, directory.path());
            backup.runtime_options.durability = durability;
            backup.runtime_options.consumer_offset_durability = durability;
            backup.open_persistence().await.unwrap();
            backup
        };
        let mut backup = open_backup().await;
        backup.on_replicate(retained_prepare(&origin, 1)).await;
        // Replicated durability writes op 1 without a barrier, and recovery
        // drops what was never synced, so sync it before the crash.
        let persistence = backup.persistence.as_ref().unwrap();
        persistence.sync();
        backup.start_persistence();
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        assert!(persistence.is_durable_through(1), "{durability:?}");
        drop(backup);

        let mut receiver = open_backup().await;
        assert_eq!(
            receiver.consensus().sequencer().current_sequence(),
            1,
            "{durability:?}"
        );
        assert!(receiver.log.journal().inner.holds_op(1), "{durability:?}");
        assert_eq!(receiver.consensus().commit_min(), 0, "{durability:?}");
        receiver.consensus().advance_commit_max(3);
        receiver.repair = Some(armed_session(3, 2));
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, 3))
            .await;

        let conclusion = receiver.complete_repair(&repair_config()).await;
        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 2, to_op: 3 },
            "{durability:?}"
        );
        assert_eq!(receiver.consensus().commit_min(), 1, "{durability:?}");
        assert_eq!(
            receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
            Some(MESSAGE_LESS_EXTERNAL_OFFSET),
            "{durability:?}"
        );
        let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
        receiver
            .install_state_transfer(
                &repair_config(),
                offer.commit_op,
                Vec::new(),
                &offer.offsets.1,
            )
            .await
            .unwrap();
        assert_holds_message_less_state(&receiver, &origin, &transition);
    }
}

fn batch_stats(base_offset: u64, message_count: u32) -> CommittedBatchStats {
    CommittedBatchStats {
        producer: None,
        timestamp: 0,
        base_offset,
        message_count,
        size_bytes: 128,
    }
}

#[test]
fn given_send_messages_when_offsets_resolved_should_confirm_base_offset() {
    let namespace = IggyNamespace::new(3, 7, 5);
    let stats = batch_stats(42, 3);

    let body = send_messages_reply_body(namespace.inner(), Some(stats));
    let (response, consumed) = SendMessagesResponse::decode(&body).unwrap();

    assert_eq!(consumed, body.len());
    assert_eq!(
        response.confirmations,
        vec![SendMessagesConfirmationResponse {
            stream_id: 3,
            topic_id: 7,
            partition_id: 5,
            base_offset: 42,
        }]
    );
}

#[test]
fn given_send_messages_when_offsets_unavailable_should_reply_zero_confirmations() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let body = send_messages_reply_body(namespace.inner(), None);

    assert_eq!(&body[..], &[0, 0, 0, 0]);
    let (response, _) = SendMessagesResponse::decode(&body).unwrap();
    assert_eq!(response.confirmations, []);
}

#[test]
fn given_batch_stats_when_end_offset_derived_should_span_the_message_run() {
    assert_eq!(batch_stats(9, 1).end_offset(), 9);
    assert_eq!(batch_stats(9, 4).end_offset(), 12);
}

#[test]
fn given_result_framed_operation_when_committed_should_reply_empty_result_section() {
    assert_eq!(
        &committed_reply_body(Operation::StoreConsumerOffset)[..],
        &[0, 0, 0, 0]
    );
}

#[test]
fn given_unframed_operation_when_committed_should_reply_empty_body() {
    assert!(committed_reply_body(Operation::DeleteSegments).is_empty());
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_an_unsyncable_messages_writer_when_installing_should_refuse_the_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let wal = directory.path().join("wal");
    let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
    let messages_writer =
        MessagesWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), false, false, None)
            .await
            .unwrap();
    let index_path = directory.path().join("segment.index");
    let index_writer = IggyIndexWriter::new(
        index_path.to_str().unwrap(),
        Rc::new(AtomicU64::new(0)),
        false,
        false,
    )
    .await
    .unwrap();
    let mut partition = test_partition();
    partition.log.add_persisted_segment(
        Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
        SegmentStorage::default(),
        Some(Rc::new(messages_writer)),
        Some(Rc::new(index_writer)),
    );
    partition.persistence = Some(Rc::clone(&persistence));

    partition.barrier_install_files_locked();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.failure().is_some());
}
