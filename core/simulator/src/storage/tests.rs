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

use super::{
    Crash, DurableFile, DurableStorage, FaultMode, OpenMode, SimStorage, StorageOperation,
};
use crate::packet::PacketSimulatorOptions;
use consensus::MetadataHandle;
use futures::{executor::block_on, poll};
use iggy_binary_protocol::{Command, Operation, PrepareHeader};
use journal::{DurableAppend, PartitionPrepareJournal};
use partitions::{PartitionPersistence, install_backup};
use server_common::{Message, iobuf::Owned};
use std::io;
use std::path::Path;
use std::rc::Rc;

const DIRECTORY: &str = "/partition";
const WAL: &str = "/partition/wal";
const MATERIALIZED_FILES: &[&str] = &[
    "/partition/0.log",
    "/partition/0.index",
    "/partition/offsets/consumers/1",
    "/partition/offsets/groups/9",
    "/partition/superblock.a",
];

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Append,
    CertifyView,
    Checkpoint,
    Truncate,
    Reset,
    Purge,
}

#[test]
fn process_crash_preserves_completed_writes_but_power_loss_requires_file_and_directory_sync() {
    block_on(async {
        let storage = storage_for_partition().await;
        storage
            .create_directories(Path::new(DIRECTORY))
            .await
            .unwrap();
        storage.sync_directory(Path::new("/")).await.unwrap();
        let path = Path::new("/partition/value");
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        file.write(0, b"buffered".to_vec()).await.unwrap();
        storage.crash(Crash::Process);
        assert_eq!(
            storage
                .open(path, OpenMode::Read)
                .await
                .unwrap()
                .read(0, 8)
                .await
                .unwrap(),
            b"buffered"
        );
        storage.crash(Crash::PowerLoss);
        assert!(!storage.exists(path).await.unwrap());
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        file.write(0, b"synced".to_vec()).await.unwrap();
        file.sync().await.unwrap();
        storage.crash(Crash::PowerLoss);
        assert!(
            !storage.exists(path).await.unwrap(),
            "file sync must not imply directory sync"
        );
        replace(&storage, path, b"durable").await.unwrap();
        storage.crash(Crash::PowerLoss);
        assert_eq!(
            storage
                .open(path, OpenMode::Read)
                .await
                .unwrap()
                .read(0, 7)
                .await
                .unwrap(),
            b"durable"
        );
        assert!(
            file.sync().await.is_err(),
            "an old process cannot complete into the new one"
        );
    });
}

#[test]
fn wal_fault_sweep_preserves_acknowledged_history_at_every_io_boundary() {
    block_on(async {
        let mut cases = 0;
        for mutation in [
            Mutation::Append,
            Mutation::CertifyView,
            Mutation::Checkpoint,
            Mutation::Truncate,
            Mutation::Reset,
            Mutation::Purge,
        ] {
            let (storage, mut journal) = baseline().await;
            storage.clear_trace();
            mutate(&storage, &mut journal, mutation).await.unwrap();
            let trace = storage.trace();
            for (cut, operation) in trace.iter().enumerate() {
                for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                    for crash in [Crash::Process, Crash::PowerLoss] {
                        for writeback in [false, true] {
                            let (storage, mut journal) = baseline().await;
                            storage.fail_at(cut, mode);
                            let completed = mutate(&storage, &mut journal, mutation).await.is_ok();
                            drop(journal);
                            if writeback {
                                storage.writeback();
                            }
                            storage.crash(crash);
                            let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone()).await.unwrap_or_else(|error| {
                                panic!("{mutation:?} cut {cut} {operation:?} {mode:?} {crash:?} writeback={writeback}: {error}");
                            });
                            assert_recovery(&storage, &recovered, mutation, completed).await;
                            cases += 1;
                        }
                    }
                }
            }
        }
        eprintln!("partition WAL fault cases: {cases}");
    });
}

#[test]
fn transfer_fault_sweep_restores_one_complete_materialization_including_the_wal() {
    block_on(async {
        let (storage, mut journal) = baseline().await;
        storage.clear_trace();
        install(&storage, &mut journal).await.unwrap();
        let trace = storage.trace();
        let mut cases = 0;
        for (cut, operation) in trace.iter().enumerate() {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                for crash in [Crash::Process, Crash::PowerLoss] {
                    let (storage, mut journal) = baseline().await;
                    storage.fail_at(cut, mode);
                    let completed = install(&storage, &mut journal).await.is_ok();
                    drop(journal);
                    storage.crash(crash);
                    install_backup::recover_with_storage(Path::new(DIRECTORY), &storage)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("install cut {cut} {operation:?} {mode:?} {crash:?}: {error}")
                        });
                    let recovered = PartitionPrepareJournal::open_with_storage(
                        Path::new(WAL),
                        42,
                        7,
                        storage.clone(),
                    )
                    .await
                    .unwrap();
                    let value = storage
                        .open(Path::new("/partition/state"), OpenMode::Read)
                        .await
                        .unwrap()
                        .read(0, 3)
                        .await
                        .unwrap();
                    match recovered.checkpoint_op() {
                        0 => {
                            assert!(!completed);
                            assert_eq!(value, b"old");
                            assert_eq!(recovered.head(), 3);
                        }
                        7 => {
                            assert_eq!(value, b"new");
                            assert_eq!(recovered.head(), 7);
                        }
                        other => panic!("mixed installed state at {other}"),
                    }
                    let expected: &[u8] = if recovered.checkpoint_op() == 0 {
                        b"old"
                    } else {
                        b"new"
                    };
                    for path in MATERIALIZED_FILES {
                        assert_eq!(
                            storage
                                .open(Path::new(path), OpenMode::Read)
                                .await
                                .unwrap()
                                .read(0, 3)
                                .await
                                .unwrap(),
                            expected
                        );
                    }
                    cases += 1;
                }
            }
        }
        eprintln!("partition transfer fault cases: {cases}");
    });
}

#[test]
fn durable_quorum_covers_buffered_predecessors_and_losing_unsynced_replicas() {
    block_on(async {
        for replicas in [1, 2, 3, 5, 7] {
            let sim = crate::Simulator::new(
                replicas,
                std::iter::empty(),
                PacketSimulatorOptions::default(),
            );
            let quorum = sim.replicas[0].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()
                .unwrap()
                .quorum_replication();
            let first = prepare(1, 0);
            let second = prepare(2, first.header().checksum);
            let mut disks = Vec::new();
            for replica in 0..replicas {
                let storage = storage_for_partition().await;
                let mut journal = PartitionPrepareJournal::open_with_storage(
                    Path::new(WAL),
                    42,
                    7,
                    storage.clone(),
                )
                .await
                .unwrap();
                journal
                    .append_buffered(first.clone().into_frozen())
                    .await
                    .unwrap();
                if replica < quorum {
                    journal.append(second.clone().into_frozen()).await.unwrap();
                } else {
                    journal
                        .append_buffered(second.clone().into_frozen())
                        .await
                        .unwrap();
                }
                disks.push(storage);
            }
            let mut survivors = 0;
            for storage in disks {
                storage.crash(Crash::PowerLoss);
                let journal =
                    PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                        .await
                        .unwrap();
                if journal.head() == 2 {
                    assert!(journal.contains(first.header()));
                    assert!(journal.contains(second.header()));
                    survivors += 1;
                } else {
                    assert_eq!(journal.head(), 0);
                }
            }
            assert_eq!(survivors, quorum);
        }
    });
}

#[test]
fn stalled_writer_does_not_release_acks_or_block_another_partition() {
    block_on(async {
        let storage = storage_for_partition().await;
        let (persistence, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        let first = prepare(1, 0);
        persistence
            .append(first.clone().into_frozen(), true)
            .unwrap();
        storage.pause_writes();
        assert!(persistence.start());
        let mut writer = Box::pin(Rc::clone(&persistence).run());
        assert!(poll!(&mut writer).is_pending());
        assert!(!persistence.is_durable(first.header()));
        let independent = storage_for_partition().await;
        let mut journal =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, independent)
                .await
                .unwrap();
        journal.append(first.clone().into_frozen()).await.unwrap();
        assert!(journal.contains(first.header()));
        persistence.truncate_from(1);
        let replacement = prepare_with_payload(1, 0, b"replacement");
        persistence
            .append(replacement.clone().into_frozen(), true)
            .unwrap();
        storage.resume_writes();
        writer.await;
        assert!(!persistence.is_durable(first.header()));
        assert!(persistence.is_durable(replacement.header()));
        storage.crash(Crash::PowerLoss);
        let journal = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert!(journal.contains(replacement.header()));
    });
}

#[test]
fn queue_capacity_and_retirement_withhold_unpersisted_acknowledgments() {
    block_on(async {
        let storage = storage_for_partition().await;
        let (persistence, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        let mut parent = 0;
        let mut accepted = 0;
        loop {
            let prepare = prepare_with_payload(accepted + 1, parent, b"queued");
            parent = prepare.header().checksum;
            match persistence.append(prepare.into_frozen(), true) {
                Ok(()) => accepted += 1,
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                    break;
                }
            }
        }
        assert!(accepted > 0);
        assert!(!persistence.is_durable_through(accepted));
        persistence.retire();
        assert!(!persistence.start());
        storage.crash(Crash::PowerLoss);
        let journal = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(journal.head(), 0);
    });
}

#[test]
fn interrupted_rollback_can_itself_restart_at_every_io_boundary() {
    block_on(async {
        let storage = interrupted_install().await;
        storage.clear_trace();
        install_backup::recover_with_storage(Path::new(DIRECTORY), &storage)
            .await
            .unwrap();
        let trace = storage.trace();
        let mut cases = 0;
        for (cut, operation) in trace.iter().enumerate() {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                for crash in [Crash::Process, Crash::PowerLoss] {
                    let storage = interrupted_install().await;
                    storage.fail_at(cut, mode);
                    let _ =
                        install_backup::recover_with_storage(Path::new(DIRECTORY), &storage).await;
                    storage.crash(crash);
                    install_backup::recover_with_storage(Path::new(DIRECTORY), &storage)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("rollback cut {cut} {operation:?} {mode:?} {crash:?}: {error}")
                        });
                    let journal = PartitionPrepareJournal::open_with_storage(
                        Path::new(WAL),
                        42,
                        7,
                        storage.clone(),
                    )
                    .await
                    .unwrap();
                    assert_eq!(journal.head(), 3);
                    assert_eq!(journal.checkpoint_op(), 0);
                    assert_eq!(
                        storage
                            .open(Path::new("/partition/state"), OpenMode::Read)
                            .await
                            .unwrap()
                            .read(0, 3)
                            .await
                            .unwrap(),
                        b"old"
                    );
                    cases += 1;
                }
            }
        }
        eprintln!("partition rollback fault cases: {cases}");
    });
}

#[test]
fn failed_durable_completion_never_releases_a_prepare_ack() {
    block_on(async {
        let storage = storage_for_partition().await;
        let (persistence, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        let first = prepare(1, 0);
        persistence
            .append(first.clone().into_frozen(), true)
            .unwrap();
        storage.clear_trace();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let trace = storage.trace();
        for cut in 0..trace.len() {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                let storage = storage_for_partition().await;
                let (persistence, _) =
                    PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                        .await
                        .unwrap();
                persistence
                    .append(first.clone().into_frozen(), true)
                    .unwrap();
                storage.fail_at(cut, mode);
                assert!(persistence.start());
                Rc::clone(&persistence).run().await;
                assert!(persistence.failure().is_some());
                assert!(!persistence.is_durable(first.header()));
                assert!(!persistence.is_durable_through(1));
            }
        }
    });
}

#[test]
fn synchronized_corruption_in_any_record_block_is_refused() {
    block_on(async {
        for block in 0..8 {
            let (storage, journal) = baseline().await;
            drop(journal);
            let mut file = storage
                .open(
                    Path::new("/partition/wal/prepares-0.wal"),
                    OpenMode::ReadWrite,
                )
                .await
                .unwrap();
            let offset = block * 4096 + 40;
            let mut byte = file.read(offset, 1).await.unwrap();
            byte[0] ^= 1;
            file.write(offset, byte).await.unwrap();
            file.sync().await.unwrap();
            storage.crash(Crash::PowerLoss);
            assert!(
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                    .await
                    .is_err()
            );
        }
    });
}

async fn interrupted_install() -> SimStorage {
    let (storage, mut journal) = baseline().await;
    install_backup::begin_with_storage(Path::new(DIRECTORY), &storage)
        .await
        .unwrap();
    replace(&storage, Path::new("/partition/state"), b"new")
        .await
        .unwrap();
    journal.reset(7, None).await.unwrap();
    drop(journal);
    storage.crash(Crash::PowerLoss);
    storage
}

#[test]
fn lost_frontier_cannot_turn_a_durable_journal_into_an_empty_one() {
    block_on(async {
        let (storage, journal) = baseline().await;
        drop(journal);
        storage
            .remove_file(Path::new("/partition/wal/frontier"))
            .await
            .unwrap();
        storage.sync_directory(Path::new(WAL)).await.unwrap();
        storage.crash(Crash::PowerLoss);
        assert!(
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                .await
                .is_err()
        );
    });
}

#[test]
fn first_open_recovers_after_each_initialization_fault() {
    block_on(async {
        let storage = storage_for_partition().await;
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
        let trace = storage.trace();
        for (cut, operation) in trace.iter().enumerate() {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                for crash in [Crash::Process, Crash::PowerLoss] {
                    let storage = storage_for_partition().await;
                    storage.fail_at(cut, mode);
                    let _ = PartitionPrepareJournal::open_with_storage(
                        Path::new(WAL),
                        42,
                        7,
                        storage.clone(),
                    )
                    .await;
                    storage.crash(crash);
                    let journal =
                        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                            .await
                            .unwrap_or_else(|error| {
                                panic!(
                                    "first open cut {cut} {operation:?} {mode:?} {crash:?}: {error}"
                                )
                            });
                    assert_eq!(journal.head(), 0);
                }
            }
        }
        eprintln!(
            "partition WAL initialization fault cases: {}",
            trace.len() * 6
        );
    });
}

#[test]
fn deleting_and_recreating_a_partition_fences_an_old_writer_completion() {
    block_on(async {
        let storage = storage_for_partition().await;
        let (old, _) = PartitionPersistence::open_with_storage(
            Path::new("/partition/prepares-7"),
            42,
            7,
            storage.clone(),
        )
        .await
        .unwrap();
        let original = prepare(1, 0);
        old.append(original.clone().into_frozen(), true).unwrap();
        storage.pause_writes();
        assert!(old.start());
        let mut writer = Box::pin(Rc::clone(&old).run());
        assert!(poll!(&mut writer).is_pending());
        old.retire();
        storage.remove_tree(Path::new(DIRECTORY)).await.unwrap();
        storage.sync_directory(Path::new("/")).await.unwrap();
        storage.resume_writes();
        storage
            .create_directories(Path::new(DIRECTORY))
            .await
            .unwrap();
        storage.sync_directory(Path::new("/")).await.unwrap();
        let (new, _) = PartitionPersistence::open_with_storage(
            Path::new("/partition/prepares-8"),
            42,
            8,
            storage.clone(),
        )
        .await
        .unwrap();
        writer.await;
        assert!(!old.is_durable(original.header()));
        let replacement = prepare_with_payload(1, 0, b"new incarnation");
        new.append(replacement.clone().into_frozen(), true).unwrap();
        assert!(new.start());
        Rc::clone(&new).run().await;
        assert!(new.is_durable(replacement.header()));
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(
            Path::new("/partition/prepares-8"),
            42,
            8,
            storage,
        )
        .await
        .unwrap();
        assert!(recovered.contains(replacement.header()));
        assert!(!recovered.contains(original.header()));
    });
}

#[test]
fn independent_message_and_offset_barriers_cover_the_required_prefix() {
    block_on(async {
        for message_policy in [
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Persisted,
        ] {
            for offset_policy in [
                iggy_common::Durability::Replicated,
                iggy_common::Durability::Persisted,
            ] {
                let storage = storage_for_partition().await;
                let (persistence, _) =
                    PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                        .await
                        .unwrap();
                let first = prepare(1, 0);
                let store = prepare(2, first.header().checksum).transmute_header(
                    |old, header: &mut PrepareHeader| {
                        *header = old;
                        header.operation = Operation::StoreConsumerOffset;
                        header.checksum = header.identity_checksum();
                    },
                );
                let delete = prepare(3, store.header().checksum).transmute_header(
                    |old, header: &mut PrepareHeader| {
                        *header = old;
                        header.operation = Operation::DeleteConsumerOffset;
                        header.checksum = header.identity_checksum();
                    },
                );
                persistence
                    .append(first.into_frozen(), message_policy.is_persisted())
                    .unwrap();
                persistence
                    .append(store.into_frozen(), offset_policy.is_persisted())
                    .unwrap();
                persistence
                    .append(delete.into_frozen(), offset_policy.is_persisted())
                    .unwrap();
                assert!(persistence.start());
                Rc::clone(&persistence).run().await;
                storage.crash(Crash::PowerLoss);
                let recovered =
                    PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                        .await
                        .unwrap();
                // A shared barrier may persist weaker successors in the same batch.
                let expected = if offset_policy.is_persisted() || message_policy.is_persisted() {
                    3
                } else {
                    0
                };
                assert_eq!(recovered.head(), expected);
                assert_eq!(recovered.prepares().await.unwrap().len() as u64, expected);
            }
        }
    });
}

#[test]
fn queued_prepares_share_a_barrier_and_survive_power_loss_together() {
    block_on(async {
        let (storage, persistence) = queued_batch(65).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.failure().is_none());
        let trace = storage.trace();
        assert_eq!(
            trace
                .iter()
                .filter(|operation| **operation == StorageOperation::FileSync)
                .count(),
            4
        );
        assert_eq!(
            trace
                .iter()
                .filter(|operation| **operation == StorageOperation::DirectorySync)
                .count(),
            2
        );
        assert_eq!(
            trace
                .iter()
                .filter(|operation| **operation == StorageOperation::Write)
                .count(),
            4
        );
        assert!(persistence.is_durable_through(65));
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(recovered.prepares().await.unwrap().len(), 65);
    });
}

#[test]
fn failed_group_barrier_never_acknowledges_a_partial_batch() {
    block_on(async {
        let (storage, persistence) = queued_batch(4).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let trace = storage.trace();
        for cut in 0..trace.len() {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                let (storage, persistence) = queued_batch(4).await;
                storage.fail_at(cut, mode);
                assert!(persistence.start());
                Rc::clone(&persistence).run().await;
                let acknowledged = persistence.is_durable_through(4);
                if persistence.failure().is_some() {
                    assert!(!acknowledged);
                }
                storage.crash(Crash::PowerLoss);
                let recovered =
                    PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                        .await
                        .unwrap();
                assert!(matches!(recovered.head(), 0 | 4));
                if acknowledged {
                    assert_eq!(recovered.head(), 4);
                }
            }
        }
    });
}

#[test]
fn checkpoint_syncs_the_retained_writer_before_reclaiming_its_history() {
    block_on(async {
        let (storage, persistence) = queued_batch(4).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let path = Path::new("/partition/offset");
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        file.write(0, b"offset".to_vec()).await.unwrap();
        persistence.retain_offset_file(path.to_string_lossy().into_owned(), file);
        storage.remove_file(path).await.unwrap();
        persistence.retire_offset_file(path.to_str().unwrap());
        persistence.checkpoint_files(4, Vec::new(), vec![Path::new(DIRECTORY).to_path_buf()]);
        storage.fail_at(0, FaultMode::Before);
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.failure().is_some());
        assert_eq!(persistence.checkpoint_op(), 0);
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(recovered.checkpoint_op(), 0);
        assert_eq!(recovered.head(), 4);
    });
}

#[test]
fn checkpoint_barriers_complete_before_wal_reclamation() {
    block_on(async {
        let (storage, persistence) = queued_batch(4).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let path = Path::new("/partition/materialized");
        storage
            .create_directories(Path::new(DIRECTORY))
            .await
            .unwrap();
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        file.write(0, b"committed".to_vec()).await.unwrap();
        persistence.checkpoint_files(
            4,
            vec![path.to_path_buf()],
            vec![Path::new(DIRECTORY).to_path_buf()],
        );
        assert!(persistence.checkpoint_pending());
        assert!(!persistence.needs_checkpoint());
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(!persistence.checkpoint_pending());
        assert_eq!(persistence.checkpoint_op(), 4);
        storage.crash(Crash::PowerLoss);
        let recovered =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        assert_eq!(recovered.checkpoint_op(), 4);
        assert_eq!(
            storage
                .open(path, OpenMode::Read)
                .await
                .unwrap()
                .read(0, 9)
                .await
                .unwrap(),
            b"committed"
        );
    });
}

#[test]
fn failed_materialization_keeps_wal_coverage_and_fences_completion() {
    block_on(async {
        for missing_directory in [false, true] {
            let (storage, persistence) = queued_batch(4).await;
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            let missing = vec![Path::new("/partition/missing").to_path_buf()];
            let (files, directories) = if missing_directory {
                (Vec::new(), missing)
            } else {
                (missing, Vec::new())
            };
            persistence.checkpoint_files(4, files, directories);
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert_eq!(
                persistence.failure().unwrap().kind(),
                io::ErrorKind::NotFound
            );
            assert_eq!(persistence.checkpoint_op(), 0);
            storage.crash(Crash::PowerLoss);
            let recovered =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                    .await
                    .unwrap();
            assert_eq!(recovered.head(), 4);
            assert_eq!(recovered.prepares().await.unwrap().len(), 4);
        }
    });
}

#[test]
fn obsolete_wal_generations_are_reclaimed_after_restart_and_failed_unlink() {
    block_on(async {
        let (storage, mut journal) = baseline().await;
        storage.clear_trace();
        journal.checkpoint(2).await.unwrap();
        let unlink = storage
            .trace()
            .iter()
            .position(|operation| *operation == StorageOperation::Unlink)
            .unwrap();
        for restart in [false, true] {
            let (storage, mut journal) = baseline().await;
            storage.fail_at(unlink, FaultMode::Before);
            journal.checkpoint(2).await.unwrap();
            storage.clear_trace();
            let obsolete = Path::new("/partition/wal/prepares-0.wal");
            assert!(storage.exists(obsolete).await.unwrap());
            if restart {
                drop(journal);
                storage.crash(Crash::PowerLoss);
                journal = PartitionPrepareJournal::open_with_storage(
                    Path::new(WAL),
                    42,
                    7,
                    storage.clone(),
                )
                .await
                .unwrap();
            } else {
                let parent = journal
                    .prepares()
                    .await
                    .unwrap()
                    .last()
                    .map(|prepare| {
                        bytemuck::checked::from_bytes::<PrepareHeader>(
                            &prepare.as_slice()[..size_of::<PrepareHeader>()],
                        )
                        .checksum
                    })
                    .unwrap();
                journal
                    .append(prepare(4, parent).into_frozen())
                    .await
                    .unwrap();
            }
            assert!(!storage.exists(obsolete).await.unwrap());
            assert_eq!(journal.checkpoint_op(), 2);
            assert!(journal.prepares().await.unwrap().iter().any(|prepare| {
                bytemuck::checked::from_bytes::<PrepareHeader>(
                    &prepare.as_slice()[..size_of::<PrepareHeader>()],
                )
                .op == 2
            }));
        }
    });
}

#[test]
fn dropping_a_stalled_writer_restores_ownership_and_releases_drain_waiters() {
    block_on(async {
        let (storage, persistence) = queued_batch(1).await;
        storage.pause_writes();
        assert!(persistence.start());
        let mut writer = Box::pin(Rc::clone(&persistence).run());
        assert!(poll!(&mut writer).is_pending());
        let mut drain = Box::pin(persistence.drain());
        assert!(poll!(&mut drain).is_pending());
        drop(writer);
        assert_eq!(drain.await.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert!(!persistence.start());
    });
}

#[test]
fn rename_rejects_a_nonempty_directory_and_a_fault_is_transient() {
    block_on(async {
        let storage = storage_for_partition().await;
        storage
            .create_directories(Path::new("/source"))
            .await
            .unwrap();
        storage
            .create_directories(Path::new("/target/child"))
            .await
            .unwrap();
        assert_eq!(
            storage
                .rename(Path::new("/source"), Path::new("/target"))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::DirectoryNotEmpty
        );
        assert!(storage.exists(Path::new("/source")).await.unwrap());
        storage.fail_at(0, FaultMode::Before);
        assert!(storage.remove_tree(Path::new("/target")).await.is_err());
        storage.remove_tree(Path::new("/target")).await.unwrap();
        assert!(!storage.exists(Path::new("/target")).await.unwrap());
    });
}

async fn storage_for_partition() -> SimStorage {
    let storage = SimStorage::default();
    storage
        .create_directories(Path::new(DIRECTORY))
        .await
        .unwrap();
    storage.sync_directory(Path::new("/")).await.unwrap();
    storage.clear_trace();
    storage
}

async fn queued_batch(count: u64) -> (SimStorage, Rc<PartitionPersistence<SimStorage>>) {
    let storage = storage_for_partition().await;
    let (persistence, _) =
        PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    let mut parent = 0;
    for op in 1..=count {
        let prepare = prepare(op, parent);
        parent = prepare.header().checksum;
        persistence.append(prepare.into_frozen(), true).unwrap();
    }
    storage.clear_trace();
    (storage, persistence)
}

async fn baseline() -> (SimStorage, PartitionPrepareJournal<SimStorage>) {
    let storage = storage_for_partition().await;
    let mut journal =
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    let mut parent = 0;
    for op in 1..=3 {
        let prepare = prepare(op, parent);
        parent = prepare.header().checksum;
        journal.append(prepare.into_frozen()).await.unwrap();
    }
    replace(&storage, Path::new("/partition/state"), b"old")
        .await
        .unwrap();
    for path in MATERIALIZED_FILES {
        let path = Path::new(path);
        storage
            .create_directories(path.parent().unwrap())
            .await
            .unwrap();
        replace(&storage, path, b"old").await.unwrap();
    }
    storage
        .sync_directory(Path::new("/partition/offsets"))
        .await
        .unwrap();
    storage.sync_directory(Path::new(DIRECTORY)).await.unwrap();
    (storage, journal)
}

async fn mutate(
    storage: &SimStorage,
    journal: &mut PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
) -> io::Result<()> {
    match mutation {
        Mutation::Append => {
            let entries = journal.prepares().await?;
            let last = bytemuck::checked::from_bytes::<PrepareHeader>(
                &entries.last().unwrap().as_slice()[..size_of::<PrepareHeader>()],
            );
            journal
                .append(prepare(4, last.checksum).into_frozen())
                .await
        }
        Mutation::CertifyView => {
            let entries = journal.prepares().await?;
            let last = bytemuck::checked::from_bytes::<PrepareHeader>(
                &entries.last().unwrap().as_slice()[..size_of::<PrepareHeader>()],
            );
            let next = prepare(4, last.checksum);
            let checksum = next.header().checksum;
            journal.append_buffered(next.into_frozen()).await?;
            journal.certify_log_view(2, 4, checksum).await
        }
        Mutation::Checkpoint => {
            replace(storage, Path::new("/partition/materialized"), b"1,2").await?;
            journal.checkpoint(2).await
        }
        Mutation::Truncate => journal.truncate_from(3).await,
        Mutation::Reset => {
            replace(storage, Path::new("/partition/materialized"), b"1-7").await?;
            journal.reset(7, None).await
        }
        Mutation::Purge => {
            journal.mark_purge(9, 3).await?;
            storage.remove_file(Path::new("/partition/state")).await?;
            storage.sync_directory(Path::new(DIRECTORY)).await?;
            replace(storage, Path::new("/partition/purge.gen"), b"9").await
        }
    }
}

async fn install(
    storage: &SimStorage,
    journal: &mut PartitionPrepareJournal<SimStorage>,
) -> io::Result<()> {
    install_backup::begin_with_storage(Path::new(DIRECTORY), storage).await?;
    replace(storage, Path::new("/partition/state"), b"new").await?;
    for path in MATERIALIZED_FILES {
        replace(storage, Path::new(path), b"new").await?;
    }
    journal.reset(7, None).await?;
    install_backup::finish_with_storage(Path::new(DIRECTORY), storage).await
}

async fn replace(storage: &SimStorage, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = storage.open(&temporary, OpenMode::Create).await?;
    file.write(0, bytes.to_vec()).await?;
    file.sync().await?;
    storage.rename(&temporary, path).await?;
    storage.sync_directory(path.parent().unwrap()).await
}

async fn assert_recovery(
    storage: &SimStorage,
    journal: &PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
    completed: bool,
) {
    match mutation {
        Mutation::Append => {
            assert!((3..=4).contains(&journal.head()));
            if completed {
                assert_eq!(journal.head(), 4);
            }
        }
        Mutation::CertifyView => {
            assert!((3..=4).contains(&journal.head()));
            if completed {
                assert_eq!(journal.certified_log_view(), Some(2));
            }
            if journal.certified_log_view() == Some(2) {
                assert_eq!(journal.head(), 4);
            }
        }
        Mutation::Checkpoint => {
            assert_eq!(journal.head(), 3);
            assert!([0, 2].contains(&journal.checkpoint_op()));
            if journal.checkpoint_op() == 2 {
                assert_eq!(
                    storage
                        .open(Path::new("/partition/materialized"), OpenMode::Read)
                        .await
                        .unwrap()
                        .read(0, 3)
                        .await
                        .unwrap(),
                    b"1,2"
                );
            }
            if completed {
                assert_eq!(journal.checkpoint_op(), 2);
            }
        }
        Mutation::Truncate => {
            assert!([2, 3].contains(&journal.head()));
            if completed {
                assert_eq!(journal.head(), 2);
            }
        }
        Mutation::Reset => {
            assert!([3, 7].contains(&journal.head()));
            if journal.head() == 7 {
                assert_eq!(journal.checkpoint_op(), 7);
                assert_eq!(
                    storage
                        .open(Path::new("/partition/materialized"), OpenMode::Read)
                        .await
                        .unwrap()
                        .read(0, 3)
                        .await
                        .unwrap(),
                    b"1-7"
                );
            }
            if completed {
                assert_eq!(journal.head(), 7);
            }
        }
        Mutation::Purge => {
            assert_eq!(journal.head(), 3);
            assert!([(0, 0), (9, 3)].contains(&journal.purge_marker()));
            if storage
                .exists(Path::new("/partition/purge.gen"))
                .await
                .unwrap()
            {
                assert_eq!(journal.purge_marker(), (9, 3));
                assert!(!storage.exists(Path::new("/partition/state")).await.unwrap());
            }
            if completed {
                assert_eq!(journal.purge_marker(), (9, 3));
                assert!(
                    storage
                        .exists(Path::new("/partition/purge.gen"))
                        .await
                        .unwrap()
                );
            }
        }
    }
    let entries = journal.prepares().await.unwrap();
    assert_eq!(
        entries.len() as u64,
        journal.head() - journal.checkpoint_op()
            + u64::from(matches!(mutation, Mutation::Checkpoint) && journal.checkpoint_op() > 0)
    );
}

fn prepare(op: u64, parent: u128) -> Message<PrepareHeader> {
    prepare_with_payload(op, parent, &vec![u8::try_from(op).unwrap(); 12 * 1024])
}

fn prepare_with_payload(op: u64, parent: u128, payload: &[u8]) -> Message<PrepareHeader> {
    let mut buffer = Owned::<4096>::zeroed(size_of::<PrepareHeader>() + payload.len());
    buffer.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(payload);
    let length = buffer.as_slice().len();
    let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
        &mut buffer.as_mut_slice()[..size_of::<PrepareHeader>()],
    );
    header.command = Command::Prepare;
    header.operation = Operation::SendMessages;
    header.group = 42;
    header.op = op;
    header.parent = parent;
    header.size = u32::try_from(length).unwrap();
    // Distinct identities are sufficient here. The WAL additionally hashes the full record.
    header.checksum_body = payload.iter().map(|byte| u128::from(*byte)).sum();
    header.checksum = header.identity_checksum();
    Message::try_from(buffer).unwrap()
}
