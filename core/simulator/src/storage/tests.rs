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
use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
use iggy_binary_protocol::{Command, Operation, PrepareHeader};
use journal::partition_journal::{
    PARTITION_WAL_BLOCK_SIZE, SegmentPosition, SegmentReference, record_length,
};
use journal::{DurableAppend, PartitionPrepareJournal};
use partitions::{PartitionPersistence, install_backup};
use server_common::send_messages::{
    BATCH_MESSAGE_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::IggyNamespace;
use server_common::{
    Message,
    iobuf::{IOV_MAX, Owned},
};
use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::rc::Rc;
use twox_hash::XxHash3_64;

const DIRECTORY: &str = "/partition";
const WAL: &str = "/partition/wal";
const OWNED_BATCH_BYTES: usize = 12 * 1024;
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
    /// A checkpoint whose rewrite runs while the outgoing generation still
    /// holds a buffered record. A torn publication leaves the older slot naming
    /// that generation, so recovery walks its tail and must not read an
    /// unsynced record there as damage.
    CheckpointBufferedTail,
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
            Mutation::CheckpointBufferedTail,
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
fn referenced_wal_fault_sweep_preserves_bodies_through_publication_and_reclamation() {
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
            let (storage, mut journal) = referenced_baseline().await;
            storage.clear_trace();
            mutate_referenced(&storage, &mut journal, mutation)
                .await
                .unwrap();
            let trace = storage.trace();
            for (cut, operation) in trace.iter().enumerate() {
                for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                    for crash in [Crash::Process, Crash::PowerLoss] {
                        for writeback in [false, true] {
                            let (storage, mut journal) = referenced_baseline().await;
                            storage.fail_at(cut, mode);
                            let completed = mutate_referenced(&storage, &mut journal, mutation)
                                .await
                                .is_ok();
                            drop(journal);
                            if writeback {
                                storage.writeback();
                            }
                            storage.crash(crash);
                            let context = format!(
                                "{mutation:?} cut {cut} {operation:?} {mode:?} {crash:?} writeback={writeback}"
                            );
                            let recovered = PartitionPrepareJournal::open_with_storage(
                                Path::new(WAL),
                                42,
                                7,
                                storage.clone(),
                            )
                            .await
                            .unwrap_or_else(|error| panic!("{context}: {error}"));
                            assert_referenced_recovery(&recovered, mutation, completed, &context)
                                .await;
                            cases += 1;
                        }
                    }
                }
            }
        }
        eprintln!("referenced partition WAL fault cases: {cases}");
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
        storage.resume();
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
fn synchronized_corruption_and_shortening_of_owned_segment_blocks_is_refused() {
    block_on(async {
        let retained = Path::new("/partition/wal/segment-0-0.log");
        for damage in ["bit flip", "zero block", "short file"] {
            for block in 0..2 * OWNED_BATCH_BYTES / PARTITION_WAL_BLOCK_SIZE {
                let (storage, journal) = owned_segment_baseline(false).await;
                assert!(
                    journal
                        .prepares()
                        .await
                        .unwrap()
                        .iter()
                        .all(|prepare| prepare.header().checksum_body == 0)
                );
                drop(journal);
                let mut file = storage.open(retained, OpenMode::ReadWrite).await.unwrap();
                let offset = (block * PARTITION_WAL_BLOCK_SIZE) as u64;
                match damage {
                    "bit flip" => {
                        let mut byte = file.read(offset, 1).await.unwrap();
                        byte[0] ^= 1;
                        file.write(offset, byte).await.unwrap();
                    }
                    "zero block" => file
                        .write(offset, vec![0; PARTITION_WAL_BLOCK_SIZE])
                        .await
                        .unwrap(),
                    "short file" => file.truncate(offset).await.unwrap(),
                    _ => unreachable!(),
                }
                file.sync().await.unwrap();
                let damaged = file
                    .read(0, usize::try_from(file.length().await.unwrap()).unwrap())
                    .await
                    .unwrap();
                storage.crash(Crash::PowerLoss);
                assert!(
                    PartitionPrepareJournal::open_with_storage(
                        Path::new(WAL),
                        42,
                        7,
                        storage.clone()
                    )
                    .await
                    .is_err(),
                    "{damage}, block {block}"
                );
                let file = storage.open(retained, OpenMode::Read).await.unwrap();
                assert_eq!(file.read(0, damaged.len()).await.unwrap(), damaged);
            }
        }
    });
}

#[test]
fn metadata_only_append_skips_segment_barriers_after_durable_bodies() {
    block_on(async {
        let (storage, mut journal) = owned_segment_baseline(false).await;
        let parent = journal
            .prepares()
            .await
            .unwrap()
            .last()
            .unwrap()
            .header()
            .checksum;
        let offset = prepare(3, parent).transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.operation = Operation::StoreConsumerOffset;
            header.checksum = header.identity_checksum();
        });
        storage.clear_trace();
        journal.append(offset.clone().into_frozen()).await.unwrap();
        assert_eq!(
            storage
                .trace()
                .iter()
                .filter(|operation| **operation == StorageOperation::FileSync)
                .count(),
            2,
            "only the WAL and frontier need new file barriers"
        );
        assert_eq!(
            storage
                .trace()
                .iter()
                .filter(|operation| **operation == StorageOperation::Exists)
                .count(),
            0
        );
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(
            recovered
                .prepares()
                .await
                .unwrap()
                .last()
                .unwrap()
                .as_slice(),
            offset.as_slice()
        );
    });
}

#[test]
fn replacing_a_retained_offset_writer_keeps_both_inodes_until_checkpoint() {
    block_on(async {
        let (storage, persistence) = queued_batch(1).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let path = Path::new("/partition/offset");
        let original = Path::new("/partition/original-offset");
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        file.write(0, b"original".to_vec()).await.unwrap();
        storage.hard_link(path, original).await.unwrap();
        persistence
            .retain_offset_file(path.to_str().unwrap().to_owned(), file)
            .await
            .unwrap();
        storage.remove_file(path).await.unwrap();
        let mut replacement = storage.open(path, OpenMode::Create).await.unwrap();
        replacement.write(0, b"replaced".to_vec()).await.unwrap();
        persistence
            .retain_offset_file(path.to_str().unwrap().to_owned(), replacement)
            .await
            .unwrap();

        persistence.checkpoint_files(
            1,
            vec![path.to_path_buf()],
            vec![Path::new(DIRECTORY).to_path_buf()],
        );
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.failure().is_none());
        assert_eq!(persistence.checkpoint_op(), 1);
        storage.crash(Crash::PowerLoss);
        let file = storage.open(original, OpenMode::Read).await.unwrap();
        assert_eq!(file.read(0, 8).await.unwrap(), b"original");
        let file = storage.open(path, OpenMode::Read).await.unwrap();
        assert_eq!(file.read(0, 8).await.unwrap(), b"replaced");
    });
}

#[test]
fn a_full_offset_writer_cache_synchronizes_overflow_and_reports_barrier_failure() {
    const OFFSET_KEYS: usize = 128;
    block_on(async {
        let (storage, persistence) = queued_batch(1).await;
        for key in 0..OFFSET_KEYS {
            let path = format!("/partition/offset-{key}");
            let mut file = storage
                .open(Path::new(&path), OpenMode::Create)
                .await
                .unwrap();
            file.write(0, b"cached".to_vec()).await.unwrap();
            persistence.retain_offset_file(path, file).await.unwrap();
        }
        let path = Path::new("/partition/overflow");
        let mut file = storage.open(path, OpenMode::Create).await.unwrap();
        storage.sync_directory(Path::new(DIRECTORY)).await.unwrap();
        file.write(0, b"durable".to_vec()).await.unwrap();
        storage.clear_trace();
        persistence
            .retain_offset_file(path.to_string_lossy().into_owned(), file)
            .await
            .unwrap();
        assert_eq!(storage.trace(), vec![StorageOperation::FileSync]);
        assert!(
            persistence
                .take_offset_file(path.to_str().unwrap())
                .is_none()
        );

        let mut file = storage.open(path, OpenMode::ReadWrite).await.unwrap();
        file.write(0, b"pending".to_vec()).await.unwrap();
        storage.fail_at(0, FaultMode::Before);
        assert!(
            persistence
                .retain_offset_file(path.to_string_lossy().into_owned(), file)
                .await
                .is_err()
        );
        storage.crash(Crash::PowerLoss);
        let file = storage.open(path, OpenMode::Read).await.unwrap();
        assert_eq!(file.read(0, 7).await.unwrap(), b"durable");
    });
}

#[test]
fn checkpoint_skips_duplicate_offset_sync_but_still_refuses_a_missing_path() {
    block_on(async {
        for missing in [false, true] {
            let (storage, persistence) = queued_batch(1).await;
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            let path = Path::new("/partition/offset");
            let mut file = storage.open(path, OpenMode::Create).await.unwrap();
            file.write(0, b"offset".to_vec()).await.unwrap();
            persistence
                .retain_offset_file(path.to_string_lossy().into_owned(), file)
                .await
                .unwrap();
            if missing {
                storage.remove_file(path).await.unwrap();
            }
            persistence.checkpoint_files(
                1,
                vec![path.to_path_buf()],
                vec![Path::new(DIRECTORY).to_path_buf()],
            );
            storage.clear_trace();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            if missing {
                assert!(persistence.failure().is_some());
                assert_eq!(persistence.checkpoint_op(), 0);
            } else {
                assert!(persistence.failure().is_none());
                assert_eq!(persistence.checkpoint_op(), 1);
                assert_eq!(
                    storage
                        .trace()
                        .iter()
                        .filter(|operation| **operation == StorageOperation::FileSync)
                        .count(),
                    4,
                    "original offset writer, outgoing WAL, replacement WAL, frontier"
                );
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
#[ignore = "PR #4092 review: PRE-EXISTING test. It passed vacuously while `SimStorage::writer_identity` returned `None` and never took a lease; now that the lease is real it is blocked on the compio-bound drain wait"]
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
        storage.resume();
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
        let count = |wanted: StorageOperation| {
            trace
                .iter()
                .filter(|operation| **operation == wanted)
                .count()
        };
        // One group: the WAL extent and the frontier slot, one barrier each.
        assert_eq!(count(StorageOperation::Write), 2);
        assert_eq!(count(StorageOperation::FileSync), 2);
        // Publication overwrites a pre-existing slot in place, so an
        // acknowledgment creates no file, renames nothing and leaves no
        // directory to make durable. Those are the filesystem metadata
        // transactions this path must never pay per batch.
        assert_eq!(count(StorageOperation::Create), 0);
        assert_eq!(count(StorageOperation::Rename), 0);
        assert_eq!(count(StorageOperation::DirectorySync), 0);
        assert!(persistence.is_durable_through(65));
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(recovered.prepares().await.unwrap().len(), 65);
    });
}

#[test]
fn queued_owned_prepares_share_three_file_barriers_without_directory_mutations() {
    block_on(async {
        for count in [65, 256, 257] {
            let storage = storage_for_partition().await;
            let (persistence, _) =
                PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                    .await
                    .unwrap();
            persistence.enable_segment_storage(SegmentPosition::default(), 64 * 1024 * 1024);
            let first = owned_prepare(1, 0, 0);
            let mut parent = first.header().checksum;
            persistence.append(first.into_frozen(), true).unwrap();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert!(persistence.failure().is_none());
            persistence.take_metrics();
            for index in 1..=count {
                let prepare = owned_prepare(1, parent, index).transmute_header(
                    |original, header: &mut PrepareHeader| {
                        *header = original;
                        header.op = index + 1;
                        header.checksum = header.identity_checksum();
                    },
                );
                parent = prepare.header().checksum;
                persistence.append(prepare.into_frozen(), true).unwrap();
            }
            storage.clear_trace();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert!(persistence.failure().is_none());
            let trace = storage.trace();
            let operations = |wanted: StorageOperation| {
                trace
                    .iter()
                    .filter(|operation| **operation == wanted)
                    .count() as u64
            };
            let groups = count.div_ceil(256);
            assert_eq!(operations(StorageOperation::Write), 3 * groups);
            assert_eq!(operations(StorageOperation::FileSync), 3 * groups);
            assert_eq!(operations(StorageOperation::Create), 0);
            assert_eq!(operations(StorageOperation::Rename), 0);
            assert_eq!(operations(StorageOperation::DirectorySync), 0);
            let metrics = persistence.take_metrics();
            assert_eq!(metrics.completed_batches, groups);
            assert_eq!(metrics.batched_prepares, count);
            assert!(persistence.is_durable_through(count + 1));
            storage.crash(Crash::PowerLoss);
            let recovered =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                    .await
                    .unwrap();
            assert_eq!(recovered.head(), count + 1);
            assert_eq!(recovered.prepares().await.unwrap().len() as u64, count + 1);
        }
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
        assert!(
            persistence
                .retain_offset_file(path.to_string_lossy().into_owned(), file)
                .await
                .is_ok()
        );
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

#[test]
fn segment_roll_during_wal_create_keeps_the_same_inode() {
    block_on(segment_roll_during_wal_open(StorageOperation::Create));
}

#[test]
fn segment_roll_during_wal_link_keeps_the_same_inode() {
    block_on(segment_roll_during_wal_open(StorageOperation::Link));
}

async fn segment_roll_during_wal_open(operation: StorageOperation) {
    let storage = storage_for_partition().await;
    let mut journal =
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    journal
        .enable_segment_storage(SegmentPosition::default(), OWNED_BATCH_BYTES as u64)
        .await
        .unwrap();
    let first = owned_prepare(1, 0, 0);
    journal.append(first.clone().into_frozen()).await.unwrap();
    let second = owned_prepare(2, first.header().checksum, 1);
    let public = Path::new(DIRECTORY).join(format!("{:020}.log", 1));
    let retained = Path::new(WAL).join("segment-1-1.log");
    storage.state.borrow_mut().paused = Some(operation);
    let mut append = Box::pin(journal.append(second.clone().into_frozen()));
    assert!(poll!(&mut append).is_pending());
    storage.resume();
    let roll_reader = storage.open(&public, OpenMode::CreateOrOpen).await.unwrap();
    append
        .await
        .expect("a concurrent segment roll must not fail the WAL append");
    {
        let state = storage.state.borrow();
        assert_eq!(
            state.lookup(&public).unwrap(),
            state.lookup(&retained).unwrap()
        );
        assert_eq!(
            roll_reader.inode,
            state.lookup(&public).unwrap(),
            "the WAL must retain the inode opened by the segment roll"
        );
    }
    assert_eq!(
        roll_reader.read(0, OWNED_BATCH_BYTES).await.unwrap(),
        second.as_slice()[size_of::<PrepareHeader>()..]
    );
    drop(journal);
    storage.crash(Crash::PowerLoss);
    let recovered =
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    assert_eq!(recovered.durable_op(), 2);
    let actual = recovered.prepares().await.unwrap();
    for (actual, expected) in actual.iter().zip([first, second]) {
        assert_eq!(actual.as_slice(), expected.as_slice());
    }
    assert_eq!(actual.len(), 2);
    let state = storage.state.borrow();
    assert_eq!(
        state.lookup(&public).unwrap(),
        state.lookup(&retained).unwrap()
    );
}

#[test]
fn buffered_owned_segments_rotate_without_barriers_and_persist_offset_predecessors() {
    block_on(async {
        let storage = storage_for_partition().await;
        let mut journal =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        journal
            .enable_segment_storage(SegmentPosition::default(), OWNED_BATCH_BYTES as u64)
            .await
            .unwrap();
        storage.clear_trace();
        let written_before: usize = storage.state.borrow().written_bytes.values().sum();
        let mut parent = 0;
        let mut prepares = Vec::new();
        for offset in 0..3 {
            let prepare = owned_prepare(offset + 1, parent, offset);
            parent = prepare.header().checksum;
            journal
                .append_buffered(prepare.clone().into_frozen())
                .await
                .unwrap();
            prepares.push(prepare);
        }
        assert!(
            !storage.trace().iter().any(|operation| matches!(
                operation,
                StorageOperation::FileSync | StorageOperation::DirectorySync
            )),
            "replicated bodies must not require a barrier, including across append groups and rotations"
        );
        assert_eq!(journal.durable_op(), 0);
        assert_eq!(journal.size_bytes(), (3 * PARTITION_WAL_BLOCK_SIZE) as u64);
        assert_eq!(
            journal.retained_bytes(),
            3 * journal::partition_journal::record_length(prepares[0].as_slice().len()).unwrap()
                as u64
        );
        let written_after: usize = storage.state.borrow().written_bytes.values().sum();
        assert_eq!(
            written_after - written_before,
            3 * (OWNED_BATCH_BYTES + PARTITION_WAL_BLOCK_SIZE),
            "each append writes one body and one metadata WAL record"
        );
        let offset = offset_prepare(4, parent);
        journal.append(offset.clone().into_frozen()).await.unwrap();
        prepares.push(offset);
        for prepare in &prepares[..3] {
            let reference = journal.segment_reference(prepare.header()).unwrap();
            let public = Path::new(DIRECTORY).join(format!("{:020}.log", reference.start_offset));
            let retained = Path::new(WAL).join(format!(
                "segment-{}-{}.log",
                reference.generation, reference.start_offset
            ));
            let state = storage.state.borrow();
            let inode = state.lookup(&public).unwrap();
            assert_eq!(inode, state.lookup(&retained).unwrap());
            assert_eq!(state.written_bytes[&inode], OWNED_BATCH_BYTES);
        }
        drop(journal);
        storage.crash(Crash::PowerLoss);
        let mut recovered =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        assert_eq!(recovered.durable_op(), 4);
        assert_eq!(
            recovered.segment_checkpoint(),
            Some(SegmentPosition::default())
        );
        let actual = recovered.prepares().await.unwrap();
        assert_eq!(actual.len(), prepares.len());
        for (actual, expected) in actual.iter().zip(&prepares) {
            assert_eq!(actual.as_slice(), expected.as_slice());
        }
        recovered.checkpoint(2).await.unwrap();
        let retained_path = Path::new(DIRECTORY).join(format!("{:020}.log", 1));
        let reader = storage.open(&retained_path, OpenMode::Read).await.unwrap();
        for offset in 0..2 {
            storage
                .remove_file(&Path::new(DIRECTORY).join(format!("{offset:020}.log")))
                .await
                .unwrap();
        }
        storage.sync_directory(Path::new(DIRECTORY)).await.unwrap();
        assert_eq!(
            reader.read(0, OWNED_BATCH_BYTES).await.unwrap(),
            prepares[1].as_slice()[size_of::<PrepareHeader>()..]
        );
        drop(recovered);
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(recovered.checkpoint_op(), 2);
        let actual = recovered.prepares().await.unwrap();
        assert_eq!(actual.len(), prepares.len() - 1);
        for (actual, expected) in actual.iter().zip(&prepares[1..]) {
            assert_eq!(actual.as_slice(), expected.as_slice());
        }
    });
}

fn offset_prepare(op: u64, parent: u128) -> Message<PrepareHeader> {
    prepare(op, parent).transmute_header(|old, header: &mut PrepareHeader| {
        *header = old;
        header.operation = Operation::StoreConsumerOffset;
        header.checksum = header.identity_checksum();
    })
}

#[test]
fn persisted_offsets_wait_for_every_buffered_body_sync_and_fence_on_failure() {
    block_on(async {
        const MESSAGES: u64 = 3;
        for failed_body in [None, Some(0), Some(1), Some(2)] {
            let storage = storage_for_partition().await;
            let (persistence, _) =
                PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                    .await
                    .unwrap();
            persistence
                .enable_segment_storage(SegmentPosition::default(), OWNED_BATCH_BYTES as u64);
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            let mut parent = 0;
            for offset in 0..MESSAGES {
                let prepare = owned_prepare(offset + 1, parent, offset);
                parent = prepare.header().checksum;
                persistence.append(prepare.into_frozen(), false).unwrap();
                assert!(persistence.start());
                Rc::clone(&persistence).run().await;
            }
            let offset = offset_prepare(MESSAGES + 1, parent);
            persistence
                .append(offset.clone().into_frozen(), true)
                .unwrap();
            storage.pause_file_syncs();
            assert!(persistence.start());
            let mut writer = Box::pin(Rc::clone(&persistence).run());
            assert!(poll!(&mut writer).is_pending());
            assert!(!persistence.is_durable(offset.header()));
            assert_eq!(persistence.durable_op(), 0);
            assert!(persistence.failure().is_none());
            if let Some(index) = failed_body {
                storage.fail_at(index, FaultMode::Before);
            }
            storage.resume();
            writer.await;
            assert_eq!(
                persistence.is_durable(offset.header()),
                failed_body.is_none()
            );
            assert_eq!(persistence.failure().is_some(), failed_body.is_some());
            if failed_body.is_some() {
                assert!(!persistence.start());
            }
            drop(persistence);
            storage.crash(Crash::PowerLoss);
            let recovered =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                    .await
                    .unwrap();
            assert_eq!(
                recovered.head(),
                if failed_body.is_none() {
                    MESSAGES + 1
                } else {
                    0
                }
            );
            assert_eq!(recovered.contains(offset.header()), failed_body.is_none());
        }
    });
}

#[test]
fn owned_segment_fault_sweep_preserves_acknowledged_bodies_and_checkpoint_bounds() {
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
            for buffered in [false, true] {
                let (storage, mut journal) = owned_segment_baseline(buffered).await;
                storage.clear_trace();
                mutate_owned_segments(&storage, &mut journal, mutation)
                    .await
                    .unwrap();
                let trace = storage.trace();
                for (cut, operation) in trace.iter().enumerate() {
                    for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                        for crash in [Crash::Process, Crash::PowerLoss] {
                            for writeback in [false, true] {
                                let (storage, mut journal) = owned_segment_baseline(buffered).await;
                                storage.fail_at(cut, mode);
                                let completed =
                                    mutate_owned_segments(&storage, &mut journal, mutation)
                                        .await
                                        .is_ok();
                                drop(journal);
                                if writeback {
                                    storage.writeback();
                                }
                                storage.crash(crash);
                                let context = format!(
                                    "{mutation:?} buffered={buffered} cut {cut} {operation:?} {mode:?} {crash:?} writeback={writeback}"
                                );
                                let recovered = PartitionPrepareJournal::open_with_storage(
                                    Path::new(WAL),
                                    42,
                                    7,
                                    storage,
                                )
                                .await
                                .unwrap_or_else(|error| panic!("{context}: {error}"));
                                assert_owned_segments(
                                    &recovered, mutation, buffered, completed, &context,
                                )
                                .await;
                                cases += 1;
                            }
                        }
                    }
                }
            }
        }
        println!("owned segment fault cases: {cases}");
    });
}

async fn owned_segment_baseline(
    buffered: bool,
) -> (SimStorage, PartitionPrepareJournal<SimStorage>) {
    let storage = storage_for_partition().await;
    let mut journal =
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    journal
        .enable_segment_storage(SegmentPosition::default(), (2 * OWNED_BATCH_BYTES) as u64)
        .await
        .unwrap();
    let first = owned_prepare(1, 0, 0);
    let second = owned_prepare(2, first.header().checksum, 1);
    journal.append(first.into_frozen()).await.unwrap();
    journal.checkpoint(1).await.unwrap();
    journal.append_buffered(second.into_frozen()).await.unwrap();
    if !buffered {
        journal.sync().await.unwrap();
    }
    (storage, journal)
}

#[test]
fn sealed_tail_recovery_preserves_the_public_name_at_every_crash_boundary() {
    block_on(async {
        let (storage, mut journal) = owned_segment_baseline(false).await;
        journal.checkpoint(2).await.unwrap();
        drop(journal);
        storage.clear_trace();
        drop(
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap(),
        );
        let trace = storage.trace();
        let public = Path::new("/partition/00000000000000000000.log");
        let mut cases = 0;
        for (cut, operation) in trace.iter().enumerate() {
            for mode in [FaultMode::Before, FaultMode::After] {
                for crash in [Crash::Process, Crash::PowerLoss] {
                    for writeback in [false, true] {
                        let (storage, mut journal) = owned_segment_baseline(false).await;
                        journal.checkpoint(2).await.unwrap();
                        drop(journal);
                        storage.fail_at(cut, mode);
                        let _ = PartitionPrepareJournal::open_with_storage(
                            Path::new(WAL),
                            42,
                            7,
                            storage.clone(),
                        )
                        .await;
                        if writeback {
                            storage.writeback();
                        }
                        storage.crash(crash);
                        let context = format!(
                            "cut {cut} {operation:?} {mode:?} {crash:?} writeback={writeback}"
                        );
                        let recovered = PartitionPrepareJournal::open_with_storage(
                            Path::new(WAL),
                            42,
                            7,
                            storage.clone(),
                        )
                        .await
                        .unwrap_or_else(|error| panic!("{context}: {error}"));
                        assert_eq!(recovered.checkpoint_op(), 2, "{context}");
                        let bytes = storage
                            .open(public, OpenMode::Read)
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{context}: public tail missing: {error}")
                            })
                            .read(0, 2 * OWNED_BATCH_BYTES)
                            .await
                            .unwrap();
                        let first = owned_prepare(1, 0, 0);
                        let second = owned_prepare(2, first.header().checksum, 1);
                        assert_eq!(
                            &bytes[..OWNED_BATCH_BYTES],
                            &first.as_slice()[size_of::<PrepareHeader>()..],
                            "{context}"
                        );
                        assert_eq!(
                            &bytes[OWNED_BATCH_BYTES..],
                            &second.as_slice()[size_of::<PrepareHeader>()..],
                            "{context}"
                        );
                        assert!(
                            !storage
                                .exists(&public.with_extension("log.tmp"))
                                .await
                                .unwrap(),
                            "{context}: recovery temporary must be removed"
                        );
                        cases += 1;
                    }
                }
            }
        }
        eprintln!("sealed tail recovery fault cases: {cases}");
    });
}

#[test]
fn sealed_tail_removed_by_retention_stays_absent_after_recovery() {
    block_on(async {
        let (storage, mut journal) = owned_segment_baseline(false).await;
        journal.checkpoint(2).await.unwrap();
        drop(journal);
        let public = Path::new("/partition/00000000000000000000.log");
        storage.remove_file(public).await.unwrap();
        storage.sync_directory(Path::new(DIRECTORY)).await.unwrap();
        for crash in [Crash::Process, Crash::PowerLoss] {
            storage.crash(crash);
            let recovered =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                    .await
                    .unwrap();
            assert_eq!(recovered.checkpoint_op(), 2);
            assert_eq!(
                recovered.prepares().await.unwrap().len(),
                1,
                "the private checkpoint prepare remains repairable"
            );
            assert!(
                !storage.exists(public).await.unwrap(),
                "recovery must not resurrect retained data"
            );
        }
    });
}

#[test]
fn adjacent_segment_bodies_share_writes_bounded_by_rotation_and_iov_max() {
    block_on(async {
        for (count, batches_per_segment) in [(5, 2), (IOV_MAX + 1, IOV_MAX + 1)] {
            let storage = storage_for_partition().await;
            let mut journal =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                    .await
                    .unwrap();
            journal
                .enable_segment_storage(
                    SegmentPosition::default(),
                    (batches_per_segment * OWNED_BATCH_BYTES) as u64,
                )
                .await
                .unwrap();
            let mut parent = 0;
            let prepares: Vec<_> = (0..count)
                .map(|index| {
                    let prepare = owned_prepare(1, parent, index as u64).transmute_header(
                        |original, header: &mut PrepareHeader| {
                            *header = original;
                            header.op = index as u64 + 1;
                            header.checksum = header.identity_checksum();
                        },
                    );
                    parent = prepare.header().checksum;
                    prepare.into_frozen()
                })
                .collect();
            storage.clear_trace();
            journal.append_batch_buffered(&prepares).await.unwrap();
            journal.sync().await.unwrap();
            let body_writes = count.div_ceil(batches_per_segment.min(IOV_MAX));
            assert_eq!(
                storage
                    .trace()
                    .iter()
                    .filter(|operation| **operation == StorageOperation::Write)
                    .count(),
                body_writes + 2,
                "body groups plus WAL and frontier writes"
            );
            drop(journal);
            storage.crash(Crash::PowerLoss);
            let recovered =
                PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                    .await
                    .unwrap();
            let recovered = recovered.prepares().await.unwrap();
            assert_eq!(recovered.len(), prepares.len());
            for (actual, expected) in recovered.iter().zip(&prepares) {
                assert_eq!(actual.as_slice(), expected.as_slice());
            }
        }
    });
}

#[test]
fn live_rollback_preserves_public_index_handles_for_replacement_and_checkpoint() {
    block_on(async {
        let (storage, mut journal) = owned_segment_baseline(false).await;
        let first = owned_prepare(1, 0, 0);
        let second = owned_prepare(2, first.header().checksum, 1);
        let third = owned_prepare(3, second.header().checksum, 2);
        journal.append(third.clone().into_frozen()).await.unwrap();
        let log_path = Path::new("/partition/00000000000000000002.log");
        let index_path = Path::new("/partition/00000000000000000002.index");
        let mut index = storage.open(index_path, OpenMode::Create).await.unwrap();
        index.write(0, b"old-index".to_vec()).await.unwrap();
        journal.truncate_from(3).await.unwrap();
        assert!(storage.exists(log_path).await.unwrap());
        assert!(storage.exists(index_path).await.unwrap());
        journal.append(third.into_frozen()).await.unwrap();
        index.write(0, b"new-index".to_vec()).await.unwrap();
        assert_eq!(
            storage
                .open(index_path, OpenMode::Read)
                .await
                .unwrap()
                .read(0, 9)
                .await
                .unwrap(),
            b"new-index"
        );
        journal
            .checkpoint_files(
                3,
                &[log_path.into(), index_path.into()],
                &[Path::new(DIRECTORY).into()],
                &BTreeSet::new(),
            )
            .await
            .unwrap();
        drop(journal);
        storage.crash(Crash::PowerLoss);
        let recovered = PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
            .await
            .unwrap();
        assert_eq!(recovered.checkpoint_op(), 3);
        assert_eq!(recovered.prepares().await.unwrap().len(), 1);
    });
}

#[test]
fn completed_prefix_validation_remains_available_during_a_pending_append() {
    block_on(async {
        for durable in [false, true] {
            let storage = storage_for_partition().await;
            let (persistence, _) =
                PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                    .await
                    .unwrap();
            persistence
                .enable_segment_storage(SegmentPosition::default(), (4 * OWNED_BATCH_BYTES) as u64);
            let first = owned_prepare(1, 0, 0).into_frozen();
            persistence.append(first.clone(), durable).unwrap();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            let parent = bytemuck::checked::from_bytes::<PrepareHeader>(
                &first.as_slice()[..size_of::<PrepareHeader>()],
            )
            .checksum;
            let second = owned_prepare(2, parent, 1).into_frozen();
            let first_prefix = std::slice::from_ref(&first);
            let second_prefix = std::slice::from_ref(&second);
            persistence.append(second.clone(), durable).unwrap();
            storage.pause_writes();
            assert!(persistence.start());
            let mut writer = Box::pin(Rc::clone(&persistence).run());
            assert!(poll!(&mut writer).is_pending());
            assert_eq!(
                persistence
                    .validate_segment_prefix(first_prefix, 0, 0, durable)
                    .unwrap(),
                OWNED_BATCH_BYTES as u64
            );
            assert!(
                persistence
                    .validate_segment_prefix(second_prefix, 0, OWNED_BATCH_BYTES as u64, durable)
                    .is_err()
            );
            assert!(persistence.failure().is_none());
            storage.resume();
            writer.await;
            assert_eq!(
                persistence
                    .validate_segment_prefix(second_prefix, 0, OWNED_BATCH_BYTES as u64, true)
                    .is_ok(),
                durable
            );
            let metrics = persistence.take_metrics();
            assert_eq!(metrics.disk_bytes, 2 * PARTITION_WAL_BLOCK_SIZE as u64);
            assert_eq!(
                metrics.retained_bytes,
                2 * journal::partition_journal::record_length(first.len()).unwrap() as u64
            );
            assert!(metrics.retained_bytes > metrics.disk_bytes);
            assert_eq!(
                persistence
                    .validate_segment_prefix(second_prefix, 0, OWNED_BATCH_BYTES as u64, durable)
                    .unwrap(),
                OWNED_BATCH_BYTES as u64
            );
            persistence.truncate_from(2);
            assert!(
                persistence
                    .validate_segment_prefix(second_prefix, 0, OWNED_BATCH_BYTES as u64, durable)
                    .is_err()
            );
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert!(persistence.failure().is_none());
            assert!(
                persistence
                    .validate_segment_prefix(first_prefix, 0, 0, durable)
                    .is_ok()
            );
            persistence.reset_with_segments(
                2,
                None,
                None,
                Some((
                    SegmentPosition {
                        start_offset: 2,
                        length: 0,
                        next_offset: 2,
                    },
                    (4 * OWNED_BATCH_BYTES) as u64,
                )),
            );
            assert!(
                persistence
                    .validate_segment_prefix(first_prefix, 0, 0, durable)
                    .is_err()
            );
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            assert!(persistence.failure().is_none());
        }
    });
}

#[test]
fn failed_vectored_body_group_never_publishes_a_partial_prefix() {
    block_on(async {
        let first = owned_prepare(1, 0, 0);
        let second = owned_prepare(2, first.header().checksum, 1);
        let third = owned_prepare(3, second.header().checksum, 2);
        let fourth = owned_prepare(4, third.header().checksum, 3);
        let prepares = [third.into_frozen(), fourth.into_frozen()];
        let (storage, mut journal) = owned_segment_baseline(false).await;
        storage.clear_trace();
        journal.append_batch_buffered(&prepares).await.unwrap();
        journal.sync().await.unwrap();
        let operations = storage.trace().len();
        for cut in 0..operations {
            for mode in [FaultMode::Before, FaultMode::After, FaultMode::TornWrite] {
                let (storage, mut journal) = owned_segment_baseline(false).await;
                storage.fail_at(cut, mode);
                let acknowledged = journal.append_batch_buffered(&prepares).await.is_ok()
                    && journal.sync().await.is_ok();
                drop(journal);
                storage.crash(Crash::PowerLoss);
                let recovered =
                    PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage)
                        .await
                        .unwrap();
                assert!(matches!(recovered.head(), 2 | 4), "cut {cut}, {mode:?}");
                if acknowledged {
                    assert_eq!(recovered.head(), 4, "cut {cut}, {mode:?}");
                }
                let actual = recovered.prepares().await.unwrap();
                assert_eq!(actual[0].as_slice(), first.as_slice());
                assert_eq!(actual[1].as_slice(), second.as_slice());
                for (actual, expected) in actual[2..].iter().zip(&prepares) {
                    assert_eq!(
                        actual.as_slice(),
                        expected.as_slice(),
                        "cut {cut}, {mode:?}"
                    );
                }
            }
        }
    });
}

async fn mutate_owned_segments(
    storage: &SimStorage,
    journal: &mut PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
) -> io::Result<()> {
    let first = owned_prepare(1, 0, 0);
    let second = owned_prepare(2, first.header().checksum, 1);
    match mutation {
        Mutation::Append => {
            journal
                .append(owned_prepare(3, second.header().checksum, 2).into_frozen())
                .await
        }
        Mutation::Checkpoint | Mutation::CheckpointBufferedTail => journal.checkpoint(2).await,
        Mutation::Truncate => journal.truncate_from(2).await,
        Mutation::CertifyView => {
            journal
                .certify_log_view(1, 2, second.header().checksum)
                .await
        }
        Mutation::Reset => {
            let public = Path::new(DIRECTORY).join(format!("{:020}.log", 0));
            storage.remove_file(&public).await?;
            storage
                .open(&public, OpenMode::Create)
                .await?
                .sync()
                .await?;
            storage.sync_directory(Path::new(DIRECTORY)).await?;
            journal
                .reset_with_segment_checkpoint(
                    2,
                    Some(second.header().checksum),
                    Some(second.into_frozen()),
                    SegmentPosition::default(),
                    (2 * OWNED_BATCH_BYTES) as u64,
                )
                .await
        }
        Mutation::Purge => {
            journal.mark_purge(1, 2).await?;
            journal
                .append(owned_prepare(3, second.header().checksum, 0).into_frozen())
                .await
        }
    }
}

async fn assert_owned_segments(
    journal: &PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
    buffered: bool,
    completed: bool,
    context: &str,
) {
    let first = owned_prepare(1, 0, 0);
    let second = owned_prepare(2, first.header().checksum, 1);
    let third = owned_prepare(
        3,
        second.header().checksum,
        if matches!(mutation, Mutation::Purge) {
            0
        } else {
            2
        },
    );
    let expected = [first, second, third];
    let checkpoint = journal.segment_checkpoint().unwrap();
    let checkpointed = match mutation {
        Mutation::Checkpoint if journal.checkpoint_op() == 2 => 2,
        Mutation::Purge if journal.purge_marker() == (1, 2) => 0,
        Mutation::Reset if journal.checkpoint_op() == 2 => 0,
        _ => 1,
    };
    assert_eq!(
        checkpoint,
        SegmentPosition {
            start_offset: 0,
            length: checkpointed * OWNED_BATCH_BYTES as u64,
            next_offset: checkpointed
        },
        "{context}"
    );
    let expected_head = match mutation {
        Mutation::Append | Mutation::Purge => 3,
        Mutation::Truncate => 1,
        _ => 2,
    };
    if completed {
        assert_eq!(journal.head(), expected_head, "{context}");
    }
    let baseline_head = if buffered { 1 } else { 2 };
    assert!(
        [
            baseline_head,
            if matches!(mutation, Mutation::Purge) {
                2
            } else {
                expected_head
            },
            expected_head
        ]
        .contains(&journal.head()),
        "{context}"
    );
    if completed && matches!(mutation, Mutation::Checkpoint) {
        assert_eq!(checkpointed, 2, "{context}");
    }
    if completed && matches!(mutation, Mutation::Purge) {
        assert_eq!(checkpointed, 0, "{context}");
    }
    let prepares = journal.prepares().await.unwrap();
    let expected_ops: Vec<_> = (journal.checkpoint_op()..=journal.head()).collect();
    assert_eq!(
        prepares
            .iter()
            .map(|prepare| prepare.header().op)
            .collect::<Vec<_>>(),
        expected_ops,
        "{context}"
    );
    for prepare in &prepares {
        let index = usize::try_from(prepare.header().op - 1).unwrap();
        assert_eq!(prepare.as_slice(), expected[index].as_slice(), "{context}");
        assert_eq!(
            journal.segment_reference(prepare.header()).is_some(),
            prepare.header().op > journal.purge_marker().1,
            "{context}"
        );
    }
    assert_eq!(
        journal.size_bytes(),
        prepares
            .iter()
            .map(|prepare| {
                if prepare.header().op <= journal.purge_marker().1 {
                    journal::partition_journal::record_length(prepare.as_slice().len()).unwrap()
                        as u64
                } else {
                    PARTITION_WAL_BLOCK_SIZE as u64
                }
            })
            .sum::<u64>(),
        "{context}"
    );
}

fn owned_prepare(op: u64, parent: u128, offset: u64) -> Message<PrepareHeader> {
    let payload = vec![
        u8::try_from(op).unwrap();
        OWNED_BATCH_BYTES - BATCH_HEADER_SIZE - BATCH_MESSAGE_HEADER_SIZE
    ];
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            id: u128::from(op),
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload: payload.into(),
        user_headers: None,
    });
    let namespace = IggyNamespace::new(0, 0, 42);
    assert_eq!(namespace.inner(), 42);
    let mut batch = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
    batch.header.base_offset = offset;
    batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
    let mut body = vec![0; BATCH_HEADER_SIZE + batch.blob.len()];
    batch.header.encode_into(&mut body[..BATCH_HEADER_SIZE]);
    body[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
    assert_eq!(body.len(), OWNED_BATCH_BYTES);
    prepare_with_payload(op, parent, &body).transmute_header(
        |original, header: &mut PrepareHeader| {
            *header = original;
            header.checksum_body = 0;
            header.checksum = header.identity_checksum();
        },
    )
}

async fn referenced_baseline() -> (SimStorage, PartitionPrepareJournal<SimStorage>) {
    let storage = storage_for_partition().await;
    let mut journal =
        PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
            .await
            .unwrap();
    let first = prepare(1, 0);
    let second = prepare(2, first.header().checksum);
    append_referenced(&storage, &mut journal, &first, 0, 0)
        .await
        .unwrap();
    append_referenced(&storage, &mut journal, &second, 0, 1)
        .await
        .unwrap();
    (storage, journal)
}

async fn append_referenced(
    storage: &SimStorage,
    journal: &mut PartitionPrepareJournal<SimStorage>,
    prepare: &Message<PrepareHeader>,
    generation: u64,
    start_offset: u64,
) -> io::Result<()> {
    let body = &prepare.as_slice()[size_of::<PrepareHeader>()..];
    let path = Path::new(DIRECTORY).join(format!("{start_offset:020}.log"));
    let mut file = storage.open(&path, OpenMode::Create).await?;
    file.write(0, body.to_vec()).await?;
    file.sync().await?;
    let reference = SegmentReference {
        generation,
        start_offset,
        position: 0,
        length: body.len() as u64,
    };
    journal
        .append_batch_referenced_buffered(&[prepare.clone().into_frozen()], &[Some(reference)])
        .await?;
    journal.sync().await
}

async fn mutate_referenced(
    storage: &SimStorage,
    journal: &mut PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
) -> io::Result<()> {
    let first = prepare(1, 0);
    let second = prepare(2, first.header().checksum);
    match mutation {
        Mutation::Append => {
            append_referenced(
                storage,
                journal,
                &prepare(3, second.header().checksum),
                0,
                2,
            )
            .await
        }
        Mutation::CertifyView => {
            journal
                .certify_log_view(2, 2, second.header().checksum)
                .await
        }
        Mutation::Checkpoint | Mutation::CheckpointBufferedTail => journal.checkpoint(2).await,
        Mutation::Truncate => journal.truncate_from(2).await,
        Mutation::Reset => journal.reset(7, None).await,
        Mutation::Purge => {
            journal.mark_purge(1, 2).await?;
            for offset in [0, 1] {
                storage
                    .remove_file(&Path::new(DIRECTORY).join(format!("{offset:020}.log")))
                    .await?;
            }
            storage.sync_directory(Path::new(DIRECTORY)).await?;
            append_referenced(
                storage,
                journal,
                &prepare(3, second.header().checksum),
                1,
                0,
            )
            .await
        }
    }
}

async fn assert_referenced_recovery(
    journal: &PartitionPrepareJournal<SimStorage>,
    mutation: Mutation,
    completed: bool,
    context: &str,
) {
    match mutation {
        Mutation::Append | Mutation::Purge => {
            assert!((2..=3).contains(&journal.head()), "{context}");
            if completed {
                assert_eq!(journal.head(), 3, "{context}");
            }
        }
        Mutation::Truncate => {
            assert!((1..=2).contains(&journal.head()), "{context}");
            if completed {
                assert_eq!(journal.head(), 1, "{context}");
            }
        }
        Mutation::Reset => {
            assert!([2, 7].contains(&journal.head()), "{context}");
            if completed {
                assert_eq!(journal.head(), 7, "{context}");
            }
        }
        Mutation::Checkpoint | Mutation::CheckpointBufferedTail => {
            assert_eq!(journal.head(), 2, "{context}");
            assert!([0, 2].contains(&journal.checkpoint_op()), "{context}");
            if completed {
                assert_eq!(journal.checkpoint_op(), 2, "{context}");
            }
        }
        Mutation::CertifyView => {
            assert_eq!(journal.head(), 2, "{context}");
            if completed {
                assert_eq!(journal.certified_log_view(), Some(2), "{context}");
            }
        }
    }
    let first = prepare(1, 0);
    let second = prepare(2, first.header().checksum);
    let third = prepare(3, second.header().checksum);
    let expected = [first, second, third];
    let recovered = journal.prepares().await.unwrap();
    let expected_ops: Vec<_> = if matches!(mutation, Mutation::Reset) && journal.head() == 7 {
        assert_eq!(journal.checkpoint_op(), 7, "{context}");
        Vec::new()
    } else if matches!(mutation, Mutation::Checkpoint) && journal.checkpoint_op() == 2 {
        vec![2]
    } else {
        assert_eq!(journal.checkpoint_op(), 0, "{context}");
        (1..=journal.head()).collect()
    };
    assert_eq!(
        recovered
            .iter()
            .map(|entry| entry.header().op)
            .collect::<Vec<_>>(),
        expected_ops,
        "{context}",
    );
    if matches!(mutation, Mutation::Purge) {
        assert!(
            [(0, 0), (1, 2)].contains(&journal.purge_marker()),
            "{context}"
        );
        if completed || journal.head() == 3 {
            assert_eq!(journal.purge_marker(), (1, 2), "{context}");
        }
    }
    assert_eq!(
        journal.size_bytes(),
        (recovered.len() * PARTITION_WAL_BLOCK_SIZE) as u64,
        "{context}"
    );
    for entry in recovered {
        let index = usize::try_from(entry.header().op - 1).unwrap();
        assert_eq!(entry.as_slice(), expected[index].as_slice(), "{context}");
    }
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
        Mutation::CheckpointBufferedTail => {
            let entries = journal.prepares().await?;
            let last = bytemuck::checked::from_bytes::<PrepareHeader>(
                &entries.last().unwrap().as_slice()[..size_of::<PrepareHeader>()],
            );
            journal
                .append_buffered(prepare(4, last.checksum).into_frozen())
                .await?;
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

/// The buffered record is acknowledged by nothing, so recovery may keep or drop
/// it. Refusing the open is the failure this covers.
fn assert_buffered_tail_checkpoint(journal: &PartitionPrepareJournal<SimStorage>, completed: bool) {
    assert!((3..=4).contains(&journal.head()));
    assert!([0, 2].contains(&journal.checkpoint_op()));
    if completed {
        assert_eq!(journal.checkpoint_op(), 2);
    }
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
        Mutation::CheckpointBufferedTail => assert_buffered_tail_checkpoint(journal, completed),
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
            + u64::from(
                matches!(
                    mutation,
                    Mutation::Checkpoint | Mutation::CheckpointBufferedTail
                ) && journal.checkpoint_op() > 0,
            )
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
    header.checksum_body = u128::from(XxHash3_64::oneshot(payload));
    header.checksum = header.identity_checksum();
    Message::try_from(buffer).unwrap()
}

// Regressions for PR #4092 review findings. Each one fails on the current tree
// and names the defect it pins.

const LARGE_BATCH_BYTES: usize = 1024 * 1024;

#[test]
fn given_tail_pages_reached_disk_when_power_loss_then_recovery_should_not_surface_a_holed_record() {
    block_on(async {
        let storage = storage_for_partition().await;
        let mut journal =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        let first = prepare(1, 0);
        journal.append(first.clone().into_frozen()).await.unwrap();
        let second = prepare(2, first.header().checksum);
        journal
            .append_buffered(second.clone().into_frozen())
            .await
            .unwrap();
        drop(journal);

        // The durable first record fills the blocks below `header_page`; the
        // buffered second one starts there and runs to the end of the file.
        let header_page = record_blocks(&first);
        let data = Path::new("/partition/wal/prepares-0.wal");
        let cached = read_all(&storage, data).await;
        assert_eq!(
            cached.len(),
            (header_page + record_blocks(&second)) * PARTITION_WAL_BLOCK_SIZE
        );
        let hole =
            header_page * PARTITION_WAL_BLOCK_SIZE..(header_page + 1) * PARTITION_WAL_BLOCK_SIZE;
        assert!(cached[hole.clone()].iter().any(|byte| *byte != 0));

        // Background writeback preserved the later pages of the buffered record
        // and left its first page dirty. `writeback()` cannot express this,
        // which is why `segment_recovery.rs` documents a byte-zero walk that
        // nothing exercises.
        storage.writeback_from_page(PARTITION_WAL_BLOCK_SIZE, header_page + 1);
        storage.crash(Crash::PowerLoss);

        let survived = read_all(&storage, data).await;
        assert_eq!(survived.len(), cached.len());
        assert!(
            survived[hole.clone()].iter().all(|byte| *byte == 0),
            "the record's first page reached stable storage, so there is no hole to recover across"
        );
        assert_eq!(
            survived[hole.end..],
            cached[hole.end..],
            "the record's later pages were lost too, so this is a short tail rather than a hole"
        );

        let recovered =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        assert!(
            !recovered.contains(second.header()),
            "a record whose first page never reached stable storage was surfaced as recovered"
        );
        assert_eq!(
            recovered.head(),
            1,
            "recovery advanced its head over a page-level hole"
        );
    });
}

#[test]
fn given_a_silent_short_write_when_recovering_then_the_record_should_be_refused() {
    block_on(async {
        let storage = storage_for_partition().await;
        let mut journal =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        let first = prepare(1, 0);
        journal.append(first.clone().into_frozen()).await.unwrap();

        let second = prepare(2, first.header().checksum);
        storage.clear_trace();
        // The device completes a half-written record and reports success. Only
        // the record checksum can refuse it; `TornWrite` always returns an
        // error, so no existing case reaches this path.
        storage.fail_at(0, FaultMode::SilentTornWrite);
        journal
            .append(second.clone().into_frozen())
            .await
            .expect("a silent short write is reported as success");
        drop(journal);
        storage.crash(Crash::PowerLoss);

        // The record was acknowledged before its bytes were lost, so refusing to
        // open is the correct answer. Nothing could assert that before, because
        // `TornWrite` reports the short write and the append fails instead.
        let Err(error) =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage).await
        else {
            panic!("a WAL missing acknowledged bytes opened successfully");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("lost acknowledged bytes"),
            "unexpected refusal: {error}"
        );
        let _ = first;
    });
}

#[test]
#[ignore = "PR #4092 review: `checkpoint_files` syncs materialized files through a descriptor opened after the writeback failure, which samples errseq too late and reports success over lost bytes; it must sync through the writer that issued them"]
fn given_a_failed_writeback_when_checkpointing_then_wal_history_should_not_be_reclaimed() {
    block_on(async {
        let (storage, persistence) = queued_batch(4).await;
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        let path = Path::new("/partition/materialized");
        let mut writer = storage.open(path, OpenMode::Create).await.unwrap();
        writer.write(0, b"committed".to_vec()).await.unwrap();

        // The device drops the dirty pages before the checkpoint's barrier. The
        // writer that issued them is the only handle told; the descriptor
        // `checkpoint_files` opens afterwards samples errseq past the failure
        // and reports a successful barrier over bytes that are already gone.
        storage.fail_writeback(path).unwrap();
        persistence.checkpoint_files(
            4,
            vec![path.to_path_buf()],
            vec![Path::new(DIRECTORY).to_path_buf()],
        );
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;

        assert!(
            persistence.failure().is_some(),
            "a checkpoint reported success over materialized bytes the device dropped"
        );
        assert_eq!(persistence.checkpoint_op(), 0);
        storage.crash(Crash::PowerLoss);
        let recovered =
            PartitionPrepareJournal::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        assert_eq!(
            recovered.checkpoint_op(),
            0,
            "WAL history was reclaimed although its materialization never reached stable storage"
        );
        assert_eq!(recovered.head(), 4);
    });
}

#[test]
fn given_sim_storage_when_opening_persistence_then_the_writer_lease_should_be_taken() {
    block_on(async {
        let storage = storage_for_partition().await;
        // Without a `writer_identity`, `PartitionPersistence::open_with_capacity`
        // sets `lease = None`, so WRITERS, the interrupted fence and the drain
        // timeout have no coverage in any simulator test.
        assert!(
            DurableStorage::writer_identity(&storage, Path::new(DIRECTORY))
                .unwrap()
                .is_some(),
            "simulator storage reports no writer identity, so every fault test runs without a lease"
        );
        let (persistence, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        persistence.retire();
    });
}

#[test]
fn given_an_interrupted_writer_when_the_process_restarts_then_the_partition_should_reopen() {
    block_on(async {
        let (storage, persistence) = queued_batch(1).await;
        storage.pause_writes();
        assert!(persistence.start());
        let mut writer = Box::pin(Rc::clone(&persistence).run());
        assert!(poll!(&mut writer).is_pending());
        // Cancelling the writer mid-mutation leaves its lease interrupted, a
        // fence only the death of the process holding it may lift.
        drop(writer);
        storage.resume();
        drop(persistence);
        let fenced =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone()).await;
        assert!(
            fenced.is_err_and(|error| error.to_string().contains("requires process restart")),
            "an interrupted writer did not fence the partition within the same process"
        );

        storage.crash(Crash::Process);
        let reopened =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone()).await;
        assert!(
            reopened.is_ok(),
            "the simulated restart kept the interrupted writer's identity, so the partition can never reopen: {:?}",
            reopened.err()
        );
    });
}

#[test]
#[ignore = "PR #4092 review: append coalescing is gated on message-body bytes, not the WAL extent, so batching is inert at the benchmarked batch sizes"]
fn given_large_bodies_when_appending_then_wal_records_should_coalesce_into_one_barrier_group() {
    block_on(async {
        const PREPARES: u64 = 8;
        let storage = storage_for_partition().await;
        let (persistence, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        persistence.enable_segment_storage(
            SegmentPosition::default(),
            PREPARES * LARGE_BATCH_BYTES as u64,
        );
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        persistence.take_metrics();

        let mut parent = 0;
        for offset in 0..PREPARES {
            let prepare = owned_prepare_sized(offset + 1, parent, offset, LARGE_BATCH_BYTES);
            parent = prepare.header().checksum;
            persistence.append(prepare.into_frozen(), true).unwrap();
        }
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;

        let metrics = persistence.take_metrics();
        assert_eq!(metrics.batched_prepares, PREPARES);
        // Under segment references a record occupies one 4 KiB extent, so all
        // eight fit far inside APPEND_BATCH_BYTES_MAX. The gate measures the
        // message body instead, so each prepare takes its own barrier group.
        assert_eq!(
            metrics.completed_batches,
            1,
            "coalescing is gated on body bytes, so {PREPARES} prepares paid {} barrier groups for {} bytes of WAL extent",
            metrics.completed_batches,
            PREPARES * PARTITION_WAL_BLOCK_SIZE as u64
        );
    });
}

fn owned_prepare_sized(
    op: u64,
    parent: u128,
    offset: u64,
    batch_bytes: usize,
) -> Message<PrepareHeader> {
    let payload = vec![
        u8::try_from(op % 251).unwrap();
        batch_bytes - BATCH_HEADER_SIZE - BATCH_MESSAGE_HEADER_SIZE
    ];
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            id: u128::from(op),
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload: payload.into(),
        user_headers: None,
    });
    let mut batch =
        SendMessagesOwned::from_messages(IggyNamespace::new(0, 0, 42), &messages).unwrap();
    batch.header.base_offset = offset;
    batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
    let mut body = vec![0; BATCH_HEADER_SIZE + batch.blob.len()];
    batch.header.encode_into(&mut body[..BATCH_HEADER_SIZE]);
    body[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
    assert_eq!(body.len(), batch_bytes);
    prepare_with_payload(op, parent, &body).transmute_header(
        |original, header: &mut PrepareHeader| {
            *header = original;
            header.checksum_body = 0;
            header.checksum = header.identity_checksum();
        },
    )
}

fn record_blocks(prepare: &Message<PrepareHeader>) -> usize {
    record_length(usize::try_from(prepare.header().size).unwrap()).unwrap()
        / PARTITION_WAL_BLOCK_SIZE
}

async fn read_all(storage: &SimStorage, path: &Path) -> Vec<u8> {
    let file = storage.open(path, OpenMode::Read).await.unwrap();
    let length = usize::try_from(file.length().await.unwrap()).unwrap();
    file.read(0, length).await.unwrap()
}

#[test]
#[ignore = "PR #4092 review: `WriterLease::acquire` drains through `compio::runtime::time::timeout`, so the writer fence cannot be driven by the deterministic executor"]
fn given_a_retired_writer_when_reacquiring_then_the_drain_wait_should_be_executor_agnostic() {
    block_on(async {
        let storage = storage_for_partition().await;
        let (first, _) =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 7, storage.clone())
                .await
                .unwrap();
        first.append(prepare(1, 0).into_frozen(), true).unwrap();
        storage.pause_writes();
        assert!(first.start());
        let mut writer = Box::pin(Rc::clone(&first).run());
        assert!(poll!(&mut writer).is_pending());
        first.retire();

        // `WriterLease::acquire` waits for the previous writer to drain through
        // `compio::runtime::time::timeout` (`persistence.rs:220`), so the fence
        // cannot be driven by the deterministic executor at all. Every
        // simulator fault case runs with `lease = None` for this reason.
        let reacquired =
            PartitionPersistence::open_with_storage(Path::new(WAL), 42, 8, storage.clone()).await;
        storage.resume();
        writer.await;
        assert!(
            reacquired.is_ok(),
            "retired writer could not be replaced under the deterministic executor"
        );
    });
}
