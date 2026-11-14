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

use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;

use super::*;
use crate::segment_chain::{
    ChainError, ChainRecord, HashSource, ObjectPrefix, chain_path, hash_segment_file, read_chain,
};

pub(super) const PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 1,
    topic_id: 1,
    partition_id: 0,
    created_revision: 0,
};

/// [`partition_with_sealed_segments`] whose chain opens once the first
/// `before_chain` of `records` are committed.
pub(super) async fn tiered_partition(
    records: &[Vec<u8>],
    before_chain: usize,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let batch_len = build_segment_record(partition.namespace(), 0).len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let (before, after) = records.split_at(before_chain);
    Box::pin(commit_records(&mut partition, 1, before)).await;
    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    Box::pin(commit_records(
        &mut partition,
        before.len() as u64 + 1,
        after,
    ))
    .await;
    (directory, partition)
}

/// Every sealed segment's record matches the segment, and its hash is the
/// hash of the segment's `.log` as the disk holds it.
async fn assert_records_hash_the_logs<B: MessageBus>(
    partition: &IggyPartition<B>,
    directory: &Path,
    recorded: &[u64],
) {
    for start_offset in recorded {
        let segment = partition
            .log
            .segments()
            .iter()
            .find(|segment| segment.start_offset == *start_offset)
            .unwrap();
        assert!(segment.sealed);
        let record = partition.sealed_segment_record(*start_offset).unwrap();
        assert_eq!(record.source, HashSource::CommitPath);
        assert_eq!(
            (record.end_offset, record.size),
            (segment.end_offset, segment.size.as_bytes_u64())
        );
        let log = directory.join(format!("{start_offset:020}.log"));
        let on_disk = hash_segment_file(&log.to_string_lossy(), record.size)
            .await
            .unwrap();
        assert_eq!(
            (record.hash, record.crc32c),
            (on_disk.blake3, on_disk.crc32c),
            "segment {start_offset}"
        );
    }
}

#[compio::test]
async fn given_a_tiered_partition_when_segments_seal_should_record_the_hash_of_each_log() {
    let (directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    assert_records_hash_the_logs(&partition, directory.path(), &[0, 2]).await;
    let active = partition.log.active_segment().start_offset;
    assert!(partition.sealed_segment_record(active).is_none());

    let chain = read_chain(&directory.path().to_string_lossy())
        .await
        .unwrap()
        .unwrap();
    let expected: Vec<_> = [0, 2]
        .into_iter()
        .map(|start| ChainRecord::Sealed(partition.sealed_segment_record(start).unwrap()))
        .collect();
    assert_eq!(chain.records, expected);
    assert!(!chain.torn_tail);
}

/// The production path: the WAL writes the bodies at prepare time, and the
/// materialization only hashes them.
#[compio::test]
async fn given_wal_written_bodies_when_a_segment_seals_should_record_the_hash_of_its_log() {
    const RECORDS_PER_SEGMENT: u64 = 2;
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let record_len = checksummed_segment_prepare(1, 0, 0, b"payload")
        .as_slice()
        .len()
        - size_of::<PrepareHeader>();
    let segment_size = IggyByteSize::from(RECORDS_PER_SEGMENT * record_len as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    let mut parent = 0;
    for offset in 0..=RECORDS_PER_SEGMENT {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, b"payload");
        parent = prepare.header().checksum;
        let writer = hold_wal_writer(&persistence);
        Box::pin(partition.on_replicate(prepare)).await;
        writer.await;
        Box::pin(commit_on_heartbeat(&mut partition, &config, op)).await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }
    assert!(partition.fatal().is_none());
    assert!(
        partition
            .log
            .messages_writers()
            .last()
            .is_some_and(Option::is_none),
        "the segment must have no writer, so the WAL wrote its bodies"
    );
    assert_records_hash_the_logs(&partition, &directory, &[0]).await;
}

/// A single replica keeps no transfer checksum, so the hash alone holds the
/// WAL-written bodies in the save it feeds on.
#[compio::test]
async fn given_a_single_replica_with_wal_written_bodies_when_a_segment_seals_should_record_its_hash()
 {
    const RECORDS_PER_SEGMENT: u64 = 2;
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(0, 1);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, &directory);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let record_len = checksummed_segment_prepare(1, 0, 0, b"payload")
        .as_slice()
        .len()
        - size_of::<PrepareHeader>();
    let segment_size = IggyByteSize::from(RECORDS_PER_SEGMENT * record_len as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    let mut parent = 0;
    for offset in 0..=RECORDS_PER_SEGMENT {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, b"payload");
        let header = *prepare.header();
        parent = header.checksum;
        persistence
            .append(prepare.clone().into_frozen(), false)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(header));
        });
        Box::pin(partition.append_repaired_send_messages(prepare))
            .await
            .unwrap();
        partition.consensus.advance_commit_max(op);
        Box::pin(partition.commit_journal(&config)).await;
        Box::pin(settle_partition_io(&mut partition, &config)).await;
    }
    assert!(partition.fatal().is_none());
    assert!(
        partition.segment_checksum_cache.borrow().is_empty(),
        "a singleton keeps no transfer checksum"
    );
    assert_records_hash_the_logs(&partition, &directory, &[0]).await;
}

/// A segment that held bytes before the chain opened cannot be hashed on the
/// commit path, so it is hashed from disk once it seals, while the next one
/// is hashed as it is written.
#[compio::test]
async fn given_a_chain_opened_mid_segment_when_that_segment_seals_should_hash_it_from_disk() {
    let (directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 1)).await;
    let first = partition.sealed_segment_record(0).unwrap();
    assert_eq!(first.source, HashSource::Disk);
    let log = directory.path().join(format!("{:020}.log", 0));
    let on_disk = hash_segment_file(&log.to_string_lossy(), first.size)
        .await
        .unwrap();
    assert_eq!((first.hash, first.crc32c), (on_disk.blake3, on_disk.crc32c));
    assert_records_hash_the_logs(&partition, directory.path(), &[2]).await;
}

#[compio::test]
async fn given_a_chain_on_disk_when_a_partition_reopens_it_should_know_the_sealed_hashes() {
    let (directory, partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let first = partition.sealed_segment_record(0).unwrap();
    drop(partition);
    let path = directory.path().to_string_lossy().into_owned();

    let mut reopened = Box::new(test_partition_with_replicas(3));
    reopened.set_partition_dir(path.clone());
    Box::pin(reopened.open_segment_chain(PREFIX)).await.unwrap();
    assert_eq!(reopened.object_prefix(), Some(PREFIX));
    assert_eq!(reopened.sealed_segment_record(0), Some(first));

    let mut other = Box::new(test_partition_with_replicas(3));
    other.set_partition_dir(path);
    let foreign = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    assert!(matches!(
        Box::pin(other.open_segment_chain(foreign)).await,
        Err(ChainError::Prefix { recorded }) if recorded == PREFIX
    ));
}

/// A creation whose directory sync failed leaves a chain whose name may not
/// survive a power loss, so opening it again has to sync the directory too.
#[compio::test]
async fn given_a_chain_whose_directory_sync_failed_when_reopened_should_sync_before_accepting_it() {
    const NO_DIRECTORY_READ: u32 = 0o300;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_string_lossy().into_owned();
    let mut partition = Box::new(test_partition_with_replicas(3));
    partition.set_partition_dir(path.clone());
    let mode = std::fs::metadata(directory.path())
        .unwrap()
        .permissions()
        .mode();
    std::fs::set_permissions(directory.path(), Permissions::from_mode(NO_DIRECTORY_READ)).unwrap();
    let directory_read_denied = std::fs::File::open(directory.path()).is_err();

    let created = Box::pin(partition.open_segment_chain(PREFIX)).await;
    let renamed = std::fs::metadata(chain_path(&path)).is_ok();
    let retried = Box::pin(partition.open_segment_chain(PREFIX)).await;
    let accepted = partition.object_prefix();
    std::fs::set_permissions(directory.path(), Permissions::from_mode(mode)).unwrap();
    // Root ignores the mode, so the directory sync cannot be made to fail.
    if !directory_read_denied {
        return;
    }

    assert!(matches!(created, Err(ChainError::Io(_))));
    assert!(renamed, "the creation must fail after its rename");
    assert!(matches!(retried, Err(ChainError::Io(_))));
    assert_eq!(accepted, None);

    Box::pin(partition.open_segment_chain(PREFIX))
        .await
        .unwrap();
    assert_eq!(partition.object_prefix(), Some(PREFIX));
}
