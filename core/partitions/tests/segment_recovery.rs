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

#![allow(clippy::future_not_send)]

use bytes::Bytes;
use iggy_common::{IggyByteSize, IggyError, PartitionStats};
use partitions::segment_anchor::SegmentAnchor;
use partitions::state_transfer::STAGING_SUFFIX;
use partitions::{
    PartitionPathLayout, PartitionRecoveryError, PartitionRecoveryRefusal, PartitionsConfig,
    RecoveredSegment, load_persisted_segments, load_persisted_segments_with_checkpoint,
};
use server_common::send_messages::{
    COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    calculate_batch_checksum,
};
use server_common::sharding::IggyNamespace;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use tempfile::{TempDir, tempdir};

const IGGY_INDEX_SIZE: usize = std::mem::size_of::<u64>() * 3;

const STREAM_ID: usize = 1;
const TOPIC_ID: usize = 1;
const PARTITION_ID: usize = 1;
const SEGMENT_MAX_SIZE: u64 = 16 * 1024 * 1024;
const FIXTURE_TIMESTAMP: u64 = 1_700_000_000_000_000;
// Longer than one batch header and nonzero in the header's reserved
// region, so no prefix of it decodes as a batch.
const GARBAGE: [u8; 384] = [0xAB; 384];

/// Same relative shape as the default server config, rooted at the tempdir.
fn test_config(tmp: &TempDir) -> PartitionsConfig {
    PartitionsConfig {
        messages_required_to_save: iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
        size_of_messages_required_to_save: IggyByteSize::from(
            iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
        ),
        validate_checksum: true,
        segment_size: IggyByteSize::from(SEGMENT_MAX_SIZE),
        preallocate_segments: iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
        key_context: Arc::default(),
        path_layout: PartitionPathLayout {
            streams_root: tmp.path().join("streams").to_string_lossy().into_owned(),
        },
    }
}

fn prepare_partition_dir(config: &PartitionsConfig) -> String {
    let partition_path = config.get_partition_path(STREAM_ID, TOPIC_ID, PARTITION_ID);
    fs::create_dir_all(&partition_path).expect("create partition dir");
    partition_path
}

// Batch header wire offsets the zero-padded fixtures plant values at.
const HEADER_BATCH_LENGTH_OFFSET: usize = 32;
const HEADER_MESSAGE_COUNT_OFFSET: usize = 48;

/// One fixed-width zero-padded record of the shape foreign storage
/// formats emit: a monotone u64 where the batch header keeps
/// `batch_length`, a nonzero u32 where it keeps `message_count`, zeros
/// everywhere else -- so its header decodes without any of it being a
/// batch.
fn zero_padded_record(claimed_batch_length: u64, sequence: u32) -> Vec<u8> {
    let mut record = vec![0u8; COMMAND_HEADER_SIZE];
    record[HEADER_BATCH_LENGTH_OFFSET..HEADER_BATCH_LENGTH_OFFSET + 8]
        .copy_from_slice(&claimed_batch_length.to_le_bytes());
    record[HEADER_MESSAGE_COUNT_OFFSET..HEADER_MESSAGE_COUNT_OFFSET + 4]
        .copy_from_slice(&sequence.to_le_bytes());
    record
}

/// One valid on-disk batch record: real message frames with their
/// per-message checksums, and the server-owned header fields stamped the
/// way persistence stamps them.
fn encoded_batch(base_offset: u64, message_count: usize) -> Vec<u8> {
    encoded_batch_with_payload(
        base_offset,
        message_count,
        &Bytes::from_static(b"segment-recovery-fixture"),
    )
}

fn encoded_batch_with_payload(base_offset: u64, message_count: usize, payload: &Bytes) -> Vec<u8> {
    encoded_batch_stamped(base_offset, message_count, payload, PARTITION_ID as u64)
}

/// Like [`encoded_batch`], but stamped with an arbitrary `partition_id`
/// and re-checksummed, so a foreign record verifies while contradicting
/// the identity of the partition being recovered.
fn encoded_foreign_batch(base_offset: u64, message_count: usize, partition_id: u64) -> Vec<u8> {
    encoded_batch_stamped(
        base_offset,
        message_count,
        &Bytes::from_static(b"segment-recovery-fixture"),
        partition_id,
    )
}

fn encoded_batch_stamped(
    base_offset: u64,
    message_count: usize,
    payload: &Bytes,
    partition_id: u64,
) -> Vec<u8> {
    let mut messages = IggyMessages::with_capacity(message_count);
    for _ in 0..message_count {
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                origin_timestamp: FIXTURE_TIMESTAMP,
                ..IggyMessageHeader::default()
            },
            payload: payload.clone(),
            user_headers: None,
        });
    }
    let namespace = IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID);
    let SendMessagesOwned { mut header, blob } =
        SendMessagesOwned::from_messages(namespace, &messages).expect("encode fixture batch");
    header.partition_id = partition_id;
    header.base_offset = base_offset;
    header.base_timestamp = FIXTURE_TIMESTAMP;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);
    let mut record = vec![0u8; header.total_size()];
    header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
    record[COMMAND_HEADER_SIZE..].copy_from_slice(&blob);
    record
}

/// One sparse index entry, mirroring the `IggyIndexWriter` layout the
/// recovery reader expects.
fn index_entry(offset: u64, position: u64) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&offset.to_le_bytes());
    entry.extend_from_slice(&FIXTURE_TIMESTAMP.to_le_bytes());
    entry.extend_from_slice(&position.to_le_bytes());
    entry
}

/// Writes a segment's `.log` and `.index` fixtures and returns their
/// paths as `(messages_path, index_path)`.
fn write_segment(
    config: &PartitionsConfig,
    start_offset: u64,
    log: &[u8],
    index: &[u8],
) -> (String, String) {
    let messages_path = config.get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
    let index_path = config.get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
    fs::write(&messages_path, log).expect("write log fixture");
    fs::write(&index_path, index).expect("write index fixture");
    (messages_path, index_path)
}

/// Path of the anchor beside the segment planted at `start_offset`.
fn anchor_fixture_path(config: &PartitionsConfig, start_offset: u64) -> String {
    partitions::segment_anchor::anchor_path(
        &config.get_partition_path(STREAM_ID, TOPIC_ID, PARTITION_ID),
        start_offset,
    )
}

/// Write the anchor a plant at `planted_start` leaves behind, naming the tail
/// it sealed.
fn write_anchor_fixture(
    config: &PartitionsConfig,
    planted_start: u64,
    sealed_start: u64,
    sealed_end: u64,
) {
    let anchor = SegmentAnchor {
        planted_start,
        sealed_start,
        sealed_end,
    };
    fs::write(
        anchor_fixture_path(config, planted_start),
        anchor.to_bytes(),
    )
    .expect("write anchor fixture");
}

/// Recover expecting a refusal, with `context` naming what should have failed.
async fn refusal(config: &PartitionsConfig, context: &str) -> PartitionRecoveryError {
    match recover(config).await {
        Ok(recovered) => panic!("{context}, got {} segments", recovered.len()),
        Err(error) => error,
    }
}

fn len_of(path: &str) -> u64 {
    fs::metadata(path).expect("stat fixture file").len()
}

fn bytes_of(path: &str) -> Vec<u8> {
    fs::read(path).expect("read fixture file")
}

/// Bytes of a segment file after recovery moved it into the first fence
/// directory, keyed by the name it had at its original path.
fn fenced_bytes(partition_path: &str, original_path: &str) -> Vec<u8> {
    let fenced_dir = format!("{partition_path}.fenced.0");
    let name = Path::new(original_path)
        .file_name()
        .expect("fixture file name");
    fs::read(Path::new(&fenced_dir).join(name)).expect("read fenced fixture file")
}

async fn recover(
    config: &PartitionsConfig,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    recover_with(config, false).await
}

async fn recover_under_fsync(
    config: &PartitionsConfig,
    durable_segments: bool,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    recover_with(config, durable_segments).await
}

async fn recover_with(
    config: &PartitionsConfig,
    durable_segments: bool,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    load_persisted_segments(
        config,
        IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID),
        IggyByteSize::from(SEGMENT_MAX_SIZE),
        durable_segments,
        &PartitionStats::default(),
    )
    .await
}

fn read_u64_le(bytes: &[u8], at: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(raw)
}

#[test]
fn given_torn_log_tail_when_recovering_should_truncate_files_to_walked_bounds() {
    let runtime = server_common::executor::create_shard_executor().unwrap();
    runtime.block_on(async {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 3);
        let valid_len = log.len() as u64;
        log.extend_from_slice(&GARBAGE);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config).await.expect("recover torn-tail segment");

        assert_eq!(recovered.len(), 1);
        let segment = &recovered[0].segment;
        assert_eq!(segment.end_offset, 2);
        assert_eq!(segment.size, IggyByteSize::from(valid_len));
        assert_eq!(segment.current_position, valid_len);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "torn tail bytes must be gone from disk"
        );
        assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
    });
}

#[compio::test]
async fn given_torn_index_tail_when_recovering_should_floor_index_to_whole_entries() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let log = encoded_batch(0, 2);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&GARBAGE[..10]);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config).await.expect("recover torn-index segment");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        len_of(&index_path),
        IGGY_INDEX_SIZE as u64,
        "partial index entry must be gone from disk"
    );
    assert_eq!(len_of(&messages_path), log.len() as u64);
}

#[compio::test]
async fn given_no_recoverable_bytes_when_recovering_should_fence_files_and_seed_empty() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    let partition_path = prepare_partition_dir(&config);
    let (messages_path, index_path) = write_segment(&config, 0, &GARBAGE, &GARBAGE[..10]);

    let recovered = recover(&config).await.expect("recover segment as empty");

    assert_eq!(recovered.len(), 1);
    let segment = &recovered[0].segment;
    assert_eq!(segment.size, IggyByteSize::default());
    assert_eq!(segment.end_offset, 0);
    assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
    assert_eq!(len_of(&index_path), 0, "the served index must be empty");
    assert_eq!(
        fenced_bytes(&partition_path, &messages_path),
        GARBAGE,
        "the unreadable log bytes must survive in the fence directory"
    );
    assert_eq!(
        fenced_bytes(&partition_path, &index_path),
        &GARBAGE[..10],
        "the unreadable index bytes must survive in the fence directory"
    );
}

#[compio::test]
async fn given_recovered_partition_when_recovering_again_should_change_nothing() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 3);
    log.extend_from_slice(&GARBAGE);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let first = recover(&config).await.expect("first recovery");
    let sizes_after_first = (len_of(&messages_path), len_of(&index_path));
    let bounds_after_first = (first[0].segment.end_offset, first[0].segment.size);
    drop(first);

    let second = recover(&config).await.expect("second recovery");

    assert_eq!(
        (len_of(&messages_path), len_of(&index_path)),
        sizes_after_first,
        "a second recovery must not move the files"
    );
    assert_eq!(
        (second[0].segment.end_offset, second[0].segment.size),
        bounds_after_first
    );
}

#[compio::test]
async fn given_unopenable_index_when_recovering_should_fail_stop_without_truncating() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 1);
    log.extend_from_slice(&GARBAGE);
    let messages_path = config.get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    let index_path = config.get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    fs::write(&messages_path, &log).expect("write log fixture");
    // Self-referential symlink: every open or stat that follows it fails
    // with ELOOP, root or not (unlike permission bits, which root
    // bypasses).
    symlink(&index_path, &index_path).expect("create self-referential index symlink");

    // Recovery stats the index before opening it (a missing one routes to
    // the index-less walk), so an ELOOP surfaces from the stat.
    let error = recover(&config)
        .await
        .err()
        .expect("an unstattable index must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Iggy(IggyError::CannotReadFileMetadata)
        ),
        "expected CannotReadFileMetadata, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "fail-stop must leave the log untouched"
    );
}

#[compio::test]
async fn given_unopenable_log_when_recovering_index_less_should_fail_stop_without_truncating() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let messages_path = config.get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    let index_path = config.get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    fs::write(&index_path, &GARBAGE[..10]).expect("write torn index fixture");
    // See the index variant above; the log stem is still collected by the
    // directory sweep, so recovery reaches the stat and must fail stop
    // there instead of recovering the segment as empty.
    symlink(&messages_path, &messages_path).expect("create self-referential log symlink");

    let error = recover(&config)
        .await
        .err()
        .expect("an unstattable log must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Iggy(IggyError::CannotReadFileMetadata)
        ),
        "expected CannotReadFileMetadata, got {error:?}"
    );
    assert_eq!(
        len_of(&index_path),
        10,
        "fail-stop must leave the torn index untouched"
    );
    assert!(
        fs::symlink_metadata(&messages_path)
            .expect("lstat log symlink")
            .file_type()
            .is_symlink(),
        "fail-stop must leave the log symlink in place"
    );
}

#[compio::test]
async fn given_clean_segment_when_recovering_should_leave_files_untouched() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let log = encoded_batch(0, 4);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let recovered = recover(&config).await.expect("recover clean segment");

    assert_eq!(recovered.len(), 1);
    let segment = &recovered[0].segment;
    assert_eq!(segment.end_offset, 3);
    assert!(!segment.sealed, "the tail segment must accept writes");
    assert_eq!(len_of(&messages_path), log.len() as u64);
    assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
}

#[compio::test]
async fn given_torn_mid_chain_segment_when_recovering_should_truncate_it_too() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Sealed segment holding offsets 0..=2 plus a garbage tail, then the
    // tail segment holding offsets 3..=4.
    let mut sealed_log = encoded_batch(0, 3);
    let sealed_valid_len = sealed_log.len() as u64;
    sealed_log.extend_from_slice(&GARBAGE);
    let (sealed_messages_path, _sealed_index_path) =
        write_segment(&config, 0, &sealed_log, &index_entry(0, 0));
    let tail_log = encoded_batch(3, 2);
    let (tail_messages_path, _tail_index_path) =
        write_segment(&config, 3, &tail_log, &index_entry(3, 0));

    let recovered = recover(&config).await.expect("recover two-segment chain");

    assert_eq!(recovered.len(), 2);
    assert!(recovered[0].segment.sealed);
    assert_eq!(recovered[0].segment.end_offset, 2);
    assert!(!recovered[1].segment.sealed);
    assert_eq!(recovered[1].segment.end_offset, 4);
    assert_eq!(
        len_of(&sealed_messages_path),
        sealed_valid_len,
        "a mid-chain torn tail must be truncated too"
    );
    assert_eq!(len_of(&tail_messages_path), tail_log.len() as u64);
}

#[compio::test]
async fn given_sealed_segments_when_recovering_should_keep_write_cursors_on_the_tail_only() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    for start_offset in [0, 2, 4] {
        write_segment(
            &config,
            start_offset,
            &encoded_batch(start_offset, 2),
            &index_entry(start_offset, 0),
        );
    }

    let recovered = recover(&config).await.expect("recover three-segment chain");

    assert_eq!(recovered.len(), 3);
    for sealed in &recovered[..2] {
        assert!(sealed.segment.sealed);
        assert!(
            sealed.storage.messages_size.is_none() && sealed.storage.index_size.is_none(),
            "a sealed segment must not keep write cursors"
        );
        assert!(sealed.storage.messages_reader.is_some());
    }
    let tail = &recovered[2].storage;
    assert!(
        tail.messages_size.is_some() && tail.index_size.is_some(),
        "the tail segment must keep its write cursors"
    );
}

#[compio::test]
async fn given_valid_batch_after_damage_when_recovering_should_refuse_and_preserve_files() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 3);
    log.extend_from_slice(&GARBAGE);
    log.extend_from_slice(&encoded_batch(3, 1));
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a surviving batch past damage must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                ..
            }
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "a refusal must leave the log byte-identical"
    );
    assert_eq!(
        bytes_of(&index_path),
        index,
        "a refusal must leave the index byte-identical"
    );
}

#[compio::test]
async fn given_garbage_head_with_valid_batch_later_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = GARBAGE.to_vec();
    log.extend_from_slice(&encoded_batch(5, 1));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("a lost head with valid batches later must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                ..
            }
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), &GARBAGE[..10]);
}

#[compio::test]
async fn given_offset_gap_after_valid_batches_when_recovering_index_less_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 2);
    log.extend_from_slice(&encoded_batch(5, 1));
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("an offset gap inside one segment must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                    expected_offset: 2,
                    found_offset: 5,
                    ..
                },
                ..
            }
        ),
        "expected an offset-discontinuity refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_multi_batch_torn_tail_when_recovering_index_less_should_truncate_at_break() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 2);
    log.extend_from_slice(&encoded_batch(2, 2));
    let valid_len = log.len() as u64;
    log.extend_from_slice(&GARBAGE);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let recovered = recover(&config).await.expect("recover multi-batch tail");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 3);
    assert_eq!(
        len_of(&messages_path),
        valid_len,
        "the walk must keep every whole batch before the tear"
    );
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the index must be rebuilt from the walked batches"
    );
    assert!(
        fs::metadata(format!("{index_path}{STAGING_SUFFIX}")).is_err(),
        "the staged rebuild must be renamed into place, not copied"
    );

    let sizes_after_first = (len_of(&messages_path), len_of(&index_path));
    drop(recovered);
    let second = recover(&config).await.expect("second recovery");
    assert_eq!(second[0].segment.end_offset, 3);
    assert_eq!(
        (len_of(&messages_path), len_of(&index_path)),
        sizes_after_first,
        "recovering over a rebuilt index must be a no-op"
    );
}

#[compio::test]
async fn given_holed_chain_when_recovering_should_leave_every_segment_untouched() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // First segment carries a torn tail that WOULD truncate; the hole to
    // the next segment must refuse the chain before that happens.
    let mut first_log = encoded_batch(0, 3);
    first_log.extend_from_slice(&GARBAGE);
    let first_index = index_entry(0, 0);
    let (first_messages_path, first_index_path) =
        write_segment(&config, 0, &first_log, &first_index);
    let next_log = encoded_batch(10, 1);
    let next_index = index_entry(10, 0);
    let (next_messages_path, next_index_path) = write_segment(&config, 10, &next_log, &next_index);

    let error = recover(&config)
        .await
        .err()
        .expect("a holed chain must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&first_messages_path),
        first_log,
        "a refused chain must leave even truncation candidates byte-identical"
    );
    assert_eq!(bytes_of(&first_index_path), first_index);
    assert_eq!(bytes_of(&next_messages_path), next_log);
    assert_eq!(bytes_of(&next_index_path), next_index);
}

/// Valid, checksum-clean anchor bytes COPIED beside a later segment must not
/// authorise the wider gap they now sit in front of. The record names its own
/// plant, and the guard matches that against the file it was found beside, so
/// an operator's `cp` buys nothing.
#[compio::test]
async fn given_an_anchor_copied_beside_a_later_segment_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 500, &encoded_batch(500, 1), &index_entry(500, 0));
    // The anchor a legitimate plant at 10 would have left, moved beside the
    // segment at 500 without touching a byte of it.
    let stolen = fs::read({
        write_anchor_fixture(&config, 10, 0, 2);
        anchor_fixture_path(&config, 10)
    })
    .expect("read the legitimate anchor");
    fs::remove_file(anchor_fixture_path(&config, 10)).expect("unlink the original");
    fs::write(anchor_fixture_path(&config, 500), &stolen).expect("copy it beside 500");

    let error = refusal(&config, "a copied anchor must not cover a wider gap").await;
    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
}

/// The shape the boot re-anchor leaves: a sealed tail, then the next segment
/// planted above it, with the anchor beside the plant naming the tail.
#[compio::test]
async fn given_an_anchored_gap_when_recovering_should_accept_the_chain() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));
    write_anchor_fixture(&config, 10, 0, 2);

    let recovered = recover(&config)
        .await
        .expect("a gap the plant recorded is the re-anchor's, not damage");

    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0].segment.start_offset, 0);
    assert_eq!(recovered[1].segment.start_offset, 10);
}

/// The regression this record exists to close: with no anchor the gap is a
/// segment that went missing, and admitting it serves a holed log silently.
#[compio::test]
async fn given_an_unanchored_gap_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));

    let error = refusal(&config, "a gap no plant recorded must refuse recovery").await;

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
}

/// An anchor names WHICH segment it sealed, so one left behind by an earlier
/// chain cannot legitimise a gap it never saw.
#[compio::test]
async fn given_an_anchor_naming_another_segment_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));
    // Right shape, wrong predecessor: this anchor describes a tail ending at
    // 5, and the chain's tail ends at 2.
    write_anchor_fixture(&config, 10, 0, 5);

    let error = refusal(&config, "an anchor for a different tail must not cover it").await;
    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
}

/// A corrupt anchor proves nothing, so the gap it would have covered stays
/// damage rather than becoming legitimate by default.
#[compio::test]
async fn given_a_corrupt_anchor_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));
    let path = anchor_fixture_path(&config, 10);
    let mut bytes = SegmentAnchor {
        planted_start: 10,
        sealed_start: 0,
        sealed_end: 2,
    }
    .to_bytes();
    bytes[8] ^= 1;
    fs::write(&path, bytes).expect("write corrupt anchor");

    let error = refusal(&config, "a corrupt anchor must not cover a gap").await;
    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
}

/// Every crash cycle leaves one more re-anchor gap, and the guard runs on the
/// NEXT boot -- before the re-anchor -- so by the second cycle the earlier gap
/// is no longer the last pair. A rule keyed on the last pair alone would
/// refuse this, and the solo arm tombstones a chain with bytes in it, taking a
/// healthy partition dark from the second crash onward.
#[compio::test]
async fn given_gaps_from_several_crash_cycles_when_recovering_should_accept_the_chain() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 2), &index_entry(10, 0));
    write_segment(&config, 20, &encoded_batch(20, 1), &index_entry(20, 0));
    write_anchor_fixture(&config, 10, 0, 2);
    write_anchor_fixture(&config, 20, 10, 11);

    let recovered = recover(&config)
        .await
        .expect("anchored gaps accumulate one per crash, not one total");

    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0].segment.start_offset, 0);
    assert_eq!(recovered[1].segment.start_offset, 10);
    assert_eq!(recovered[2].segment.start_offset, 20);
}

/// One lost segment in the middle of a chain whose OTHER gaps are all
/// anchored. The anchors say nothing about this pair, so it must still refuse
/// -- the shape a single monotone ceiling could not separate from a plant.
#[compio::test]
async fn given_a_lost_segment_among_anchored_gaps_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 2), &index_entry(10, 0));
    // 20 was an ordinary rotation off 12, then went missing; 30 is a plant.
    write_segment(&config, 30, &encoded_batch(30, 1), &index_entry(30, 0));
    write_anchor_fixture(&config, 10, 0, 2);

    let error = refusal(&config, "a lost middle segment must refuse recovery").await;
    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole {
                    previous_start: 10,
                    previous_end: 11,
                    next_start: 30,
                    ..
                },
                ..
            }
        ),
        "expected a hole refusal naming the lost pair, got {error:?}"
    );
}

#[compio::test]
async fn given_a_later_checkpoint_when_recovering_should_preserve_sealed_index_evidence() {
    for damaged in [false, true] {
        let directory = tempdir().unwrap();
        let config = test_config(&directory);
        prepare_partition_dir(&config);
        let first = encoded_batch(0, 1);
        let mut sealed = first.clone();
        sealed.extend(encoded_batch(1, 1));
        let mut index = index_entry(0, 0);
        index.extend(index_entry(1, first.len() as u64));
        if damaged {
            index.extend(index_entry(2, sealed.len() as u64 + 8));
            index.extend(index_entry(3, sealed.len() as u64 + 128));
        }
        let (messages_path, index_path) = write_segment(&config, 0, &sealed, &index);
        let tail = encoded_batch(2, 1);
        write_segment(&config, 2, &tail, &index_entry(2, 0));
        let checkpoint = journal::partition_journal::SegmentPosition {
            start_offset: 2,
            length: tail.len() as u64,
            next_offset: 3,
        };
        let result = load_persisted_segments_with_checkpoint(
            &config,
            IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID),
            IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
            true,
            &PartitionStats::default(),
            Some(checkpoint),
        )
        .await;
        if damaged {
            assert!(
                matches!(
                    result,
                    Err(PartitionRecoveryError::Refused {
                        reason: PartitionRecoveryRefusal::FsyncedLogLoss { .. },
                        ..
                    })
                ),
                "sealed index evidence must not be erased by an unrelated checkpoint"
            );
        } else {
            let recovered = result.unwrap();
            assert_eq!(recovered.len(), 2);
            assert_eq!(recovered[0].segment.end_offset, 1);
            assert!(recovered[0].storage.messages_size.is_none());
        }
        assert_eq!(bytes_of(&messages_path), sealed);
        assert_eq!(bytes_of(&index_path), index);
    }
}

#[compio::test]
async fn given_checkpointed_prefix_when_recovering_should_hide_tails_and_refuse_overlaps() {
    let tmp = tempdir().unwrap();
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let committed = encoded_batch(0, 3);
    let mut physical = committed.clone();
    physical.extend(encoded_batch(3, 3));
    let (messages_path, _) = write_segment(&config, 0, &physical, &index_entry(0, 0));
    let (uncommitted_path, _) = write_segment(&config, 6, &encoded_batch(6, 3), &[]);
    let checkpoint = journal::partition_journal::SegmentPosition {
        start_offset: 0,
        length: committed.len() as u64,
        next_offset: 3,
    };
    let namespace = IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID);
    let recovered = load_persisted_segments_with_checkpoint(
        &config,
        namespace,
        IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
        true,
        &PartitionStats::default(),
        Some(checkpoint),
    )
    .await
    .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 2);
    assert_eq!(recovered[0].segment.size.as_bytes_u64(), checkpoint.length);
    assert_eq!(bytes_of(&messages_path), physical);
    assert!(Path::new(&uncommitted_path).exists());
    drop(recovered);
    let (overlap, _) = write_segment(&config, 1, &[], &[]);
    let result = load_persisted_segments_with_checkpoint(
        &config,
        namespace,
        IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
        true,
        &PartitionStats::default(),
        Some(checkpoint),
    )
    .await;
    assert!(matches!(
        result,
        Err(PartitionRecoveryError::Refused {
            reason: PartitionRecoveryRefusal::Hole { .. },
            ..
        })
    ));
    assert!(Path::new(&overlap).exists());
    assert_eq!(bytes_of(&messages_path), physical);
}

/// No re-anchor plants a segment whose range a predecessor already covers, so
/// an anchor must not launder an overlap either. Reachable because each end
/// offset is walked from its own file with nothing clamping it against the
/// next start.
#[compio::test]
async fn given_overlapping_segments_when_recovering_should_refuse_even_when_anchored() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // `0.log` walks to 0..=15 while `10.log` claims 10 onward: the pair runs
    // backwards, which no legitimate chain does.
    write_segment(&config, 0, &encoded_batch(0, 16), &index_entry(0, 0));
    write_segment(&config, 10, &encoded_batch(10, 3), &index_entry(10, 0));
    write_anchor_fixture(&config, 10, 0, 15);

    let error = refusal(&config, "an overlap must refuse however it is recorded").await;
    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
}

/// An anchor whose segment is gone can only mislead a later guard, so the boot
/// sweep collects it the way it collects an orphaned index.
#[compio::test]
async fn given_an_anchor_with_no_segment_when_recovering_should_sweep_it() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    write_segment(&config, 0, &encoded_batch(0, 3), &index_entry(0, 0));
    // The window a crash between the anchor write and the plant leaves.
    write_anchor_fixture(&config, 10, 0, 2);
    let orphan = anchor_fixture_path(&config, 10);

    let recovered = recover(&config).await.expect("recover the intact chain");

    assert_eq!(recovered.len(), 1);
    assert!(
        !Path::new(&orphan).exists(),
        "an anchor naming a segment that does not exist must be swept"
    );
}

#[compio::test]
async fn given_non_monotone_index_entries_when_recovering_should_rebuild_the_index_from_the_log() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let batch2 = encoded_batch(2, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    log.extend_from_slice(&batch2);
    let last_position = (batch0.len() + batch1.len()) as u64;
    // Interior garbage entry: ascending against its predecessor, so only
    // the offset regression to the (valid) last entry exposes it. That
    // last entry alone would anchor a clean walk and keep the garbage,
    // so the interior check must run before the walk trusts the tail.
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(50, 100));
    index.extend_from_slice(&index_entry(2, last_position));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("a self-contradicting index over a whole log must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 2);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the index must be rebuilt from the walked batches"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_ascending_index_entry_pointing_at_another_batch_when_recovering_should_rebuild_the_index()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let batch2 = encoded_batch(2, 1);
    let batch3 = encoded_batch(3, 1);
    let batch2_position = (batch0.len() + batch1.len()) as u64;
    let batch3_position = batch2_position + batch2.len() as u64;
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    log.extend_from_slice(&batch2);
    log.extend_from_slice(&batch3);

    // Both columns ascend and the last entry is valid, but the interior
    // entry for offset 1 points at the valid batch for offset 2. A poll
    // starting from this entry decodes cleanly and can skip offset 1, so
    // structural monotonicity alone cannot make the index safe to retain.
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch2_position));
    index.extend_from_slice(&index_entry(3, batch3_position));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index entry pointing at another batch must be rebuilt");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 3);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "every retained index entry must match its log batch"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_index_entry_below_segment_start_when_recovering_should_rebuild_the_index_from_the_log()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let first = encoded_batch(5, 1);
    let second = encoded_batch(6, 1);
    let mut log = first.clone();
    log.extend_from_slice(&second);
    // The last entry is valid and would anchor a clean walk on its own;
    // only the first-entry check catches the offset below the start.
    let mut index = index_entry(3, 0);
    index.extend_from_slice(&index_entry(6, first.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 5, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index claiming offsets below the segment start must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 6);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(5, 0),
        "the index must be rebuilt from the walked batches"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_index_entry_above_segment_start_when_recovering_should_rebuild_the_index_from_the_log()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let first = encoded_batch(5, 1);
    let second = encoded_batch(6, 1);
    let third = encoded_batch(7, 1);
    let mut log = first.clone();
    log.extend_from_slice(&second);
    log.extend_from_slice(&third);
    // An index that lost its own head: the last entry is valid and
    // anchors a clean walk, so the segment would be accepted with a start
    // timestamp taken from a chunk that is not its first.
    let mut index = index_entry(6, 0);
    index.extend_from_slice(&index_entry(7, (first.len() + second.len()) as u64));
    let (messages_path, index_path) = write_segment(&config, 5, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index starting above the segment start must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 7);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(5, 0),
        "the rebuilt index must start at the segment's own first batch"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_index_first_entry_past_byte_zero_when_recovering_should_rebuild_the_index_from_the_log()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let first = encoded_batch(0, 1);
    let second = encoded_batch(1, 1);
    let third = encoded_batch(2, 1);
    let mut log = first.clone();
    log.extend_from_slice(&second);
    log.extend_from_slice(&third);
    // The offset column opens where the segment does and the entries
    // ascend, so every other check reads this index as healthy. Only the
    // position is wrong: byte 0 is described by nothing, while the last
    // entry lands on its batch and would anchor a clean walk.
    let mut index = index_entry(0, first.len() as u64);
    index.extend_from_slice(&index_entry(2, (first.len() + second.len()) as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index whose first entry skips byte 0 must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 2);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the rebuilt index must open at byte 0"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_index_less_wide_batches_when_recovering_should_rebuild_strided_index() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // First batch alone crosses the rebuild stride, so the second batch
    // must get its own entry at the first batch's total size.
    let wide = encoded_batch_with_payload(0, 1, &Bytes::from(vec![0x42u8; 70 * 1024]));
    let narrow = encoded_batch(1, 1);
    let mut log = wide.clone();
    log.extend_from_slice(&narrow);
    let (_messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let recovered = recover(&config).await.expect("recover wide-batch segment");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    let rebuilt = bytes_of(&index_path);
    assert_eq!(rebuilt.len(), 2 * IGGY_INDEX_SIZE);
    let entry = |index: usize| {
        let at = index * IGGY_INDEX_SIZE;
        (
            read_u64_le(&rebuilt, at),
            read_u64_le(&rebuilt, at + 8),
            read_u64_le(&rebuilt, at + 16),
        )
    };
    assert_eq!(entry(0), (0, FIXTURE_TIMESTAMP, 0));
    assert_eq!(entry(1), (1, FIXTURE_TIMESTAMP, wide.len() as u64));
}

#[compio::test]
async fn given_zero_padded_records_when_probing_should_refuse_on_verify_budget() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Torn index forces the index-less walk, and the garbage head keeps
    // it from decoding anything, so the whole file is probe residue.
    // Each record's header decodes and claims an 8 KiB batch that fits,
    // so aligned candidates pay a (fast-failing) verify; 128 claims
    // exhaust the residue-derived verify budget partway. Exhaustion must
    // refuse rather than recover empty: nothing past the budget horizon
    // was scanned, so serving an empty segment would re-mint offsets
    // over bytes the probe never classified.
    let mut log = GARBAGE.to_vec();
    for record in 0..128u32 {
        log.extend_from_slice(&zero_padded_record(
            8 * 1024 + u64::from(record),
            record + 1,
        ));
    }
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("an exhausted probe over an unwalked residue must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue { .. },
                ..
            }
        ),
        "expected a budget-exhausted refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "a refusal must leave the log byte-identical"
    );
}

#[compio::test]
async fn given_wide_zeros_residue_when_recovering_should_truncate_at_break() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The canonical torn flush chunk: a crash under `durable_segments =
    // false` leaves the file extended far past its written-back pages,
    // reading as zeros -- residue bounded by the CHUNK (up to a whole
    // segment), not by one record. No survivor decodes anywhere in it,
    // so recovery must truncate to the walked prefix, at any residue
    // width and regardless of any configured message size.
    let mut log = encoded_batch(0, 3);
    let valid_len = log.len() as u64;
    log.resize(log.len() + 512 * 1024, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let recovered = recover(&config)
        .await
        .expect("a zero-filled torn flush chunk must truncate, not refuse");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 2);
    assert_eq!(
        len_of(&messages_path),
        valid_len,
        "the zero-filled residue must be gone from disk"
    );
    assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
}

#[compio::test]
async fn given_indexed_offset_regression_when_recovering_should_refuse_without_panicking() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // A decodable batch claiming offsets below the segment start: absorbed,
    // it would regress the recovered end offset below the start and
    // underflow the message-count arithmetic.
    let mut log = encoded_batch(100, 1);
    log.extend_from_slice(&encoded_batch(5, 1));
    let index = index_entry(100, 0);
    let (messages_path, index_path) = write_segment(&config, 100, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("an indexed walk hitting a regressed offset must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                    expected_offset: 101,
                    found_offset: 5,
                    ..
                },
                ..
            }
        ),
        "expected an offset-discontinuity refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_indexed_forward_offset_gap_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // A verified batch opening a forward gap is durable data whose
    // offsets this chain never promised: absorbing it would inflate the
    // recovered message count and mint a segment state transfer can
    // never install, so recovery refuses and keeps every byte.
    let mut log = encoded_batch(0, 2);
    log.extend_from_slice(&encoded_batch(5, 1));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let error = recover(&config)
        .await
        .err()
        .expect("a verified forward offset gap in an indexed chain must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                    expected_offset: 2,
                    found_offset: 5,
                    ..
                },
                ..
            }
        ),
        "expected an offset-discontinuity refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index_entry(0, 0));
}

// `base_offset` sits at header bytes 8..16, so flips inside that range
// leave the per-message checksums clean and trip the BATCH checksum on
// the offset field specifically -- pinning that `base_offset` is hashed.
const HEADER_BASE_OFFSET_OFFSET: usize = 8;

#[compio::test]
async fn given_bit_flipped_base_offset_upward_when_recovering_should_truncate_at_break() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // An upward flip wears the forward-gap shape (header decodes,
    // base_offset ahead of the chain) but fails the batch checksum:
    // damage, not data, so the walk breaks and the tail truncates.
    let mut log = encoded_batch(0, 2);
    let valid_len = log.len() as u64;
    let mut corrupt = encoded_batch(2, 1);
    corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
    log.extend_from_slice(&corrupt);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let recovered = recover(&config)
        .await
        .expect("an unverified gap batch with nothing verifying past it must truncate");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        len_of(&messages_path),
        valid_len,
        "the unverified gap batch must be gone from disk"
    );
    assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
}

#[compio::test]
async fn given_bit_flipped_base_offset_downward_when_recovering_should_truncate_at_break() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The same flip downward wears the regression shape. It must earn
    // the same verdict as the upward one -- verify fails, walk breaks,
    // tail truncates -- rather than a permanent refusal: one bit must
    // not get opposite verdicts by direction.
    let mut log = encoded_batch(0, 2);
    let valid_len = log.len() as u64;
    let mut corrupt = encoded_batch(2, 1);
    corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x02;
    log.extend_from_slice(&corrupt);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

    let recovered = recover(&config)
        .await
        .expect("an unverified regressing batch with nothing verifying past it must truncate");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        len_of(&messages_path),
        valid_len,
        "the unverified regressing batch must be gone from disk"
    );
    assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
}

#[compio::test]
async fn given_bit_flipped_base_offset_before_valid_batch_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // A verifying batch past the failing gap batch is durable data:
    // truncating there would erase it, so recovery must refuse and keep
    // every byte.
    let mut log = encoded_batch(0, 2);
    let mut corrupt = encoded_batch(2, 1);
    corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
    log.extend_from_slice(&corrupt);
    log.extend_from_slice(&encoded_batch(6, 1));
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a verifying batch past the failing gap batch must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                ..
            }
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_failing_gap_batch_at_index_anchor_when_recovering_should_probe_for_survivors() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The failing gap batch is the FIRST thing past the last index
    // entry, so the anchored walk breaks with nothing walked and the log
    // rebuild takes over. Its probe must still find the verifying batch
    // past the damage and refuse as interior damage, rather than
    // truncating a survivor away with the index.
    let mut corrupt = encoded_batch(5, 1);
    corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
    let mut log = corrupt;
    log.extend_from_slice(&encoded_batch(6, 1));
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a survivor past the anchor's failing batch must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                ..
            }
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_failing_gap_batch_at_index_anchor_with_no_survivor_when_recovering_should_fence_and_recover_empty()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    let partition_path = prepare_partition_dir(&config);
    // Same anchor shape with nothing verifying past it. The index backs
    // nothing, so it is dropped and the log walked on its own; the log
    // proves nothing either, which is the ordinary unreadable-pair
    // verdict: bytes fenced aside, fresh empty files seeded.
    let mut log = encoded_batch(5, 1);
    log[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an anchor batch that fails its checksum must not refuse the partition");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.size, IggyByteSize::default());
    assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
    assert_eq!(len_of(&index_path), 0, "the served index must be empty");
    assert_eq!(
        fenced_bytes(&partition_path, &messages_path),
        log,
        "the undecodable log bytes must survive in the fence directory"
    );
    assert_eq!(
        fenced_bytes(&partition_path, &index_path),
        index,
        "the unbacked index bytes must survive in the fence directory"
    );
}

#[compio::test]
async fn given_index_entry_past_the_log_end_when_recovering_should_rebuild_the_index_from_the_log()
{
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The index reached disk one entry ahead of the log it indexes -- no
    // barrier orders the two files -- so the last entry points exactly at
    // the log end. The log is whole; only the index overshot.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index running ahead of a whole log must recover, not refuse");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "an index the log contradicts must be rebuilt from the walked batches"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_index_anchor_on_torn_batch_when_recovering_should_rebuild_index_and_truncate_log_tail()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The last entry points at a batch whose header made it to disk but
    // whose body did not: a torn tail on both files at once.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let batch2 = encoded_batch(2, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let torn_position = log.len() as u64;
    log.extend_from_slice(&batch2[..COMMAND_HEADER_SIZE + 8]);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, torn_position));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("a torn batch under the last index entry must truncate, not refuse");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        len_of(&messages_path),
        torn_position,
        "the torn batch must be gone from disk"
    );
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the index must describe the batches the walk proved, not the torn one"
    );
    drop(recovered);

    // A repaired pair must be a fixpoint: a boot loop that re-refused
    // what it just repaired is the failure this replaces.
    let reopened = recover(&config)
        .await
        .expect("re-recovering a repaired segment must not refuse");

    assert_eq!(reopened[0].segment.end_offset, 1);
    assert_eq!(
        (len_of(&messages_path), len_of(&index_path)),
        (torn_position, IGGY_INDEX_SIZE as u64),
        "a second recovery must not move the files"
    );
}

#[compio::test]
async fn given_index_anchor_past_the_log_end_with_survivor_past_damage_when_recovering_should_refuse_interior_damage()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Dropping the index must not weaken the survivor rule: the byte-0
    // walk still ends at the garbage, and the batch verifying past it is
    // durable data truncation would erase.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    log.extend_from_slice(&GARBAGE);
    log.extend_from_slice(&encoded_batch(6, 1));
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a survivor past the damage must refuse recovery even after stepping back");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                ..
            }
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

/// The `.log` and `.index` fixtures for a segment whose SECOND chunk lost
/// its body while its header page survived, and whose last index entry
/// points past the log end. Anchoring on the highest entry the log still
/// proves lands on the third chunk, ABOVE the damage, and the damage probe
/// only ever looks forward from where a walk stopped -- so an anchored
/// walk runs clean to EOF and mints an `end_offset` covering an offset
/// whose bytes are gone. Returned with the damaged and surviving positions
/// so the tests can name them.
fn segment_damaged_below_its_last_provable_entry() -> (Vec<u8>, Vec<u8>, u64, u64) {
    let mut torn = encoded_batch(1, 1);
    torn[COMMAND_HEADER_SIZE..].fill(0);
    let batch2 = encoded_batch(2, 1);
    let mut log = encoded_batch(0, 1);
    let damage_position = log.len() as u64;
    log.extend_from_slice(&torn);
    let survivor_position = log.len() as u64;
    log.extend_from_slice(&batch2);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, damage_position));
    index.extend_from_slice(&index_entry(2, survivor_position));
    index.extend_from_slice(&index_entry(3, log.len() as u64));
    (log, index, damage_position, survivor_position)
}

#[compio::test]
async fn given_damage_below_the_highest_provable_index_entry_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let (log, index, damage_position, survivor_position) =
        segment_damaged_below_its_last_provable_entry();
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("damage below the highest provable entry must refuse, not be walked past");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage {
                    damage_position: at,
                    survivor_position: past,
                    ..
                },
                ..
            } if *at == damage_position && *past == survivor_position
        ),
        "expected an interior-damage refusal naming the damaged chunk, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_damage_below_a_valid_last_index_entry_without_fsync_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let (log, mut index, damage_position, survivor_position) =
        segment_damaged_below_its_last_provable_entry();
    index.truncate(index.len() - IGGY_INDEX_SIZE);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a valid last entry must not hide earlier damage without fsync");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage {
                    damage_position: at,
                    survivor_position: past,
                    ..
                },
                ..
            } if *at == damage_position && *past == survivor_position
        ),
        "expected an interior-damage refusal below the valid last entry, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_damage_below_the_highest_provable_index_entry_under_fsync_when_recovering_should_refuse()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The gap between the index and the log is exactly one entry here, so
    // the `durable_segments` guard reads this as the benign in-flight chunk
    // and lets it through. That verdict is about the INDEX; the log is
    // still walked from byte 0, which is what catches the damage.
    let (log, index, damage_position, _) = segment_damaged_below_its_last_provable_entry();
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("a passing step-back depth must not stand in for reading the log");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage {
                    damage_position: at,
                    ..
                },
                ..
            } if *at == damage_position
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_an_offset_gap_below_the_highest_provable_index_entry_when_recovering_should_refuse()
{
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Nothing here is damaged: both batches verify. What is missing is
    // offsets 1..4, and only a walk that starts at byte 0 can notice.
    // An anchor on the entry for offset 5 takes that entry's own offset
    // as the chain expectation, so the gap below it reads as continuous.
    let mut log = encoded_batch(0, 1);
    let gap_position = log.len() as u64;
    log.extend_from_slice(&encoded_batch(5, 1));
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(5, gap_position));
    index.extend_from_slice(&index_entry(6, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("an offset gap below the highest provable entry must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                    expected_offset: 1,
                    found_offset: 5,
                    position,
                    ..
                },
                ..
            } if *position == gap_position
        ),
        "expected an offset-discontinuity refusal at the gap, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_no_provable_index_entry_when_recovering_should_rebuild_the_index_from_the_log() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Every entry points past the log end, so nothing in the index backs
    // the walk -- and the first entry is not even at the segment's first
    // byte, which the consistency scan catches before the walk runs. The
    // log is whole and describes itself either way, so the index is
    // dropped and rebuilt from it.
    let log = encoded_batch(0, 2);
    let mut index = index_entry(0, log.len() as u64);
    index.extend_from_slice(&index_entry(2, log.len() as u64 + 64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index the log backs nowhere must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the index must be rebuilt from the walked batches"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "rebuilding the index must not touch a whole log"
    );
}

#[compio::test]
async fn given_an_index_entry_over_an_empty_log_when_recovering_should_fence_and_recover_empty() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    let partition_path = prepare_partition_dir(&config);
    // The crash shape concurrent persists make ordinary: the first
    // persist into a fresh segment fsyncs its one index entry and its log
    // chunk at the same time, and the entry lands while the log does not.
    // Nothing was acked for those bytes, so the segment is simply empty.
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &[], &index);

    let recovered = recover(&config)
        .await
        .expect("an index entry over an empty log must not refuse the partition");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.size, IggyByteSize::default());
    assert_eq!(len_of(&messages_path), 0);
    assert_eq!(len_of(&index_path), 0, "the unbacked entry must be gone");
    assert_eq!(
        fenced_bytes(&partition_path, &index_path),
        index,
        "the unbacked index bytes must survive in the fence directory"
    );
    drop(recovered);

    // Fixpoint: the seeded empty pair holds no bytes to fence, so a
    // second boot must not open another fence directory.
    recover(&config).await.expect("second recovery");

    assert_eq!((len_of(&messages_path), len_of(&index_path)), (0, 0));
    assert!(
        !Path::new(&format!("{partition_path}.fenced.1")).exists(),
        "a repaired empty segment must not be fenced again"
    );
}

#[compio::test]
async fn given_an_index_entry_over_a_torn_batch_when_recovering_should_fence_and_recover_empty() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    let partition_path = prepare_partition_dir(&config);
    // The same crash one flush later: the entry is durable and the log
    // holds only the head of the batch it points at.
    let batch = encoded_batch(0, 1);
    let log = batch[..COMMAND_HEADER_SIZE + 8].to_vec();
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("an index entry over a torn batch must not refuse the partition");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.size, IggyByteSize::default());
    assert_eq!(len_of(&messages_path), 0, "the torn head must be gone");
    assert_eq!(len_of(&index_path), 0, "the unbacked entry must be gone");
    assert_eq!(
        fenced_bytes(&partition_path, &messages_path),
        log,
        "the torn bytes must survive in the fence directory"
    );
    assert_eq!(fenced_bytes(&partition_path, &index_path), index);
}

#[compio::test]
async fn given_an_index_entry_contradicting_a_healthy_log_when_recovering_should_rebuild_the_index()
{
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The entry claims offset 7 where the log holds offset 0. The log
    // verifies and the entry is derived data, so the entry is what is
    // wrong: rebuild the index rather than reading the mismatch as a
    // break in the chain.
    let batch0 = encoded_batch(0, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&encoded_batch(1, 1));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(7, 0));

    let recovered = recover(&config)
        .await
        .expect("an index entry contradicting a healthy log must be rebuilt, not refused");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the rebuilt entry must describe the batch the log actually holds"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_two_unprovable_index_entries_when_recovering_should_rebuild_the_index_from_the_log()
{
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Two unprovable entries for two different reasons: one lands
    // mid-batch inside the log, the next past its end. Neither the depth
    // of the gap nor the reason for it changes the verdict without
    // `durable_segments`: the whole index goes and the log speaks for itself.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, batch0.len() as u64 + 8));
    index.extend_from_slice(&index_entry(3, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("a multi-entry index overshoot must rebuild from what the log proves");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "every entry must come from the walk, not just the unbacked ones be dropped"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_two_unprovable_index_entries_under_fsync_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The same fixture the lenient rebuild above accepts. Under
    // `durable_segments` the second-from-last entry was durable before its
    // chunk was acked, so a log that cannot back it lost acked bytes.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, batch0.len() as u64 + 8));
    index.extend_from_slice(&index_entry(3, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("a multi-entry overshoot under durable_segments must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::FsyncedLogLoss {
                    entry_count: 4,
                    provable_entries: 2,
                    ..
                },
                ..
            }
        ),
        "expected an fsynced-log-loss refusal naming the step-back, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_one_unprovable_index_entry_under_fsync_when_recovering_should_rebuild_the_index() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The crash window `durable_segments` cannot close: the entry for the
    // chunk still in flight reached disk, its log bytes did not, and no
    // ack was ever sent for them. Exactly one entry deep, so it recovers.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, log.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover_under_fsync(&config, true)
        .await
        .expect("a one-entry overshoot is the crash window, not data loss");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the index must be rebuilt from the walk, not floored to the entry below"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_a_mid_chunk_tear_below_the_last_entry_under_fsync_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // Chunk 1 held two batches under one entry; its second batch is torn
    // and the in-flight chunk 2's log bytes never landed. Entry 2 exists,
    // so the log completed an fdatasync through its position: the tear
    // sits inside bytes a completed flush made durable, a loss the
    // entry-granular step-back (depth 1 here) cannot see.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut torn = encoded_batch(2, 1);
    torn[COMMAND_HEADER_SIZE..].fill(0);
    let mut log = batch0.clone();
    let chunk1_position = log.len() as u64;
    log.extend_from_slice(&batch1);
    let torn_position = log.len() as u64;
    log.extend_from_slice(&torn);
    let durable = log.len() as u64;
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, chunk1_position));
    index.extend_from_slice(&index_entry(3, durable));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("a rebuild proving less than the last entry's position must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::FsyncedRebuildShortfall {
                    walked_position,
                    durable_position,
                    ..
                },
                ..
            } if *walked_position == torn_position && *durable_position == durable
        ),
        "expected an fsynced-rebuild-shortfall refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_a_verified_offset_gap_past_an_empty_anchor_batch_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The empty batch verifies and carries the anchor's own offset, so
    // the expectation it confirms is the log's, not the index's. The
    // verified batch after it that does not continue the chain is durable
    // data past a hole; treating the mismatch as a stale-anchor break
    // would truncate it and re-mint the offsets in between.
    let empty = encoded_batch(0, 0);
    let mut log = empty.clone();
    let survivor_position = log.len() as u64;
    log.extend_from_slice(&encoded_batch(5, 1));
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("a verified offset gap past an empty anchor batch must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                    expected_offset: 0,
                    found_offset: 5,
                    position,
                    ..
                },
                ..
            } if *position == survivor_position
        ),
        "expected an offset-discontinuity refusal past the empty anchor, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_only_an_empty_anchor_batch_under_fsync_when_recovering_should_not_count_a_phantom_message()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    let partition_path = prepare_partition_dir(&config);
    let log = encoded_batch(0, 0);
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover_under_fsync(&config, true)
        .await
        .expect("a checksum-valid empty record carries no message range");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.size, IggyByteSize::default());
    assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
    assert_eq!(len_of(&index_path), 0, "the served index must be empty");
    assert_eq!(fenced_bytes(&partition_path, &messages_path), log);
    assert_eq!(fenced_bytes(&partition_path, &index_path), index);
}

#[compio::test]
async fn given_an_index_the_log_backs_nowhere_under_fsync_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The gap is measured the same way when no entry proves at all. The
    // index still starts at the segment's own first byte, so it is not
    // the mis-strided shape the consistency scan drops; the log simply
    // lost the tail of its FIRST indexed chunk, which two entries the log
    // backs nowhere is one more than the in-flight chunk can explain.
    let whole = encoded_batch(0, 2);
    let log = whole[..COMMAND_HEADER_SIZE + 8].to_vec();
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(2, whole.len() as u64));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("an index backed nowhere under durable_segments must refuse, not rebuild");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::FsyncedLogLoss {
                    entry_count: 2,
                    provable_entries: 0,
                    ..
                },
                ..
            }
        ),
        "expected an fsynced-log-loss refusal over the dropped index, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_a_lone_entry_over_an_empty_log_under_fsync_when_recovering_should_not_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The first flush into a fresh segment, crashed between the two
    // fsyncs: one entry and no log bytes. Whether any batches in that
    // flush were acknowledged depends on the flush thresholds, but only
    // one entry can belong to the interrupted flush. `durable_segments` must
    // not turn that shape into a tombstone.
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &[], &index);

    let recovered = recover_under_fsync(&config, true)
        .await
        .expect("one entry over an empty log is the crash window, not data loss");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.size, IggyByteSize::default());
    assert_eq!((len_of(&messages_path), len_of(&index_path)), (0, 0));
}

#[compio::test]
async fn given_index_anchor_on_batch_with_torn_body_when_recovering_should_truncate_it_and_rebuild_index()
 {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The last entry points at a batch whose header page reached disk
    // whole but whose body did not: the header decodes, fits the file,
    // and continues the chain from the entry, so only the checksum can
    // tell it from a durable batch. The entry is no evidence for the
    // bytes under it, because the log and the index persist
    // concurrently.
    let batch0 = encoded_batch(0, 1);
    let batch1 = encoded_batch(1, 1);
    let mut torn = encoded_batch(2, 1);
    torn[COMMAND_HEADER_SIZE..].fill(0);
    let mut log = batch0.clone();
    log.extend_from_slice(&batch1);
    let torn_position = log.len() as u64;
    log.extend_from_slice(&torn);
    let mut index = index_entry(0, 0);
    index.extend_from_slice(&index_entry(1, batch0.len() as u64));
    index.extend_from_slice(&index_entry(2, torn_position));
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let recovered = recover(&config)
        .await
        .expect("a torn body under an intact header must truncate, not refuse");

    assert_eq!(recovered.len(), 1);
    assert_eq!(
        recovered[0].segment.end_offset, 1,
        "the torn batch's offset must not be minted from its header"
    );
    assert_eq!(
        len_of(&messages_path),
        torn_position,
        "the torn batch must be gone from disk"
    );
    assert_eq!(
        bytes_of(&index_path),
        index_entry(0, 0),
        "the entry pointing at the torn batch must be gone from disk"
    );
    drop(recovered);

    let reopened = recover(&config)
        .await
        .expect("re-recovering a repaired segment must not refuse");

    assert_eq!(reopened[0].segment.end_offset, 1);
    assert_eq!(
        (len_of(&messages_path), len_of(&index_path)),
        (torn_position, IGGY_INDEX_SIZE as u64),
        "a second recovery must not move the files"
    );
}

#[compio::test]
async fn given_index_entries_packed_closer_than_their_batches_under_fsync_when_measuring_the_gap_should_refuse_on_verify_budget()
 {
    // A mis-strided index over a log of back-to-back headers, each
    // claiming a batch 8 KiB wide: every entry lands on a header that
    // decodes, matches the entry, and fits the file, so each step back
    // costs a whole-batch verify that never passes. The claims overlap
    // many times over, so an unbudgeted search would hash close to
    // entries x claim bytes; it must give up and refuse instead, leaving
    // the files byte-identical. Only `durable_segments` walks the index
    // backward at all -- without it the gap is not measured, so there is
    // nothing here to bound.
    const CLAIMED_BATCH_BYTES: usize = 8 * 1024;
    const ENTRIES: u64 = 64;
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = Vec::new();
    let mut index = Vec::new();
    for offset in 0..ENTRIES {
        let mut header = encoded_batch(offset, 1)[..COMMAND_HEADER_SIZE].to_vec();
        header[HEADER_BATCH_LENGTH_OFFSET..HEADER_BATCH_LENGTH_OFFSET + 8]
            .copy_from_slice(&(CLAIMED_BATCH_BYTES as u64).to_le_bytes());
        index.extend_from_slice(&index_entry(offset, log.len() as u64));
        log.extend_from_slice(&header);
    }
    // Padding so the last claim still fits inside the file.
    log.resize(log.len() + CLAIMED_BATCH_BYTES, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover_under_fsync(&config, true)
        .await
        .err()
        .expect("exhausting the verify budget while measuring the gap must refuse");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::UnverifiedResidue {
                    verified_bytes,
                    verify_budget_bytes,
                    ..
                },
                ..
            } if verified_bytes > verify_budget_bytes
        ),
        "expected a verify-budget refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&messages_path),
        log,
        "a refusal must leave the log byte-identical"
    );
    assert_eq!(
        bytes_of(&index_path),
        index,
        "a refusal must leave the index byte-identical"
    );
}

#[compio::test]
async fn given_foreign_partition_batch_when_recovering_indexed_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // A verified batch stamped for partition 7 that continues the chain
    // EXACTLY: only the partition_id comparison can catch it, and
    // adopting it would serve foreign data under this partition's
    // offsets.
    let mut log = encoded_batch(0, 2);
    log.extend_from_slice(&encoded_foreign_batch(2, 1, 7));
    let index = index_entry(0, 0);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

    let error = recover(&config)
        .await
        .err()
        .expect("a verified foreign batch in an indexed chain must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::ForeignBatch {
                    batch_partition_id: 7,
                    ..
                },
                ..
            }
        ),
        "expected a foreign-batch refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
    assert_eq!(bytes_of(&index_path), index);
}

#[compio::test]
async fn given_foreign_partition_batch_when_recovering_index_less_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 2);
    log.extend_from_slice(&encoded_foreign_batch(2, 1, 7));
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("a verified foreign batch in an index-less chain must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::ForeignBatch {
                    batch_partition_id: 7,
                    ..
                },
                ..
            }
        ),
        "expected a foreign-batch refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_bit_flipped_batch_length_when_recovering_should_truncate_at_break() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // A corrupted-upward length claim (the classic all-ones flip) is not
    // a plausible batch: the walk must break at the header itself, never
    // sizing an allocation or a read by what the header claims.
    let mut log = encoded_batch(0, 2);
    let valid_len = log.len() as u64;
    log.extend_from_slice(&zero_padded_record(0xFFFF_FFFF, 1));
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let recovered = recover(&config)
        .await
        .expect("an implausible length claim in the tail must truncate, not refuse");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert_eq!(
        len_of(&messages_path),
        valid_len,
        "the walk must break at the implausible header and truncate there"
    );
}

#[compio::test]
async fn given_bit_flipped_batch_length_before_valid_batch_when_recovering_should_refuse() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let mut log = encoded_batch(0, 2);
    let valid_len = log.len() as u64;
    log.extend_from_slice(&zero_padded_record(0xFFFF_FFFF, 1));
    log.extend_from_slice(&encoded_batch(2, 1));
    let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

    let error = recover(&config)
        .await
        .err()
        .expect("a surviving batch past an implausible header must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::InteriorDamage {
                    damage_position,
                    survivor_position,
                    ..
                },
                ..
            } if *damage_position == valid_len
                && *survivor_position == valid_len + COMMAND_HEADER_SIZE as u64
        ),
        "expected an interior-damage refusal, got {error:?}"
    );
    assert_eq!(bytes_of(&messages_path), log);
}

#[compio::test]
async fn given_refused_chain_when_index_rebuilt_should_stage_without_touching_final() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    // The index-less first segment wants a rebuild; the hole to the next
    // segment refuses the chain in pass B, before any install.
    let first_log = encoded_batch(0, 2);
    let first_index = GARBAGE[..10].to_vec();
    let (first_messages_path, first_index_path) =
        write_segment(&config, 0, &first_log, &first_index);
    write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));

    let error = recover(&config)
        .await
        .err()
        .expect("a holed chain must refuse recovery");

    assert!(
        matches!(
            &error,
            PartitionRecoveryError::Refused {
                reason: PartitionRecoveryRefusal::Hole { .. },
                ..
            }
        ),
        "expected a hole refusal, got {error:?}"
    );
    assert_eq!(
        bytes_of(&format!("{first_index_path}{STAGING_SUFFIX}")),
        index_entry(0, 0),
        "pass A must stage the rebuilt index beside the final one"
    );
    assert_eq!(
        bytes_of(&first_index_path),
        first_index,
        "a refusal must leave the final index byte-identical"
    );
    assert_eq!(bytes_of(&first_messages_path), first_log);
}

#[compio::test]
async fn given_orphaned_index_staging_when_recovering_should_sweep_it() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let log = encoded_batch(0, 2);
    let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));
    let staging_path = format!("{index_path}{STAGING_SUFFIX}");
    fs::write(&staging_path, GARBAGE).expect("write orphaned staging fixture");

    let recovered = recover(&config).await.expect("recover clean segment");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 1);
    assert!(
        fs::metadata(&staging_path).is_err(),
        "an orphaned staging file must be swept at boot"
    );
    assert_eq!(len_of(&messages_path), log.len() as u64);
    assert_eq!(len_of(&index_path), IGGY_INDEX_SIZE as u64);
}
#[compio::test]
async fn given_absent_index_when_recovering_should_walk_index_less_and_rebuild() {
    let tmp = tempdir().expect("tempdir");
    let config = test_config(&tmp);
    prepare_partition_dir(&config);
    let log = encoded_batch(0, 4);
    let messages_path = config.get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    let index_path = config.get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
    fs::write(&messages_path, &log).expect("write log fixture");

    let recovered = recover(&config)
        .await
        .expect("a log with no index beside it must recover, not abort the boot");

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].segment.end_offset, 3);
    assert_eq!(len_of(&messages_path), log.len() as u64);
    assert_eq!(
        len_of(&index_path),
        IGGY_INDEX_SIZE as u64,
        "the walk must install a rebuilt index over the missing one"
    );
}
