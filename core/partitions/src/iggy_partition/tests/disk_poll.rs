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

/// Fail-closed disk read: an unreadable EARLIER segment must stop the walk
/// (return `Faulted`) rather than skip forward and serve a LATER segment's
/// messages, which would punch a silent gap into the poll. The second
/// segment holds a real, matchable batch at a higher offset; before the
/// fix, a missing first segment did `continue` and the walk served that
/// batch (offset 5 in response to an offset-0 poll) - the exact skip.
#[compio::test]
async fn read_disk_faults_closed_when_earlier_segment_unreadable() {
    let namespace = IggyNamespace::new(1, 1, 0);

    // Unique temp dir; the first segment file is deliberately never created.
    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-faulted-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // Second segment starts at offset 5 and holds a valid batch there.
    let later_record = build_segment_record(namespace, 5);
    let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
    let later_len = later_record.len() as u64;
    {
        let mut file = compio::fs::File::create(&later_path)
            .await
            .expect("create later segment file");
        let (written, _) = file.write_all_at(later_record, 0).await.into();
        written.expect("write later segment record");
        file.sync_all().await.expect("flush later segment file");
    }

    // First segment claims persisted bytes but its file is absent, so the
    // open exhausts retries -> the walk must fault-close before segment two.
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![
            DiskSegment {
                start_offset: 0,
                persisted: 512,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
            DiskSegment {
                start_offset: 5,
                persisted: later_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
        ],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Retryable)),
        "unreadable first segment must fault-close, not skip forward to the later segment",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Sizing the first read from the requested count makes it routinely
/// narrower than one batch, which the walk answers by re-reading the same
/// position four times as wide. A partition whose mean message is small
/// and whose next batch is not must still serve that batch.
#[compio::test]
async fn read_disk_serves_a_batch_wider_than_the_sized_chunk() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-wide-batch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let payload = Bytes::from(vec![0x5Au8; 256 << 10]);
    let record = build_segment_record_with_payload(namespace, 0, payload.clone());
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    // One byte per message floors the first read at 64 KiB, a quarter of
    // the batch waiting at offset 0.
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: Some(1),
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;

    let DiskReadOutcome::Matched {
        fragments, matched, ..
    } = outcome
    else {
        panic!("a batch wider than the first read must still be served");
    };
    assert_eq!(matched, 1);
    let served: u64 = fragments.iter().map(|fragment| fragment.len() as u64).sum();
    assert_eq!(served, record_len);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fail-closed disk read on a CORRUPT (present-but-undecodable) batch in an
/// EARLIER segment: like a missing/unreadable segment, the walk must stop
/// (`Faulted`) rather than skip past the garbage and serve a LATER
/// segment's valid batch at a higher offset, which would punch a silent gap
/// into the poll. The first segment's file exists and claims persisted bytes
/// but holds non-decodable data; the second segment holds a real batch at
/// offset 5.
#[compio::test]
async fn read_disk_faults_closed_when_earlier_segment_corrupt() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-corrupt-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // First segment (start_offset 0): garbage bytes that never decode into a
    // complete batch.
    let corrupt_record = vec![0xABu8; 512];
    let corrupt_len = corrupt_record.len() as u64;
    let corrupt_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&corrupt_path)
            .await
            .expect("create corrupt segment file");
        let (written, _) = file.write_all_at(corrupt_record, 0).await.into();
        written.expect("write corrupt segment record");
        file.sync_all().await.expect("flush corrupt segment file");
    }

    // Second segment (start_offset 5): a valid, matchable batch.
    let later_record = build_segment_record(namespace, 5);
    let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
    let later_len = later_record.len() as u64;
    {
        let mut file = compio::fs::File::create(&later_path)
            .await
            .expect("create later segment file");
        let (written, _) = file.write_all_at(later_record, 0).await.into();
        written.expect("write later segment record");
        file.sync_all().await.expect("flush later segment file");
    }

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![
            DiskSegment {
                start_offset: 0,
                persisted: corrupt_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
            DiskSegment {
                start_offset: 5,
                persisted: later_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            },
        ],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "corrupt earlier segment must fault-close, not skip forward to the later segment",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A segment whose bytes decode cleanly but do not match their own
/// `batch_checksum`: bit rot at rest, not a torn write. Unverified, the batch is
/// served and a consumer reads data provably not what was written.
///
/// Detection only, per the operator knob: the poll fails closed and reports, with
/// no attempt to repair.
#[compio::test]
async fn read_disk_faults_closed_on_batch_checksum_mismatch() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-bitrot-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // Structurally valid with one payload byte flipped, so every length and
    // offset still decodes and only the checksum disagrees.
    let mut record = build_segment_record(namespace, 0);
    let last = record.len() - 1;
    record[last] ^= 0x01;
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let plan = |validate_checksum| DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let query = MessageLookup::Offset {
        offset: 0,
        count: 10,
        ceiling: u64::MAX,
    };

    let outcome = plan(true).read_disk(query).await;
    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "a batch that fails its own checksum must fault-close"
    );

    // What the opt-out costs. The shipped default is `true` because of it.
    let outcome = plan(false).read_disk(query).await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "verification off is an explicit opt-out: the corrupt batch is served"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A disk plan over one segment of a simulated (file-less) partition that
/// persisted `persisted` bytes.
fn file_less_plan(persisted: u64) -> DiskReadPlan {
    DiskReadPlan {
        partition_dir: PartitionDirResolution::NoFiles,
        bytes_per_message: None,
        widest_batch_bytes: 0,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
        validate_checksum: true,
    }
}

/// A simulated (file-less) partition has no segment files by design. Until
/// a flush persists anything, the journal is its only tier, so a disk poll
/// must stay `Empty` and the caller serves the journal.
#[compio::test]
async fn read_disk_serves_journal_when_partition_has_no_files() {
    let outcome = file_less_plan(0)
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Empty),
        "file-less (simulated) storage must serve the journal tier, not fault",
    );
}

/// A flush evicts what it persisted from the journal, and a file-less
/// partition cannot read those bytes back. Serving the journal forward
/// would answer a poll below the resident tail with that tail and skip the
/// flushed offsets, so the poll must fail closed.
#[compio::test]
async fn read_disk_faults_closed_when_file_less_partition_flushed() {
    let outcome = file_less_plan(512)
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Unreadable)),
        "flushed offsets of a file-less partition must fault-close, not serve the journal",
    );
}

/// A live partition whose dir is transiently unresolvable (mid-rotation)
/// may hold disk-resident data the walk cannot reach; the poll must
/// fault-close instead of letting the journal-forward skip those offsets.
#[compio::test]
async fn read_disk_faults_closed_when_partition_dir_unresolvable() {
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Unresolvable,
        bytes_per_message: None,
        widest_batch_bytes: 0,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: 512,
            read_state: SealedSegmentHandle::default(),
            sealed: false,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
        validate_checksum: true,
    };

    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        })
        .await;

    assert!(
        matches!(outcome, DiskReadOutcome::Faulted(DiskFault::Retryable)),
        "unresolvable dir over file-backed data must fault-close, not serve the journal",
    );
}

/// A sealed-segment poll opens the file once and caches the read fd; a later
/// poll of the same segment reuses the cached descriptor. Proven by
/// unlinking the file after the first read: a fresh open-by-path would now
/// fail, so a successful second read can only come from the cached fd (which
/// reads the still-open, unlinked inode).
#[compio::test]
async fn read_disk_caches_and_reuses_sealed_segment_fd() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-fdcache-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let handle = SealedSegmentHandle::default();
    // The pump touches the poll's start segment before cloning its handle
    // into the plan, so a cache-eligible handle is always tracked.
    handle.tracked.set(true);
    assert!(handle.fd.borrow().is_none(), "fd cache slot starts empty");

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let first = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(first, DiskReadOutcome::Matched { .. }),
        "first sealed poll must match the batch",
    );
    assert!(
        handle.fd.borrow().is_some(),
        "first sealed poll must populate the read-fd cache slot",
    );

    // Unlink the file: a fresh open-by-path would fail now, so the second
    // read succeeding proves the cached fd was reused.
    std::fs::remove_file(&path).expect("unlink segment file");

    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let second = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(second, DiskReadOutcome::Matched { .. }),
        "cached fd must serve the read after the segment path is unlinked",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// An untracked handle (a sealed segment the walk crosses without being the
/// poll's start segment, or a slot evicted mid-poll) opens its file
/// transiently: the read succeeds but no fd is retained, so the sealed LRU
/// cap stays a true bound on resident descriptors.
#[compio::test]
async fn read_disk_does_not_retain_fd_for_untracked_handle() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-untracked-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    let path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&path)
            .await
            .expect("create segment file");
        let (written, _) = file.write_all_at(record, 0).await.into();
        written.expect("write segment record");
        file.sync_all().await.expect("flush segment file");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 0,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the transient open must still serve the read",
    );
    assert!(
        handle.fd.borrow().is_none(),
        "an untracked handle must not retain the fd",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A sealed-segment poll reloads the dropped sparse index from the `.index`
/// file and resolves the start byte from it, skipping the full-segment scan.
/// Proven by prefixing the `.log` with bytes a scan from position 0 would
/// fault on: only an index that jumps straight to the batch reads it.
#[compio::test]
async fn read_disk_reloads_sealed_index_to_skip_scan() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-idxreload-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    // `.log`: an undecodable prefix (a scan from byte 0 faults on it) then a
    // valid batch at offset 5. `.index`: one sparse entry mapping offset 5
    // to the batch's byte position, so the poll jumps past the prefix.
    let prefix = vec![0xABu8; 512];
    let prefix_len = prefix.len() as u64;
    let batch = build_segment_record(namespace, 5);
    let mut log_bytes = prefix;
    log_bytes.extend_from_slice(&batch);
    let log_len = log_bytes.len() as u64;
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&log_path)
            .await
            .expect("create segment log");
        let (written, _) = file.write_all_at(log_bytes, 0).await.into();
        written.expect("write segment log");
        file.sync_all().await.expect("flush segment log");
    }

    let index_bytes = crate::iggy_index::IggyIndexCache::serialize(
        &crate::iggy_index::IggyIndex::new(5, 0, prefix_len),
    );
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    {
        let mut file = compio::fs::File::create(&index_path)
            .await
            .expect("create segment index");
        let (written, _) = file.write_all_at(index_bytes, 0).await.into();
        written.expect("write segment index");
        file.sync_all().await.expect("flush segment index");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: log_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        // Byte 0, exactly what disk_poll_start returns for a sealed segment
        // whose resident index was dropped.
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: 5,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the reloaded sparse index must skip the prefix; a scan from byte 0 would fault",
    );
    assert!(
        handle.index.borrow().is_some(),
        "the sealed poll must cache the reloaded sparse index",
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A sparse index past `SEALED_INDEX_RESIDENT_MAX_BYTES` (a dense flush
/// cadence can make it track every message, hundreds of MB per segment) is
/// binary-searched on file instead of materialized: the poll still resolves
/// the exact start byte (proven by the poison prefix a byte-0 scan would
/// fault on) while the handle's index slot stays empty.
#[compio::test]
async fn read_disk_resolves_oversized_index_without_materializing() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let dir = std::env::temp_dir().join(format!(
        "iggy-read-disk-bigidx-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    compio::fs::create_dir_all(&dir)
        .await
        .expect("create temp partition dir");
    let partition_dir = dir.to_string_lossy().into_owned();

    let index_size = crate::iggy_index::IGGY_INDEX_SIZE as u64;
    let entry_count = crate::poll_plan::SEALED_INDEX_RESIDENT_MAX_BYTES / index_size + 1;
    let target_offset = entry_count - 1;

    let prefix = vec![0xABu8; 512];
    let prefix_len = prefix.len() as u64;
    let batch = build_segment_record(namespace, target_offset);
    let mut log_bytes = prefix;
    log_bytes.extend_from_slice(&batch);
    let log_len = log_bytes.len() as u64;
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    {
        let mut file = compio::fs::File::create(&log_path)
            .await
            .expect("create segment log");
        let (written, _) = file.write_all_at(log_bytes, 0).await.into();
        written.expect("write segment log");
        file.sync_all().await.expect("flush segment log");
    }

    // Every entry below the target points at byte 0 (the poison prefix),
    // so only an exact lower-bound hit on the last entry reads the batch.
    let mut index_bytes =
        Vec::with_capacity(usize::try_from(entry_count * index_size).expect("fits in usize"));
    for entry in 0..entry_count {
        index_bytes.extend_from_slice(&entry.to_le_bytes());
        index_bytes.extend_from_slice(&entry.to_le_bytes());
        let position = if entry == target_offset {
            prefix_len
        } else {
            0
        };
        index_bytes.extend_from_slice(&position.to_le_bytes());
    }
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    {
        let mut file = compio::fs::File::create(&index_path)
            .await
            .expect("create segment index");
        let (written, _) = file.write_all_at(index_bytes, 0).await.into();
        written.expect("write segment index");
        file.sync_all().await.expect("flush segment index");
    }

    let handle = SealedSegmentHandle::default();
    let plan = DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: log_len,
            read_state: Rc::clone(&handle),
            sealed: true,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let outcome = plan
        .read_disk(MessageLookup::Offset {
            offset: target_offset,
            count: 1,
            ceiling: u64::MAX,
        })
        .await;
    assert!(
        matches!(outcome, DiskReadOutcome::Matched { .. }),
        "the on-file lower bound must resolve past the poison prefix",
    );
    assert!(
        handle.index.borrow().is_none(),
        "an index past the resident cap must never be materialized",
    );

    let _ = std::fs::remove_dir_all(&dir);
}
