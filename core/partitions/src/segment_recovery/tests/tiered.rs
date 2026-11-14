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

//! Boot rules for a partition whose chain records segments that left the disk.

use super::segment_recovery_tests::{
    FIXTURE_TIMESTAMP, PARTITION_ID, SEGMENT_MAX_SIZE, STREAM_ID, TOPIC_ID, encoded_batch,
    index_entry, prepare_partition_dir, test_config, write_segment,
};
use super::*;
use crate::segment_anchor::{SegmentAnchor, anchor_path, write_anchor};
use crate::segment_chain::{
    HEADER_SIZE, HashSource, RECORD_SIZE, append_record, chain_path, create_chain,
};
use journal::partition_journal::SegmentPosition;
use std::fs;
use tempfile::{TempDir, tempdir};

const PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: STREAM_ID as u64,
    topic_id: TOPIC_ID as u64,
    partition_id: PARTITION_ID as u64,
    created_revision: 5,
};

const MESSAGES_PER_SEGMENT: u64 = 3;

/// Three contiguous local segments, the last one active.
const STARTS: [u64; 3] = [0, 3, 6];

struct Fixture {
    _root: TempDir,
    config: PartitionsConfig,
    directory: String,
    starts: Vec<u64>,
    logs: Vec<Vec<u8>>,
}

fn fixture() -> Fixture {
    fixture_at(&STARTS)
}

fn fixture_at(starts: &[u64]) -> Fixture {
    let root = tempdir().unwrap();
    let config = test_config(&root);
    let directory = prepare_partition_dir(&config);
    let logs = starts
        .iter()
        .map(|start| {
            let log = encoded_batch(*start, usize::try_from(MESSAGES_PER_SEGMENT).unwrap());
            write_segment(&config, *start, &log, &index_entry(*start, 0));
            log
        })
        .collect();
    Fixture {
        _root: root,
        config,
        directory,
        starts: starts.to_vec(),
        logs,
    }
}

impl Fixture {
    fn record(&self, slot: usize) -> SegmentRecord {
        let start_offset = self.start(slot);
        SegmentRecord {
            start_offset,
            end_offset: start_offset + MESSAGES_PER_SEGMENT - 1,
            start_timestamp: FIXTURE_TIMESTAMP,
            end_timestamp: FIXTURE_TIMESTAMP,
            max_timestamp: FIXTURE_TIMESTAMP,
            size: self.logs[slot].len() as u64,
            incarnation: PREFIX.created_revision,
            hash: *blake3::hash(&self.logs[slot]).as_bytes(),
            crc32c: crc32c::crc32c(&self.logs[slot]),
            source: HashSource::CommitPath,
        }
    }

    fn start(&self, slot: usize) -> u64 {
        self.starts[slot]
    }

    async fn write_chain(&self, records: &[ChainRecord]) {
        create_chain(&self.directory, &PREFIX).await.unwrap();
        let path = chain_path(&self.directory);
        for (index, record) in records.iter().enumerate() {
            append_record(&path, (HEADER_SIZE + index * RECORD_SIZE) as u64, record)
                .await
                .unwrap();
        }
    }

    fn messages_path(&self, start_offset: u64) -> String {
        self.config
            .get_messages_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset)
    }

    fn index_path(&self, start_offset: u64) -> String {
        self.config
            .get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset)
    }

    /// The unlink an eviction job makes after its record is durable.
    fn unlink_log(&self, start_offset: u64) {
        fs::remove_file(self.messages_path(start_offset)).unwrap();
    }

    async fn recover(
        &self,
        checkpoint: Option<SegmentPosition>,
        stats: &PartitionStats,
    ) -> Result<RecoveredLog, PartitionRecoveryError> {
        load_persisted_segments_with_checkpoint(
            &self.config,
            IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID),
            IggyByteSize::from(SEGMENT_MAX_SIZE),
            false,
            stats,
            checkpoint,
        )
        .await
    }

    async fn refusal(&self, checkpoint: Option<SegmentPosition>) -> PartitionRecoveryRefusal {
        match self.recover(checkpoint, &PartitionStats::default()).await {
            Err(PartitionRecoveryError::Refused { reason, .. }) => reason,
            Err(error) => panic!("expected a refusal, got {error}"),
            Ok(_) => panic!("expected a refusal, the chain was accepted"),
        }
    }
}

fn layout(recovered: &RecoveredLog) -> Vec<(u64, u64, SegmentLocation)> {
    recovered
        .segments
        .iter()
        .map(|recovered| {
            (
                recovered.segment.start_offset,
                recovered.segment.end_offset,
                recovered.segment.location,
            )
        })
        .collect()
}

/// A crash after the unlink, before the owner saw the eviction finish.
#[compio::test]
async fn given_a_remote_record_and_no_log_when_recovering_should_keep_the_segment_tiered() {
    let fixture = fixture();
    let record = fixture.record(0);
    fixture
        .write_chain(&[ChainRecord::Sealed(record), ChainRecord::Remote(record)])
        .await;
    fixture.unlink_log(0);
    let stats = PartitionStats::default();

    let recovered = fixture.recover(None, &stats).await.unwrap();

    assert_eq!(
        layout(&recovered),
        [
            (0, 2, SegmentLocation::Remote),
            (3, 5, SegmentLocation::Local),
            (6, 8, SegmentLocation::Local),
        ]
    );
    let tiered = &recovered.segments[0];
    assert!(tiered.segment.sealed);
    assert_eq!(tiered.segment.size.as_bytes_u64(), record.size);
    assert_eq!(tiered.segment.max_timestamp, record.max_timestamp);
    assert!(tiered.storage.messages_reader.is_none());
    assert_eq!(
        tiered.storage.segment_and_index_paths().1,
        Some(fixture.index_path(0))
    );
    assert!(
        Path::new(&fixture.index_path(0)).exists(),
        "boot keeps the index of a segment whose log left the disk"
    );
    let total: u64 = fixture.logs.iter().map(|log| log.len() as u64).sum();
    assert_eq!(stats.size_bytes_inconsistent(), total);
    assert_eq!(stats.segments_count_inconsistent(), 3);
    assert_eq!(stats.messages_count_inconsistent(), 9);
    assert_eq!(recovered.chain.unwrap().records.len(), 2);
}

/// A crash between the record's fsync and the unlink leaves a local segment,
/// whose eviction resumes from the record.
#[compio::test]
async fn given_a_remote_record_beside_its_log_when_recovering_should_keep_the_segment_local() {
    let fixture = fixture();
    let record = fixture.record(0);
    fixture
        .write_chain(&[ChainRecord::Sealed(record), ChainRecord::Remote(record)])
        .await;

    let recovered = fixture
        .recover(None, &PartitionStats::default())
        .await
        .unwrap();

    assert_eq!(
        recovered.segments[0].segment.location,
        SegmentLocation::Local
    );
    assert!(recovered.segments[0].storage.messages_reader.is_some());
    assert_eq!(
        recovered.chain.unwrap().live_segments().get(&0),
        Some(&ChainRecord::Remote(record))
    );
    assert_eq!(fs::read(fixture.messages_path(0)).unwrap(), fixture.logs[0]);
}

/// A crash mid-append tears the record before the unlink ever ran.
#[compio::test]
async fn given_a_torn_last_record_when_recovering_should_truncate_it_and_keep_the_log() {
    let fixture = fixture();
    let record = fixture.record(0);
    fixture.write_chain(&[ChainRecord::Sealed(record)]).await;
    let path = chain_path(&fixture.directory);
    let mut torn = fs::read(&path).unwrap();
    torn.extend_from_slice(&ChainRecord::Remote(record).encode()[..RECORD_SIZE / 2]);
    fs::write(&path, &torn).unwrap();

    let recovered = fixture
        .recover(None, &PartitionStats::default())
        .await
        .unwrap();

    let chain = recovered.chain.unwrap();
    assert!(!chain.torn_tail);
    assert_eq!(chain.records, [ChainRecord::Sealed(record)]);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        (HEADER_SIZE + RECORD_SIZE) as u64
    );
    assert_eq!(
        recovered.segments[0].segment.location,
        SegmentLocation::Local
    );
}

#[compio::test]
async fn given_an_undecodable_record_or_header_when_recovering_should_refuse_before_any_sweep() {
    let fixture = fixture();
    let first = fixture.record(0);
    fixture
        .write_chain(&[
            ChainRecord::Remote(first),
            ChainRecord::Sealed(fixture.record(1)),
        ])
        .await;
    fixture.unlink_log(0);
    let path = chain_path(&fixture.directory);
    let pristine = fs::read(&path).unwrap();

    let mut damaged = pristine.clone();
    damaged[HEADER_SIZE + 9] ^= 1;
    fs::write(&path, &damaged).unwrap();
    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::ChainUnreadable { record: Some(0) }
    ));

    let mut damaged = pristine;
    damaged[1] ^= 1;
    fs::write(&path, &damaged).unwrap();
    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::ChainUnreadable { record: None }
    ));
    assert!(
        Path::new(&fixture.index_path(0)).exists(),
        "a refused chain leaves the tiered segment's index for the quarantine"
    );
}

#[compio::test]
async fn given_a_record_of_another_incarnation_when_recovering_should_refuse() {
    let fixture = fixture();
    let foreign = SegmentRecord {
        incarnation: PREFIX.created_revision + 1,
        ..fixture.record(0)
    };
    fixture.write_chain(&[ChainRecord::Sealed(foreign)]).await;

    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::ForeignChainRecord {
            start_offset: 0,
            incarnation: 6,
            created_revision: 5,
        }
    ));
}

#[compio::test]
async fn given_a_tiered_segment_outside_the_front_when_recovering_should_refuse() {
    let fixture = fixture();
    fixture
        .write_chain(&[ChainRecord::Remote(fixture.record(1))])
        .await;
    fixture.unlink_log(3);
    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::TieredOutsidePrefix {
            tiered_start: 3,
            local_start: Some(0),
        }
    ));

    let fixture = fixture_at(&STARTS[..1]);
    fixture
        .write_chain(&[ChainRecord::Remote(fixture.record(0))])
        .await;
    fixture.unlink_log(0);
    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::TieredOutsidePrefix {
            tiered_start: 0,
            local_start: None,
        }
    ));
}

#[compio::test]
async fn given_a_tiered_segment_reaching_the_checkpoint_segment_when_recovering_should_refuse() {
    let fixture = fixture();
    fixture
        .write_chain(&[
            ChainRecord::Remote(fixture.record(0)),
            ChainRecord::Remote(fixture.record(1)),
        ])
        .await;
    fixture.unlink_log(0);
    fixture.unlink_log(3);
    let checkpoint = SegmentPosition {
        start_offset: 3,
        length: fixture.logs[1].len() as u64,
        next_offset: 6,
        first_timestamp: 0,
    };

    assert!(matches!(
        fixture.refusal(Some(checkpoint)).await,
        PartitionRecoveryRefusal::TieredAtCheckpoint {
            tiered_start: 3,
            checkpoint_start: 3,
        }
    ));
}

/// Contiguity runs over record bounds, so a lost `.log` with no record is
/// still a hole behind a tiered front.
#[compio::test]
async fn given_a_missing_log_without_a_record_after_the_tiered_front_when_recovering_should_refuse_a_hole()
 {
    let fixture = fixture();
    fixture
        .write_chain(&[ChainRecord::Remote(fixture.record(0))])
        .await;
    fixture.unlink_log(0);
    fixture.unlink_log(3);

    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::Hole {
            previous_start: 0,
            previous_end: 2,
            next_start: 6,
            ..
        }
    ));
}

/// The boot re-anchor's gap stays admitted when the segments around it are
/// tiered, and the anchor of a tiered segment is kept like its index.
#[compio::test]
async fn given_an_anchored_gap_between_tiered_segments_when_recovering_should_admit_it() {
    let fixture = fixture_at(&[0, 10, 13]);
    write_anchor(
        &fixture.directory,
        SegmentAnchor {
            planted_start: 10,
            sealed_start: 0,
            sealed_end: 2,
        },
    )
    .await
    .unwrap();
    fixture
        .write_chain(&[
            ChainRecord::Remote(fixture.record(0)),
            ChainRecord::Remote(fixture.record(1)),
        ])
        .await;
    fixture.unlink_log(0);
    fixture.unlink_log(10);

    let recovered = fixture
        .recover(None, &PartitionStats::default())
        .await
        .unwrap();

    assert_eq!(
        layout(&recovered),
        [
            (0, 2, SegmentLocation::Remote),
            (10, 12, SegmentLocation::Remote),
            (13, 15, SegmentLocation::Local),
        ]
    );
    assert!(Path::new(&anchor_path(&fixture.directory, 10)).exists());
}

#[compio::test]
async fn given_a_front_above_the_trim_point_when_recovering_should_refuse() {
    let fixture = fixture();
    fixture.write_chain(&[ChainRecord::TrimPoint(1)]).await;
    fixture.unlink_log(0);
    fs::remove_file(fixture.index_path(0)).unwrap();

    assert!(matches!(
        fixture.refusal(None).await,
        PartitionRecoveryRefusal::FrontAboveTrimPoint {
            first_start: 3,
            trim_point: 1,
        }
    ));
}

/// Retention raises the trim point before it unlinks, so a record at or below
/// it describes a segment retention removed, not one to serve.
#[compio::test]
async fn given_records_the_trim_point_retired_when_recovering_should_drop_them() {
    let fixture = fixture();
    fixture
        .write_chain(&[
            ChainRecord::Remote(fixture.record(0)),
            ChainRecord::TrimPoint(2),
        ])
        .await;
    fixture.unlink_log(0);

    let recovered = fixture
        .recover(None, &PartitionStats::default())
        .await
        .unwrap();

    assert_eq!(
        layout(&recovered),
        [
            (3, 5, SegmentLocation::Local),
            (6, 8, SegmentLocation::Local)
        ]
    );
    assert!(
        !Path::new(&fixture.index_path(0)).exists(),
        "the index of a retired segment is swept like any orphan"
    );
}
