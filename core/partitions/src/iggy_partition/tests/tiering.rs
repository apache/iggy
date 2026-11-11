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

use super::eviction::{KEEP_NOTHING, chain_records, evict};
use super::segment_chain::PREFIX;
use super::*;
use crate::SegmentEviction;
use crate::segment_chain::{
    ChainRecord, HashSource, ObjectPrefix, chain_path, create_chain, hash_segment_file,
};

/// A partition with [`RECEIVER_OPS`] one-message ops and no chain: two sealed
/// segments and an active one that holds a message.
async fn untiered_partition() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
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
    Box::pin(commit_records(
        &mut partition,
        1,
        &segment_records(RECEIVER_OPS),
    ))
    .await;
    (directory, partition)
}

async fn enable(partition: &mut IggyPartition<IggyMessageBus>, prefix: ObjectPrefix) {
    partition.enable_tiering(prefix);
    Box::pin(settle_partition_io(partition, &repair_config())).await;
}

/// The sealed records of the chain on disk, each checked against the hash of
/// its `.log`, as `(start offset, source)`.
async fn recorded(directory: &Path) -> Vec<(u64, HashSource)> {
    let mut sealed = Vec::new();
    for record in chain_records(directory).await {
        let ChainRecord::Sealed(segment) = record else {
            continue;
        };
        let log = directory.join(format!("{:020}.log", segment.start_offset));
        let on_disk = hash_segment_file(&log.to_string_lossy(), segment.size)
            .await
            .unwrap();
        assert_eq!(
            (segment.hash, segment.crc32c),
            (on_disk.blake3, on_disk.crc32c),
            "segment {}",
            segment.start_offset
        );
        sealed.push((segment.start_offset, segment.source));
    }
    sealed
}

#[compio::test]
async fn given_a_partition_with_sealed_segments_when_tiering_is_enabled_should_record_each_from_disk()
 {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    assert!(!partition.is_tiered());
    assert_eq!(
        Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await,
        SegmentEviction::default(),
        "a partition without a chain evicts nothing"
    );

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert!(partition.is_tiered());
    assert_eq!(partition.object_prefix(), Some(PREFIX));
    assert_eq!(partition.chain_trim_point(), None, "the front is at zero");
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(0, HashSource::Disk), (2, HashSource::Disk)]
    );
    for start_offset in [0, 2] {
        assert!(partition.sealed_segment_record(start_offset).is_some());
    }

    // The active segment held a message before the chain existed, so it
    // seals without a commit-path hash and is hashed from disk too. The
    // segment after it started under the chain.
    Box::pin(commit_records(
        &mut partition,
        RECEIVER_OPS + 1,
        &segment_records(4),
    ))
    .await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [
            (0, HashSource::Disk),
            (2, HashSource::Disk),
            (4, HashSource::Disk),
            (6, HashSource::CommitPath),
        ]
    );
}

#[compio::test]
async fn given_a_front_that_retention_moved_when_tiering_is_enabled_should_record_it_as_the_trim_point()
 {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let first_end = partition.log.segments()[0].end_offset;
    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    let front = partition.log.segments()[0].start_offset;
    assert_eq!(front, first_end + 1);

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert_eq!(partition.chain_trim_point(), Some(front - 1));
    assert_eq!(
        chain_records(directory.path()).await[0],
        ChainRecord::TrimPoint(front - 1)
    );
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(front, HashSource::Disk)]
    );
}

#[compio::test]
async fn given_a_complete_chain_when_tiering_is_enabled_again_should_append_nothing() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    Box::pin(enable(&mut partition, PREFIX)).await;
    let chain = std::fs::read(chain_path(&directory.path().to_string_lossy())).unwrap();

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert_eq!(
        std::fs::read(chain_path(&directory.path().to_string_lossy())).unwrap(),
        chain
    );
}

#[compio::test]
async fn given_a_chain_of_another_incarnation_when_tiering_is_enabled_should_stay_untiered() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let foreign = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    create_chain(&directory.path().to_string_lossy(), &foreign)
        .await
        .unwrap();

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert!(!partition.is_tiered());
    assert_eq!(chain_records(directory.path()).await, Vec::new());
    // The request is dropped, so the lane does not ask again on every pass.
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
}
