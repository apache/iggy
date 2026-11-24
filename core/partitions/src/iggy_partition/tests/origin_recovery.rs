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

//! An empty placing partition that installs what a recovery took of its
//! retired cluster, and goes on from there.

use super::placed_batches::{
    placed_segment_prepare, placed_segment_size, placing_backup_with_wal, stored_offsets,
};
use super::*;
use crate::origin_recovery::{RecoveryInstallError, RecoveryPlan};
use crate::segment_chain::object_prefix;
use iggy_binary_protocol::batch::{BatchHeader, BatchProducer};
use iggy_binary_protocol::placement::{PlacedBatch, Placement};
use iggy_object_store::{ManifestSegment, PartitionManifest, SEGMENT_HASH_LEN};

const NOW: u64 = 1_700_000_000_000_000;
/// The manifest of the retired cluster lists offsets 0 through 9.
const FLOOR: u64 = 9;

fn segment(start_offset: u64, end_offset: u64) -> ManifestSegment {
    ManifestSegment {
        start_offset,
        end_offset,
        start_timestamp: NOW + start_offset,
        end_timestamp: NOW + end_offset,
        max_timestamp: NOW + end_offset,
        size: 1024,
        crc32c: 5,
        hash: [6; SEGMENT_HASH_LEN],
    }
}

/// A one-message placed batch at `offset`, whose messages are the
/// `offset`-th blob of a fresh object of the retired cluster.
fn recovered_batch(offset: u64, producer: Option<BatchProducer>) -> PlacedBatch {
    let blob = vec![u8::try_from(offset).unwrap(); 32];
    PlacedBatch::new(
        BatchHeader {
            partition_id: 0,
            base_offset: offset,
            base_timestamp: NOW + 100 + offset,
            origin_timestamp: NOW,
            batch_length: 256 + 32,
            batch_checksum: 0,
            message_count: 1,
            producer,
        },
        Placement::of(0x5eed, offset * 64, &blob, 0),
    )
}

/// A placing backup with a WAL, with the machines every builder registers.
async fn empty_partition(root: &Path) -> (PartitionsConfig, IggyPartition<RecordingBus>) {
    let (config, mut partition) = Box::pin(placing_backup_with_wal(root)).await;
    partition.register_builtin_state_machines().unwrap();
    let directory = config.get_partition_path(1, 1, 0);
    set_offset_dirs_under(&mut partition, Path::new(&directory));
    (config, partition)
}

/// The plan of this partition: two segments up to the floor, two proven
/// batches and one adopted one above it, the last of an idempotent producer.
fn plan_of<B: MessageBus, SB: SuperblockStore>(partition: &IggyPartition<B, SB>) -> RecoveryPlan {
    let prefix = object_prefix(partition.namespace(), partition.created_revision());
    RecoveryPlan {
        retired_cluster_uid: 0x0a0a,
        prefix,
        manifest: Some(PartitionManifest {
            prefix,
            published_at: NOW,
            floor: Some(FLOOR),
            trim_point: None,
            segments_keyed_by_both_offsets: true,
            segments: vec![segment(0, 4), segment(5, FLOOR)],
        }),
        proven_end: FLOOR + 3,
        batches: vec![
            recovered_batch(FLOOR + 1, None),
            recovered_batch(FLOOR + 2, None),
            recovered_batch(FLOOR + 3, BatchProducer::new(42, 1, 0)),
        ],
    }
}

#[compio::test]
async fn given_a_recovery_plan_when_an_empty_partition_installs_it_should_hold_its_batches_and_go_on_after_the_cutover()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(empty_partition(root.path())).await;
    let directory = config.get_partition_path(1, 1, 0);
    let plan = plan_of(&partition);

    Box::pin(partition.install_recovery_plan(&config, &plan))
        .await
        .unwrap();
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.offset.load(Ordering::Acquire), FLOOR + 3);
    assert_eq!(partition.mint_frontier(), FLOOR + 4);
    assert_eq!(partition.long_term_floor(), Some(FLOOR));
    assert_eq!(
        partition.long_term_segments(),
        plan.manifest.as_ref().unwrap().segments.as_slice()
    );
    assert_eq!(
        stored_offsets(&directory),
        [FLOOR + 1, FLOOR + 2, FLOOR + 3]
    );
    let producers = partition.producer_entries();
    assert_eq!(producers.len(), 1);
    assert_eq!(producers[0].0, 42);

    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let writer = hold_wal_writer(&persistence);
    Box::pin(partition.on_replicate(placed_segment_prepare(1, 0, FLOOR + 4))).await;
    writer.await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(partition.offset.load(Ordering::Acquire), FLOOR + 4);
    assert_eq!(
        stored_offsets(&directory),
        [FLOOR + 1, FLOOR + 2, FLOOR + 3, FLOOR + 4]
    );
    assert_eq!(partition.long_term_floor(), Some(FLOOR));
}

#[compio::test]
async fn given_a_plan_of_only_a_manifest_when_installing_should_start_the_offsets_after_its_floor()
{
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(empty_partition(root.path())).await;
    let mut plan = plan_of(&partition);
    plan.batches.clear();
    plan.proven_end = FLOOR + 1;

    Box::pin(partition.install_recovery_plan(&config, &plan))
        .await
        .unwrap();
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.mint_frontier(), FLOOR + 1);
    assert_eq!(partition.long_term_floor(), Some(FLOOR));
    assert_eq!(partition.producer_entries().len(), 0);
}

#[compio::test]
async fn given_a_partition_that_is_not_empty_or_another_one_when_installing_should_refuse() {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(empty_partition(root.path())).await;
    let mut other = plan_of(&partition);
    other.prefix.created_revision += 1;
    if let Some(manifest) = other.manifest.as_mut() {
        manifest.prefix = other.prefix;
    }

    assert!(matches!(
        Box::pin(partition.install_recovery_plan(&config, &other)).await,
        Err(RecoveryInstallError::OtherPartition { .. })
    ));

    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let writer = hold_wal_writer(&persistence);
    Box::pin(partition.on_replicate(placed_segment_prepare(1, 0, 0))).await;
    writer.await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    let plan = plan_of(&partition);

    assert!(matches!(
        Box::pin(partition.install_recovery_plan(&config, &plan)).await,
        Err(RecoveryInstallError::NotEmpty)
    ));
    assert_eq!(stored_offsets(&config.get_partition_path(1, 1, 0)), [0]);
}

/// The server's order at boot, as for any placing backup with a WAL: the
/// installed batches, the next offset and the long-term log all come back.
#[compio::test]
async fn given_an_installed_plan_when_the_partition_restarts_should_keep_its_offsets_and_its_long_term_log()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(empty_partition(root.path())).await;
    let directory = config.get_partition_path(1, 1, 0);
    let plan = plan_of(&partition);
    Box::pin(partition.install_recovery_plan(&config, &plan))
        .await
        .unwrap();
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    hold_wal_writer(partition.persistence.as_ref().unwrap()).await;
    drop(partition);

    let mut partition = Box::pin(reopen(&config, &directory)).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.long_term_floor(), Some(FLOOR));
    assert_eq!(partition.mint_frontier(), FLOOR + 4);
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let writer = hold_wal_writer(&persistence);
    Box::pin(partition.on_replicate(placed_segment_prepare(1, 0, FLOOR + 4))).await;
    writer.await;
    Box::pin(commit_on_heartbeat(&mut partition, &config, 1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert!(partition.fatal().is_none());
    assert_eq!(
        stored_offsets(&directory),
        [FLOOR + 1, FLOOR + 2, FLOOR + 3, FLOOR + 4]
    );
}

/// The placing backup of `directory` as a boot opens it again. The offset
/// counter starts where the sized segments end, as the server's boot sets it.
async fn reopen(config: &PartitionsConfig, directory: &str) -> IggyPartition<RecordingBus> {
    let wal = Path::new(directory).join("prepares-0");
    let namespace = IggyNamespace::new(1, 1, 0);
    let recovered_wal = PartitionPersistence::open(&wal, namespace.inner(), 0)
        .await
        .unwrap();
    let recovered = crate::load_placed_segments(
        config,
        namespace,
        placed_segment_size(),
        &PartitionStats::default(),
        recovered_wal.0.segment_checkpoint(),
    )
    .await
    .unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.register_builtin_state_machines().unwrap();
    partition.set_places_batches(true);
    partition.set_partition_dir(directory.to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.runtime_options.segment_size = Some(placed_segment_size());
    Box::pin(partition.restore_retry_checkpoint(recovered_wal.0.checkpoint_op()))
        .await
        .unwrap();
    partition.log.retire_front().unwrap();
    crate::partition_storage::hydrate_partition_log(&mut partition, directory, 1, 1, 0, recovered)
        .await
        .unwrap();
    partition
        .open_persistence_with_recovered(
            journal::partition_journal::PARTITION_WAL_BYTES_MAX,
            std::time::Duration::ZERO,
            Some(recovered_wal),
        )
        .await
        .unwrap();
    let sized_end = partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.size.as_bytes_u64() > 0)
        .map(|segment| segment.end_offset)
        .max()
        .unwrap();
    partition.offset.store(sized_end, Ordering::Release);
    partition.dirty_offset.store(sized_end, Ordering::Relaxed);
    partition.set_offset_space_used(true);
    set_offset_dirs_under(&mut partition, Path::new(directory));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
}
