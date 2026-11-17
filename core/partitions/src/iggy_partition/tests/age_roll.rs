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
use iggy_common::IggyExpiry;

/// Microseconds a batch's stamp may run past its segment's first one.
const MAX_AGE: u64 = 100;
/// Large enough that no test here rolls by size.
const SEGMENT_BYTES: u64 = 1024 * 1024;

type Layout = Vec<(u64, u64, bool)>;

fn layout(partition: &IggyPartition<RecordingBus>) -> Layout {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset, segment.sealed))
        .collect()
}

/// A backup of a three-replica group whose WAL owns segment bodies, rolling
/// them by `segment_max_age` as well as by size.
async fn aged_backup(
    directory: &std::path::Path,
    segment_max_age: IggyExpiry,
) -> (IggyPartition<RecordingBus>, PartitionsConfig) {
    let mut config = repair_config();
    config.path_layout.streams_root = directory.to_string_lossy().into_owned();
    let partition_directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&partition_directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(partition_directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.runtime_options.segment_size = Some(IggyByteSize::from(SEGMENT_BYTES));
    partition.runtime_options.segment_max_age = Some(segment_max_age);
    partition.log.active_segment_mut().max_size = IggyByteSize::from(SEGMENT_BYTES);
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    (partition, config)
}

/// Replicate and commit one single-message batch per stamp, from offset
/// `first_op - 1`, returning the last prepare's checksum.
async fn replicate_stamped(
    partition: &mut IggyPartition<RecordingBus>,
    config: &PartitionsConfig,
    first_op: u64,
    mut parent: u128,
    stamps: &[u64],
) -> u128 {
    for (op, stamp) in (first_op..).zip(stamps) {
        let prepare = stamped_segment_prepare(op, parent, op - 1, b"payload", *stamp);
        parent = prepare.header().checksum;
        partition.on_replicate(prepare).await;
        partition
            .persistence
            .as_ref()
            .unwrap()
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        settle_partition_io(partition, config).await;
        commit_on_heartbeat(partition, config, op).await;
        settle_partition_io(partition, config).await;
    }
    partition.flush_committed_messages(config).await.unwrap();
    settle_partition_io(partition, config).await;
    parent
}

/// Where the WAL placed the bodies of ops `1..=last_op`.
fn wal_segment_starts(partition: &IggyPartition<RecordingBus>, last_op: u64) -> Vec<u64> {
    let persistence = partition.persistence.as_ref().unwrap();
    let mut starts: Vec<u64> = (1..=last_op)
        .map(|op| persistence.segment_reference(op).unwrap().start_offset)
        .collect();
    starts.dedup();
    starts
}

#[compio::test]
async fn given_stamps_past_the_age_bound_when_materializing_should_roll_where_the_wal_rolled() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, config) = aged_backup(root.path(), IggyExpiry::from(MAX_AGE)).await;

    replicate_stamped(
        &mut partition,
        &config,
        1,
        0,
        &[1_000, 1_050, 1_099, 1_100, 1_150, 1_200],
    )
    .await;

    assert!(partition.fatal().is_none());
    assert_eq!(
        layout(&partition),
        vec![(0, 2, true), (3, 4, true), (5, 5, false)]
    );
    assert_eq!(wal_segment_starts(&partition, 6), [0, 3, 5]);
    let directory = std::path::PathBuf::from(partition.partition_dir().unwrap());
    for segment in partition.log.segments() {
        let public = directory.join(format!("{:020}.log", segment.start_offset));
        assert_eq!(
            std::fs::metadata(public).unwrap().len(),
            segment.size.as_bytes_u64()
        );
    }
}

#[compio::test]
async fn given_a_first_stamp_ahead_of_later_ones_when_materializing_should_wait_for_the_clock() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, config) = aged_backup(root.path(), IggyExpiry::from(MAX_AGE)).await;

    replicate_stamped(
        &mut partition,
        &config,
        1,
        0,
        &[1_000_000, 500, 900_000, 1_000_099, 1_000_100],
    )
    .await;

    assert!(partition.fatal().is_none());
    assert_eq!(layout(&partition), vec![(0, 3, true), (4, 4, false)]);
    assert_eq!(wal_segment_starts(&partition, 5), [0, 4]);
}

#[compio::test]
async fn given_no_age_bound_when_stamps_spread_out_should_roll_by_size_alone() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, config) = aged_backup(root.path(), IggyExpiry::NeverExpire).await;

    replicate_stamped(
        &mut partition,
        &config,
        1,
        0,
        &[1_000, 1_000_000, 2_000_000],
    )
    .await;

    assert!(partition.fatal().is_none());
    assert_eq!(layout(&partition), vec![(0, 2, false)]);
}

/// Expiry removes only sealed segments, so on a partition that never fills
/// one the age roll is what lets `message_expiry` reach its messages.
#[compio::test]
async fn given_a_slow_partition_when_its_messages_expire_should_remove_only_what_the_age_roll_sealed()
 {
    const STAMPS: [u64; 6] = [1_000, 1_050, 1_099, 1_100, 1_150, 1_200];
    for (segment_max_age, retained) in [
        (IggyExpiry::from(MAX_AGE), vec![(5, 5, false)]),
        (IggyExpiry::NeverExpire, vec![(0, 5, false)]),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut partition, config) = aged_backup(root.path(), segment_max_age).await;
        replicate_stamped(&mut partition, &config, 1, 0, &STAMPS).await;
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        persistence.request_checkpoint();
        partition.checkpoint_persistence(&config).await;
        settle_partition_io(&mut partition, &config).await;
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        settle_partition_io(&mut partition, &config).await;

        partition
            .clean_expired_segments(IggyTimestamp::from(10_000), IggyExpiry::from(1_000), None)
            .await;
        settle_partition_io(&mut partition, &config).await;

        assert!(partition.fatal().is_none());
        assert_eq!(layout(&partition), retained, "{segment_max_age:?}");
    }
}

/// Committed together, the batches reach materialization as one prefix, so the
/// chunk itself has to stop at the batch that ages its segment out.
#[compio::test]
async fn given_one_commit_across_age_rolls_when_materializing_should_split_its_chunks_at_them() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, config) = aged_backup(root.path(), IggyExpiry::from(MAX_AGE)).await;
    let mut parent = 0;
    for (op, stamp) in (1..).zip([1_000, 1_050, 1_099, 1_100, 1_150, 1_200]) {
        let prepare = stamped_segment_prepare(op, parent, op - 1, b"payload", stamp);
        parent = prepare.header().checksum;
        partition.on_replicate(prepare).await;
    }
    partition
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    commit_on_heartbeat(&mut partition, &config, 6).await;
    settle_partition_io(&mut partition, &config).await;
    partition.flush_committed_messages(&config).await.unwrap();
    settle_partition_io(&mut partition, &config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(
        layout(&partition),
        vec![(0, 2, true), (3, 4, true), (5, 5, false)]
    );
    assert_eq!(wal_segment_starts(&partition, 6), [0, 3, 5]);
}

/// A transfer resets the receiver's WAL around the installed active segment,
/// which has to carry the age bound and that segment's first stamp, or the WAL
/// and materialization roll the next batches at different places.
#[compio::test]
async fn given_an_installed_aged_segment_when_replicating_on_should_roll_where_the_origin_would() {
    let first = stamped_segment_prepare(1, 0, 0, b"payload", 1_000);
    let second = stamped_segment_prepare(2, first.header().checksum, 1, b"payload", 1_050);
    let mut body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    body.extend_from_slice(&second.as_slice()[size_of::<PrepareHeader>()..]);
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(second.header().checksum),
        checkpoint_prepare: second.as_slice().to_vec(),
        next_offset: 2,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let root = tempfile::tempdir().unwrap();
    let (mut receiver, config) = aged_backup(root.path(), IggyExpiry::from(MAX_AGE)).await;
    let partition_dir = receiver.partition_dir().unwrap();
    set_offset_dirs_under(&mut receiver, std::path::Path::new(&partition_dir));
    receiver.consensus().advance_commit_max(2);
    let staged = receiver
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    receiver
        .install_state_transfer(&config, 2, vec![staged], &offsets.encode())
        .await
        .unwrap();
    settle_partition_io(&mut receiver, &config).await;
    assert_eq!(receiver.log.active_segment().start_timestamp, 1_000);

    replicate_stamped(
        &mut receiver,
        &config,
        3,
        second.header().checksum,
        &[1_099, 1_100],
    )
    .await;

    assert!(receiver.fatal().is_none());
    assert_eq!(layout(&receiver), vec![(0, 2, true), (3, 3, false)]);
    assert_eq!(
        receiver
            .persistence
            .as_ref()
            .unwrap()
            .segment_reference(4)
            .unwrap()
            .start_offset,
        3
    );
}
