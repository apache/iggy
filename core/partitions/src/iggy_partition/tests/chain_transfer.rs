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

//! A tiered partition's chain crossing a state transfer.

use super::eviction::{KEEP_NOTHING, chain_records, confirm, evict, reboot};
use super::floor_eviction::advance_over;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::SegmentLocation;
use crate::segment_chain::{
    Chain, ChainRecord, HEADER_SIZE, RECORD_SIZE, chain_path, decode_chain,
};
use crate::state_transfer::{PartitionInstallError, PartitionStateTransferOffer};

/// Sealed segments of a partition with [`ORIGIN_OPS`] one-message ops.
const ORIGIN_SEALED: [u64; 3] = [0, 2, 4];

/// An origin whose first two sealed segments left the disk.
async fn tiered_origin() -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);
    let floor = origin.sealed_segment_record(2).unwrap().end_offset;
    Box::pin(evict(&mut origin, floor, KEEP_NOTHING)).await;
    assert_eq!(
        locations(&origin),
        [
            SegmentLocation::Remote,
            SegmentLocation::Remote,
            SegmentLocation::Local,
            SegmentLocation::Local
        ]
    );
    (directory, origin)
}

/// An empty receiver at the path boot recovery looks in under `root`.
async fn receiver_on_disk(root: &Path) -> (PartitionsConfig, Box<IggyPartition<IggyMessageBus>>) {
    let mut config = repair_config();
    config.path_layout.streams_root = root.to_string_lossy().into_owned();
    let directory = config.get_partition_path(1, 1, 0);
    std::fs::create_dir_all(&directory).unwrap();
    let mut receiver = Box::new(test_partition_with_replicas(3));
    set_offset_dirs_under(&mut receiver, Path::new(&directory));
    receiver.set_partition_dir(directory);
    receiver.log.retire_front().unwrap();
    receiver.install_empty_segment(&config, 0).await.unwrap();
    receiver.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    (config, receiver)
}

fn locations<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<SegmentLocation> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.location)
        .collect()
}

fn layout<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<(u64, u64, u64, SegmentLocation)> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| {
            (
                segment.start_offset,
                segment.end_offset,
                segment.size.as_bytes_u64(),
                segment.location,
            )
        })
        .collect()
}

fn offered_chain(offer: &PartitionStateTransferOffer) -> Chain {
    decode_chain(&offer.chain.as_ref().unwrap().1).unwrap()
}

/// Queues the install of `offer` with `chain_bytes` in place of its chain.
async fn install_with_chain(
    receiver: &mut IggyPartition<IggyMessageBus>,
    offer: &PartitionStateTransferOffer,
    chain_bytes: &[u8],
) -> Result<(), PartitionInstallError> {
    let mut staged = Vec::with_capacity(offer.segments.len());
    for source in &offer.segments {
        let mut bytes = std::fs::read(&source.log_path).unwrap();
        bytes.truncate(usize::try_from(source.entry.len).unwrap());
        staged.push(
            receiver
                .spill_transfer_segment(&source.entry, bytes, None)
                .await
                .unwrap(),
        );
    }
    receiver.queue_state_transfer_install_with_chain(
        offer.commit_op,
        staged,
        &offer.offsets.1,
        Some(chain_bytes),
        0,
    )
}

#[compio::test]
async fn given_a_tiered_origin_when_offering_should_carry_its_chain_and_no_remote_bytes() {
    let (_directory, mut origin) = Box::pin(tiered_origin()).await;

    let offer = Box::pin(serve_offer(&mut origin)).await;

    let offered: Vec<u64> = offer
        .segments
        .iter()
        .map(|source| source.entry.frontier)
        .collect();
    assert_eq!(offered, [4, 6], "remote segments send no bytes");
    let manifest = offer.manifest();
    assert_eq!(manifest.len(), offer.artifact_count());
    assert_eq!(
        manifest[2].kind,
        consensus::state_manifest::artifact_kind::SEGMENT_CHAIN
    );
    assert!(matches!(
        offer.artifact_at(2),
        Some(crate::state_transfer::PartitionArtifactSource::Chain(_))
    ));
    assert!(matches!(
        offer.artifact_at(3),
        Some(crate::state_transfer::PartitionArtifactSource::Offsets(_))
    ));
    let chain = offered_chain(&offer);
    assert_eq!(chain.prefix, PREFIX);
    let sealed = ORIGIN_SEALED.map(|start| origin.sealed_segment_record(start).unwrap());
    assert_eq!(
        chain.records,
        [
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Remote(sealed[1]),
            ChainRecord::Sealed(sealed[2]),
            // The local segment that an advance confirmed carries its proof.
            ChainRecord::Advanced(crate::tiered_floor::AdvanceTieredFloor {
                start: sealed[2].start_offset,
                up_to: sealed[2].end_offset,
                hash: sealed[2].hash,
            }),
        ]
    );
}

/// A replica rebuilt by a transfer never applies the advances below the floor
/// it starts from, so the proof that a local segment is stored has to arrive
/// with the chain, or that segment would stay on its disk for good.
#[compio::test]
async fn given_a_confirmed_local_segment_when_its_chain_is_installed_should_let_the_receiver_evict_it()
 {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    assert_eq!(locations(&receiver)[2], SegmentLocation::Local);
    drop(receiver);

    // The proof is in the chain file, so it also survives the restart.
    let mut rebooted = Box::pin(reboot(&config)).await;
    Box::pin(evict(&mut rebooted, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(locations(&rebooted)[2], SegmentLocation::Remote);
}

/// The plan is pinned in a turn that can come before the I/O lane records an
/// advance the origin applied. The receiver starts from a floor that covers
/// the advance, so it never applies it again.
#[compio::test]
async fn given_an_advance_not_yet_in_the_chain_when_its_chain_is_installed_should_let_the_receiver_evict_the_segment()
 {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let mut origin = Box::new(origin);
    let advance = Box::pin(advance_over(directory.path(), (0, 1))).await;
    origin.note_tiered_advance(advance);
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    drop(receiver);

    let mut rebooted = Box::pin(reboot(&config)).await;
    Box::pin(evict(&mut rebooted, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(locations(&rebooted)[0], SegmentLocation::Remote);
}

/// A rebuilt replica holds the sender's records and front, keeps no tiered
/// bytes, and boots back into the same layout.
#[compio::test]
async fn given_a_tiered_offer_when_installed_should_hold_the_senders_chain_and_boot_into_it() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    assert_eq!(layout(&receiver), layout(&origin));
    assert_eq!(receiver.object_prefix(), Some(PREFIX));
    for start_offset in ORIGIN_SEALED {
        assert_eq!(
            receiver.sealed_segment_record(start_offset),
            origin.sealed_segment_record(start_offset)
        );
    }
    for start_offset in [0, 2] {
        for extension in ["log", "index"] {
            assert!(
                !directory
                    .join(format!("{start_offset:020}.{extension}"))
                    .exists()
            );
        }
    }
    assert_eq!(
        chain_records(&directory).await,
        offered_chain(&offer).records
    );
    assert_eq!(
        receiver.stats.size_bytes_inconsistent(),
        origin.stats.size_bytes_inconsistent()
    );
    drop(receiver);

    let rebooted = Box::pin(reboot(&config)).await;
    assert_eq!(layout(&rebooted)[..3], layout(&origin)[..3]);
    assert_eq!(rebooted.object_prefix(), Some(PREFIX));
}

/// An eviction whose record is durable but whose `.log` stayed: the segment
/// is still local, so its bytes travel and its record travels as sealed.
#[compio::test]
async fn given_an_eviction_cut_short_before_its_unlink_when_offering_should_send_the_segment_as_local()
 {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);
    let floor = origin.sealed_segment_record(2).unwrap().end_offset;
    let log = directory.path().join(format!("{:020}.log", 2));
    let bytes = std::fs::read(&log).unwrap();
    // Unlinking a directory fails even for root.
    std::fs::remove_file(&log).unwrap();
    std::fs::create_dir(&log).unwrap();
    Box::pin(evict(&mut origin, floor, KEEP_NOTHING)).await;
    std::fs::remove_dir(&log).unwrap();
    std::fs::write(&log, bytes).unwrap();
    let sealed = ORIGIN_SEALED.map(|start| origin.sealed_segment_record(start).unwrap());
    assert_eq!(
        chain_records(directory.path()).await[3..],
        [
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Remote(sealed[1])
        ],
        "the interrupted eviction left its record"
    );
    assert_eq!(
        locations(&origin),
        [
            SegmentLocation::Remote,
            SegmentLocation::Local,
            SegmentLocation::Local,
            SegmentLocation::Local
        ]
    );

    let offer = Box::pin(serve_offer(&mut origin)).await;

    let offered: Vec<u64> = offer
        .segments
        .iter()
        .map(|source| source.entry.frontier)
        .collect();
    assert_eq!(offered, [2, 4, 6]);
    // The receiver gets the bytes of both sealed segments, so it also gets the
    // advances that let it evict them, the interrupted one included.
    let advanced = |segment: crate::segment_chain::SegmentRecord| {
        ChainRecord::Advanced(crate::tiered_floor::AdvanceTieredFloor {
            start: segment.start_offset,
            up_to: segment.end_offset,
            hash: segment.hash,
        })
    };
    assert_eq!(
        offered_chain(&offer).records,
        [
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Sealed(sealed[1]),
            ChainRecord::Sealed(sealed[2]),
            advanced(sealed[1]),
            advanced(sealed[2]),
        ]
    );
    let root = tempfile::tempdir().unwrap();
    let (_config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    assert_eq!(layout(&receiver), layout(&origin));
}

/// A chain as the file holds it, with a segment's sealed record ahead of its
/// remote one and a segment the trim point retired: the install plants,
/// counts and keeps the latest record of each live segment.
#[compio::test]
async fn given_a_chain_with_superseded_records_when_installed_should_keep_the_latest_of_each() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let first_end = origin.sealed_segment_record(0).unwrap().end_offset;
    let retired = origin.sealed_segment_record(0).unwrap();
    Box::pin(origin.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut origin, &repair_config())).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let sealed = [2, 4].map(|start| origin.sealed_segment_record(start).unwrap());
    let mut chain = offered_chain(&offer);
    chain.records = vec![
        ChainRecord::Sealed(retired),
        ChainRecord::Sealed(sealed[0]),
        ChainRecord::Remote(retired),
        ChainRecord::Sealed(sealed[1]),
        ChainRecord::TrimPoint(first_end),
        ChainRecord::Remote(sealed[0]),
    ];
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_with_chain(&mut receiver, &offer, &chain.encode()))
        .await
        .unwrap();
    Box::pin(finish_install_io(&mut receiver)).await.unwrap();

    assert_eq!(layout(&receiver), layout(&origin));
    assert_eq!(receiver.chain_trim_point(), Some(first_end));
    assert!(receiver.sealed_segment_record(0).is_none());
    for record in sealed {
        assert_eq!(
            receiver.sealed_segment_record(record.start_offset),
            Some(record)
        );
    }
    assert_eq!(
        receiver.stats.size_bytes_inconsistent(),
        origin.stats.size_bytes_inconsistent()
    );
    assert_eq!(
        receiver.stats.messages_count_inconsistent(),
        origin.stats.messages_count_inconsistent()
    );
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    assert_eq!(chain_records(&directory).await, chain.records);
}

/// The trim point travels, so the receiver's boot accepts the same front.
#[compio::test]
async fn given_a_chain_with_a_trim_point_when_installed_should_keep_it() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let first_end = origin.sealed_segment_record(0).unwrap().end_offset;
    Box::pin(origin.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut origin, &repair_config())).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    assert_eq!(
        offered_chain(&offer).records.first(),
        Some(&ChainRecord::TrimPoint(first_end))
    );
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(receiver.chain_trim_point(), Some(first_end));
    assert_eq!(receiver.log.segments()[0].start_offset, first_end + 1);
    drop(receiver);
    let rebooted = Box::pin(reboot(&config)).await;
    assert_eq!(rebooted.chain_trim_point(), Some(first_end));
}

#[compio::test]
async fn given_a_chain_the_receiver_cannot_take_when_installing_should_refuse_it() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let chain = offered_chain(&offer);

    let mut torn = chain.encode();
    torn.truncate(torn.len() - 1);
    assert!(matches!(
        Box::pin(refusal(&offer, torn)).await,
        PartitionInstallError::ChainUnreadable
    ));

    let moved = crate::segment_chain::ObjectPrefix {
        partition_id: PREFIX.partition_id + 1,
        ..PREFIX
    };
    let foreign = Chain {
        prefix: moved,
        ..chain.clone()
    };
    assert!(matches!(
        Box::pin(refusal(&offer, foreign.encode())).await,
        PartitionInstallError::ForeignChain { recorded } if recorded == moved
    ));

    let mut mixed = chain.clone();
    if let ChainRecord::Remote(segment) = &mut mixed.records[1] {
        segment.incarnation += 1;
    }
    assert!(matches!(
        Box::pin(refusal(&offer, mixed.encode())).await,
        PartitionInstallError::ForeignChain { recorded } if recorded == PREFIX
    ));

    // Remote segments 0 and 4, with 2 missing between them.
    let mut gapped = chain.clone();
    gapped.records.remove(1);
    if let ChainRecord::Sealed(segment) = gapped.records[1] {
        gapped.records[1] = ChainRecord::Remote(segment);
    }
    gapped.valid_len -= RECORD_SIZE as u64;
    assert!(matches!(
        Box::pin(refusal(&offer, gapped.encode())).await,
        PartitionInstallError::SegmentSetHole {
            previous_end: 1,
            next_start: 4,
        }
    ));

    let mut holed = chain.clone();
    holed.records.remove(1);
    holed.valid_len -= RECORD_SIZE as u64;
    assert!(matches!(
        Box::pin(refusal(&offer, holed.encode())).await,
        PartitionInstallError::SegmentSetHole {
            previous_end: 1,
            next_start: 4,
        }
    ));

    let mut trimmed = chain;
    trimmed.records.insert(0, ChainRecord::TrimPoint(0));
    trimmed.records.remove(1);
    trimmed.valid_len = (HEADER_SIZE + trimmed.records.len() * RECORD_SIZE) as u64;
    assert!(matches!(
        Box::pin(refusal(&offer, trimmed.encode())).await,
        PartitionInstallError::ChainFrontAboveTrimPoint {
            first_start: 2,
            trim_point: 0,
        }
    ));
}

/// Why a fresh receiver refuses `offer` carrying `chain_bytes`.
async fn refusal(
    offer: &PartitionStateTransferOffer,
    chain_bytes: Vec<u8>,
) -> PartitionInstallError {
    let (_directory, mut receiver) = Box::pin(transfer_receiver_on_disk()).await;
    Box::pin(install_with_chain(&mut receiver, offer, &chain_bytes))
        .await
        .unwrap_err()
}

/// A receiver with a directory, on the same bus as [`tiered_partition`].
async fn transfer_receiver_on_disk() -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    let config = repair_config();
    let (directory, receiver) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let receiver = Box::new(receiver);
    receiver.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    (directory, receiver)
}

/// An origin without a chain offers none, and a receiver with one drops it.
#[compio::test]
async fn given_an_offer_without_a_chain_when_a_tiered_receiver_installs_should_drop_its_chain() {
    let (_origin_directory, origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let mut origin = Box::new(origin);
    let offer = Box::pin(serve_offer(&mut origin)).await;
    assert!(offer.chain.is_none());
    drop(origin);
    let (directory, receiver) = Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    let mut receiver = Box::new(receiver);

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert!(receiver.object_prefix().is_none());
    assert!(!Path::new(&chain_path(&directory.path().to_string_lossy())).exists());
}
