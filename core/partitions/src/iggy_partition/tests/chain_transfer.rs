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
use super::remote_poll::{attach, layout};
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::segment_chain::{
    Chain, ChainRecord, HEADER_SIZE, RECORD_SIZE, chain_path, decode_chain,
};
use crate::segment_upload::{SegmentUpload, upload_segment};
use crate::state_transfer::{PartitionInstallError, PartitionStateTransferOffer};
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{RemoteObjects, SegmentLocation};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_object_store::{
    MemoryStore, ObjectMeta, ObjectRange, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    SegmentKey, SegmentObject,
};

/// Sealed segments of a partition with [`ORIGIN_OPS`] one-message ops.
const ORIGIN_SEALED: [u64; 3] = [0, 2, 4];

/// An origin whose store holds every sealed segment, and whose first two
/// left the disk.
async fn tiered_origin() -> (tempfile::TempDir, Box<IggyPartition<IggyMessageBus>>) {
    let (directory, origin, _store) = Box::pin(tiered_origin_with_store()).await;
    (directory, origin)
}

/// [`tiered_origin`] with the store it reads from.
async fn tiered_origin_with_store() -> (
    tempfile::TempDir,
    Box<IggyPartition<IggyMessageBus>>,
    Rc<MemoryStore>,
) {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);
    let store = Box::pin(store_sealed(&mut origin, directory.path())).await;
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
    (directory, origin, store)
}

/// Upload every sealed segment of `partition` from `directory` to a new store
/// that `partition` then reads from.
async fn store_sealed(
    partition: &mut IggyPartition<IggyMessageBus>,
    directory: &Path,
) -> Rc<MemoryStore> {
    let store = Rc::new(MemoryStore::new());
    for start_offset in ORIGIN_SEALED {
        let path = |extension: &str| {
            directory
                .join(format!("{start_offset:020}.{extension}"))
                .to_string_lossy()
                .into_owned()
        };
        let upload = SegmentUpload {
            prefix: PREFIX,
            record: partition.sealed_segment_record(start_offset).unwrap(),
            log_path: path("log"),
            index_path: path("index"),
        };
        upload_segment(&*store, &layout(), &upload).await.unwrap();
    }
    attach(partition, Rc::clone(&store) as Rc<dyn RemoteObjects>);
    store
}

fn object_key(start_offset: u64, object: SegmentObject) -> String {
    layout().segment_key(&SegmentKey {
        prefix: PREFIX,
        start_offset,
        object,
    })
}

/// Delete the `object` of the segment at `start_offset` from `store`.
async fn lose(store: &MemoryStore, start_offset: u64, object: SegmentObject) {
    let deleted = store
        .delete(&[object_key(start_offset, object)])
        .await
        .unwrap();
    assert!(deleted.is_complete());
}

fn offered(offer: &PartitionStateTransferOffer) -> Vec<u64> {
    offer
        .segments
        .iter()
        .map(|source| source.entry.frontier)
        .collect()
}

/// The advance that confirmed `segment`, as a chain carries it.
const fn advanced(segment: crate::segment_chain::SegmentRecord) -> ChainRecord {
    ChainRecord::Advanced(AdvanceTieredFloor {
        start: segment.start_offset,
        up_to: segment.end_offset,
        hash: segment.hash,
    })
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

/// The segments without where their bytes are: a receiver holds none of the
/// bytes of a segment the store has, while the sender may still hold them.
fn extents<B: MessageBus>(partition: &IggyPartition<B>) -> Vec<(u64, u64, u64)> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| {
            (
                segment.start_offset,
                segment.end_offset,
                segment.size.as_bytes_u64(),
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
async fn given_a_tiered_origin_when_offering_should_carry_its_chain_and_no_stored_bytes() {
    let (_directory, mut origin) = Box::pin(tiered_origin()).await;

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(
        offered(&offer),
        [6],
        "a segment the store holds sends no bytes, on the sender's disk or not"
    );
    let manifest = offer.manifest();
    assert_eq!(manifest.len(), offer.artifact_count());
    assert_eq!(
        manifest[1].kind,
        consensus::state_manifest::artifact_kind::SEGMENT_CHAIN
    );
    assert!(matches!(
        offer.artifact_at(1),
        Some(crate::state_transfer::PartitionArtifactSource::Chain(_))
    ));
    assert!(matches!(
        offer.artifact_at(2),
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
            // Still on the sender's disk, and remote for the receiver.
            ChainRecord::Remote(sealed[2]),
        ]
    );
}

/// The store holds the segment, so the receiver needs none of its bytes: it
/// starts with the segment remote and reads it from the store.
#[compio::test]
async fn given_a_stored_segment_on_the_senders_disk_when_installed_should_be_remote_on_the_receiver()
 {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    assert_eq!(locations(&origin)[2], SegmentLocation::Local);
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    let remote_front = [
        SegmentLocation::Remote,
        SegmentLocation::Remote,
        SegmentLocation::Remote,
        SegmentLocation::Local,
    ];
    assert_eq!(locations(&receiver), remote_front);
    assert_eq!(extents(&receiver), extents(&origin));
    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    assert!(!directory.join(format!("{:020}.log", 4)).exists());
    drop(receiver);

    let rebooted = Box::pin(reboot(&config)).await;
    assert_eq!(locations(&rebooted), remote_front);
}

/// A floor advance once matched the segment, and the store has since lost
/// it. The sender still holds the bytes, so they travel, with the advance
/// that lets the receiver evict the segment once the store holds it again.
#[compio::test]
async fn given_a_stored_segment_missing_from_the_store_when_offering_should_send_its_bytes() {
    let (_origin_directory, mut origin, store) = Box::pin(tiered_origin_with_store()).await;
    lose(&store, 4, SegmentObject::Log).await;

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer), [4, 6]);
    let sealed = ORIGIN_SEALED.map(|start| origin.sealed_segment_record(start).unwrap());
    assert_eq!(
        offered_chain(&offer).records,
        [
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Remote(sealed[1]),
            ChainRecord::Sealed(sealed[2]),
            advanced(sealed[2]),
        ]
    );
    let root = tempfile::tempdir().unwrap();
    let (_config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    assert_eq!(locations(&receiver), locations(&origin));
}

#[compio::test]
async fn given_a_stored_segment_that_differs_in_the_store_when_offering_should_send_its_bytes() {
    let (_origin_directory, mut origin, store) = Box::pin(tiered_origin_with_store()).await;
    let key = object_key(4, SegmentObject::Log);
    let size = origin.sealed_segment_record(4).unwrap().size;
    let other = vec![0xA5; usize::try_from(size).unwrap()];
    store
        .put(&key, PutBody::from_bytes(other), &PutOptions::default())
        .await
        .unwrap();

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer), [4, 6]);
}

/// The receiver takes the stored front as the front of its log, so the
/// first segment the store lacks ends it, and every segment after it sends
/// its bytes, stored or not.
#[compio::test]
async fn given_a_stored_segment_missing_from_the_store_when_offering_should_send_the_bytes_of_every_segment_after_it()
 {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);
    let store = Box::pin(store_sealed(&mut origin, directory.path())).await;
    lose(&store, 2, SegmentObject::Index).await;

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer), [2, 4, 6]);
    let sealed = ORIGIN_SEALED.map(|start| origin.sealed_segment_record(start).unwrap());
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
}

/// A store that answers no request.
struct Unreachable;

impl RemoteObjects for Unreachable {
    fn get_range<'a>(
        &'a self,
        _key: &'a str,
        _range: std::ops::Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        async { Err(ObjectStoreError::Unavailable("unreachable".to_owned())) }.boxed_local()
    }

    fn head<'a>(
        &'a self,
        _key: &'a str,
    ) -> LocalBoxFuture<'a, Result<ObjectMeta, ObjectStoreError>> {
        async { Err(ObjectStoreError::Unavailable("unreachable".to_owned())) }.boxed_local()
    }
}

#[compio::test]
async fn given_a_store_that_does_not_answer_when_offering_should_send_the_bytes_on_the_senders_disk()
 {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    attach(&mut origin, Rc::new(Unreachable));

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(
        offered(&offer),
        [4, 6],
        "a segment whose `.log` left the disk still travels as its record"
    );
}

#[compio::test]
async fn given_no_store_to_ask_when_offering_should_send_the_bytes_on_the_senders_disk() {
    let (_directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);

    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer), [0, 2, 4, 6]);
}

/// Each offer build asks the store again: an earlier answer does not outlive
/// the build it was given for.
#[compio::test]
async fn given_an_object_lost_after_an_offer_when_offering_again_should_send_its_bytes() {
    let (_origin_directory, mut origin, store) = Box::pin(tiered_origin_with_store()).await;
    let first = Box::pin(serve_offer(&mut origin)).await;
    assert_eq!(offered(&first), [6]);
    lose(&store, 4, SegmentObject::Log).await;

    Box::pin(commit_records(
        &mut origin,
        ORIGIN_OPS + 1,
        &segment_records(1),
    ))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer).first(), Some(&4));
}

/// The shard drops a build nobody pulls any more, and the store's answer
/// with it.
#[compio::test]
async fn given_an_object_lost_during_an_abandoned_build_when_offering_again_should_send_its_bytes()
{
    let (_origin_directory, mut origin, store) = Box::pin(tiered_origin_with_store()).await;
    let config = repair_config();
    assert!(matches!(
        Box::pin(after_store_check(&mut origin, &config)).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    lose(&store, 4, SegmentObject::Log).await;

    origin.clear_state_transfer_offer_cache();
    let offer = Box::pin(serve_offer(&mut origin)).await;

    assert_eq!(offered(&offer), [4, 6]);
}

/// A rebuilt replica that later leads the partition publishes its manifest.
/// The records of the segments it received no bytes of are all it has to
/// list them, so they have to count as stored without an advance of its own.
#[compio::test]
async fn given_a_tiered_offer_when_installed_should_list_every_stored_segment_in_its_manifest() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    let now = IggyTimestamp::now();
    let stored = origin.manifest(now).unwrap().segments;
    assert_eq!(stored.len(), ORIGIN_SEALED.len());

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(receiver.manifest(now).unwrap().segments, stored);
    drop(receiver);
    let rebooted = Box::pin(reboot(&config)).await;
    assert_eq!(rebooted.manifest(now).unwrap().segments, stored);
}

/// A segment the store holds behind one it does not hold yet still travels as
/// bytes. A replica rebuilt by a transfer never applies the advances below the
/// floor it starts from, so the proof that the segment is stored has to arrive
/// with the chain, or it would stay on the receiver's disk for good.
#[compio::test]
async fn given_a_stored_segment_behind_an_unstored_one_when_its_chain_is_installed_should_let_the_receiver_evict_it()
 {
    let (_origin_directory, origin) =
        Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &[2, 4]);
    let offer = Box::pin(serve_offer(&mut origin)).await;
    assert_eq!(offered(&offer), [0, 2, 4, 6]);
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    assert_eq!(locations(&receiver)[1], SegmentLocation::Local);
    drop(receiver);

    // The proof is in the chain file, so it also survives the restart.
    let mut rebooted = Box::pin(reboot(&config)).await;
    confirm(&mut rebooted, &[0]);
    Box::pin(evict(&mut rebooted, u64::MAX, KEEP_NOTHING)).await;

    assert_eq!(
        locations(&rebooted)[..3],
        [SegmentLocation::Remote; 3],
        "only the first segment was confirmed after the install"
    );
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

/// A rebuilt replica holds the sender's records and front, keeps no bytes of
/// a segment the store holds, and boots back into the same layout.
#[compio::test]
async fn given_a_tiered_offer_when_installed_should_hold_the_senders_chain_and_boot_into_it() {
    let (_origin_directory, mut origin) = Box::pin(tiered_origin()).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
    assert_eq!(extents(&receiver), extents(&origin));
    assert_eq!(receiver.object_prefix(), Some(PREFIX));
    for start_offset in ORIGIN_SEALED {
        assert_eq!(
            receiver.sealed_segment_record(start_offset),
            origin.sealed_segment_record(start_offset)
        );
    }
    for start_offset in ORIGIN_SEALED {
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
    assert_eq!(extents(&rebooted)[..3], extents(&origin)[..3]);
    assert_eq!(locations(&rebooted)[..3], [SegmentLocation::Remote; 3]);
    assert_eq!(rebooted.object_prefix(), Some(PREFIX));
}

/// An eviction whose record is durable but whose `.log` stayed: the segment
/// is still local on the sender. The store holds it, so it travels as its
/// record like any other stored segment.
#[compio::test]
async fn given_an_eviction_cut_short_before_its_unlink_when_offering_should_send_the_segment_as_remote()
 {
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ORIGIN_OPS), 0)).await;
    let mut origin = Box::new(origin);
    confirm(&mut origin, &ORIGIN_SEALED);
    Box::pin(store_sealed(&mut origin, directory.path())).await;
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

    assert_eq!(offered(&offer), [6]);
    assert_eq!(
        offered_chain(&offer).records,
        [
            ChainRecord::Remote(sealed[0]),
            ChainRecord::Remote(sealed[1]),
            ChainRecord::Remote(sealed[2]),
        ]
    );
    let root = tempfile::tempdir().unwrap();
    let (_config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
    assert_eq!(extents(&receiver), extents(&origin));
    assert_eq!(locations(&receiver)[..3], [SegmentLocation::Remote; 3]);
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
        ChainRecord::Remote(sealed[1]),
    ];
    let root = tempfile::tempdir().unwrap();
    let (config, mut receiver) = Box::pin(receiver_on_disk(root.path())).await;

    Box::pin(install_with_chain(&mut receiver, &offer, &chain.encode()))
        .await
        .unwrap();
    Box::pin(finish_install_io(&mut receiver)).await.unwrap();

    assert_eq!(extents(&receiver), extents(&origin));
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

    // Remote segments 0 and 2, with 4 missing before the first local one.
    let mut holed = chain.clone();
    holed.records.remove(2);
    holed.valid_len -= RECORD_SIZE as u64;
    assert!(matches!(
        Box::pin(refusal(&offer, holed.encode())).await,
        PartitionInstallError::SegmentSetHole {
            previous_end: 3,
            next_start: 6,
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
