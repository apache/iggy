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

//! A geo topic on three nodes that share one `fs` object store, whose every
//! node loses its disk, and the cluster rebuilt on empty disks whose replica
//! 0 the operator tells to recover it.
//!
//! The first test stores, beside the objects the origin wrote, the
//! self-describing objects that an origin leaves when its proofs stop short:
//! batches above the manifest's floor, the proofs of some of them, and
//! claims of others, one of which a claim of a higher view overrides. The
//! second one recovers what the origin's own writer stored and proved.

use std::future::Future;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_object_store::{
    FreshBatch, FreshIndex, FreshObjectHeader, FreshObjectId, FreshPartition, FreshProofRow,
    FsStore, KeyLayout, ObjectStore, PutBody, PutOptions, RecoveryRecord, SealedFreshObject,
    StoreIdentity,
};
use server_common::send_messages::{
    IggyMessage as StoredMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::IggyNamespace;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};

use super::geo_mode_faults::{
    LEADER_RESTART_PAUSE, NODES, await_store_claim, cluster_with_fs_store, metadata_leader,
    store_on_disk,
};
use crate::server::scenarios::geo_mode_faults_scenario::{
    Answer, Bucket, Ledger, MESSAGE_BYTES, PARTITION_ID, POLL_INTERVAL, STORE_PREFIX, Topics,
    geo_manifest, message, read_indexes,
};

/// Sends the origin commits and rewrites into long-term segments before
/// every node loses its disk.
const PUBLISHED_SENDS: u64 = 10;
/// The life of the writer that the stored fresh objects come from.
const STARTED_AT: u64 = 1_000;
/// The index that the payload of a claim that loses its offset carries.
const LOSER_INDEX: u64 = 999_999;
/// How long the cluster rebuilt on empty disks may take to recover and to
/// serve what it recovered.
const RECOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(180);

#[tokio::test]
async fn given_a_bucket_of_an_origin_that_lost_every_disk_when_a_cluster_with_empty_disks_recovers_it_should_serve_every_proven_message_once_in_order_and_the_adopted_tail()
 {
    let store = store_on_disk();
    let bucket = Bucket::Fs(store.path().to_owned());
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    await_store_claim(&mut harness, store.path()).await;
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let topics = Topics::create(&client, "geo-recovery").await;
    let mut ledger = Ledger::default();
    for _ in 0..PUBLISHED_SENDS {
        assert_eq!(
            ledger
                .send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }
    let (stream_id, topic_id) = topics.geo_prefix_ids(&client).await;
    bucket
        .await_rewritten(stream_id, topic_id, PUBLISHED_SENDS - 1)
        .await;
    harness.kill_cluster().expect("kill every node");

    let objects = bucket.objects().await;
    let layout = KeyLayout::new(STORE_PREFIX).expect("the default prefix");
    let retired = std::str::from_utf8(&objects[&layout.identity_key()])
        .ok()
        .and_then(|text| text.parse::<StoreIdentity>().ok())
        .expect("the bucket names the cluster that lost its disks")
        .cluster_uid;
    let manifest = geo_manifest(&objects, stream_id, topic_id).expect("the manifest");
    let floor = manifest.floor.expect("the manifest lists segments");
    assert_eq!(floor, PUBLISHED_SENDS - 1, "every send is published");
    let origin = Origin {
        namespace: IggyNamespace::new(
            usize::try_from(stream_id).unwrap(),
            usize::try_from(topic_id).unwrap(),
            PARTITION_ID as usize,
        ),
        created_revision: manifest.prefix.created_revision,
        cluster_uid: retired,
        base_timestamp: manifest.segments.last().unwrap().max_timestamp,
    };
    let written = origin.objects(floor + 1);
    put_objects(store.path(), &written.objects).await;

    // Only replica 0 runs the recovery, so only it is told to. The other
    // nodes learn which partitions to install from committed metadata.
    harness
        .node_mut(0)
        .add_env("IGGY_GEO_RECOVER_CLUSTER_UID", format!("{retired:032x}"));
    for node in 0..NODES {
        harness
            .restart_node_from_clean_slate(node)
            .expect("restart the node without its disk");
    }
    let successor = await_recovery(&mut harness, &bucket, &layout, retired).await;

    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let expected: Vec<u64> = (0..=written.cutover_through).collect();
    let read = await_read(&client, &topics, expected.len()).await;
    assert_eq!(
        read, expected,
        "every proven and adopted message, once and in order"
    );
    let timestamps = timestamps_from(&client, &topics, floor + 1).await;
    assert_eq!(
        timestamps, written.timestamps,
        "the timestamps each batch committed at"
    );

    let next = written.cutover_through + 1;
    let mut sent = vec![message(next, MESSAGE_BYTES)];
    client
        .send_messages(
            &topics.stream,
            &topics.geo,
            &Partitioning::partition_id(PARTITION_ID),
            &mut sent,
        )
        .await
        .expect("the recovered partition takes messages");
    let read = await_read(&client, &topics, expected.len() + 1).await;
    assert_eq!(
        read.last(),
        Some(&next),
        "the next send goes after the cutover"
    );

    // Without replica 0, the two nodes that were not told to recover serve
    // the partition alone, from what they installed.
    harness.kill_node(0).expect("kill replica 0");
    let others = harness
        .root_client_for_node(1)
        .await
        .expect("connect a root client to another node");
    assert_eq!(
        await_read(&others, &topics, read.len()).await,
        read,
        "the nodes without recover_cluster_uid serve the same messages"
    );
    assert_eq!(
        timestamps_from(&others, &topics, floor + 1).await[..timestamps.len()],
        timestamps[..],
        "and at the same timestamps"
    );

    let objects = bucket.objects().await;
    let record = RecoveryRecord::decode(&objects[&layout.recovery_key(retired)])
        .expect("the recovery record decodes");
    assert_eq!(record.successor_cluster_uid, successor);
    let [cutover] = record.partitions.as_slice() else {
        panic!("one partition: {record:?}");
    };
    assert_eq!(cutover.prefix, manifest.prefix);
    assert_eq!(
        (cutover.published_end, cutover.proven_end, cutover.cutover),
        (
            floor + 1,
            written.proven_through + 1,
            written.cutover_through + 1
        )
    );
    let report: serde_json::Value =
        serde_json::from_slice(&objects[&layout.recovery_report_key(retired)])
            .expect("the report is JSON");
    let partition = &report["partitions"][0];
    assert_eq!(partition["floor"], floor);
    assert_eq!(partition["proven_through"], written.proven_through);
    assert_eq!(partition["cutover_through"], written.cutover_through);
    let mut reasons: Vec<&str> = partition["dropped"]
        .as_array()
        .expect("dropped batches")
        .iter()
        .filter_map(|dropped| dropped["reason"].as_str())
        .collect();
    reasons.sort_unstable();
    assert_eq!(reasons, ["lost_overlap", "past_gap"]);
    assert_eq!(
        report["skipped_objects"],
        serde_json::json!([]),
        "every object of the origin describes itself: {report}"
    );
}

/// Sends of the real-writer test in one round, before it checks whether the
/// bucket proves batches the manifest does not list yet.
const ROUND_SENDS: u64 = 20;
/// Rounds the real-writer test sends at most.
const ROUNDS: u64 = 10;

#[tokio::test]
async fn given_a_geo_topic_that_a_real_writer_proved_when_every_disk_is_lost_and_a_rebuilt_cluster_recovers_it_should_serve_every_proven_message_once_in_order()
 {
    let store = store_on_disk();
    let bucket = Bucket::Fs(store.path().to_owned());
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    await_store_claim(&mut harness, store.path()).await;
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let topics = Topics::create(&client, "geo-real-recovery").await;
    let (stream_id, topic_id) = topics.geo_prefix_ids(&client).await;
    let namespace = IggyNamespace::new(
        usize::try_from(stream_id).unwrap(),
        usize::try_from(topic_id).unwrap(),
        PARTITION_ID as usize,
    );
    let layout = KeyLayout::new(STORE_PREFIX).expect("the default prefix");
    let mut ledger = Ledger::default();
    for _ in 0..ROUND_SENDS {
        assert_eq!(
            ledger
                .send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }
    bucket
        .await_rewritten(stream_id, topic_id, ROUND_SENDS - 1)
        .await;
    let mut proven = Proven::default();
    for _ in 0..ROUNDS {
        for _ in 0..ROUND_SENDS {
            assert_eq!(
                ledger
                    .send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                    .await,
                Answer::Acked
            );
        }
        let sent = ledger.count(Answer::Acked) as u64;
        proven = await_proven(&bucket, &layout, namespace, stream_id, topic_id, sent).await;
        if proven.above_floor > 0 {
            break;
        }
    }
    let sent = ledger.count(Answer::Acked) as u64;
    eprintln!(
        "{sent} sends, the manifest lists {} of them and proofs alone the other {}",
        sent - proven.above_floor,
        proven.above_floor
    );
    assert!(
        proven.above_floor > 0 && proven.above_floor < sent,
        "the manifest lists some sends and the proofs alone the others: {proven:?} of {sent}"
    );
    harness.kill_cluster().expect("kill every node");
    let retired = proven.cluster_uid;

    harness
        .node_mut(0)
        .add_env("IGGY_GEO_RECOVER_CLUSTER_UID", format!("{retired:032x}"));
    for node in 0..NODES {
        harness
            .restart_node_from_clean_slate(node)
            .expect("restart the node without its disk");
    }
    let successor = await_recovery(&mut harness, &bucket, &layout, retired).await;
    assert_ne!(successor, retired);

    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let read = await_read(&client, &topics, usize::try_from(sent).unwrap()).await;
    assert_eq!(
        read,
        (0..sent).collect::<Vec<_>>(),
        "every acknowledged send, which the bucket proved, once and in order"
    );
    ledger.assert_read(&read);
    let record = RecoveryRecord::decode(&bucket.objects().await[&layout.recovery_key(retired)])
        .expect("the recovery record decodes");
    let [cutover] = record.partitions.as_slice() else {
        panic!("one partition: {record:?}");
    };
    assert_eq!(
        (cutover.proven_end, cutover.cutover),
        (sent, sent),
        "the proofs alone take back the sends above the floor"
    );
}

/// What the bucket proves of the geo partition.
#[derive(Debug, Default)]
struct Proven {
    /// The cluster whose objects hold the proofs.
    cluster_uid: u128,
    /// Sends above the floor of the manifest that proofs bind to offsets.
    above_floor: u64,
}

/// Waits until the manifest and the proof rows of the fresh objects in the
/// bucket together bind every offset below `sent`, and returns how many of
/// them the proofs alone bind.
async fn await_proven(
    bucket: &Bucket,
    layout: &KeyLayout,
    namespace: IggyNamespace,
    stream_id: u64,
    topic_id: u64,
    sent: u64,
) -> Proven {
    let deadline = Instant::now() + RECOVERY_DEADLINE;
    loop {
        let objects = bucket.objects().await;
        let published_end = geo_manifest(&objects, stream_id, topic_id)
            .and_then(|manifest| manifest.floor)
            .map_or(0, |floor| floor + 1);
        let mut rows = std::collections::BTreeMap::new();
        let mut cluster_uid = 0;
        for (key, bytes) in &objects {
            let Some(id) = layout.parse_fresh_key(key).map(FreshObjectId::from_raw) else {
                continue;
            };
            let Ok(index) = FreshIndex::decode(bytes, id) else {
                continue;
            };
            cluster_uid = index.header().cluster_uid;
            if let Some(row) = index.partition(namespace.inner()) {
                for proof in index.proofs_of(row) {
                    rows.insert(proof.base_offset, proof.message_count);
                }
            }
        }
        let mut next = published_end;
        while let Some(count) = rows.get(&next) {
            next += u64::from(*count);
        }
        if next >= sent {
            return Proven {
                cluster_uid,
                above_floor: sent.saturating_sub(published_end),
            };
        }
        assert!(
            Instant::now() < deadline,
            "the bucket binds offsets below {next} of {sent}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits until the store names a cluster other than `retired`, restarting
/// the metadata leader until replica 0 leads, since only replica 0 recovers.
/// Returns the cluster that owns the store then.
async fn await_recovery(
    harness: &mut integration::harness::TestHarness,
    bucket: &Bucket,
    layout: &KeyLayout,
    retired: u128,
) -> u128 {
    let deadline = Instant::now() + RECOVERY_DEADLINE;
    loop {
        let objects = bucket.objects().await;
        let owner = objects
            .get(&layout.identity_key())
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|text| text.parse::<StoreIdentity>().ok())
            .map(|identity| identity.cluster_uid);
        if let Some(owner) = owner.filter(|owner| *owner != retired) {
            return owner;
        }
        assert!(
            Instant::now() < deadline,
            "the rebuilt cluster never recovered the cluster that lost its disks"
        );
        match metadata_leader(harness).await {
            Some(leader) if leader != 0 => {
                harness
                    .restart_node(leader)
                    .expect("restart the metadata leader");
                sleep(LEADER_RESTART_PAUSE).await;
            }
            _ => sleep(POLL_INTERVAL).await,
        }
    }
}

/// Waits until the geo partition serves `count` messages from offset 0, and
/// returns the indexes their payloads carry.
async fn await_read(client: &IggyClient, topics: &Topics, count: usize) -> Vec<u64> {
    let deadline = Instant::now() + RECOVERY_DEADLINE;
    loop {
        if let Ok(read) = read_indexes(client, &topics.stream, &topics.geo).await
            && read.len() >= count
        {
            return read;
        }
        assert!(
            Instant::now() < deadline,
            "the recovered partition never served {count} messages"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The timestamp of each message of the geo partition from `offset` on.
async fn timestamps_from(client: &IggyClient, topics: &Topics, offset: u64) -> Vec<u64> {
    let polled = client
        .poll_messages(
            &topics.stream,
            &topics.geo,
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(offset),
            PollOptions {
                count: 100,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll the recovered messages");
    polled
        .messages
        .iter()
        .map(|message| message.header.timestamp)
        .collect()
}

/// The partition of the origin that lost its disks, as its objects name it.
struct Origin {
    namespace: IggyNamespace,
    created_revision: u64,
    cluster_uid: u128,
    /// The timestamp of the last published batch.
    base_timestamp: u64,
}

/// What the stored objects give back.
struct Written {
    objects: Vec<SealedFreshObject>,
    proven_through: u64,
    cutover_through: u64,
    /// The timestamp of each message from the floor on, as committed or as
    /// adopted.
    timestamps: Vec<u64>,
}

impl Origin {
    /// The objects of one writer life above `first`, the floor plus one:
    ///
    /// - the first object holds two batches, the second a third and proves
    ///   the first two, and a confirmation proves the third: five messages
    ///   come back at the offsets and timestamps their proofs bind;
    /// - two more batches that no proof names claim the three offsets after
    ///   them, and come back at those offsets;
    /// - a claim of a lower view on the first of those offsets loses it;
    /// - a claim past a gap is left out.
    fn objects(&self, first: u64) -> Written {
        let base = self.base_timestamp;
        let first_object = self.object(1, 1, base + 10, first, &[(first, 2), (first + 2, 1)], &[]);
        let proofs = [
            self.proof(&first_object, 0, first, base + 100),
            self.proof(&first_object, 1, first + 2, base + 101),
        ];
        let second_object = self.object(2, 1, base + 20, first + 3, &[(first + 3, 2)], &proofs);
        let confirmation = self.object(
            3,
            1,
            base + 30,
            first,
            &[],
            &[self.proof(&second_object, 0, first + 3, base + 102)],
        );
        let claims = self.object(
            4,
            1,
            base + 500,
            first + 5,
            &[(first + 5, 1), (first + 6, 2)],
            &[],
        );
        let loser = self.object(5, 0, base + 600, first + 5, &[(LOSER_INDEX, 1)], &[]);
        let past_gap = self.object(6, 1, base + 700, first + 9, &[(first + 9, 1)], &[]);
        Written {
            objects: vec![
                first_object,
                second_object,
                confirmation,
                claims,
                loser,
                past_gap,
            ],
            proven_through: first + 4,
            cutover_through: first + 7,
            timestamps: [
                base + 100,
                base + 100,
                base + 101,
                base + 102,
                base + 102,
                base + 500,
                base + 501,
                base + 501,
            ]
            .to_vec(),
        }
    }

    fn id(sequence: u64) -> FreshObjectId {
        FreshObjectId::new(0, 1, STARTED_AT, sequence).expect("a fresh object id")
    }

    /// The object `sequence` of this partition, with batches of the
    /// messages `(first index, count)`, claimed from `claimed_first`.
    fn object(
        &self,
        sequence: u64,
        view: u32,
        sealed_at: u64,
        claimed_first: u64,
        batches: &[(u64, u32)],
        proofs: &[FreshProofRow],
    ) -> SealedFreshObject {
        let batches = batches
            .iter()
            .map(|(first, count)| {
                let blob = self.blob(*first, *count);
                FreshBatch {
                    message_count: *count,
                    origin_timestamp: self.base_timestamp,
                    blob_checksum: placement::blob_checksum(&blob),
                    producer: None,
                    blob,
                }
            })
            .collect();
        SealedFreshObject::seal(
            FreshObjectHeader {
                object_id: Self::id(sequence),
                cluster_uid: self.cluster_uid,
                sealed_at,
            },
            &[FreshPartition {
                namespace: self.namespace.inner(),
                created_revision: self.created_revision,
                view,
                claimed_first_offset: claimed_first,
                proven_below: 0,
                batches,
                proofs: proofs.to_vec(),
            }],
        )
        .expect("seal a fresh object")
    }

    /// The blob of the messages `first..first + count`, as the shard stores
    /// one.
    fn blob(&self, first: u64, count: u32) -> Bytes {
        let mut messages = IggyMessages::with_capacity(count as usize);
        for index in first..first + u64::from(count) {
            let payload = Bytes::from(payload(index));
            messages.push(StoredMessage {
                header: IggyMessageHeader {
                    id: u128::from(index) + 1,
                    origin_timestamp: self.base_timestamp,
                    payload_length: u32::try_from(payload.len()).unwrap(),
                    ..IggyMessageHeader::default()
                },
                payload,
                user_headers: None,
            });
        }
        SendMessagesOwned::from_messages(self.namespace, &messages)
            .expect("the messages make a batch")
            .blob
    }

    /// The proof that batch `batch` of `source` committed at `base_offset`
    /// and `base_timestamp`.
    fn proof(
        &self,
        source: &SealedFreshObject,
        batch: usize,
        base_offset: u64,
        base_timestamp: u64,
    ) -> FreshProofRow {
        let index = &source.index;
        let row = index
            .partition(self.namespace.inner())
            .expect("the partition's row");
        let (position, batch_row) = index.batches_of(row).nth(batch).expect("the batch");
        let object_id = index.header().object_id;
        let length = u64::from(batch_row.blob_length);
        let placed = PlacedBatch::new(
            BatchHeader {
                partition_id: u64::from(PARTITION_ID),
                base_offset,
                base_timestamp,
                origin_timestamp: batch_row.origin_timestamp,
                batch_length: BATCH_HEADER_SIZE as u64 + length,
                batch_checksum: 0,
                message_count: batch_row.message_count,
                producer: None,
            },
            Placement {
                object_id: object_id.get(),
                position,
                length,
                blob_checksum: batch_row.blob_checksum,
                proven_below: row.proven_below,
            },
        );
        let mut bytes = [0; PLACED_BATCH_SIZE];
        placed.encode_into(&mut bytes);
        FreshProofRow {
            object_id,
            position,
            base_offset,
            base_timestamp,
            message_count: batch_row.message_count,
            placed_checksum: placement::checksum_of(&bytes),
        }
    }
}

/// The payload of the message `index`, as the scenario's messages carry it.
fn payload(index: u64) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; MESSAGE_BYTES];
    payload[..8].copy_from_slice(&index.to_le_bytes());
    payload
}

/// Store `objects` under their keys in the `fs` store under `root`.
async fn put_objects(root: &Path, objects: &[SealedFreshObject]) {
    let objects: Vec<(u128, Bytes)> = objects
        .iter()
        .map(|object| (object.index.header().object_id.get(), object.bytes.clone()))
        .collect();
    on_store(root.to_owned(), move |store| async move {
        let layout = KeyLayout::new(STORE_PREFIX).expect("the default prefix");
        for (object_id, bytes) in objects {
            store
                .put(
                    &layout.fresh_key(object_id),
                    PutBody::from_bytes(bytes),
                    &PutOptions::default(),
                )
                .await
                .expect("store a fresh object");
        }
    })
    .await;
}

/// Run `work` on an `fs` store under `root`, on a thread of its own with the
/// runtime the store needs.
async fn on_store<T, F, Fut>(root: PathBuf, work: F) -> T
where
    T: Send + 'static,
    F: FnOnce(FsStore) -> Fut + Send + 'static,
    Fut: Future<Output = T>,
{
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        let runtime = compio::runtime::Runtime::new().expect("a compio runtime");
        let _ = sender.send(runtime.block_on(work(FsStore::new(root))));
    });
    receiver.await.expect("the store thread answers")
}
