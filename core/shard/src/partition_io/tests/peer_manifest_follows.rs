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

//! The primary of a shadow partition, which reads the peer manifests that a
//! listing found changed and commits them.

use super::segment_uploads::{namespace, pumped, until};
use super::*;
use crate::ListedPeerManifest;
use iggy_object_store::{
    KeyLayout, ManifestSegment, MemoryStore, ObjectPrefix, ObjectStore, PartitionManifest, PutBody,
    PutOptions, SEGMENT_HASH_LEN,
};
use partitions::RemoteObjects;

const ORIGIN: &str = "dc-a";

/// The peer's partition incarnation that the shadow partition follows.
const PEER_PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 7,
    topic_id: 3,
    partition_id: 0,
    created_revision: 41,
};

fn peer_layout() -> KeyLayout {
    KeyLayout::new(ORIGIN).unwrap()
}

fn manifest(segments: u64) -> PartitionManifest {
    let segments: Vec<ManifestSegment> = (0..segments)
        .map(|index| ManifestSegment {
            start_offset: index * 10,
            end_offset: index * 10 + 9,
            start_timestamp: 1,
            end_timestamp: 2,
            max_timestamp: 2,
            size: 1000,
            crc32c: 0,
            hash: [0; SEGMENT_HASH_LEN],
        })
        .collect();
    PartitionManifest {
        prefix: PEER_PREFIX,
        published_at: 1,
        floor: segments.last().map(|segment| segment.end_offset),
        trim_point: None,
        segments_keyed_by_both_offsets: false,
        segments,
    }
}

/// A shard whose partition [`namespace`] follows the peer's partition, and
/// the peer's store.
fn follower() -> (IoTestShard, Rc<MemoryStore>) {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let partitions = owner.plane.partitions();
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    partition.register_builtin_state_machines().unwrap();
    partition.follow_peer_log(ORIGIN, PEER_PREFIX).unwrap();
    let store = Rc::new(MemoryStore::new());
    owner.set_peer_manifest_objects(vec![(
        ORIGIN.to_owned(),
        Rc::clone(&store) as Rc<dyn RemoteObjects>,
    )]);
    (owner, store)
}

#[allow(clippy::future_not_send)]
async fn publish(store: &MemoryStore, manifest: &PartitionManifest) {
    store
        .put(
            &peer_layout().manifest_key(&PEER_PREFIX),
            PutBody::from_bytes(manifest.encode()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
}

fn listed(owner: &IoTestShard, etag: &str) -> Vec<ListedPeerManifest> {
    vec![ListedPeerManifest {
        namespace: namespace(),
        created_revision: owner
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .created_revision(),
        key: peer_layout().manifest_key(&PEER_PREFIX),
        etag: etag.to_owned(),
    }]
}

fn committed(owner: &IoTestShard) -> Option<PartitionManifest> {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .unwrap()
        .committed_peer_manifest()
        .cloned()
}

/// Reads, gets and refused manifests so far.
fn counts(owner: &IoTestShard) -> (u64, u64, u64) {
    owner.metrics().peer_manifest_counts(ORIGIN)
}

#[compio::test]
async fn given_a_changed_manifest_when_listed_should_read_and_commit_it_once() {
    let (owner, store) = follower();
    let first = manifest(2);
    let second = manifest(3);
    publish(&store, &first).await;

    pumped(&owner, async {
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e1"),
        );
        until(&owner, |owner| committed(owner).as_ref() == Some(&first)).await;
        assert_eq!(counts(&owner).1, 1);

        // The same etag again: the partition committed it, so nothing is read.
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e1"),
        );
        compio::time::sleep(TASK_POLL_INTERVAL * 20).await;
        assert_eq!(counts(&owner).1, 1);

        publish(&store, &second).await;
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e2"),
        );
        until(&owner, |owner| committed(owner).as_ref() == Some(&second)).await;
    })
    .await;

    assert_eq!(counts(&owner), (0, 2, 0));
    let partition = owner.plane.partitions().get_by_ns(&namespace()).unwrap();
    let applied = partition.peer_log().and_then(partitions::PeerLog::applied);
    assert_eq!(applied.and_then(|applied| applied.floor), Some(29));
}

#[compio::test]
async fn given_a_regressing_manifest_when_listed_should_refuse_it_once_and_keep_the_committed_one()
{
    let (owner, store) = follower();
    let applied = manifest(3);
    publish(&store, &applied).await;

    pumped(&owner, async {
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e1"),
        );
        until(&owner, |owner| committed(owner).as_ref() == Some(&applied)).await;

        publish(&store, &manifest(1)).await;
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e2"),
        );
        until(&owner, |owner| counts(owner).2 == 1).await;

        // The refused etag is not read again until it changes.
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e2"),
        );
        compio::time::sleep(TASK_POLL_INTERVAL * 20).await;
    })
    .await;

    assert_eq!(counts(&owner), (0, 2, 1));
    assert_eq!(committed(&owner), Some(applied));
}

#[compio::test]
async fn given_a_partition_that_follows_nothing_when_listed_should_read_nothing() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let store = Rc::new(MemoryStore::new());
    publish(&store, &manifest(1)).await;
    owner.set_peer_manifest_objects(vec![(
        ORIGIN.to_owned(),
        Rc::clone(&store) as Rc<dyn RemoteObjects>,
    )]);

    owner
        .on_peer_manifests_listed(ORIGIN, IggyTimestamp::now(), listed(&owner, "e1"))
        .await;
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;

    assert_eq!(owner.inbox_len(), 0, "no read is on its way");
    assert_eq!(counts(&owner), (0, 0, 0));
    assert_eq!(committed(&owner), None);
}
