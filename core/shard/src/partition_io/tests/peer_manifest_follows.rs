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

use std::ops::Range;

use super::segment_uploads::{namespace, pumped, until};
use super::*;
use crate::peers::{PeerIdentity, PeerStatus, SharedPeerStatus};
use crate::{ListedPeerManifest, PeerManifestSource};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_object_store::{
    KeyLayout, ManifestSegment, MemoryStore, ObjectPrefix, ObjectRange, ObjectStore,
    ObjectStoreError, PartitionManifest, PutBody, PutOptions, SEGMENT_HASH_LEN, StoreIdentity,
};
use partitions::RemoteObjects;

const ORIGIN: &str = "dc-a";
/// The cluster that the node reads the peer as.
const PEER_CLUSTER: u128 = 0x0a0a;
/// A cluster that claims the peer's root in its place.
const OTHER_CLUSTER: u128 = 0x0b0b;

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
/// the peer's store, which [`PEER_CLUSTER`] claimed and the node reads.
fn follower() -> (IoTestShard, Rc<MemoryStore>) {
    let (owner, store, _) = follower_through(|store| store as Rc<dyn RemoteObjects>);
    (owner, store)
}

/// [`follower`] whose shard reads the peer's store through what `objects`
/// makes of it, with the peer's status.
fn follower_through(
    objects: impl FnOnce(Rc<MemoryStore>) -> Rc<dyn RemoteObjects>,
) -> (IoTestShard, Rc<MemoryStore>, SharedPeerStatus) {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let partitions = owner.plane.partitions();
    let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
    partition.register_builtin_state_machines().unwrap();
    partition.follow_peer_log(ORIGIN, PEER_PREFIX).unwrap();
    let store = Rc::new(MemoryStore::new());
    assert!(
        StoreIdentity {
            cluster_uid: PEER_CLUSTER
        }
        .claim(&*store, &peer_layout())
        .now_or_never()
        .unwrap()
        .unwrap()
    );
    let status = SharedPeerStatus::default();
    status.set(PeerStatus {
        identity: PeerIdentity::Verified {
            cluster_uid: PEER_CLUSTER,
        },
        ..PeerStatus::default()
    });
    owner.set_peer_manifest_objects(vec![(
        ORIGIN.to_owned(),
        PeerManifestSource {
            objects: objects(Rc::clone(&store)),
            identity_key: peer_layout().identity_key(),
            status: status.clone(),
        },
    )]);
    (owner, store, status)
}

/// The peer's store, as a pass reads it: a read of the manifest first hands
/// the root to [`OTHER_CLUSTER`], or every read of the identity object gets
/// no answer.
struct Peer {
    store: Rc<MemoryStore>,
    fault: Fault,
}

#[derive(Clone, Copy)]
enum Fault {
    ClaimedDuringThePass,
    IdentityUnanswered,
}

impl RemoteObjects for Peer {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        async move {
            let identity = key == peer_layout().identity_key();
            match self.fault {
                Fault::IdentityUnanswered if identity => {
                    return Err(ObjectStoreError::Transport("reset".to_owned()));
                }
                Fault::ClaimedDuringThePass if !identity => {
                    StoreIdentity {
                        cluster_uid: OTHER_CLUSTER,
                    }
                    .replace(&*self.store, &peer_layout())
                    .await?;
                }
                Fault::IdentityUnanswered | Fault::ClaimedDuringThePass => {}
            }
            ObjectStore::get_range(&*self.store, key, range).await
        }
        .boxed_local()
    }
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

/// A root that another cluster claimed while the pass read its manifest
/// names other bytes under the same keys, so the pass commits nothing and
/// asks shard 0 to check the peer at once.
#[compio::test]
async fn given_a_root_claimed_by_another_cluster_during_the_pass_when_listed_should_commit_nothing_and_ask_for_a_check()
 {
    let (owner, store, status) = follower_through(|store| {
        Rc::new(Peer {
            store,
            fault: Fault::ClaimedDuringThePass,
        })
    });
    publish(&store, &manifest(2)).await;

    pumped(&owner, async {
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e1"),
        );
        until(&owner, |owner| {
            owner.metrics().peer_manifest_read_failures(ORIGIN) == 1
        })
        .await;
    })
    .await;

    assert_eq!(committed(&owner), None);
    assert!(status.take_check_request(), "shard 0 checks the peer now");
}

/// A read of the identity object that gets no answer tells nothing of the
/// root: the pass commits nothing, and no check is asked for.
#[compio::test]
async fn given_an_identity_read_without_an_answer_when_listed_should_commit_nothing_and_ask_for_no_check()
 {
    let (owner, store, status) = follower_through(|store| {
        Rc::new(Peer {
            store,
            fault: Fault::IdentityUnanswered,
        })
    });
    publish(&store, &manifest(2)).await;

    pumped(&owner, async {
        owner.send_peer_manifests_listed(
            0,
            ORIGIN.to_owned(),
            IggyTimestamp::now(),
            listed(&owner, "e1"),
        );
        until(&owner, |owner| {
            owner.metrics().peer_manifest_read_failures(ORIGIN) == 1
        })
        .await;
    })
    .await;

    assert_eq!(committed(&owner), None);
    assert!(!status.take_check_request());
    assert_eq!(
        status.get().identity,
        PeerIdentity::Verified {
            cluster_uid: PEER_CLUSTER
        }
    );
}

#[compio::test]
async fn given_a_partition_that_follows_nothing_when_listed_should_read_nothing() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    let store = Rc::new(MemoryStore::new());
    publish(&store, &manifest(1)).await;
    owner.set_peer_manifest_objects(vec![(
        ORIGIN.to_owned(),
        PeerManifestSource {
            objects: Rc::clone(&store) as Rc<dyn RemoteObjects>,
            identity_key: peer_layout().identity_key(),
            status: SharedPeerStatus::default(),
        },
    )]);

    owner
        .on_peer_manifests_listed(ORIGIN, IggyTimestamp::now(), listed(&owner, "e1"))
        .await;
    compio::time::sleep(TASK_POLL_INTERVAL * 10).await;

    assert_eq!(owner.inbox_len(), 0, "no read is on its way");
    assert_eq!(counts(&owner), (0, 0, 0));
    assert_eq!(committed(&owner), None);
}
