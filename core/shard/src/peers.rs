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

//! The stores of the geo peers of a node: the other clusters whose buckets
//! it reads.
//!
//! Every shard holds one [`PeerStore`] per peer, each with a tier service of
//! its own over a [`ReadOnlyStore`]. A slow peer therefore never takes the
//! job slots of this cluster's store or of another peer, and no write
//! reaches a peer, whatever job runs on its service. A partition that follows
//! a peer's log reads it through [`PeerObjects`].
//!
//! What the node knows about a peer's identity is one [`SharedPeerStatus`]
//! for the whole node. Shard 0 reads the peer's store identity object at
//! boot and on a timer, and every shard sees the result. The node remembers
//! the cluster that the first claim of another cluster names. A later check
//! that finds another claim latches the peer as changed, and the node reads
//! nothing from it again while it runs.
//!
//! Once committed metadata retires a peer's origin, shard 0 marks its status
//! retired. The node then checks it no more and runs no job on its store.
//! Only the polls of consumers still read the segments that its shadow
//! partitions committed, until their retention removes them.

#![allow(
    clippy::future_not_send,
    reason = "every peer store runs on the shard that owns it and never crosses threads"
)]

use std::ops::Range;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};

use consensus::PartitionsHandle;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_object_store::{KeyLayout, ObjectRange, ObjectStoreError, ReadOnlyStore, TierJobClass};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{ExecutorObjects, RemoteObjects, RemoteSegments};

use crate::IggyShard;
use crate::shards_table::ShardsTable;
use crate::tier_service::{TierService, TierServiceConfig};

/// The tier service of one peer on one shard.
pub type PeerService<B> = TierService<ReadOnlyStore, B>;

/// What the identity checks of one peer's store found so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PeerIdentity {
    /// No check read a claim of another cluster yet. The store did not
    /// answer, refused the read, holds no identity object, holds bytes that
    /// no cluster wrote, or names this cluster.
    #[default]
    Unverified,
    /// The root belongs to `cluster_uid`, which the first claim that a check
    /// read names, and the last check that got an answer read it again.
    Verified { cluster_uid: u128 },
    /// An earlier check verified `cluster_uid`, but the last answer was no
    /// identity object or a refusal. The node reads nothing until a check
    /// finds `cluster_uid` again.
    Unconfirmed { cluster_uid: u128 },
    /// A check found a claim other than `cluster_uid`: another cluster's,
    /// this cluster's own, or, as `None`, bytes that no cluster wrote. The
    /// peer counts as another origin, and the node reads nothing from it
    /// until it restarts.
    Changed {
        cluster_uid: u128,
        found: Option<u128>,
    },
}

/// What one read of a peer's store identity object found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRead {
    /// The object names the cluster with this id.
    Found(u128),
    /// The object names this cluster, so the peer's root is this cluster's
    /// own.
    OwnCluster(u128),
    /// The object holds bytes that no cluster wrote.
    Invalid,
    /// No object is under the identity key.
    Missing,
    /// The store did not answer, or failed in a way that a retry can get
    /// past.
    Unreachable,
    /// The store refused the read in a way that only an operator can fix,
    /// such as rejected credentials or a missing bucket.
    Refused,
}

/// What the node knows about a peer: its identity, whether its store
/// answered the last check, and whether committed metadata retired it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerStatus {
    pub identity: PeerIdentity,
    /// The last check got no answer. A verified peer stays verified through
    /// an outage, and the node still reads what the store returns.
    pub unreachable: bool,
    /// Committed metadata retired the peer's origin. The node checks it no
    /// more and runs no job on its store. Only a consumer's poll still reads
    /// the segments that its shadow partitions committed, see
    /// [`PeerStatus::serves_polls`].
    pub retired: bool,
}

/// The status of one peer, shared by every shard of the node. Shard 0 sets
/// it after each check, and every shard reads it.
#[derive(Debug, Clone, Default)]
pub struct SharedPeerStatus {
    status: Arc<Mutex<PeerStatus>>,
}

/// One peer on one shard: its origin name, the layout of its root, its tier
/// service, and what the node knows about its identity.
pub struct PeerStore<B> {
    origin: String,
    layout: KeyLayout,
    service: PeerService<B>,
    status: SharedPeerStatus,
}

/// The peers of one shard, in the order of the configuration.
pub struct PeerStores<B> {
    peers: Vec<PeerStore<B>>,
}

/// The ranged reads of one peer's store, for the polls of the partitions
/// that follow its logs. They run in the demand fetch class of the peer's
/// own tier service.
///
/// Each read asks the node's status of the peer first, as a job that takes
/// [`PeerStore::service`] does. A peer the node does not read gets no
/// request, and the read fails in a way a later poll can get past.
pub struct PeerObjects<B> {
    origin: String,
    reads: ExecutorObjects<PeerService<B>>,
    status: SharedPeerStatus,
    /// The reads of consumers' polls, which go on after the origin retired.
    polls: bool,
}

impl<B> RemoteObjects for PeerObjects<B>
where
    B: MessageBus + Clone + Unpin + 'static,
{
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        let status = self.status.get();
        let served = if self.polls {
            status.serves_polls()
        } else {
            status.serves_jobs()
        };
        if !served {
            let unread = ObjectStoreError::Unavailable(if status.retired {
                format!(
                    "geo peer {} retired, so this node follows nothing of it",
                    self.origin
                )
            } else {
                format!(
                    "this node reads nothing from geo peer {} until a check verifies its store",
                    self.origin
                )
            });
            return async move { Err(unread) }.boxed_local();
        }
        self.reads.get_range(key, range)
    }
}

impl PeerIdentity {
    /// The identity after a check that found `read`.
    #[must_use]
    pub const fn after(self, read: PeerRead) -> Self {
        match (self, read) {
            // Only an operator ends a change, and an outage changes nothing.
            (Self::Changed { .. }, _) | (_, PeerRead::Unreachable) => self,
            (Self::Unverified, PeerRead::Found(cluster_uid)) => Self::Verified { cluster_uid },
            (
                Self::Unverified,
                PeerRead::OwnCluster(_) | PeerRead::Invalid | PeerRead::Missing | PeerRead::Refused,
            ) => Self::Unverified,
            (
                Self::Verified { cluster_uid } | Self::Unconfirmed { cluster_uid },
                PeerRead::Found(found),
            ) if found == cluster_uid => Self::Verified { cluster_uid },
            (
                Self::Verified { cluster_uid } | Self::Unconfirmed { cluster_uid },
                PeerRead::Found(found) | PeerRead::OwnCluster(found),
            ) => Self::Changed {
                cluster_uid,
                found: Some(found),
            },
            (
                Self::Verified { cluster_uid } | Self::Unconfirmed { cluster_uid },
                PeerRead::Invalid,
            ) => Self::Changed {
                cluster_uid,
                found: None,
            },
            (
                Self::Verified { cluster_uid } | Self::Unconfirmed { cluster_uid },
                PeerRead::Missing | PeerRead::Refused,
            ) => Self::Unconfirmed { cluster_uid },
        }
    }

    /// The cluster that owns the peer's root while the node reads it, and
    /// `None` while it reads nothing from the peer.
    #[must_use]
    pub const fn readable(&self) -> Option<u128> {
        match self {
            Self::Verified { cluster_uid } => Some(*cluster_uid),
            Self::Unverified | Self::Unconfirmed { .. } | Self::Changed { .. } => None,
        }
    }
}

impl PeerStatus {
    /// The status after a check that found `read`.
    #[must_use]
    pub const fn after(self, read: PeerRead) -> Self {
        Self {
            identity: self.identity.after(read),
            unreachable: matches!(read, PeerRead::Unreachable),
            retired: self.retired,
        }
    }

    /// Whether the node runs jobs on the peer's store: it verified the
    /// cluster that owns the root, and the origin did not retire.
    #[must_use]
    pub const fn serves_jobs(&self) -> bool {
        !self.retired && self.identity.readable().is_some()
    }

    /// Whether a consumer's poll may read the peer's store. A retired peer
    /// is checked no more, so a node that restarted since it retired never
    /// verified it, and its polls read what its partitions committed unless
    /// a check found another cluster first.
    #[must_use]
    pub const fn serves_polls(&self) -> bool {
        self.identity.readable().is_some()
            || (self.retired && !matches!(self.identity, PeerIdentity::Changed { .. }))
    }
}

impl SharedPeerStatus {
    #[must_use]
    pub fn get(&self) -> PeerStatus {
        // A status is two plain values that a panic cannot leave half set.
        *self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn set(&self, status: PeerStatus) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = status;
    }
}

impl<B> PeerStore<B>
where
    B: MessageBus + Clone + 'static,
{
    /// The peer `origin`, whose objects are under `layout` in `store`. Its
    /// jobs run on a tier service of their own, with the caps and timeouts
    /// of `config`.
    #[must_use]
    pub fn new(
        origin: String,
        layout: KeyLayout,
        store: ReadOnlyStore,
        bus: B,
        config: &TierServiceConfig,
        status: SharedPeerStatus,
    ) -> Self {
        Self {
            origin,
            layout,
            service: TierService::new(store, bus, config),
            status,
        }
    }
}

impl<B> PeerStore<B> {
    /// The peer's origin name, as the configuration gives it.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// The layout of the peer's root, under its prefix.
    #[must_use]
    pub const fn layout(&self) -> &KeyLayout {
        &self.layout
    }

    #[must_use]
    pub fn status(&self) -> PeerStatus {
        self.status.get()
    }

    /// The verified cluster of the peer, see [`PeerIdentity::readable`].
    #[must_use]
    pub fn cluster_uid(&self) -> Option<u128> {
        self.status().identity.readable()
    }

    /// The tier service over the peer's store, while the node reads the peer.
    ///
    /// Take it for each job. A clone kept across jobs keeps reading a peer
    /// whose identity changed, and a job in flight ends on the service that
    /// started it.
    #[must_use]
    pub fn service(&self) -> Option<&PeerService<B>> {
        self.status().serves_jobs().then_some(&self.service)
    }

    /// Whether committed metadata retired the peer's origin.
    #[must_use]
    pub fn retired(&self) -> bool {
        self.status().retired
    }
}

impl<B> PeerStore<B>
where
    B: MessageBus + Clone + Unpin + 'static,
{
    /// Where the partitions that follow this peer's logs read them: the
    /// peer's tier service, under the peer's root.
    #[must_use]
    pub fn remote_segments(&self) -> RemoteSegments {
        RemoteSegments::new(Rc::new(self.objects()), self.layout.clone())
            .with_runtime(Rc::new(self.service.bus().clone()))
    }

    /// Where the primaries of this peer's shadow partitions read its
    /// manifests: the peer's tier service, in the housekeeping class, so a
    /// read that a poll waits for never queues behind one.
    #[must_use]
    pub fn manifest_objects(&self) -> Rc<dyn RemoteObjects> {
        Rc::new(PeerObjects {
            origin: self.origin.clone(),
            reads: ExecutorObjects::with_class(self.service.clone(), TierJobClass::Housekeeping),
            status: self.status.clone(),
            polls: false,
        })
    }

    fn objects(&self) -> PeerObjects<B> {
        PeerObjects {
            origin: self.origin.clone(),
            reads: ExecutorObjects::new(self.service.clone()),
            status: self.status.clone(),
            polls: true,
        }
    }
}

impl<B> Default for PeerStores<B> {
    fn default() -> Self {
        Self { peers: Vec::new() }
    }
}

impl<B> PeerStores<B> {
    #[must_use]
    pub const fn new(peers: Vec<PeerStore<B>>) -> Self {
        Self { peers }
    }

    /// The peer with the origin name `origin`.
    #[must_use]
    pub fn get(&self, origin: &str) -> Option<&PeerStore<B>> {
        self.peers.iter().find(|peer| peer.origin == origin)
    }

    pub fn iter(&self) -> impl Iterator<Item = &PeerStore<B>> {
        self.peers.iter()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.peers.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the stores of its geo peers, in place of the ones it
    /// had, and hand each partition that follows a peer's log the store of
    /// its peer. A job in flight ends on the service that started it.
    pub fn set_peer_stores(&self, stores: PeerStores<B>)
    where
        B: Clone + Unpin,
    {
        self.plane.partitions().set_peer_remote_segments(
            stores
                .iter()
                .map(|peer| (peer.origin().to_owned(), Rc::new(peer.remote_segments())))
                .collect(),
        );
        self.set_peer_manifest_objects(
            stores
                .iter()
                .map(|peer| (peer.origin().to_owned(), peer.manifest_objects()))
                .collect(),
        );
        *self.peer_stores.borrow_mut() = Rc::new(stores);
    }

    /// The stores of this shard's geo peers, empty on a node without peers.
    #[must_use]
    pub fn peer_stores(&self) -> Rc<PeerStores<B>> {
        Rc::clone(&self.peer_stores.borrow())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use futures::FutureExt;
    use iggy_object_store::{
        IdentityRead, ListRequest, MemoryStore, ObjectStore, ObjectStoreError, PutBody, PutOptions,
        RecordingStore, StoreIdentity, TierExecutor, TierJobClass, TierJobRequest, TierScope,
    };
    use message_bus::IggyMessageBus;

    use super::*;

    const PEER: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;
    const OTHER: u128 = 7;
    const OWN: u128 = 9;

    type Bus = Rc<IggyMessageBus>;

    #[test]
    fn given_each_read_when_moving_the_identity_should_learn_once_and_latch_a_change() {
        let unverified = PeerIdentity::Unverified;
        let verified = PeerIdentity::Verified { cluster_uid: PEER };
        let unconfirmed = PeerIdentity::Unconfirmed { cluster_uid: PEER };
        let changed = |found| PeerIdentity::Changed {
            cluster_uid: PEER,
            found,
        };
        // (row, identity before, read, identity after)
        for (row, before, read, after) in [
            ("first claim", unverified, PeerRead::Found(PEER), verified),
            (
                "own claim is never learned",
                unverified,
                PeerRead::OwnCluster(OWN),
                unverified,
            ),
            ("no claim yet", unverified, PeerRead::Missing, unverified),
            ("garbage first", unverified, PeerRead::Invalid, unverified),
            ("refused first", unverified, PeerRead::Refused, unverified),
            (
                "outage first",
                unverified,
                PeerRead::Unreachable,
                unverified,
            ),
            ("same claim", verified, PeerRead::Found(PEER), verified),
            ("outage keeps", verified, PeerRead::Unreachable, verified),
            ("claim gone", verified, PeerRead::Missing, unconfirmed),
            ("refused later", verified, PeerRead::Refused, unconfirmed),
            ("claim back", unconfirmed, PeerRead::Found(PEER), verified),
            ("still gone", unconfirmed, PeerRead::Missing, unconfirmed),
            (
                "outage while gone",
                unconfirmed,
                PeerRead::Unreachable,
                unconfirmed,
            ),
            (
                "another cluster",
                verified,
                PeerRead::Found(OTHER),
                changed(Some(OTHER)),
            ),
            (
                "another cluster while gone",
                unconfirmed,
                PeerRead::Found(OTHER),
                changed(Some(OTHER)),
            ),
            (
                "this cluster",
                verified,
                PeerRead::OwnCluster(OWN),
                changed(Some(OWN)),
            ),
            ("garbage later", verified, PeerRead::Invalid, changed(None)),
            (
                "a change holds",
                changed(Some(OTHER)),
                PeerRead::Found(PEER),
                changed(Some(OTHER)),
            ),
            (
                "a change holds through an outage",
                changed(None),
                PeerRead::Unreachable,
                changed(None),
            ),
        ] {
            assert_eq!(before.after(read), after, "{row}");
        }
    }

    #[test]
    fn given_each_identity_when_asking_should_read_only_a_verified_peer() {
        assert_eq!(
            PeerIdentity::Verified { cluster_uid: PEER }.readable(),
            Some(PEER)
        );
        for identity in [
            PeerIdentity::Unverified,
            PeerIdentity::Unconfirmed { cluster_uid: PEER },
            PeerIdentity::Changed {
                cluster_uid: PEER,
                found: Some(OTHER),
            },
        ] {
            assert_eq!(identity.readable(), None, "{identity:?}");
        }
    }

    #[test]
    fn given_reads_when_moving_the_status_should_flag_only_the_last_outage() {
        let status = PeerStatus::default()
            .after(PeerRead::Found(PEER))
            .after(PeerRead::Unreachable);

        assert_eq!(
            status,
            PeerStatus {
                identity: PeerIdentity::Verified { cluster_uid: PEER },
                unreachable: true,
                retired: false,
            },
            "a verified peer stays readable through an outage"
        );
        assert!(!status.after(PeerRead::Found(PEER)).unreachable);
    }

    fn peer(origin: &str, provider: RecordingStore<MemoryStore>) -> PeerStore<Bus> {
        PeerStore::new(
            origin.to_owned(),
            KeyLayout::new("iggy").unwrap(),
            ReadOnlyStore::new(provider),
            Rc::new(IggyMessageBus::new(0)),
            &TierServiceConfig::default(),
            SharedPeerStatus::default(),
        )
    }

    fn claimed(cluster_uid: u128) -> MemoryStore {
        let store = MemoryStore::new();
        let layout = KeyLayout::new("iggy").unwrap();
        assert!(
            StoreIdentity { cluster_uid }
                .claim(&store, &layout)
                .now_or_never()
                .unwrap()
                .unwrap()
        );
        store
    }

    #[test]
    fn given_two_peers_when_their_identities_move_should_hand_out_each_service_only_while_verified()
    {
        let stores = PeerStores::new(vec![
            peer("dc-b", RecordingStore::new(MemoryStore::new())),
            peer("dc-c", RecordingStore::new(MemoryStore::new())),
        ]);
        let dc_b = stores.get("dc-b").unwrap();
        let dc_c = stores.get("dc-c").unwrap();

        assert_eq!(stores.len(), 2);
        assert!(stores.get("dc-a").is_none());
        assert!(dc_b.service().is_none() && dc_c.service().is_none());

        dc_b.status
            .set(PeerStatus::default().after(PeerRead::Found(PEER)));
        assert_eq!(dc_b.cluster_uid(), Some(PEER));
        assert!(dc_b.service().is_some());
        assert!(dc_c.service().is_none(), "each peer has its own status");

        dc_b.status.set(dc_b.status().after(PeerRead::Found(OTHER)));
        assert_eq!(dc_b.cluster_uid(), None);
        assert!(dc_b.service().is_none());
        assert_eq!(
            dc_b.status().identity,
            PeerIdentity::Changed {
                cluster_uid: PEER,
                found: Some(OTHER),
            }
        );
    }

    /// Jobs on a peer's service get a store that refuses writes, so a job
    /// that tries every call reaches the peer with its reads alone.
    #[compio::test]
    async fn given_jobs_that_try_every_call_when_running_on_a_peer_should_reach_it_with_reads_only()
    {
        let provider = RecordingStore::new(claimed(PEER));
        let calls = provider.calls();
        let peer = peer("dc-b", provider);
        peer.status
            .set(PeerStatus::default().after(PeerRead::Found(PEER)));
        let scope = TierScope::new();
        let refusals = Rc::new(RefCell::new(Vec::new()));
        let identity_key = peer.layout().identity_key();

        for class in TierJobClass::ALL {
            let refusals = Rc::clone(&refusals);
            let key = identity_key.clone();
            let layout = peer.layout().clone();
            let read = peer
                .service()
                .unwrap()
                .submit(
                    TierJobRequest::new(class, scope.token()),
                    move |store| async move {
                        let put = store
                            .put(&key, PutBody::from_bytes("x"), &PutOptions::default())
                            .await;
                        let create = StoreIdentity { cluster_uid: OWN }
                            .claim(store.as_ref(), &layout)
                            .await;
                        let delete = store.delete(std::slice::from_ref(&key)).await;
                        for refused in [put.err(), create.err(), delete.err()] {
                            refusals.borrow_mut().push(refused);
                        }
                        let listed = store.list(&ListRequest::new("iggy/")).await;
                        assert!(listed.is_ok());
                        assert!(store.head(&key).await.is_ok());
                        StoreIdentity::read(store.as_ref(), &layout).await
                    },
                )
                .await;

            assert!(
                matches!(read, Ok(Ok(IdentityRead::Found(found))) if found.cluster_uid == PEER),
                "{class:?}: {read:?}"
            );
        }

        assert_eq!(
            refusals.borrow().len(),
            3 * TierJobClass::ALL.len(),
            "every put, conditional create and delete failed"
        );
        assert!(
            refusals
                .borrow()
                .iter()
                .all(|refused| matches!(refused, Some(ObjectStoreError::ReadOnly { .. })))
        );
        assert_eq!(calls.mutations(), Vec::new());
        assert_eq!(
            calls.all().len(),
            3 * TierJobClass::ALL.len(),
            "a list, a head and a ranged get per job"
        );
    }

    /// The reads of a partition that follows the peer's log ask the node's
    /// status of the peer each time, so no request leaves for a peer whose
    /// store the node does not read.
    #[compio::test]
    async fn given_a_peer_the_node_does_not_read_when_a_poll_reads_its_objects_should_send_no_request()
     {
        let key =
            KeyLayout::new("iggy")
                .unwrap()
                .incarnation_prefix(&iggy_object_store::ObjectPrefix {
                    stream_id: 1,
                    topic_id: 1,
                    partition_id: 0,
                    created_revision: 4,
                })
                + "00000000000000000000.log";
        let stored = claimed(PEER);
        stored
            .put(&key, PutBody::from_bytes("batch"), &PutOptions::default())
            .await
            .unwrap();
        let provider = RecordingStore::new(stored);
        let calls = provider.calls();
        let peer = peer("dc-b", provider);
        let objects = peer.objects();

        let unverified = objects.get_range(&key, 0..5).await;
        assert!(
            matches!(unverified, Err(ObjectStoreError::Unavailable(_))),
            "{unverified:?}"
        );
        assert_eq!(calls.all(), Vec::new());

        peer.status
            .set(PeerStatus::default().after(PeerRead::Found(PEER)));
        let verified = objects.get_range(&key, 0..5).await.unwrap();
        assert_eq!(verified.bytes.as_ref(), b"batch");
        assert_eq!(calls.all().len(), 1);

        peer.status.set(peer.status().after(PeerRead::Found(OTHER)));
        let changed = objects.get_range(&key, 0..5).await;
        assert!(
            matches!(changed, Err(ObjectStoreError::Unavailable(_))),
            "{changed:?}"
        );
        assert_eq!(calls.all().len(), 1, "a changed peer gets no request");
        assert_eq!(calls.mutations(), Vec::new());
    }

    #[test]
    fn given_a_retired_peer_when_asking_should_run_no_job_and_serve_polls_unless_another_cluster_claimed_it()
     {
        let retired = |identity| PeerStatus {
            identity,
            unreachable: false,
            retired: true,
        };
        let verified = PeerIdentity::Verified { cluster_uid: PEER };

        assert!(!retired(verified).serves_jobs());
        assert!(retired(verified).serves_polls());
        assert!(
            retired(PeerIdentity::Unverified).serves_polls(),
            "a node that restarted since the retirement checks no more"
        );
        assert!(
            !retired(PeerIdentity::Changed {
                cluster_uid: PEER,
                found: Some(OTHER),
            })
            .serves_polls()
        );
        assert!(
            PeerStatus::default()
                .after(PeerRead::Found(PEER))
                .serves_jobs()
        );
        assert!(retired(verified).after(PeerRead::Found(PEER)).retired);
    }

    /// Once its origin retired, a peer takes the reads of polls alone: no
    /// job and no listing of its manifests.
    #[compio::test]
    async fn given_a_retired_peer_when_reading_should_send_only_the_reads_of_polls() {
        let key = KeyLayout::new("iggy")
            .unwrap()
            .manifest_key(&iggy_object_store::ObjectPrefix {
                stream_id: 1,
                topic_id: 1,
                partition_id: 0,
                created_revision: 4,
            });
        let stored = claimed(PEER);
        stored
            .put(&key, PutBody::from_bytes("bytes"), &PutOptions::default())
            .await
            .unwrap();
        let provider = RecordingStore::new(stored);
        let calls = provider.calls();
        let peer = peer("dc-b", provider);
        peer.status.set(PeerStatus {
            retired: true,
            ..PeerStatus::default()
        });

        assert!(peer.service().is_none());
        assert!(peer.retired());
        let refused = peer.manifest_objects().get_range(&key, 0..5).await;
        assert!(
            matches!(&refused, Err(ObjectStoreError::Unavailable(reason)) if reason.contains("retired")),
            "{refused:?}"
        );
        assert_eq!(calls.all(), Vec::new());

        let polled = peer.objects().get_range(&key, 0..5).await.unwrap();
        assert_eq!(polled.bytes.as_ref(), b"bytes");
        assert_eq!(calls.all().len(), 1);
    }
}
