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

//! Geo topics in a simulated cluster.
//!
//! Each shard reaches the cluster's [`SimBucket`] through the tier service
//! that boot gives a server shard, see [`server::serve_tiered_storage`], and
//! reaches each peer's bucket read-only, see [`server::serve_peers`]. The
//! simulator runs no store identity check: a node counts as verified once its
//! metadata names the cluster, and then its shards hold their writers. A peer
//! counts as verified once the harness names the cluster that owns its
//! bucket, see [`Simulator::verify_geo_peers`]. Each step runs the shards'
//! [`GeoPasses`] on the virtual clock, as the partition reconciler runs them
//! after its passes.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use clock::Clock;
use configs::geo::{GeoConfig, GeoPeerConfig};
use configs::tiered_storage::TieredStorageConfig;
use consensus::MetadataHandle;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyError;
use iggy_object_store::{KeyLayout, ReadOnlyStore};
use partitions::{IggyIndexReader, IndexFile, IndexFiles};
use server::{GeoPasses, NodePlace, ServedPeer, TierHandlers};
use shard::peers::{PeerIdentity, SharedPeerStatus};

use crate::Simulator;
use crate::bucket::{ClusterId, SimBucket};
use crate::deps::SimClock;

/// The geo setup of a simulated cluster.
pub struct SimGeo {
    bucket: SimBucket,
    cluster: ClusterId,
    geo: GeoConfig,
    tiered: TieredStorageConfig,
    layout: KeyLayout,
    peers: Vec<SimPeer>,
    /// Per replica, the state of each of its shards. Empty for a replica
    /// that was never wired.
    replicas: Vec<GeoReplica>,
}

/// A peer of the cluster, and the bucket that its geo topics keep their
/// data in.
struct SimPeer {
    origin: String,
    bucket: SimBucket,
}

/// What one replica keeps between steps.
#[derive(Default)]
struct GeoReplica {
    shards: Vec<GeoShard>,
    /// What the node knows of each peer, in the order of `SimGeo::peers`.
    /// A restart starts each one unverified, as a rebooted node does.
    peer_statuses: Vec<SharedPeerStatus>,
    /// The indexes that the polls of the node fetched, which a restart
    /// keeps, as a disk keeps its files.
    index_files: Rc<MemoryIndexFiles>,
}

/// What one shard keeps between steps.
struct GeoShard {
    handlers: TierHandlers,
    passes: GeoPasses,
}

impl SimGeo {
    /// The geo setup of `cluster`, whose geo topics take `origin` as their
    /// origin and keep their data in `bucket`.
    ///
    /// # Panics
    /// When the default prefix of tiered storage is not a valid key prefix.
    #[must_use]
    pub fn new(origin: &str, cluster: ClusterId, bucket: SimBucket) -> Self {
        let defaults = configs::server::ServerConfig::default();
        let geo = GeoConfig {
            origin: origin.to_owned(),
            ..defaults.geo
        };
        let tiered = defaults.tiered_storage;
        let layout = KeyLayout::new(&tiered.prefix).expect("the default prefix is valid");
        Self {
            bucket,
            cluster,
            geo,
            tiered,
            layout,
            peers: Vec::new(),
            replicas: Vec::new(),
        }
    }

    /// Read the geo topics of the peer `origin` from `bucket`, under the
    /// same prefix as this cluster's own.
    #[must_use]
    pub fn with_peer(mut self, origin: &str, bucket: SimBucket) -> Self {
        self.geo.peers.push(GeoPeerConfig {
            origin: origin.to_owned(),
            prefix: self.tiered.prefix.clone(),
            ..GeoPeerConfig::default()
        });
        self.peers.push(SimPeer {
            origin: origin.to_owned(),
            bucket,
        });
        self
    }

    /// Retire a peer's deleted topic `grace` after its origin tombstoned
    /// it, in place of the default of a day.
    #[must_use]
    pub fn with_tombstone_grace(mut self, grace: Duration) -> Self {
        self.geo.tombstone_grace = grace.into();
        self
    }

    #[must_use]
    pub const fn bucket(&self) -> &SimBucket {
        &self.bucket
    }

    #[must_use]
    pub const fn layout(&self) -> &KeyLayout {
        &self.layout
    }

    #[must_use]
    pub fn origin(&self) -> &str {
        &self.geo.origin
    }
}

impl Simulator {
    /// Give every replica of this cluster geo topics with `geo`'s origin,
    /// bucket and peers. A replica that restarts gets them again.
    pub fn enable_geo(&mut self, geo: SimGeo) {
        self.geo = Some(geo);
        for replica in 0..self.replica_count {
            self.wire_geo(replica);
        }
    }

    #[must_use]
    pub const fn geo(&self) -> Option<&SimGeo> {
        self.geo.as_ref()
    }

    /// The cluster's `cluster_uid`, once a live replica applied the first
    /// metadata operation.
    #[must_use]
    pub fn cluster_uid(&self) -> Option<u128> {
        (0..self.replica_count)
            .filter(|replica| !self.crashed.contains(replica))
            .find_map(|replica| {
                server::own_cluster_uid(self.replicas[usize::from(replica)].shards[0].as_ref())
            })
    }

    /// Mark every peer of every live replica as verified, owned by the
    /// cluster that `owner` names for its origin. A peer that `owner` names
    /// no cluster for stays as it is.
    pub fn verify_geo_peers(&mut self, owner: impl Fn(&str) -> Option<u128>) {
        let Some(geo) = self.geo.as_ref() else {
            return;
        };
        for replica in &geo.replicas {
            for (peer, status) in geo.peers.iter().zip(&replica.peer_statuses) {
                let Some(cluster_uid) = owner(&peer.origin) else {
                    continue;
                };
                let verified = PeerIdentity::Verified { cluster_uid };
                let mut current = status.get();
                if current.identity != verified {
                    current.identity = verified;
                    status.set(current);
                }
            }
        }
    }

    /// Wire each shard of `replica` to the buckets, as boot does with the
    /// configured store and peers.
    pub(crate) fn wire_geo(&mut self, replica: u8) {
        let Some(geo) = self.geo.as_mut() else {
            return;
        };
        let index = usize::from(replica);
        if geo.replicas.len() <= index {
            geo.replicas.resize_with(index + 1, GeoReplica::default);
        }
        let state = &mut geo.replicas[index];
        state.peer_statuses = geo
            .peers
            .iter()
            .map(|_| SharedPeerStatus::default())
            .collect();
        let index_files: Rc<dyn IndexFiles> = state.index_files.clone();
        let shards = &self.replicas[index].shards;
        state.shards = shards
            .iter()
            .map(|shard| {
                let store = geo.bucket.handle(geo.cluster, self.executor.timer());
                let handlers = server::serve_tiered_storage(&geo.tiered, store, &geo.layout, shard);
                let peers = geo
                    .peers
                    .iter()
                    .zip(&state.peer_statuses)
                    .map(|(peer, status)| ServedPeer {
                        origin: peer.origin.clone(),
                        layout: geo.layout.clone(),
                        store: ReadOnlyStore::new(
                            peer.bucket.handle(geo.cluster, self.executor.timer()),
                        ),
                        status: status.clone(),
                    });
                server::serve_peers(&geo.tiered, shard, &index_files, peers);
                shard.set_long_term_rewrite_max_interval(
                    geo.geo.long_term_rewrite_max_interval.get_duration(),
                );
                GeoShard {
                    handlers,
                    passes: GeoPasses::new(&geo.geo),
                }
            })
            .collect();
        if let Some(shard) = shards.first() {
            shard
                .plane
                .metadata()
                .applied_frontier()
                .set_store_identity_verified(true);
        }
    }

    /// One step of the geo passes of every live replica: each shard takes
    /// its writers once its metadata names the cluster, then runs the passes
    /// that are due.
    pub(crate) fn drive_geo(&mut self) {
        let Some(geo) = self.geo.as_mut() else {
            return;
        };
        let now = SimClock::new(self.executor.timer()).realtime();
        for (index, replica) in self.replicas.iter().enumerate() {
            let id = u8::try_from(index).expect("replica id fits u8");
            if self.crashed.contains(&id) {
                continue;
            }
            let Some(wired) = geo.replicas.get_mut(index) else {
                continue;
            };
            let Some(first) = replica.shards.first() else {
                continue;
            };
            let place = NodePlace {
                self_replica_id: id,
                replica_count: self.replica_count,
                partition_shards: first.partition_shards(),
            };
            for (shard, state) in replica.shards.iter().zip(wired.shards.iter_mut()) {
                state
                    .handlers
                    .sync(shard, server::own_cluster_uid(shard.as_ref()));
                state.passes.pass(shard, now, place);
            }
        }
    }
}

/// The index files of one node, in memory. A file is readable once it was
/// renamed into place, as on a disk whose directory was synced.
#[derive(Debug, Default)]
pub struct MemoryIndexFiles {
    files: Rc<RefCell<BTreeMap<String, Bytes>>>,
}

/// One file that a fetch writes, kept under its name when it syncs.
struct MemoryIndexFile {
    files: Rc<RefCell<BTreeMap<String, Bytes>>>,
    path: String,
    bytes: Vec<u8>,
}

impl IndexFiles for MemoryIndexFiles {
    fn create<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<Box<dyn IndexFile>>> {
        self.files
            .borrow_mut()
            .insert(path.to_owned(), Bytes::new());
        let file = MemoryIndexFile {
            files: Rc::clone(&self.files),
            path: path.to_owned(),
            bytes: Vec::new(),
        };
        async move { Ok(Box::new(file) as Box<dyn IndexFile>) }.boxed_local()
    }

    fn rename<'a>(
        &'a self,
        source: &'a str,
        target: &'a str,
    ) -> LocalBoxFuture<'a, io::Result<()>> {
        let mut files = self.files.borrow_mut();
        let moved = files.remove(source).map(|bytes| {
            files.insert(target.to_owned(), bytes);
        });
        drop(files);
        async move { moved.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound)) }.boxed_local()
    }

    fn sync_directory<'a>(&'a self, _directory: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        async { Ok(()) }.boxed_local()
    }

    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        let removed = self.files.borrow_mut().remove(path);
        async move {
            removed
                .map(|_| ())
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }
        .boxed_local()
    }

    fn open<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, Result<IggyIndexReader, IggyError>> {
        let bytes = self.files.borrow().get(path).cloned();
        async move {
            bytes
                .map(|bytes| IggyIndexReader::from_bytes(path, bytes))
                .ok_or(IggyError::CannotReadFile)
        }
        .boxed_local()
    }
}

impl IndexFile for MemoryIndexFile {
    fn write_at(&mut self, bytes: Bytes, position: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        let start = usize::try_from(position).expect("an index fits in memory");
        let end = start + bytes.len();
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(&bytes);
        async { Ok(()) }.boxed_local()
    }

    fn sync(&mut self) -> LocalBoxFuture<'_, io::Result<()>> {
        self.files
            .borrow_mut()
            .insert(self.path.clone(), Bytes::copy_from_slice(&self.bytes));
        async { Ok(()) }.boxed_local()
    }
}
