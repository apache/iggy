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

//! Two independent clusters on this host for geo topics. Each keeps its geo
//! topics in an `fs` bucket of its own, under its test directory, and reads
//! the other's bucket as its only geo peer.
//!
//! ```ignore
//! let mut pair = GeoClusterPair::builder().nodes(3).build()?;
//! pair.start().await?;
//! let client_a = pair.a().root_client().await?;
//! client_a.create_stream("orders", &StreamCreateOptions::default()).await?;
//! let orders = Identifier::named("orders")?;
//! let topic = pair.a().create_geo_topic(&client_a, &orders, "payments", 2).await?;
//! let client_b = pair.b().root_client().await?;
//! let shadow = pair.b().wait_for_shadow_topic(&client_b, "orders", topic.id).await?;
//! assert!(pair.changes_while_stopped(GeoSide::A).await?.is_empty());
//! ```
//!
//! A cluster can also read its peer's bucket through a [`BucketMirror`], so a
//! watch on the mirror sees that cluster's reads alone.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use futures::channel::oneshot;
use futures::executor::block_on;
use iggy::prelude::{
    GeoMode, Identifier, IggyClient, IggyError, StreamClient, StreamDetails, TopicClient,
    TopicCreateOptions, TopicDetails, TopicType,
};
use iggy_object_store::{
    ErrorClass, FsStore, IdentityRead, KeyLayout, ListRequest, ListedObject, ObjectStore,
    ObjectStoreError, PartitionManifest, PositionsObject, StoreIdentity, TopicObject, TopicRead,
};
use notify::event::{AccessKind, AccessMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::time::sleep;

use crate::harness::config::TestServerConfig;
use crate::harness::error::TestBinaryError;
use crate::harness::orchestrator::TestHarness;
use crate::harness::traits::TestBinary;

/// The origins of the two clusters unless the builder names others.
pub const DEFAULT_ORIGINS: [&str; 2] = ["dc-a", "dc-b"];

/// The tombstone grace of both clusters unless the builder sets another:
/// short, so a deleted topic leaves the peer within a test.
pub const DEFAULT_TOMBSTONE_GRACE: &str = "2s";

/// How long a cluster may take to accept a geo topic: its node claims its
/// bucket first.
pub const GEO_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a cluster may take to adopt a geo topic of its peer: the
/// identity check of the peer, then a pass that lists and reads the peer's
/// topic objects.
pub const ADOPT_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the running cluster may take for [`READ_PASSES`] passes over
/// the bucket of a stopped one.
pub const READ_PASSES_TIMEOUT: Duration = Duration::from_secs(60);

/// The passes over a stopped cluster's bucket that
/// [`GeoClusterPair::changes_while_stopped`] waits for.
pub const READ_PASSES: usize = 2;

/// The bucket prefix of both clusters: the default of `[tiered_storage]` and
/// of a geo peer.
const BUCKET_PREFIX: &str = "iggy";

/// Reads of a bucket further apart than this belong to separate passes.
const READ_PASS_GAP: Duration = Duration::from_secs(2);

/// How often a [`BucketMirror`] copies what changed in its bucket.
const MIRROR_INTERVAL: Duration = Duration::from_millis(20);

/// The suffix of the scratch file a [`BucketMirror`] writes before it
/// renames it over a copy. Its name starts with a dot, which a listing
/// skips.
const MIRROR_SCRATCH: &str = ".mirror";

const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// One of the two clusters of a [`GeoClusterPair`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoSide {
    A,
    B,
}

impl GeoSide {
    pub const BOTH: [Self; 2] = [Self::A, Self::B];

    /// The other cluster, which reads this one's bucket.
    #[must_use]
    pub const fn peer(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// 0 for A and 1 for B, for arrays that hold something of each side.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }
}

/// Builds a [`GeoClusterPair`]. Nothing starts until
/// [`GeoClusterPair::start`].
pub struct GeoClusterPairBuilder {
    test_name: Option<String>,
    origins: [String; 2],
    nodes: usize,
    geo_mode: GeoMode,
    tombstone_grace: String,
    extra_envs: HashMap<String, String>,
    mirrored: Option<GeoSide>,
}

impl Default for GeoClusterPairBuilder {
    fn default() -> Self {
        Self {
            test_name: None,
            origins: DEFAULT_ORIGINS.map(str::to_owned),
            nodes: 1,
            geo_mode: GeoMode::Geo,
            tombstone_grace: DEFAULT_TOMBSTONE_GRACE.to_owned(),
            extra_envs: HashMap::from([
                // The segment cleaner stages the first long-term rewrite of a
                // partition, so a reader of the bucket waits for its tick.
                (
                    "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
                    "100ms".to_owned(),
                ),
                // A listing of the peer's positions each second would leave
                // no gap between the read passes that a watch of a stopped
                // cluster's bucket counts. Tests of the positions set their
                // own interval.
                (
                    "IGGY_GEO_POSITIONS_POLL_INTERVAL".to_owned(),
                    "5s".to_owned(),
                ),
            ]),
            mirrored: None,
        }
    }
}

impl GeoClusterPairBuilder {
    /// The prefix of both clusters' test directories, the name of the test
    /// thread by default.
    #[must_use]
    pub fn test_name(mut self, name: impl Into<String>) -> Self {
        self.test_name = Some(name.into());
        self
    }

    /// The `[geo] origin` of cluster A and of cluster B.
    #[must_use]
    pub fn origins(mut self, a: impl Into<String>, b: impl Into<String>) -> Self {
        self.origins = [a.into(), b.into()];
        self
    }

    /// Nodes in each cluster, 1 by default.
    #[must_use]
    pub const fn nodes(mut self, nodes: usize) -> Self {
        self.nodes = nodes;
        self
    }

    /// The mode of the geo topics that [`GeoCluster::create_geo_topic`]
    /// creates, [`GeoMode::Geo`] by default.
    #[must_use]
    pub const fn geo_mode(mut self, geo_mode: GeoMode) -> Self {
        self.geo_mode = geo_mode;
        self
    }

    /// `[geo] tombstone_grace` of both clusters, as the server parses it,
    /// such as `"2s"`.
    #[must_use]
    pub fn tombstone_grace(mut self, grace: impl Into<String>) -> Self {
        self.tombstone_grace = grace.into();
        self
    }

    /// An environment variable for every node of both clusters. The `[geo]`
    /// and `[tiered_storage]` settings of the pair win over it.
    #[must_use]
    pub fn extra_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_envs.insert(key.into(), value.into());
        self
    }

    /// Let cluster `side` read its peer's bucket through a [`BucketMirror`]
    /// in place of the bucket. See [`GeoCluster::peer_mirror`].
    #[must_use]
    pub const fn peer_reads_through_mirror(mut self, side: GeoSide) -> Self {
        self.mirrored = Some(side);
        self
    }

    /// Build both clusters and create their buckets.
    ///
    /// # Errors
    /// The harness of a cluster cannot be built, or a bucket directory
    /// cannot be created.
    pub fn build(self) -> Result<GeoClusterPair, TestBinaryError> {
        if self.origins[0] == self.origins[1] {
            return Err(TestBinaryError::InvalidState {
                message: format!("both clusters have the origin {:?}", self.origins[0]),
            });
        }
        let test_name = self.test_name.clone().unwrap_or_else(|| {
            thread::current()
                .name()
                .unwrap_or("geo_cluster_pair")
                .replace("::", "_")
        });
        let mut clusters = [
            self.cluster(&test_name, GeoSide::A)?,
            self.cluster(&test_name, GeoSide::B)?,
        ];
        for side in GeoSide::BOTH {
            let peer_bucket = clusters[side.peer().index()].bucket.path.clone();
            let cluster = &mut clusters[side.index()];
            let read_from = if self.mirrored == Some(side) {
                let mirror =
                    BucketMirror::start(&peer_bucket, &cluster.harness.test_dir().join("mirror"))?;
                let path = mirror.path().to_owned();
                cluster.peer_mirror = Some(mirror);
                path
            } else {
                peer_bucket
            };
            cluster.point_at_buckets(&read_from);
        }
        Ok(GeoClusterPair { clusters })
    }

    /// The cluster of `side`, with its bucket created and every setting but
    /// the bucket paths.
    fn cluster(&self, test_name: &str, side: GeoSide) -> Result<GeoCluster, TestBinaryError> {
        let origin = &self.origins[side.index()];
        let mut envs = self.extra_envs.clone();
        envs.insert("IGGY_GEO_ORIGIN".to_owned(), origin.clone());
        envs.insert(
            "IGGY_GEO_TOMBSTONE_GRACE".to_owned(),
            self.tombstone_grace.clone(),
        );
        envs.insert("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned());
        envs.insert(
            "IGGY_GEO_PEERS_0_ORIGIN".to_owned(),
            self.origins[side.peer().index()].clone(),
        );
        envs.insert("IGGY_GEO_PEERS_0_BACKEND".to_owned(), "fs".to_owned());
        let harness = TestHarness::builder()
            .test_name(format!("{test_name}_{origin}"))
            .cluster_nodes(self.nodes)
            .server(TestServerConfig::builder().extra_envs(envs).build())
            .build()?;
        let bucket = Bucket::create(&harness.test_dir().join("bucket"))?;
        Ok(GeoCluster {
            origin: origin.clone(),
            peer_origin: self.origins[side.peer().index()].clone(),
            geo_mode: self.geo_mode,
            bucket,
            peer_mirror: None,
            harness,
        })
    }
}

/// Two independent clusters that read each other's bucket as their geo
/// peer. Dropping the pair stops both before either test directory goes.
pub struct GeoClusterPair {
    clusters: [GeoCluster; 2],
}

impl GeoClusterPair {
    #[must_use]
    pub fn builder() -> GeoClusterPairBuilder {
        GeoClusterPairBuilder::default()
    }

    /// Start cluster A, then cluster B.
    ///
    /// # Errors
    /// A cluster does not start.
    pub async fn start(&mut self) -> Result<(), TestBinaryError> {
        for cluster in &mut self.clusters {
            cluster.start().await?;
        }
        Ok(())
    }

    #[must_use]
    pub const fn cluster(&self, side: GeoSide) -> &GeoCluster {
        &self.clusters[side.index()]
    }

    pub const fn cluster_mut(&mut self, side: GeoSide) -> &mut GeoCluster {
        &mut self.clusters[side.index()]
    }

    #[must_use]
    pub const fn a(&self) -> &GeoCluster {
        self.cluster(GeoSide::A)
    }

    #[must_use]
    pub const fn b(&self) -> &GeoCluster {
        self.cluster(GeoSide::B)
    }

    /// Stop `side`, watch its bucket until the other cluster read it in
    /// [`READ_PASSES`] passes, and start `side` again. Returns every change
    /// of the bucket in that time: only the other cluster ran, so it made
    /// them all, and a pair whose clusters only read their peer returns none.
    ///
    /// # Errors
    /// `side` does not stop or start again, the watch fails, or the other
    /// cluster never read the bucket within [`READ_PASSES_TIMEOUT`].
    pub async fn changes_while_stopped(
        &mut self,
        side: GeoSide,
    ) -> Result<Vec<BucketEvent>, TestBinaryError> {
        let cluster = self.cluster_mut(side);
        cluster.stop().await?;
        let watch = cluster.bucket.watch()?;
        watch
            .wait_for_read_passes(READ_PASSES, READ_PASSES_TIMEOUT)
            .await?;
        let changes = watch.changes();
        drop(watch);
        self.cluster_mut(side).start().await?;
        Ok(changes)
    }
}

impl Drop for GeoClusterPair {
    fn drop(&mut self) {
        for cluster in &mut self.clusters {
            let _ = block_on(cluster.harness.stop());
        }
    }
}

/// One cluster of a [`GeoClusterPair`]: its harness, its bucket, and the
/// origin of its peer.
pub struct GeoCluster {
    origin: String,
    peer_origin: String,
    geo_mode: GeoMode,
    bucket: Bucket,
    peer_mirror: Option<BucketMirror>,
    harness: TestHarness,
}

impl GeoCluster {
    /// Point every node at its own bucket and at `peer_bucket`, the bucket
    /// of its peer.
    fn point_at_buckets(&mut self, peer_bucket: &Path) {
        let own = self.bucket.path.to_string_lossy().into_owned();
        let peer = peer_bucket.to_string_lossy().into_owned();
        for node in 0..self.harness.cluster_size() {
            let server = self.harness.node_mut(node);
            server.add_env("IGGY_TIERED_STORAGE_PATH", own.clone());
            server.add_env("IGGY_GEO_PEERS_0_PATH", peer.clone());
        }
    }

    /// This cluster's `[geo] origin`.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// The origin of the peer whose bucket this cluster reads.
    #[must_use]
    pub fn peer_origin(&self) -> &str {
        &self.peer_origin
    }

    #[must_use]
    pub const fn geo_mode(&self) -> GeoMode {
        self.geo_mode
    }

    /// The bucket this cluster writes and its peer reads.
    #[must_use]
    pub const fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    /// The mirror through which this cluster reads its peer's bucket, `None`
    /// unless the builder asked for one with
    /// [`GeoClusterPairBuilder::peer_reads_through_mirror`].
    #[must_use]
    pub const fn peer_mirror(&self) -> Option<&BucketMirror> {
        self.peer_mirror.as_ref()
    }

    #[must_use]
    pub const fn harness(&self) -> &TestHarness {
        &self.harness
    }

    pub const fn harness_mut(&mut self) -> &mut TestHarness {
        &mut self.harness
    }

    /// Whether any node of the cluster runs.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.harness
            .all_servers()
            .iter()
            .any(TestBinary::is_running)
    }

    /// Start every node, with the data each one kept from an earlier run.
    ///
    /// # Errors
    /// See [`TestHarness::start`].
    pub async fn start(&mut self) -> Result<(), TestBinaryError> {
        self.harness.start().await
    }

    /// Stop every node. The bucket and each node's data stay.
    ///
    /// # Errors
    /// See [`TestHarness::stop`].
    pub async fn stop(&mut self) -> Result<(), TestBinaryError> {
        self.harness.stop().await
    }

    /// A TCP client logged in as root to the first node that runs.
    ///
    /// # Errors
    /// No node runs, or the login fails.
    pub async fn root_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.harness.tcp_root_client().await
    }

    /// The name of the stream in which this cluster serves the geo topics of
    /// the peer stream `peer_stream`.
    #[must_use]
    pub fn shadow_stream_name(&self, peer_stream: &str) -> String {
        format!("__shadow_{}_{peer_stream}", self.peer_origin)
    }

    /// Create a geo topic of the pair's mode with `partitions` partitions,
    /// retrying while the cluster refuses geo topics as unavailable, up to
    /// [`GEO_READY_TIMEOUT`].
    ///
    /// # Errors
    /// Any refusal but [`IggyError::FeatureUnavailable`], or that one at the
    /// timeout.
    pub async fn create_geo_topic(
        &self,
        client: &IggyClient,
        stream: &Identifier,
        name: &str,
        partitions: u32,
    ) -> Result<TopicDetails, IggyError> {
        let options = TopicCreateOptions {
            partitions_count: Some(partitions),
            topic_type: Some(TopicType::Geo),
            geo_mode: Some(self.geo_mode),
            ..TopicCreateOptions::default()
        };
        let deadline = Instant::now() + GEO_READY_TIMEOUT;
        loop {
            match client.create_topic(stream, name, &options).await {
                Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                    sleep(POLL_INTERVAL).await;
                }
                created => return created,
            }
        }
    }

    /// Wait until this cluster serves the topic `topic_id` of the peer
    /// stream `peer_stream` as a shadow topic, and return its shadow stream.
    ///
    /// # Errors
    /// The topic is not adopted within [`ADOPT_TIMEOUT`].
    pub async fn wait_for_shadow_topic(
        &self,
        client: &IggyClient,
        peer_stream: &str,
        topic_id: u32,
    ) -> Result<StreamDetails, TestBinaryError> {
        let name = self.shadow_stream_name(peer_stream);
        let stream = Identifier::named(&name).map_err(|error| TestBinaryError::InvalidState {
            message: format!("{name:?} is no stream name: {error}"),
        })?;
        let deadline = Instant::now() + ADOPT_TIMEOUT;
        loop {
            if let Ok(Some(details)) = client.get_stream(&stream).await
                && details.topics.iter().any(|topic| topic.id == topic_id)
            {
                return Ok(details);
            }
            if Instant::now() >= deadline {
                return Err(TestBinaryError::InvalidState {
                    message: format!(
                        "{} never adopted topic {topic_id} of the stream {peer_stream:?} of {}",
                        self.origin, self.peer_origin
                    ),
                });
            }
            sleep(POLL_INTERVAL).await;
        }
    }
}

/// The `fs` bucket of one cluster, read the way a peer reads it.
#[derive(Debug, Clone)]
pub struct Bucket {
    path: PathBuf,
    layout: KeyLayout,
}

impl Bucket {
    fn create(path: &Path) -> Result<Self, TestBinaryError> {
        fs::create_dir_all(path).map_err(|source| TestBinaryError::FileSystemError {
            path: path.to_owned(),
            source,
        })?;
        let layout = KeyLayout::new(BUCKET_PREFIX).map_err(store_error)?;
        Ok(Self {
            path: path.to_owned(),
            layout,
        })
    }

    /// The directory of the bucket: `[tiered_storage] path` of its cluster.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn layout(&self) -> &KeyLayout {
        &self.layout
    }

    /// The `cluster_uid` that the bucket's identity object names, `None`
    /// before a cluster claimed the bucket.
    ///
    /// # Errors
    /// The read fails, or the object is not one that a cluster wrote.
    pub async fn cluster_uid(&self) -> Result<Option<u128>, TestBinaryError> {
        let (path, layout) = (self.path.clone(), self.layout.clone());
        let read =
            on_compio(move || async move { StoreIdentity::read(&reader(&path), &layout).await })
                .await?
                .map_err(store_error)?;
        match read {
            IdentityRead::Found(identity) => Ok(Some(identity.cluster_uid)),
            IdentityRead::Missing => Ok(None),
            IdentityRead::Invalid => Err(TestBinaryError::InvalidState {
                message: "the identity object of the bucket is not one a cluster wrote".to_owned(),
            }),
        }
    }

    /// Every geo topic object in the bucket, live ones and tombstones.
    ///
    /// # Errors
    /// A listing or a read fails, or an object does not decode.
    pub async fn topic_objects(&self) -> Result<Vec<TopicObject>, TestBinaryError> {
        let (path, layout) = (self.path.clone(), self.layout.clone());
        on_compio(move || async move {
            let store = reader(&path);
            let mut objects = Vec::new();
            for listed in list(&store, &layout.topics_prefix()).await? {
                let Some(key) = layout.parse_topic_key(&listed.key) else {
                    continue;
                };
                match TopicObject::read(&store, &layout, &key).await? {
                    TopicRead::Found(object) => objects.push(object),
                    TopicRead::Missing => {}
                    TopicRead::Invalid(error) => {
                        return Err(ObjectStoreError::Malformed(format!(
                            "the topic object {}: {error}",
                            listed.key
                        )));
                    }
                }
            }
            Ok(objects)
        })
        .await?
        .map_err(store_error)
    }

    /// Every object of consumer group positions in the bucket.
    ///
    /// # Errors
    /// A listing or a read fails, or an object does not decode.
    pub async fn positions_objects(&self) -> Result<Vec<PositionsObject>, TestBinaryError> {
        let (path, layout) = (self.path.clone(), self.layout.clone());
        on_compio(move || async move {
            let store = reader(&path);
            let mut objects = Vec::new();
            for listed in list(&store, &layout.positions_prefix()).await? {
                let Some(key) = layout.parse_positions_key(&listed.key) else {
                    continue;
                };
                let Some(bytes) = read_whole(&store, &listed).await? else {
                    continue;
                };
                let object = PositionsObject::decode_under(&key, &bytes).map_err(|error| {
                    ObjectStoreError::Malformed(format!("the positions {}: {error}", listed.key))
                })?;
                objects.push(object);
            }
            Ok(objects)
        })
        .await?
        .map_err(store_error)
    }

    /// Every partition manifest in the bucket.
    ///
    /// # Errors
    /// A listing or a read fails, or a manifest does not decode.
    pub async fn manifests(&self) -> Result<Vec<PartitionManifest>, TestBinaryError> {
        let (path, layout) = (self.path.clone(), self.layout.clone());
        on_compio(move || async move {
            let store = reader(&path);
            let meta = format!("{}/meta/", layout.root());
            let mut manifests = Vec::new();
            for listed in list(&store, &meta).await? {
                if !listed.key.ends_with("/manifest.bin") {
                    continue;
                }
                let Some(bytes) = read_whole(&store, &listed).await? else {
                    continue;
                };
                let manifest = PartitionManifest::decode(&bytes).map_err(|error| {
                    ObjectStoreError::Malformed(format!("the manifest {}: {error}", listed.key))
                })?;
                manifests.push(manifest);
            }
            Ok(manifests)
        })
        .await?
        .map_err(store_error)
    }

    /// The body of the object under `key`, `None` when there is none.
    ///
    /// # Errors
    /// The read fails.
    pub async fn object(&self, key: &str) -> Result<Option<Vec<u8>>, TestBinaryError> {
        let (path, key) = (self.path.clone(), key.to_owned());
        on_compio(move || async move {
            let store = reader(&path);
            let size = match store.head(&key).await {
                Ok(meta) => meta.size,
                Err(error) if error.class() == ErrorClass::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            if size == 0 {
                return Ok(Some(Vec::new()));
            }
            store
                .get_range(&key, 0..size)
                .await
                .map(|range| Some(range.bytes.to_vec()))
        })
        .await?
        .map_err(store_error)
    }

    /// The bytes of every file in the bucket but scratch files and other
    /// dot-names, by path. An object's body is in its file as it was put, so
    /// a message stored uncompressed shows in its bytes.
    ///
    /// # Errors
    /// A directory or a file cannot be read.
    pub fn files(&self) -> Result<Vec<(PathBuf, Vec<u8>)>, TestBinaryError> {
        let mut files = Vec::new();
        let mut pending = vec![self.path.clone()];
        while let Some(directory) = pending.pop() {
            let entries =
                fs::read_dir(&directory).map_err(|source| TestBinaryError::FileSystemError {
                    path: directory.clone(),
                    source,
                })?;
            for entry in entries {
                let entry = entry.map_err(TestBinaryError::Io)?;
                let path = entry.path();
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let file_type = entry.file_type().map_err(TestBinaryError::Io)?;
                if file_type.is_dir() {
                    pending.push(path);
                } else if file_type.is_file() {
                    match fs::read(&path) {
                        Ok(bytes) => files.push((path, bytes)),
                        // Its cluster deleted it after the listing.
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(source) => {
                            return Err(TestBinaryError::FileSystemError { path, source });
                        }
                    }
                }
            }
        }
        Ok(files)
    }

    /// Start recording every read and change under the bucket.
    ///
    /// # Errors
    /// The watch cannot start.
    pub fn watch(&self) -> Result<BucketWatch, TestBinaryError> {
        BucketWatch::start(&self.path)
    }
}

/// What a [`BucketWatch`] saw under its bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketEvent {
    /// A file or directory was opened.
    Read { at: Instant, paths: Vec<PathBuf> },
    /// Anything else: a create, a write, a rename, a delete, a change of
    /// metadata, or events the watch lost.
    Change { kind: String, paths: Vec<PathBuf> },
}

/// Records every read and every change under a bucket while it lives. A
/// bucket whose cluster is stopped has no writer of its own, so whatever
/// changes it while the watch runs came from elsewhere.
pub struct BucketWatch {
    events: Arc<Mutex<Vec<BucketEvent>>>,
    _watcher: RecommendedWatcher,
}

impl BucketWatch {
    /// Watch `root` and every directory under it, also those created later.
    ///
    /// # Errors
    /// The watch cannot start.
    pub fn start(root: &Path) -> Result<Self, TestBinaryError> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
            let event = match event {
                Ok(event) => BucketEvent::from(event),
                Err(error) => BucketEvent::Change {
                    kind: format!("watch error: {error}"),
                    paths: error.paths,
                },
            };
            lock(&recorded).push(event);
        })
        .map_err(watch_error)?;
        watcher
            .watch(root, RecursiveMode::Recursive)
            .map_err(watch_error)?;
        Ok(Self {
            events,
            _watcher: watcher,
        })
    }

    /// Every read so far.
    #[must_use]
    pub fn reads(&self) -> Vec<BucketEvent> {
        lock(&self.events)
            .iter()
            .filter(|event| matches!(event, BucketEvent::Read { .. }))
            .cloned()
            .collect()
    }

    /// Every change so far.
    #[must_use]
    pub fn changes(&self) -> Vec<BucketEvent> {
        lock(&self.events)
            .iter()
            .filter(|event| matches!(event, BucketEvent::Change { .. }))
            .cloned()
            .collect()
    }

    /// The passes in which something read the bucket so far: runs of reads
    /// with no gap longer than `READ_PASS_GAP` inside.
    #[must_use]
    pub fn read_passes(&self) -> usize {
        let mut passes = 0;
        let mut last: Option<Instant> = None;
        for event in lock(&self.events).iter() {
            let BucketEvent::Read { at, .. } = event else {
                continue;
            };
            if last.is_none_or(|last| at.saturating_duration_since(last) > READ_PASS_GAP) {
                passes += 1;
            }
            last = Some(*at);
        }
        passes
    }

    /// Wait until something read the bucket in `passes` passes.
    ///
    /// # Errors
    /// Fewer passes came within `timeout`.
    pub async fn wait_for_read_passes(
        &self,
        passes: usize,
        timeout: Duration,
    ) -> Result<(), TestBinaryError> {
        let deadline = Instant::now() + timeout;
        loop {
            let seen = self.read_passes();
            if seen >= passes {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TestBinaryError::InvalidState {
                    message: format!(
                        "the bucket was read in {seen} passes within {timeout:?}, not {passes}"
                    ),
                });
            }
            sleep(POLL_INTERVAL).await;
        }
    }
}

impl From<Event> for BucketEvent {
    fn from(event: Event) -> Self {
        let lost = event.need_rescan();
        match event.kind {
            EventKind::Access(AccessKind::Close(AccessMode::Write)) => Self::Change {
                kind: "closed after a write".to_owned(),
                paths: event.paths,
            },
            EventKind::Access(_) if !lost => Self::Read {
                at: Instant::now(),
                paths: event.paths,
            },
            kind => Self::Change {
                kind: if lost {
                    "events lost".to_owned()
                } else {
                    format!("{kind:?}")
                },
                paths: event.paths,
            },
        }
    }
}

/// A copy of a bucket that a thread keeps in step with it while it lives,
/// which one cluster reads in place of its peer's bucket. The cluster that
/// owns the bucket reads it too, for its own sweeps and rewrites, so a watch
/// on the bucket cannot tell whose reads it saw. A watch on the mirror can.
///
/// Every `MIRROR_INTERVAL` the thread copies each file that is new or was
/// replaced, through a scratch file that it renames over the copy, and
/// removes the copies of files that left the bucket. It opens nothing under
/// the mirror but its scratch files, whose names start with a dot.
pub struct BucketMirror {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    copies: Arc<Mutex<Vec<MirrorCopy>>>,
    thread: Option<JoinHandle<()>>,
}

/// A file a [`BucketMirror`] copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCopy {
    pub at: Instant,
    /// The path of the copy under the mirror.
    pub path: PathBuf,
}

impl BucketMirror {
    /// Mirror the bucket at `bucket` into the directory `path`.
    ///
    /// # Errors
    /// The directory cannot be created or the thread cannot start.
    pub fn start(bucket: &Path, path: &Path) -> Result<Self, TestBinaryError> {
        fs::create_dir_all(path).map_err(|source| TestBinaryError::FileSystemError {
            path: path.to_owned(),
            source,
        })?;
        let stop = Arc::new(AtomicBool::new(false));
        let copies = Arc::new(Mutex::new(Vec::new()));
        let mut step = MirrorStep {
            bucket: bucket.to_owned(),
            mirror: path.to_owned(),
            copies: Arc::clone(&copies),
            seen: HashMap::new(),
            directories: HashSet::new(),
        };
        let stopped = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("geo-bucket-mirror".to_owned())
            .spawn(move || {
                while !stopped.load(Ordering::Acquire) {
                    step.run();
                    thread::sleep(MIRROR_INTERVAL);
                }
            })
            .map_err(TestBinaryError::Io)?;
        Ok(Self {
            path: path.to_owned(),
            stop,
            copies,
            thread: Some(thread),
        })
    }

    /// The directory the reading cluster takes for its peer's bucket.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Start recording every read and change under the mirror.
    ///
    /// # Errors
    /// The watch cannot start.
    pub fn watch(&self) -> Result<BucketWatch, TestBinaryError> {
        BucketWatch::start(&self.path)
    }

    /// Every file copied so far, oldest first.
    #[must_use]
    pub fn copies(&self) -> Vec<MirrorCopy> {
        self.copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for BucketMirror {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// What the thread of a [`BucketMirror`] knows between two passes.
struct MirrorStep {
    bucket: PathBuf,
    mirror: PathBuf,
    copies: Arc<Mutex<Vec<MirrorCopy>>>,
    /// Each file copied, by its path under the bucket, with the inode, the
    /// length and the modification time it had.
    seen: HashMap<PathBuf, (u64, u64, SystemTime)>,
    /// The directories created under the mirror.
    directories: HashSet<PathBuf>,
}

impl MirrorStep {
    /// Copy what changed and remove what left. A file that changes or goes
    /// while it is copied is taken again by the next pass.
    fn run(&mut self) {
        let mut present = HashSet::new();
        let mut pending = vec![PathBuf::new()];
        while let Some(directory) = pending.pop() {
            let Ok(entries) = fs::read_dir(self.bucket.join(&directory)) else {
                continue;
            };
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let relative = directory.join(entry.file_name());
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => pending.push(relative),
                    Ok(kind) if kind.is_file() => {
                        present.insert(relative.clone());
                        self.copy_if_changed(&relative);
                    }
                    _ => {}
                }
            }
        }
        let mirror = &self.mirror;
        self.seen.retain(|relative, _| {
            present.contains(relative) || {
                let _ = fs::remove_file(mirror.join(relative));
                false
            }
        });
    }

    fn copy_if_changed(&mut self, relative: &Path) {
        let source = self.bucket.join(relative);
        let Ok(metadata) = fs::metadata(&source) else {
            return;
        };
        let Ok(modified) = metadata.modified() else {
            return;
        };
        let version = (metadata.ino(), metadata.len(), modified);
        if self.seen.get(relative) == Some(&version) {
            return;
        }
        let Ok(bytes) = fs::read(&source) else {
            return;
        };
        let target = self.mirror.join(relative);
        let Some(parent) = target.parent().map(Path::to_owned) else {
            return;
        };
        if !self.directories.contains(&parent) {
            if fs::create_dir_all(&parent).is_err() {
                return;
            }
            self.directories.insert(parent.clone());
        }
        let Some(name) = target.file_name() else {
            return;
        };
        let scratch = parent.join(format!(".{}{MIRROR_SCRATCH}", name.to_string_lossy()));
        if fs::write(&scratch, &bytes).is_err() || fs::rename(&scratch, &target).is_err() {
            let _ = fs::remove_file(&scratch);
            return;
        }
        self.seen.insert(relative.to_owned(), version);
        self.copies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(MirrorCopy {
                at: Instant::now(),
                path: target,
            });
    }
}

fn lock(events: &Mutex<Vec<BucketEvent>>) -> MutexGuard<'_, Vec<BucketEvent>> {
    events.lock().unwrap_or_else(PoisonError::into_inner)
}

fn watch_error(error: notify::Error) -> TestBinaryError {
    TestBinaryError::InvalidState {
        message: format!("cannot watch the bucket: {error}"),
    }
}

fn store_error(error: ObjectStoreError) -> TestBinaryError {
    TestBinaryError::InvalidState {
        message: format!("cannot read the bucket: {error}"),
    }
}

/// The store a peer reads the bucket at `path` through.
fn reader(path: &Path) -> FsStore {
    FsStore::foreign(path)
}

/// Every object under `prefix`.
async fn list(store: &FsStore, prefix: &str) -> Result<Vec<ListedObject>, ObjectStoreError> {
    let request = ListRequest::new(prefix);
    let mut objects = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let page = match continuation.as_deref() {
            Some(after) => store.list(&request.after(after)).await?,
            None => store.list(&request).await?,
        };
        objects.extend(page.objects);
        match page.next {
            Some(next) => continuation = Some(next),
            None => return Ok(objects),
        }
    }
}

/// The body of a listed object, `None` when it went after the listing.
async fn read_whole(
    store: &FsStore,
    listed: &ListedObject,
) -> Result<Option<Vec<u8>>, ObjectStoreError> {
    if listed.size == 0 {
        return Ok(Some(Vec::new()));
    }
    match store.get_range(&listed.key, 0..listed.size).await {
        Ok(range) => Ok(Some(range.bytes.to_vec())),
        Err(error) if error.class() == ErrorClass::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Run `task` on a compio runtime of its own thread, which the `fs` store
/// needs, and wait for its result without blocking the test's runtime.
async fn on_compio<T, F, Fut>(task: F) -> Result<T, TestBinaryError>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    thread::Builder::new()
        .name("geo-bucket-reader".to_owned())
        .spawn(move || {
            let outcome = compio::runtime::Runtime::new().map(|runtime| runtime.block_on(task()));
            let _ = sender.send(outcome);
        })
        .map_err(TestBinaryError::Io)?;
    receiver
        .await
        .map_err(|_| TestBinaryError::InvalidState {
            message: "the bucket reader stopped without an answer".to_owned(),
        })?
        .map_err(TestBinaryError::Io)
}
