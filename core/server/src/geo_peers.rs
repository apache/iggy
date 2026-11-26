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

//! What the server does with the peers of the `[geo]` section: the checks
//! before the shards start, the read-only stores that each shard reads the
//! peers through, and the identity checks that shard 0 runs for the node.
//!
//! Each shard gets one store and one tier service per peer, because a
//! provider holds files and connections of the shard that owns it. A peer's
//! tier service takes its caps and timeouts from `[tiered_storage]`.
//!
//! A peer never stops the boot once its configuration can work, and never
//! holds up local traffic: the checks run in a task of shard 0 after the
//! shards start, each peer's on a schedule of its own, so a slow read of one
//! peer holds up no check of another. Shard 0 reads each peer's store
//! identity object at once,
//! every 5 seconds while the node reads nothing from the peer or the store
//! does not answer, and every minute after that. It checks a peer at once
//! too when committed metadata changes the clusters it expects of the peer,
//! or when a pass on any shard asks for it, see
//! [`SharedPeerStatus::adopts`].
//!
//! Each check compares the claim it reads with what committed metadata
//! recorded of the peer: the clusters that its shadow streams came from,
//! and for a retired origin the ones its retirement kept, see
//! [`StreamsInner::recorded_cluster_uids`]. A claim of one of them counts,
//! and so does any claim while metadata holds none, so a new reader trusts
//! the first claim it reads. A claim of another cluster counts only when the
//! recovery records under the root lead to it from a recorded one, as the
//! recovery of a cluster that lost every disk writes them. A check follows
//! those records only then, since a record can be large, and keeps what it
//! found for that claim. It reads at most [`RECOVERY_READS_PER_CHECK`] of
//! them, and the next check goes on from there, until the records end or
//! come back to one it read. Any other claim leaves the peer mismatched, and
//! the node reads nothing from it, see [`PeerIdentity`]. Nor does it while
//! the records have not proven a claim, also of a peer that it read. The
//! node remembers what it read in memory only, so a restarted node judges
//! the claim again.
//!
//! When committed metadata changes the clusters it expects of a peer, the
//! node stops reading a peer whose cluster they no longer cover, until a
//! check accepts a claim again.
//!
//! A peer whose origin committed metadata retired is marked so, and checked
//! only until a check in this process accepts the claim on its root. Its
//! polls wait for that one read, and no read of its identity object goes out
//! after it, unless metadata stops covering that claim.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use configs::geo::{GeoConfig, GeoPeerConfig};
use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use consensus::{MetadataHandle, PartitionsHandle};
use futures::future::{join_all, select};
use iggy_object_store::gcs::{GcsAuth, GcsStore};
use iggy_object_store::s3::{S3Store, SharedCredentials};
use iggy_object_store::{
    Backend, ErrorClass, FsStore, IdentityRead, KeyLayout, ObjectStore, ObjectStoreError,
    ReadOnlyStore, RecoveryRead, RecoveryRecord, StoreIdentity,
};
use message_bus::{IggyMessageBus, MessageBus};
use metadata::impls::metadata::StreamsFrontend;
#[cfg(doc)]
use metadata::stm::stream::StreamsInner;
use partitions::ReadCache;
use shard::metrics::ShardMetrics;
use shard::peers::{PeerIdentity, PeerRead, PeerStatus, PeerStore, PeerStores, SharedPeerStatus};
use shard::{Receiver, Sender, channel};
use tracing::{debug, error, info, warn};

use crate::s3_credentials::RoleCredentials;
use crate::server_error::ServerError;
use crate::shell::ServerShard;
use crate::store_identity::own_cluster_uid;
use crate::tiered_storage::{S3Location, gcs_config, identity_timeout, service_config};

/// How long shard 0 waits before it checks a peer again, once the node reads
/// the peer or found a change that only an operator ends.
const RECHECK_INTERVAL: Duration = Duration::from_mins(1);
/// How long shard 0 waits before it checks a peer again, while the node
/// reads nothing from the peer or its store does not answer.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// How long shard 0 waits for this cluster's `cluster_uid`, which the first
/// committed metadata operation sets.
const CLUSTER_UID_WAIT: Duration = Duration::from_secs(1);
/// The longest shard 0 waits before it looks again for a retired origin, a
/// change of the clusters that metadata expects of a peer, or a check that a
/// shard asked for. A look sends no request, and reads committed metadata
/// again only once its last read for any peer is this old.
const LOOK_WAIT: Duration = Duration::from_secs(1);
/// The most recovery records that one check reads. Each recovery of a
/// cluster that lost every disk writes one.
const RECOVERY_READS_PER_CHECK: usize = 8;

/// How the stores of each peer authorize their requests, in the order of
/// the configuration. Built once per node, so every shard's stores of a
/// peer sign with the credentials or send the token of one refresher.
#[derive(Clone, Default)]
pub struct PeerAuth(Vec<StoreAuth>);

/// How the stores of one peer authorize their requests.
#[derive(Clone)]
enum StoreAuth {
    /// An `fs` peer needs nothing.
    None,
    /// The keys of an `s3` peer: static, or fetched for the node's role.
    S3(SharedCredentials),
    /// The access token of a `gcs` peer.
    Gcs(GcsAuth),
}

/// Boot-time checks of the peers, after `Validatable::validate`. A peer
/// whose configuration can never work refuses the boot here, before any
/// shard starts. Nothing here reaches a peer's store, so a peer that does
/// not answer never stops the boot.
///
/// # Errors
/// [`ServerError::GeoPeer`] for a prefix that is not a usable key prefix,
/// or an `s3` or `gcs` endpoint that cannot address a bucket.
pub fn preflight(config: &GeoConfig) -> Result<(), ServerError> {
    for peer in &config.peers {
        layout(peer)?;
        store(peer, Some(&StoreAuth::unset(peer.backend)))?;
    }
    Ok(())
}

/// The credentials of the `s3` peers and the access tokens of the `gcs`
/// peers. Each peer that takes them from a role or a token service gets a
/// refresher of its own, which starts here, after [`preflight`]. The boot
/// never waits for a peer's first credentials.
///
/// # Errors
/// [`ServerError::GeoPeer`] for an `s3` peer without keys on a node that
/// has no role, and for a key file that cannot sign.
pub fn auth(
    config: &GeoConfig,
    tiered: &TieredStorageConfig,
    roles: &mut RoleCredentials,
) -> Result<PeerAuth, ServerError> {
    config
        .peers
        .iter()
        .map(|peer| match peer.backend {
            TieredStorageBackend::S3 => {
                crate::s3_credentials::peer_credentials(peer, &tiered.region, roles)
                    .map(StoreAuth::S3)
            }
            TieredStorageBackend::Gcs => {
                crate::gcs_credentials::peer_auth(&peer.origin, &peer.gcs).map(StoreAuth::Gcs)
            }
            TieredStorageBackend::None
            | TieredStorageBackend::Fs
            | TieredStorageBackend::MockS3 => Ok(StoreAuth::None),
        })
        .collect::<Result<_, _>>()
        .map(PeerAuth)
}

/// What the node knows about each peer, in the order of the configuration.
/// Every shard reads the same cells, and only the checks of shard 0 set
/// them.
#[must_use]
pub fn statuses(config: &GeoConfig) -> Vec<SharedPeerStatus> {
    config
        .peers
        .iter()
        .map(|_| SharedPeerStatus::default())
        .collect()
}

/// Give `shard` a read-only store and a tier service for each peer, with
/// the status of the peer from `statuses`. On shard 0, also start the
/// identity checks of the node.
///
/// Returns the stop signal of the checks' task, or `None` on a node without
/// peers and on every shard but shard 0.
pub fn start(
    config: &GeoConfig,
    tiered: &TieredStorageConfig,
    bus: &Rc<IggyMessageBus>,
    shard: &Rc<ServerShard>,
    statuses: &[SharedPeerStatus],
    auth: &PeerAuth,
) -> Option<Sender<()>> {
    if config.peers.is_empty() {
        return None;
    }
    let settings = crate::tiered_storage::read_cache_settings(tiered);
    let partitions = shard.plane.partitions();
    // A node with a store of its own made the cache already. One without
    // makes it only when it keeps what the peers' polls fetch.
    if partitions.read_cache().is_none() && settings.cache_bytes > 0 {
        partitions.set_read_cache(ReadCache::new(settings));
    }
    shard.set_peer_stores(stores(config, tiered, bus, statuses, auth));
    if shard.id != 0 {
        return None;
    }
    let checkers = checkers(config, tiered, statuses, auth);
    for checker in &checkers {
        shard
            .metrics()
            .set_geo_peer_status(&checker.origin, false, false);
    }
    info!(
        origin = %config.origin,
        peers = ?config.peers.iter().map(|peer| peer.origin.as_str()).collect::<Vec<_>>(),
        "geo peers configured; this node reads a peer once a check verifies the cluster that \
         owns its root, and never writes to a peer's store"
    );
    let (stop_tx, stop_rx) = channel(1);
    let task = compio::runtime::spawn(run(Rc::clone(shard), checkers, stop_rx));
    bus.track_background(task);
    Some(stop_tx)
}

/// One read-only store and one tier service per peer, for one shard.
/// `preflight` refused the boot already when a store or a layout fails.
fn stores<B>(
    config: &GeoConfig,
    tiered: &TieredStorageConfig,
    bus: &B,
    statuses: &[SharedPeerStatus],
    auth: &PeerAuth,
) -> PeerStores<B>
where
    B: MessageBus + Clone + 'static,
{
    let service = service_config(tiered);
    let stores = config
        .peers
        .iter()
        .zip(statuses)
        .enumerate()
        .filter_map(|(index, (peer, status))| {
            Some(PeerStore::new(
                peer.origin.clone(),
                layout(peer).ok()?,
                store(peer, auth.of(index)).ok()?,
                bus.clone(),
                &service,
                status.clone(),
            ))
        })
        .collect();
    PeerStores::new(stores)
}

/// The identity check of each peer. Each check gets a store of its own,
/// outside the peer's tier service, so that no queued job and no retry
/// delays what it finds.
fn checkers(
    config: &GeoConfig,
    tiered: &TieredStorageConfig,
    statuses: &[SharedPeerStatus],
    auth: &PeerAuth,
) -> Vec<PeerChecker<ReadOnlyStore>> {
    config
        .peers
        .iter()
        .zip(statuses)
        .enumerate()
        .filter_map(|(index, (peer, status))| {
            Some(PeerChecker::new(
                peer.origin.clone(),
                store(peer, auth.of(index)).ok()?,
                layout(peer).ok()?,
                identity_timeout(tiered),
                status.clone(),
            ))
        })
        .collect()
}

/// Check each peer on a schedule of its own, see [`PeerChecker::run`], until
/// `stop` fires or its sender is gone.
async fn run<S: ObjectStore>(
    shard: Rc<ServerShard>,
    checkers: Vec<PeerChecker<S>>,
    stop: Receiver<()>,
) {
    let metadata = MetadataView::new(|| Looks::read(&shard), || own_cluster_uid(&shard));
    let metrics = shard.metrics();
    let checks = join_all(
        checkers
            .into_iter()
            .map(|checker| checker.run(&metadata, metrics)),
    );
    select(pin!(checks), pin!(stop.recv())).await;
}

/// What committed metadata says of the peers, from one pass over its
/// streams.
#[derive(Debug, Default)]
struct Looks {
    /// The origins that it retired.
    retired: BTreeSet<String>,
    /// The clusters that it expects of each origin, see
    /// [`StreamsInner::recorded_cluster_uids`].
    recorded: BTreeMap<String, BTreeSet<u128>>,
}

impl Looks {
    fn read(shard: &ServerShard) -> Self {
        shard.plane.metadata().mux_stm.streams().read(|inner| Self {
            retired: inner.retired_origins.keys().cloned().collect(),
            recorded: inner.recorded_cluster_uids_by_origin(),
        })
    }
}

/// What the checks of every peer read of committed metadata, shared so that
/// one read serves the looks of all of them.
struct MetadataView<R, U> {
    /// One read of [`Looks`].
    read: R,
    /// This cluster's `cluster_uid`, which the first committed metadata
    /// operation sets.
    own_cluster_uid: U,
    /// The last [`Looks`] read, and when.
    last: RefCell<Option<(Instant, Rc<Looks>)>>,
}

impl<R, U> MetadataView<R, U>
where
    R: Fn() -> Looks,
    U: Fn() -> Option<u128>,
{
    const fn new(read: R, own_cluster_uid: U) -> Self {
        Self {
            read,
            own_cluster_uid,
            last: RefCell::new(None),
        }
    }

    /// What committed metadata says of the peers at `now`, read again once
    /// the last read is a [`LOOK_WAIT`] old.
    fn looks(&self, now: Instant) -> Rc<Looks> {
        let mut last = self.last.borrow_mut();
        if let Some((at, looks)) = last.as_ref()
            && now.saturating_duration_since(*at) < LOOK_WAIT
        {
            return Rc::clone(looks);
        }
        let looks = Rc::new((self.read)());
        *last = Some((now, Rc::clone(&looks)));
        looks
    }
}

/// The identity check of one peer, which shard 0 runs for the node.
struct PeerChecker<S> {
    origin: String,
    store: S,
    layout: KeyLayout,
    /// The longest one read of a check waits for the store.
    timeout: Duration,
    status: SharedPeerStatus,
    /// When the next check is due.
    due: Instant,
    /// What the last check read, so that a log line marks each change.
    last: Option<PeerRead>,
    /// The clusters that committed metadata expects of the peer, see
    /// [`StreamsInner::recorded_cluster_uids`].
    expected: BTreeSet<u128>,
    /// How far the checks followed the recovery records under the root
    /// toward the last claim that the expected clusters lacked.
    proof: Option<Proof>,
}

/// How far the checks followed the recovery records under a peer's root
/// toward the claim of `found`, from the clusters `expected`.
struct Proof {
    found: u128,
    expected: BTreeSet<u128>,
    walk: Walk,
}

enum Walk {
    /// The records lead to the claim from this expected cluster, or from
    /// none.
    Ended(Option<u128>),
    /// The chains still to follow, the next one last, each as the expected
    /// cluster it starts from and the cluster whose record it reads next.
    /// `followed` holds every cluster whose record the walk read, so a
    /// chain that comes back to one ends.
    Going {
        chains: Vec<(u128, u128)>,
        followed: BTreeSet<u128>,
    },
}

/// What one check of a peer found: the read, and the error behind an outage
/// or a refusal.
#[derive(Debug)]
struct PeerCheck {
    read: PeerRead,
    error: Option<ObjectStoreError>,
}

impl<S: ObjectStore> PeerChecker<S> {
    fn new(
        origin: String,
        store: S,
        layout: KeyLayout,
        timeout: Duration,
        status: SharedPeerStatus,
    ) -> Self {
        Self {
            origin,
            store,
            layout,
            timeout,
            status,
            due: Instant::now(),
            last: None,
            expected: BTreeSet::new(),
            proof: None,
        }
    }

    #[cfg(test)]
    fn retired(&self) -> bool {
        self.status.get().retired
    }

    /// Whether the checks of the peer go on: a peer whose origin retired is
    /// checked only until a check accepts the claim on its root. A change
    /// that only a restart ends stops them too.
    fn checks(&self) -> bool {
        let status = self.status.get();
        !status.retired
            || matches!(
                status.identity,
                PeerIdentity::Unverified
                    | PeerIdentity::Mismatched { .. }
                    | PeerIdentity::Unconfirmed { .. }
            )
    }

    fn wants_check(&self, now: Instant) -> bool {
        self.checks() && self.due <= now
    }

    /// Look at committed metadata and at the checks that the shards ask for
    /// at most a [`LOOK_WAIT`] apart, and check the peer whenever its check
    /// is due, until dropped. A peer whose origin retired is marked so, and
    /// checked no more once a check accepted the claim on its root.
    async fn run<R, U>(mut self, metadata: &MetadataView<R, U>, metrics: &ShardMetrics)
    where
        R: Fn() -> Looks,
        U: Fn() -> Option<u128>,
    {
        loop {
            let now = Instant::now();
            let looks = metadata.looks(now);
            if looks.retired.contains(&self.origin) {
                self.retire(metrics);
            }
            match looks.recorded.get(&self.origin) {
                Some(expected) => self.look(expected, now, metrics),
                None => self.look(&BTreeSet::new(), now, metrics),
            }
            let wait = match (metadata.own_cluster_uid)() {
                None => CLUSTER_UID_WAIT,
                Some(own_cluster_uid) => {
                    if self.wants_check(now) {
                        self.step(own_cluster_uid, metrics).await;
                    }
                    if self.checks() {
                        self.due.saturating_duration_since(Instant::now())
                    } else {
                        RECHECK_INTERVAL
                    }
                }
            };
            compio::time::sleep(wait.min(LOOK_WAIT)).await;
        }
    }

    /// Take the clusters that committed metadata now expects of the peer,
    /// and any check that a shard asked for. Either makes the next check
    /// due at `now`. A verified cluster that the new clusters do not cover
    /// is read no more until a check accepts a claim again.
    fn look(&mut self, expected: &BTreeSet<u128>, now: Instant, metrics: &ShardMetrics) {
        if self.status.take_check_request() {
            self.due = now;
        }
        if *expected == self.expected {
            return;
        }
        expected.clone_into(&mut self.expected);
        self.due = now;
        let mut status = self.status.get();
        let PeerIdentity::Verified { cluster_uid } = status.identity else {
            return;
        };
        if self.covers(cluster_uid) {
            return;
        }
        status.identity = PeerIdentity::Unconfirmed { cluster_uid };
        self.status.set(status);
        metrics.set_geo_peer_status(&self.origin, false, status.unreachable);
        warn!(
            origin = %self.origin,
            root = self.layout.root(),
            cluster_uid = uid(cluster_uid),
            recorded = ?self.expected.iter().copied().map(uid).collect::<Vec<_>>(),
            "committed metadata changed the clusters it expects of the geo peer, and they no \
             longer cover the cluster that this node reads, so this node reads nothing from the \
             peer until a check accepts the claim on its root"
        );
    }

    /// Whether the expected clusters cover `cluster_uid`: any while there
    /// are none, one of them, or one that the recovery records were found
    /// to lead to from one of them.
    fn covers(&self, cluster_uid: u128) -> bool {
        self.expected.is_empty()
            || self.expected.contains(&cluster_uid)
            || self.proof.as_ref().is_some_and(|proof| {
                proof.found == cluster_uid
                    && matches!(proof.walk, Walk::Ended(Some(of)) if self.expected.contains(&of))
            })
    }

    /// Mark the peer retired, once. The node runs no job on its store, and
    /// checks it no more once a check accepted the claim on its root.
    fn retire(&self, metrics: &ShardMetrics) {
        let mut status = self.status.get();
        if status.retired {
            return;
        }
        status.retired = true;
        self.status.set(status);
        metrics.set_geo_peer_retired(&self.origin);
        info!(
            origin = %self.origin,
            root = self.layout.root(),
            "the origin of the geo peer retired, so this node follows nothing of it, and checks \
             its store only until a check accepts the claim on its root. Polls of its shadow \
             topics read what they committed until their retention removes it"
        );
    }

    /// Check once, and record what the check found.
    async fn step(&mut self, own_cluster_uid: u128, metrics: &ShardMetrics) {
        let check = self.check(own_cluster_uid).await;
        let _ = self.record(&check, metrics);
    }

    /// One read of the peer's identity object that waits at most the
    /// timeout, judged against the clusters that committed metadata expects
    /// of the peer.
    async fn check(&mut self, own_cluster_uid: u128) -> PeerCheck {
        let read = StoreIdentity::read(&self.store, &self.layout);
        let check = compio::time::timeout(self.timeout, read).await.map_or_else(
            |_| PeerCheck {
                read: PeerRead::Unreachable,
                error: Some(self.no_answer("the identity check")),
            },
            |answer| PeerCheck::of(answer, own_cluster_uid),
        );
        match check.read {
            PeerRead::Found(found) => self.judge(found).await,
            _ => check,
        }
    }

    /// What a claim of `found` means for the peer: one that committed
    /// metadata expects, one that recovery records prove the successor of
    /// one it expects, one it does not know, or one that the records did not
    /// prove or refute yet.
    async fn judge(&mut self, found: u128) -> PeerCheck {
        let Some(&recorded) = self.expected.first() else {
            return PeerCheck::read(PeerRead::Found(found));
        };
        if self.expected.contains(&found) {
            return PeerCheck::read(PeerRead::Found(found));
        }
        let proof = match self.proof.take() {
            Some(proof) if proof.counts(found, &self.expected) => proof,
            _ => Proof::new(found, self.expected.clone()),
        };
        let proof = self.proof.insert(proof);
        let read = match proof.advance(&self.store, &self.layout, self.timeout).await {
            Err(error) => {
                let outage = PeerRead::Unproven {
                    found,
                    unreachable: true,
                };
                return PeerCheck::failed(error, outage);
            }
            Ok(()) => match proof.walk {
                Walk::Ended(Some(of)) => PeerRead::Successor {
                    of,
                    cluster_uid: found,
                },
                Walk::Ended(None) => PeerRead::Unknown { recorded, found },
                Walk::Going { .. } => PeerRead::Unproven {
                    found,
                    unreachable: false,
                },
            },
        };
        PeerCheck::read(read)
    }

    fn no_answer(&self, what: &str) -> ObjectStoreError {
        no_answer(what, self.timeout)
    }

    /// Move the peer's status by `check`, set its gauges, log a change, and
    /// schedule the next check. Returns the wait before that check.
    fn record(&mut self, check: &PeerCheck, metrics: &ShardMetrics) -> Duration {
        let before = self.status.get();
        let after = before.after(check.read);
        self.status.set(after);
        metrics.set_geo_peer_status(
            &self.origin,
            after.identity.readable().is_some(),
            after.unreachable,
        );
        let delay = next_check(after, check.read);
        self.due = Instant::now() + delay;
        let was = self.last.replace(check.read);
        if before == after && was == Some(check.read) {
            debug!(origin = %self.origin, ?check, "geo peer identity check");
        } else {
            self.log(before, after, check);
        }
        delay
    }

    fn log(&self, before: PeerStatus, after: PeerStatus, check: &PeerCheck) {
        let origin = self.origin.as_str();
        let root = self.layout.root();
        let error = check.error.as_ref().map(ToString::to_string);
        match (after.identity, check.read) {
            (
                PeerIdentity::Unverified
                | PeerIdentity::Mismatched { .. }
                | PeerIdentity::Unconfirmed { .. },
                PeerRead::Unproven { found, .. },
            ) => warn!(
                origin,
                root,
                found = uid(found),
                error,
                "the root of the geo peer is claimed by a cluster that its shadow streams did not \
                 come from, and this check did not finish following the recovery records under \
                 the root, so this node reads nothing from the peer until a check proves or \
                 refutes the claim"
            ),
            (PeerIdentity::Mismatched { found, .. }, _) => self.log_mismatch(found),
            (PeerIdentity::Verified { cluster_uid }, PeerRead::Successor { of, .. }) => info!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                of = uid(of),
                "the root of the geo peer belongs to this cluster_uid, which the recovery \
                 records under the root name the successor of a cluster that the shadow streams \
                 came from, so this node reads the peer"
            ),
            (PeerIdentity::Changed { cluster_uid, found }, _)
                if !matches!(before.identity, PeerIdentity::Changed { .. }) =>
            {
                error!(
                    origin,
                    root,
                    cluster_uid = uid(cluster_uid),
                    found = found.map(uid),
                    "the store identity object of the geo peer no longer names the cluster that \
                     this node read first, so the peer counts as another origin, and this node \
                     reads nothing from it until the node restarts. The server README says \
                     what to do"
                );
            }
            (PeerIdentity::Verified { cluster_uid }, PeerRead::Unreachable) => warn!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                error,
                "the store of the geo peer does not answer the identity check, so \
                 geo_peer_unreachable is raised. This node keeps reading what the store returns"
            ),
            (PeerIdentity::Verified { cluster_uid }, _) => info!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                "the root of the geo peer belongs to this cluster_uid, so this node reads the peer"
            ),
            (PeerIdentity::Unconfirmed { cluster_uid }, PeerRead::Missing) => error!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                "the store identity object of the geo peer is gone, so this node reads nothing \
                 from the peer until a check finds this cluster_uid again"
            ),
            (PeerIdentity::Unconfirmed { cluster_uid }, PeerRead::Refused) => error!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                error,
                "the store of the geo peer refused the identity check, so this node reads \
                 nothing from the peer until a check finds this cluster_uid again"
            ),
            (PeerIdentity::Unverified, PeerRead::Missing) => warn!(
                origin,
                root,
                "no cluster claimed the root of the geo peer yet, or this node does not reach \
                 the store that the peer claimed, so this node reads nothing from the peer"
            ),
            (PeerIdentity::Unverified, PeerRead::OwnCluster(cluster_uid)) => error!(
                origin,
                root,
                cluster_uid = uid(cluster_uid),
                "the root of the geo peer belongs to this cluster, so its store and prefix are \
                 this cluster's own, and this node reads nothing from it"
            ),
            (PeerIdentity::Unverified, PeerRead::Invalid) => error!(
                origin,
                root,
                "the store identity object of the geo peer holds bytes that no cluster wrote, \
                 so this node reads nothing from the peer"
            ),
            (PeerIdentity::Unverified, PeerRead::Refused) => error!(
                origin,
                root,
                error,
                "the store of the geo peer refused the identity check in a way that only an \
                 operator can fix, such as rejected credentials or a missing bucket, so this \
                 node reads nothing from the peer"
            ),
            (
                PeerIdentity::Unverified | PeerIdentity::Unconfirmed { .. },
                PeerRead::Unreachable,
            ) => {
                warn!(
                    origin,
                    root,
                    error,
                    "the store of the geo peer does not answer the identity check, so this node \
                     reads nothing from the peer until it does"
                );
            }
            (identity, read) => debug!(origin, ?identity, ?read, "geo peer identity check"),
        }
    }
}

impl<S> PeerChecker<S> {
    fn log_mismatch(&self, found: u128) {
        error!(
            origin = %self.origin,
            root = self.layout.root(),
            recorded = ?self.expected.iter().copied().map(uid).collect::<Vec<_>>(),
            found = uid(found),
            "the root of the geo peer is claimed by a cluster that its shadow streams did not \
             come from, and no recovery record under the root leads to it, so this node reads \
             nothing from the peer. Point the peer entry at the store and prefix of the cluster \
             that the shadow streams follow, or retire the origin if that cluster is gone. The \
             server README says more"
        );
    }
}

impl PeerCheck {
    const fn read(read: PeerRead) -> Self {
        Self { read, error: None }
    }

    /// What an answer to the read of a peer's identity object means, for a
    /// node of the cluster `own_cluster_uid`, before it is judged against
    /// what committed metadata expects.
    fn of(answer: Result<IdentityRead, ObjectStoreError>, own_cluster_uid: u128) -> Self {
        let read = match answer {
            Ok(IdentityRead::Found(found)) if found.cluster_uid == own_cluster_uid => {
                PeerRead::OwnCluster(found.cluster_uid)
            }
            Ok(IdentityRead::Found(found)) => PeerRead::Found(found.cluster_uid),
            Ok(IdentityRead::Invalid) => PeerRead::Invalid,
            Ok(IdentityRead::Missing) => PeerRead::Missing,
            Err(error) => return Self::failed(error, PeerRead::Unreachable),
        };
        Self { read, error: None }
    }

    /// A read of the check that failed with `error`: `outage` when a retry
    /// can get past it, a refusal that only an operator ends otherwise.
    fn failed(error: ObjectStoreError, outage: PeerRead) -> Self {
        let read = match error.class() {
            ErrorClass::Transient => outage,
            ErrorClass::NotFound | ErrorClass::PreconditionFailed | ErrorClass::Permanent => {
                PeerRead::Refused
            }
        };
        Self {
            read,
            error: Some(error),
        }
    }
}

impl Proof {
    fn new(found: u128, expected: BTreeSet<u128>) -> Self {
        let chains = expected.iter().rev().map(|&start| (start, start)).collect();
        Self {
            found,
            expected,
            walk: Walk::Going {
                chains,
                followed: BTreeSet::new(),
            },
        }
    }

    /// Whether the walk still counts for a claim of `found` under the
    /// clusters `expected`: one that ended at a cluster while that cluster
    /// is expected, any other only under the clusters it started from.
    fn counts(&self, found: u128, expected: &BTreeSet<u128>) -> bool {
        self.found == found
            && match self.walk {
                Walk::Ended(Some(of)) => expected.contains(&of),
                Walk::Ended(None) | Walk::Going { .. } => self.expected == *expected,
            }
    }

    /// Read at most [`RECOVERY_READS_PER_CHECK`] more records of the walk,
    /// each within `timeout`. A read that fails leaves the walk where it was.
    async fn advance<S: ObjectStore>(
        &mut self,
        store: &S,
        layout: &KeyLayout,
        timeout: Duration,
    ) -> Result<(), ObjectStoreError> {
        let found = self.found;
        let Walk::Going { chains, followed } = &mut self.walk else {
            return Ok(());
        };
        let mut reads = 0;
        let of = loop {
            let Some(&(start, at)) = chains.last() else {
                break None;
            };
            if followed.contains(&at) {
                chains.pop();
                continue;
            }
            if reads == RECOVERY_READS_PER_CHECK {
                return Ok(());
            }
            reads += 1;
            let read = compio::time::timeout(timeout, RecoveryRecord::read(store, layout, at))
                .await
                .map_err(|_| no_answer("a read of a recovery record", timeout))??;
            followed.insert(at);
            chains.pop();
            match read {
                RecoveryRead::Found(record) if record.successor_cluster_uid == found => {
                    break Some(start);
                }
                RecoveryRead::Found(record) => chains.push((start, record.successor_cluster_uid)),
                RecoveryRead::Missing | RecoveryRead::Invalid(_) => {}
            }
        };
        self.walk = Walk::Ended(of);
        Ok(())
    }
}

fn no_answer(what: &str, timeout: Duration) -> ObjectStoreError {
    ObjectStoreError::Transport(format!("{what} got no answer within {timeout:?}"))
}

/// How long to wait before the next check of a peer with `status`, after a
/// check that found `read`.
const fn next_check(status: PeerStatus, read: PeerRead) -> Duration {
    match (status.identity, read) {
        (PeerIdentity::Changed { .. }, _) => RECHECK_INTERVAL,
        (_, PeerRead::Unproven { .. }) => RETRY_INTERVAL,
        (PeerIdentity::Verified { .. }, _) if !status.unreachable => RECHECK_INTERVAL,
        (PeerIdentity::Mismatched { .. }, _) => RECHECK_INTERVAL,
        (
            PeerIdentity::Verified { .. }
            | PeerIdentity::Unverified
            | PeerIdentity::Unconfirmed { .. },
            _,
        ) => RETRY_INTERVAL,
    }
}

/// A `cluster_uid` as the store identity object writes it.
fn uid(cluster_uid: u128) -> String {
    format!("{cluster_uid:032x}")
}

impl PeerAuth {
    fn of(&self, index: usize) -> Option<&StoreAuth> {
        self.0.get(index)
    }
}

impl StoreAuth {
    /// What a store of `backend` is built with before [`auth`] runs. A
    /// store sends nothing when it is built, so [`preflight`] checks the
    /// rest of a peer with it and starts no refresher.
    fn unset(backend: TieredStorageBackend) -> Self {
        match backend {
            TieredStorageBackend::S3 => Self::S3(SharedCredentials::refreshed()),
            TieredStorageBackend::Gcs => Self::Gcs(GcsAuth::Anonymous),
            TieredStorageBackend::None
            | TieredStorageBackend::Fs
            | TieredStorageBackend::MockS3 => Self::None,
        }
    }
}

/// The store of `peer`, which only reads. An `s3` or `gcs` peer's store
/// authorizes its requests with `auth`.
fn store(peer: &GeoPeerConfig, auth: Option<&StoreAuth>) -> Result<ReadOnlyStore, ServerError> {
    let backend = match peer.backend {
        TieredStorageBackend::Fs => Backend::Fs(FsStore::foreign(&peer.path)),
        TieredStorageBackend::S3 => {
            let Some(StoreAuth::S3(credentials)) = auth else {
                return Err(refuse(peer, "has no s3 credentials".to_owned()));
            };
            let store = S3Store::new(S3Location::from(peer).config(credentials.clone()))
                .map_err(|error| refuse(peer, error.to_string()))?;
            Backend::S3(store)
        }
        TieredStorageBackend::Gcs => {
            let Some(StoreAuth::Gcs(auth)) = auth else {
                return Err(refuse(peer, "has no gcs credentials".to_owned()));
            };
            let store = GcsStore::new(gcs_config(&peer.gcs, auth.clone()))
                .map_err(|error| refuse(peer, error.to_string()))?;
            Backend::Gcs(store)
        }
        TieredStorageBackend::None => return Err(refuse(peer, "has no backend".to_owned())),
        TieredStorageBackend::MockS3 => {
            return Err(refuse(
                peer,
                "has a mock_s3 backend, whose objects only its own server holds".to_owned(),
            ));
        }
    };
    Ok(ReadOnlyStore::new(backend))
}

fn layout(peer: &GeoPeerConfig) -> Result<KeyLayout, ServerError> {
    KeyLayout::new(&peer.prefix).map_err(|error| {
        refuse(
            peer,
            format!(
                "prefix {:?} is not a usable key prefix: {error}",
                peer.prefix
            ),
        )
    })
}

fn refuse(peer: &GeoPeerConfig, reason: String) -> ServerError {
    ServerError::GeoPeer {
        origin: peer.origin.clone(),
        reason,
    }
}

impl<'a> From<&'a GeoPeerConfig> for S3Location<'a> {
    fn from(peer: &'a GeoPeerConfig) -> Self {
        Self {
            endpoint: &peer.endpoint,
            region: &peer.region,
            bucket: &peer.bucket,
            url_style: peer.url_style,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::future::{Future, pending};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::ops::Range;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::SystemTime;

    use futures::future::{Either, join};
    use iggy_object_store::s3::Credentials;
    use iggy_object_store::{
        DeleteOutcome, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange, PutBody,
        PutOptions, RecordingStore, StoreCalls, TierExecutor, TierJobClass, TierJobRequest,
        TierScope,
    };
    use message_bus::transports::tls::install_default_crypto_provider;

    use super::*;
    use crate::dispatch::test_support::{SpyBus, test_shard};

    const OWN: u128 = 0xaa;
    const DC_B: u128 = 0x0123_4567_89ab_cdef_0011_2233_4455_6677;
    const DC_C: u128 = 0x0c;
    const OTHER: u128 = 0x07;
    /// A cluster that a recovery rebuilt in the place of `DC_B`.
    const DC_B_RECOVERED: u128 = 0x0b01;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn fs_peer(origin: &str, path: &Path) -> GeoPeerConfig {
        GeoPeerConfig {
            origin: origin.to_owned(),
            backend: TieredStorageBackend::Fs,
            path: path.to_string_lossy().into_owned(),
            ..GeoPeerConfig::default()
        }
    }

    fn s3_peer(origin: &str, endpoint: &str) -> GeoPeerConfig {
        GeoPeerConfig {
            origin: origin.to_owned(),
            backend: TieredStorageBackend::S3,
            bucket: format!("iggy-{origin}"),
            endpoint: endpoint.to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: "AKIDPEEREXAMPLE".to_owned(),
            secret_access_key: "peerSecretKeyExample".to_owned(),
            ..GeoPeerConfig::default()
        }
    }

    fn geo(peers: Vec<GeoPeerConfig>) -> GeoConfig {
        GeoConfig {
            origin: "dc-a".to_owned(),
            peers,
            ..GeoConfig::default()
        }
    }

    /// Claims the root `iggy` of the `fs` store at `path` for `cluster_uid`,
    /// as the peer cluster's replica 0 does.
    async fn claim(path: &Path, cluster_uid: u128) {
        assert!(
            StoreIdentity { cluster_uid }
                .claim(&FsStore::new(path), &layout())
                .await
                .unwrap()
        );
    }

    /// A store that never answers.
    struct SilentStore;

    impl ObjectStore for SilentStore {
        fn put(
            &self,
            _: &str,
            _: PutBody,
            _: &PutOptions,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            pending()
        }

        fn get_range(
            &self,
            _: &str,
            _: Range<u64>,
        ) -> impl Future<Output = Result<ObjectRange, ObjectStoreError>> {
            pending()
        }

        fn head(&self, _: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>> {
            pending()
        }

        fn delete(
            &self,
            _: &[String],
        ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>> {
            pending()
        }

        fn list(
            &self,
            _: &ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, ObjectStoreError>> {
            pending()
        }
    }

    /// Nothing at boot reaches a peer, so a peer that is not there yet never
    /// stops the boot. Only a configuration that can never work does.
    #[test]
    fn given_peers_when_preflighting_should_refuse_only_a_configuration_that_can_never_work() {
        // An https client needs the provider that the bootstrap installs.
        install_default_crypto_provider();
        let directory = tempfile::tempdir().unwrap();
        let not_mounted = directory.path().join("not-mounted-yet");
        let reachable_later = geo(vec![
            fs_peer("dc-b", &not_mounted),
            s3_peer("dc-c", "http://127.0.0.1:9"),
            s3_peer("dc-d", ""),
        ]);

        assert!(preflight(&reachable_later).is_ok());
        for peer in [
            GeoPeerConfig {
                prefix: "iggy/".to_owned(),
                ..fs_peer("dc-b", directory.path())
            },
            s3_peer("dc-b", "http://store/with-a-path"),
        ] {
            let error = preflight(&geo(vec![peer])).unwrap_err();
            assert!(
                matches!(error, ServerError::GeoPeer { ref origin, .. } if origin == "dc-b"),
                "{error}"
            );
        }
    }

    #[test]
    fn given_each_answer_when_judging_a_check_should_tell_what_it_read() {
        let found = |cluster_uid| Ok(IdentityRead::Found(StoreIdentity { cluster_uid }));
        let read = |answer| PeerCheck::of(answer, OWN).read;

        assert_eq!(read(found(DC_B)), PeerRead::Found(DC_B));
        assert_eq!(read(found(OWN)), PeerRead::OwnCluster(OWN));
        assert_eq!(read(Ok(IdentityRead::Invalid)), PeerRead::Invalid);
        assert_eq!(read(Ok(IdentityRead::Missing)), PeerRead::Missing);
        for outage in [
            ObjectStoreError::Transport("reset".to_owned()),
            ObjectStoreError::Throttled("slow down".to_owned()),
            ObjectStoreError::Unavailable("503".to_owned()),
        ] {
            assert_eq!(read(Err(outage)), PeerRead::Unreachable);
        }
        for refusal in [
            ObjectStoreError::Auth("denied".to_owned()),
            ObjectStoreError::BucketNotFound("iggy-dc-b".to_owned()),
            ObjectStoreError::Malformed("the file has no object header".to_owned()),
        ] {
            assert_eq!(read(Err(refusal)), PeerRead::Refused);
        }
    }

    #[compio::test]
    async fn given_a_peer_that_never_answers_when_checking_should_give_up_at_the_timeout() {
        let mut checker = PeerChecker::new(
            "dc-b".to_owned(),
            SilentStore,
            layout(),
            Duration::from_millis(10),
            SharedPeerStatus::default(),
        );

        let check = checker.check(OWN).await;

        assert_eq!(check.read, PeerRead::Unreachable);
        assert!(
            matches!(check.error, Some(ObjectStoreError::Transport(_))),
            "{check:?}"
        );
    }

    /// Each row gives the identity that the node keeps, both gauges
    /// (verified, unreachable), and the wait before the next check.
    #[test]
    fn given_checks_when_recording_should_move_the_status_and_gauges_and_pick_the_next_check() {
        let status = SharedPeerStatus::default();
        let mut checker = PeerChecker::new(
            "dc-b".to_owned(),
            MemoryStore::new(),
            layout(),
            Duration::from_secs(5),
            status.clone(),
        );
        let metrics = ShardMetrics::for_shard();
        let mut record = |read| {
            let delay = checker.record(&PeerCheck { read, error: None }, &metrics);
            (
                status.get().identity,
                metrics.geo_peer_status_value("dc-b"),
                delay,
            )
        };
        let verified = PeerIdentity::Verified { cluster_uid: DC_B };

        assert_eq!(
            record(PeerRead::Unreachable),
            (PeerIdentity::Unverified, (false, true), RETRY_INTERVAL)
        );
        assert_eq!(
            record(PeerRead::Found(DC_B)),
            (verified, (true, false), RECHECK_INTERVAL)
        );
        assert_eq!(
            record(PeerRead::Unreachable),
            (verified, (true, true), RETRY_INTERVAL),
            "a verified peer stays readable through an outage"
        );
        assert_eq!(
            record(PeerRead::Missing),
            (
                PeerIdentity::Unconfirmed { cluster_uid: DC_B },
                (false, false),
                RETRY_INTERVAL
            )
        );
        assert_eq!(
            record(PeerRead::Found(DC_B)),
            (verified, (true, false), RECHECK_INTERVAL)
        );
        let changed = PeerIdentity::Changed {
            cluster_uid: DC_B,
            found: Some(OTHER),
        };
        assert_eq!(
            record(PeerRead::Found(OTHER)),
            (changed, (false, false), RECHECK_INTERVAL)
        );
        assert_eq!(
            record(PeerRead::Found(DC_B)),
            (changed, (false, false), RECHECK_INTERVAL),
            "only a restart ends a change"
        );
    }

    /// A cluster with two peers hands each shard a store per peer, and each
    /// store gives out its service and its cluster once a check verified it.
    #[compio::test]
    async fn given_two_peers_when_checked_should_give_the_shard_each_store_and_its_cluster() {
        let (dc_b, dc_c) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        claim(dc_b.path(), DC_B).await;
        claim(dc_c.path(), DC_C).await;
        let config = geo(vec![
            fs_peer("dc-b", dc_b.path()),
            fs_peer("dc-c", dc_c.path()),
        ]);
        let tiered = TieredStorageConfig::default();
        let statuses = statuses(&config);
        let bus = SpyBus::default();
        let shard = test_shard(&bus, 0, 1, 1);
        assert!(shard.peer_stores().is_empty());

        shard.set_peer_stores(stores(
            &config,
            &tiered,
            &bus,
            &statuses,
            &PeerAuth::default(),
        ));
        let peers = shard.peer_stores();
        assert_eq!(peers.len(), 2);
        assert!(
            peers.iter().all(|peer| peer.service().is_none()),
            "no check ran yet"
        );
        let metrics = ShardMetrics::for_shard();
        for mut checker in checkers(&config, &tiered, &statuses, &PeerAuth::default()) {
            checker.step(OWN, &metrics).await;
        }

        let scope = TierScope::new();
        for (origin, cluster_uid) in [("dc-b", DC_B), ("dc-c", DC_C)] {
            let peer = peers.get(origin).unwrap();
            assert_eq!(peer.cluster_uid(), Some(cluster_uid), "{origin}");
            assert_eq!(metrics.geo_peer_status_value(origin), (true, false));
            let read = peer
                .service()
                .expect("a verified peer gives out its service")
                .submit(
                    TierJobRequest::new(TierJobClass::DemandFetch, scope.token()),
                    |store| async move { StoreIdentity::read(store.as_ref(), &layout()).await },
                )
                .await;
            assert!(
                matches!(read, Ok(Ok(IdentityRead::Found(found))) if found.cluster_uid == cluster_uid),
                "{origin}: {read:?}"
            );
        }
    }

    /// The checks send a peer reads only, also around a change.
    #[compio::test]
    async fn given_a_recording_store_when_checking_a_peer_should_send_it_reads_only() {
        let memory = MemoryStore::new();
        StoreIdentity { cluster_uid: DC_B }
            .claim(&memory, &layout())
            .await
            .unwrap();
        let provider = RecordingStore::new(memory);
        let calls = provider.calls();
        let status = SharedPeerStatus::default();
        let mut checker = PeerChecker::new(
            "dc-b".to_owned(),
            ReadOnlyStore::new(provider),
            layout(),
            Duration::from_secs(5),
            status.clone(),
        );
        let metrics = ShardMetrics::for_shard();

        for _ in 0..3 {
            checker.step(OWN, &metrics).await;
        }
        checker.step(DC_B, &metrics).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Changed {
                cluster_uid: DC_B,
                found: Some(DC_B),
            },
            "a peer that names this cluster after a check verified it counts as changed"
        );
        assert_eq!(calls.mutations(), Vec::new());
        assert_eq!(calls.all().len(), 4, "one ranged read per check");
    }

    /// A listing through the store of an `fs` peer leaves the scratch file
    /// of a put of the peer, however old.
    #[compio::test]
    async fn given_the_scratch_file_of_a_peers_put_when_listing_its_store_should_keep_it() {
        let directory = tempfile::tempdir().unwrap();
        claim(directory.path(), DC_B).await;
        let scratch = directory
            .path()
            .join("iggy/meta/.identity.0123456789abcdef0123456789abcdef");
        std::fs::File::create(&scratch)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_hours(2))
            .unwrap();
        let store = store(&fs_peer("dc-b", directory.path()), None).unwrap();

        let page = store.list(&ListRequest::new("iggy/")).await.unwrap();

        assert_eq!(page.objects.len(), 1, "the identity object: {page:?}");
        assert!(
            scratch.exists(),
            "the listing removed the peer's scratch file"
        );
    }

    /// A peer whose store refuses connections never holds up the checks, and
    /// the node reads nothing from it.
    #[compio::test]
    async fn given_an_s3_peer_that_refuses_connections_when_checking_should_find_it_unreachable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let config = geo(vec![s3_peer("dc-b", &endpoint)]);
        let statuses = statuses(&config);
        let metrics = ShardMetrics::for_shard();
        let tiered = TieredStorageConfig::default();

        for mut checker in checkers(
            &config,
            &tiered,
            &statuses,
            &auth(&config, &tiered, &mut RoleCredentials::default()).unwrap(),
        ) {
            checker.step(OWN, &metrics).await;
        }

        assert_eq!(
            statuses[0].get(),
            PeerStatus {
                identity: PeerIdentity::Unverified,
                unreachable: true,
                retired: false,
            }
        );
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, true));
    }

    #[test]
    fn given_peers_when_building_their_auth_should_give_only_gcs_peers_a_token() {
        let directory = tempfile::tempdir().unwrap();
        let mut gcs = GeoPeerConfig {
            origin: "dc-c".to_owned(),
            backend: TieredStorageBackend::Gcs,
            ..GeoPeerConfig::default()
        };
        gcs.gcs.bucket = "iggy-dc-c".to_owned();
        gcs.gcs.endpoint = "http://localhost:4443".to_owned();
        gcs.gcs.auth = configs::tiered_storage::GcsAuth::None;
        let config = geo(vec![fs_peer("dc-b", directory.path()), gcs.clone()]);

        let auth = auth(
            &config,
            &TieredStorageConfig::default(),
            &mut RoleCredentials::default(),
        )
        .unwrap();

        assert!(matches!(auth.of(0), Some(StoreAuth::None)));
        assert!(matches!(
            auth.of(1),
            Some(StoreAuth::Gcs(GcsAuth::Anonymous))
        ));
        assert!(store(&gcs, auth.of(1)).is_ok());
        assert!(
            matches!(store(&gcs, None), Err(ServerError::GeoPeer { ref origin, .. }) if origin == "dc-c"),
            "a gcs peer without its token gets no store"
        );
        assert!(preflight(&config).is_ok());
    }

    /// The root of `dc-b` in a store of its own, claimed by `claim`, with
    /// the recovery records `(retired, successor)`.
    async fn root(claim: u128, recoveries: &[(u128, u128)]) -> RecordingStore<MemoryStore> {
        RecordingStore::new(root_store(claim, recoveries).await)
    }

    /// [`root`] without the recording.
    async fn root_store(claim: u128, recoveries: &[(u128, u128)]) -> MemoryStore {
        let store = MemoryStore::new();
        for &(retired, successor) in recoveries {
            let record = RecoveryRecord {
                retired_cluster_uid: retired,
                successor_cluster_uid: successor,
                recovered_at: 1,
                adopted_unproven: false,
                partitions: Vec::new(),
            };
            assert!(record.create(&store, &layout()).await.unwrap());
        }
        StoreIdentity { cluster_uid: claim }
            .claim(&store, &layout())
            .await
            .unwrap();
        store
    }

    /// The recovery records `(retired, successor)` from `DC_B` through
    /// `steps` clusters, the claim of the last.
    fn chain(steps: usize) -> Vec<(u128, u128)> {
        let clusters: Vec<u128> = std::iter::once(DC_B)
            .chain((1..=steps).map(|step| 0x1_0000 + u128::try_from(step).unwrap()))
            .collect();
        clusters.windows(2).map(|pair| (pair[0], pair[1])).collect()
    }

    /// A store whose reads of recovery records fail as `fault` says.
    struct RecoveryFaults {
        store: Rc<MemoryStore>,
        fault: Rc<Cell<Option<RecoveryFault>>>,
    }

    #[derive(Clone, Copy)]
    enum RecoveryFault {
        /// The connection resets.
        Reset,
        /// The store rejects the credentials.
        Refused,
        /// No answer comes.
        Silent,
    }

    impl RecoveryFaults {
        fn new(store: MemoryStore, fault: Option<RecoveryFault>) -> Self {
            Self {
                store: Rc::new(store),
                fault: Rc::new(Cell::new(fault)),
            }
        }
    }

    impl ObjectStore for RecoveryFaults {
        fn put(
            &self,
            key: &str,
            body: PutBody,
            options: &PutOptions,
        ) -> impl Future<Output = Result<(), ObjectStoreError>> {
            ObjectStore::put(&*self.store, key, body, options)
        }

        fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> impl Future<Output = Result<ObjectRange, ObjectStoreError>> {
            ObjectStore::get_range(&*self.store, key, range)
        }

        fn head(&self, key: &str) -> impl Future<Output = Result<ObjectMeta, ObjectStoreError>> {
            let fault = self.fault.get().filter(|_| key.contains("/recovery/"));
            async move {
                match fault {
                    Some(RecoveryFault::Reset) => {
                        Err(ObjectStoreError::Transport("reset".to_owned()))
                    }
                    Some(RecoveryFault::Refused) => {
                        Err(ObjectStoreError::Auth("denied".to_owned()))
                    }
                    Some(RecoveryFault::Silent) => pending().await,
                    None => ObjectStore::head(&*self.store, key).await,
                }
            }
        }

        fn delete(
            &self,
            keys: &[String],
        ) -> impl Future<Output = Result<DeleteOutcome, ObjectStoreError>> {
            ObjectStore::delete(&*self.store, keys)
        }

        fn list(
            &self,
            request: &ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, ObjectStoreError>> {
            ObjectStore::list(&*self.store, request)
        }
    }

    /// A checker of `dc-b` as a node that just started runs it: it read
    /// nothing yet, and committed metadata recorded `expected` of the peer.
    fn restarted(
        store: RecordingStore<MemoryStore>,
        expected: &[u128],
        retired: bool,
    ) -> (PeerChecker<ReadOnlyStore>, SharedPeerStatus, StoreCalls) {
        let calls = store.calls();
        let status = SharedPeerStatus::default();
        status.set(PeerStatus {
            retired,
            ..PeerStatus::default()
        });
        let mut checker = PeerChecker::new(
            "dc-b".to_owned(),
            ReadOnlyStore::new(store),
            layout(),
            Duration::from_secs(5),
            status.clone(),
        );
        checker.look(
            &expected.iter().copied().collect(),
            Instant::now(),
            &ShardMetrics::for_shard(),
        );
        (checker, status, calls)
    }

    /// A checker of `dc-b` that verified [`DC_B`], whose root
    /// [`DC_B_RECOVERED`] now claims with the recovery record that proves it,
    /// and whose reads of recovery records fail with `fault` until it is
    /// cleared.
    async fn claimed_by_successor(
        fault: RecoveryFault,
    ) -> (
        PeerChecker<RecoveryFaults>,
        SharedPeerStatus,
        Rc<Cell<Option<RecoveryFault>>>,
    ) {
        let store = RecoveryFaults::new(
            root_store(DC_B_RECOVERED, &[(DC_B, DC_B_RECOVERED)]).await,
            Some(fault),
        );
        let fault = Rc::clone(&store.fault);
        let status = SharedPeerStatus::default();
        status.set(PeerStatus::default().after(PeerRead::Found(DC_B)));
        let mut checker = PeerChecker::new(
            "dc-b".to_owned(),
            store,
            layout(),
            Duration::from_secs(5),
            status.clone(),
        );
        checker.look(
            &BTreeSet::from([DC_B]),
            Instant::now(),
            &ShardMetrics::for_shard(),
        );
        assert!(status.get().serves_polls());
        (checker, status, fault)
    }

    /// A node that restarted after another cluster took the peer's root
    /// under the same prefix finds a claim that its shadow streams did not
    /// come from, and reads nothing from the peer: no poll, no job, no
    /// cached block.
    #[compio::test]
    async fn given_shadows_of_one_cluster_when_a_restarted_node_finds_the_root_claimed_by_another_should_read_nothing_from_it()
     {
        let (mut checker, status, _) = restarted(root(OTHER, &[]).await, &[DC_B], false);
        let metrics = ShardMetrics::for_shard();

        checker.step(OWN, &metrics).await;

        let after = status.get();
        assert_eq!(
            after.identity,
            PeerIdentity::Mismatched {
                recorded: DC_B,
                found: OTHER,
            }
        );
        assert!(!after.serves_polls() && !after.serves_jobs());
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, false));
    }

    /// A recovery of the cluster that the shadow streams came from replaces
    /// the claim and keeps every key, and its records lead from that cluster
    /// to the claim, through every recovery since. A check follows them once
    /// for the claim.
    #[compio::test]
    async fn given_recovery_records_that_lead_to_the_claim_when_checking_should_read_the_peer_as_the_successor()
     {
        let recoveries = [(DC_B, OTHER), (OTHER, DC_B_RECOVERED)];
        let (mut checker, status, calls) =
            restarted(root(DC_B_RECOVERED, &recoveries).await, &[DC_B], false);
        let metrics = ShardMetrics::for_shard();

        checker.step(OWN, &metrics).await;
        let first = calls.all().len();
        checker.step(OWN, &metrics).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified {
                cluster_uid: DC_B_RECOVERED
            }
        );
        assert!(status.get().serves_polls());
        assert_eq!(
            calls.all().len(),
            first + 1,
            "the second check reads the identity object alone"
        );
    }

    /// A recovery record of a recorded cluster that names another successor
    /// than the claim proves nothing of it.
    #[compio::test]
    async fn given_a_recovery_record_that_leads_elsewhere_when_checking_should_leave_the_peer_mismatched()
     {
        let (mut checker, status, _) =
            restarted(root(OTHER, &[(DC_B, DC_C)]).await, &[DC_B], false);

        checker.step(OWN, &ShardMetrics::for_shard()).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Mismatched {
                recorded: DC_B,
                found: OTHER,
            }
        );
    }

    /// A node that holds no shadow stream of the peer yet takes the first
    /// claim it reads.
    #[compio::test]
    async fn given_no_shadow_streams_when_checking_should_trust_the_first_claim() {
        let (mut checker, status, _) = restarted(root(OTHER, &[]).await, &[], false);

        checker.step(OWN, &ShardMetrics::for_shard()).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified { cluster_uid: OTHER }
        );
    }

    /// A mismatch ends once committed metadata expects the claim, such as
    /// when the shadow streams of the old cluster are gone, without a
    /// restart.
    #[compio::test]
    async fn given_a_mismatched_peer_when_metadata_stops_expecting_the_old_cluster_should_check_at_once_and_read_the_peer()
     {
        let (mut checker, status, _) = restarted(root(OTHER, &[]).await, &[DC_B], false);
        let metrics = ShardMetrics::for_shard();
        checker.step(OWN, &metrics).await;
        assert!(!checker.wants_check(Instant::now()), "the next check waits");

        checker.look(&BTreeSet::new(), Instant::now(), &metrics);
        assert!(checker.wants_check(Instant::now()));
        checker.step(OWN, &metrics).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified { cluster_uid: OTHER }
        );
    }

    /// A pass on any shard that saw the root move asks for a check, which
    /// shard 0 runs at its next look.
    #[test]
    fn given_a_check_that_a_shard_asked_for_when_looking_should_make_it_due() {
        let (mut checker, status, _) =
            restarted(RecordingStore::new(MemoryStore::new()), &[], false);
        let later = Instant::now() + RECHECK_INTERVAL;
        checker.due = later;
        let metrics = ShardMetrics::for_shard();

        checker.look(&BTreeSet::new(), Instant::now(), &metrics);
        assert_eq!(checker.due, later);
        status.request_check();
        checker.look(&BTreeSet::new(), Instant::now(), &metrics);

        assert!(checker.wants_check(Instant::now()));
        assert!(!status.take_check_request(), "the look took the request");
    }

    /// The root of a retired origin that another cluster took is not read
    /// after a restart, and a poll of its shadow topics gets nothing.
    #[compio::test]
    async fn given_a_retired_origin_whose_root_another_cluster_took_when_a_restarted_node_checks_should_serve_no_poll()
     {
        let (mut checker, status, _) = restarted(root(OTHER, &[]).await, &[DC_B], true);

        assert!(!status.get().serves_polls(), "no poll before the check");
        checker.step(OWN, &ShardMetrics::for_shard()).await;

        assert!(!status.get().serves_polls());
        assert!(
            checker.checks(),
            "the checks go on until one accepts a claim"
        );
    }

    /// A retired origin whose root its own cluster still claims serves the
    /// polls of its shadow topics after one read of the identity object, and
    /// gets no other read from the checks.
    #[compio::test]
    async fn given_a_retired_origin_with_its_own_claim_when_a_restarted_node_checks_should_serve_polls_after_one_read_and_then_check_no_more()
     {
        let (mut checker, status, calls) = restarted(root(DC_B, &[]).await, &[DC_B], true);
        assert!(!status.get().serves_polls());
        assert!(checker.wants_check(Instant::now()));

        let metrics = ShardMetrics::for_shard();
        checker.step(OWN, &metrics).await;
        checker.look(&BTreeSet::from([DC_B, DC_C]), Instant::now(), &metrics);

        assert!(status.get().serves_polls());
        assert!(!status.get().serves_jobs());
        assert!(
            !checker.wants_check(Instant::now() + RECHECK_INTERVAL * 10),
            "no check once the claim was accepted"
        );
        assert_eq!(calls.all().len(), 1, "one read of the identity object");
    }

    /// A retired origin that a node verified while committed metadata named
    /// no cluster of the peer, by trusting the first claim, serves no poll
    /// once metadata names clusters that do not cover that claim, and is
    /// checked again at once.
    #[compio::test]
    async fn given_a_retired_peer_verified_before_metadata_named_its_cluster_when_metadata_names_another_should_serve_no_poll_and_check_again()
     {
        let (mut checker, status, _) = restarted(root(OTHER, &[]).await, &[], true);
        let metrics = ShardMetrics::for_shard();
        checker.step(OWN, &metrics).await;
        assert!(status.get().serves_polls(), "the first claim counts");
        assert!(!checker.checks());

        checker.look(&BTreeSet::from([DC_B]), Instant::now(), &metrics);

        assert!(
            !status.get().serves_polls(),
            "no poll before the next check"
        );
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, false));
        assert!(checker.wants_check(Instant::now()));
        checker.step(OWN, &metrics).await;
        assert_eq!(
            status.get().identity,
            PeerIdentity::Mismatched {
                recorded: DC_B,
                found: OTHER,
            }
        );
        assert!(!status.get().serves_polls());
    }

    /// A change of the expected clusters that still covers the cluster the
    /// node reads, through the recovery records too, keeps the peer readable
    /// without another read of a record. One that does not cover it stops
    /// the reads.
    #[compio::test]
    async fn given_a_peer_read_as_a_successor_when_metadata_changes_its_clusters_should_keep_reading_only_a_covered_cluster()
     {
        let (mut checker, status, calls) = restarted(
            root(DC_B_RECOVERED, &[(DC_B, DC_B_RECOVERED)]).await,
            &[DC_B],
            false,
        );
        let metrics = ShardMetrics::for_shard();
        checker.step(OWN, &metrics).await;
        let first = calls.all().len();

        checker.look(&BTreeSet::from([DC_B, DC_C]), Instant::now(), &metrics);
        assert!(status.get().serves_polls());
        checker.step(OWN, &metrics).await;
        assert!(status.get().serves_polls());
        assert_eq!(calls.all().len(), first + 1, "the identity object alone");

        checker.look(&BTreeSet::from([DC_C]), Instant::now(), &metrics);
        assert_eq!(
            status.get().identity,
            PeerIdentity::Unconfirmed {
                cluster_uid: DC_B_RECOVERED
            }
        );
        assert!(!status.get().serves_polls());
    }

    /// A verified peer whose root another cluster claims serves nothing when
    /// the store does not answer the read of the recovery record that would
    /// prove the new owner, and serves again once a check proves it.
    #[compio::test]
    async fn given_a_verified_peer_when_another_claim_appears_and_its_recovery_record_gets_no_answer_should_read_nothing_until_a_check_proves_it()
     {
        let (mut checker, status, fault) = claimed_by_successor(RecoveryFault::Reset).await;
        let metrics = ShardMetrics::for_shard();

        checker.step(OWN, &metrics).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Unconfirmed { cluster_uid: DC_B }
        );
        assert!(!status.get().serves_polls() && !status.get().serves_jobs());
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, true));
        fault.set(None);
        checker.step(OWN, &metrics).await;
        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified {
                cluster_uid: DC_B_RECOVERED
            }
        );
        assert!(status.get().serves_polls());
    }

    /// A refusal of the read of a recovery record is no outage: the peer
    /// serves nothing, `geo_peer_unreachable` stays 0, and the check reports
    /// the refusal with its error, as it does for the identity object.
    #[compio::test]
    async fn given_a_verified_peer_when_the_store_refuses_the_read_of_a_recovery_record_should_report_a_refusal_and_not_raise_unreachable()
     {
        let (mut checker, status, fault) = claimed_by_successor(RecoveryFault::Refused).await;
        let metrics = ShardMetrics::for_shard();

        let check = checker.check(OWN).await;
        assert_eq!(check.read, PeerRead::Refused);
        assert!(
            matches!(check.error, Some(ObjectStoreError::Auth(_))),
            "{check:?}"
        );
        checker.record(&check, &metrics);

        assert_eq!(
            status.get(),
            PeerStatus {
                identity: PeerIdentity::Unconfirmed { cluster_uid: DC_B },
                unreachable: false,
                retired: false,
            }
        );
        assert!(!status.get().serves_polls() && !status.get().serves_jobs());
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, false));
        fault.set(None);
        checker.step(OWN, &metrics).await;
        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified {
                cluster_uid: DC_B_RECOVERED
            }
        );
        assert!(status.get().serves_polls());
    }

    /// Recovery records longer than one check reads are followed across
    /// checks, each going on where the last one stopped, so every record is
    /// read once. The node reads nothing from the peer until they prove the
    /// claim.
    #[compio::test]
    async fn given_more_recovery_records_than_one_check_reads_when_checking_should_go_on_at_the_next_check_and_read_the_peer_as_the_successor()
     {
        let recoveries = chain(2 * RECOVERY_READS_PER_CHECK + 1);
        let claim = recoveries.last().unwrap().1;
        let (mut checker, status, calls) =
            restarted(root(claim, &recoveries).await, &[DC_B], false);
        let metrics = ShardMetrics::for_shard();

        for _ in 0..2 {
            checker.step(OWN, &metrics).await;
            assert_eq!(status.get().identity, PeerIdentity::Unverified);
            assert!(!status.get().serves_polls());
            assert!(checker.wants_check(Instant::now() + RETRY_INTERVAL));
        }
        checker.step(OWN, &metrics).await;

        assert_eq!(
            status.get().identity,
            PeerIdentity::Verified { cluster_uid: claim }
        );
        assert_eq!(
            calls.all().len(),
            3 + 2 * recoveries.len(),
            "three reads of the identity object, and a head and a get of each record"
        );
    }

    /// Recovery records that come back to a cluster they left end the walk,
    /// also when the cycle is longer than one check reads, and prove
    /// nothing.
    #[compio::test]
    async fn given_recovery_records_that_come_back_to_a_cluster_when_checking_should_end_the_walk_and_leave_the_peer_mismatched()
     {
        let mut recoveries = chain(RECOVERY_READS_PER_CHECK + 1);
        recoveries.push((recoveries.last().unwrap().1, DC_B));
        let (mut checker, status, calls) =
            restarted(root(OTHER, &recoveries).await, &[DC_B], false);
        let metrics = ShardMetrics::for_shard();

        checker.step(OWN, &metrics).await;
        assert_eq!(status.get().identity, PeerIdentity::Unverified);
        checker.step(OWN, &metrics).await;
        assert_eq!(
            status.get().identity,
            PeerIdentity::Mismatched {
                recorded: DC_B,
                found: OTHER,
            }
        );
        let ended = calls.all().len();
        checker.step(OWN, &metrics).await;

        assert_eq!(calls.all().len(), ended + 1, "the identity object alone");
    }

    #[test]
    fn given_a_retired_origin_when_retiring_its_checker_should_mark_the_status_and_gauge() {
        let directory = tempfile::tempdir().unwrap();
        let config = geo(vec![fs_peer("dc-b", directory.path())]);
        let statuses = statuses(&config);
        let metrics = ShardMetrics::for_shard();
        let checkers = checkers(
            &config,
            &TieredStorageConfig::default(),
            &statuses,
            &PeerAuth::default(),
        );

        checkers[0].retire(&metrics);
        checkers[0].retire(&metrics);

        assert!(checkers[0].retired());
        assert!(statuses[0].get().retired);
        assert!(metrics.geo_peer_retired_value("dc-b"));
        assert!(!statuses[0].get().serves_jobs());
    }

    /// Wait until `done` holds.
    async fn until(done: impl Fn() -> bool) {
        while !done() {
            compio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A check of `dc-b` that waits for the read of a recovery record holds
    /// up no check of `dc-c`: the check that a shard asks for of `dc-c` runs
    /// within a look, and finds the claim that replaced the one it read.
    #[compio::test]
    async fn given_a_peer_whose_recovery_record_never_answers_when_a_shard_asks_to_check_another_peer_should_check_that_one_at_once()
     {
        let waiting_status = SharedPeerStatus::default();
        let waiting = PeerChecker::new(
            "dc-b".to_owned(),
            RecoveryFaults::new(root_store(OTHER, &[]).await, Some(RecoveryFault::Silent)),
            layout(),
            RECHECK_INTERVAL * 10,
            waiting_status.clone(),
        );
        let dc_c = RecoveryFaults::new(root_store(DC_C, &[]).await, None);
        let dc_c_root = Rc::clone(&dc_c.store);
        let dc_c_status = SharedPeerStatus::default();
        let asked = PeerChecker::new(
            "dc-c".to_owned(),
            dc_c,
            layout(),
            Duration::from_secs(5),
            dc_c_status.clone(),
        );
        let metadata = MetadataView::new(
            || Looks {
                retired: BTreeSet::new(),
                recorded: BTreeMap::from([("dc-b".to_owned(), BTreeSet::from([DC_B]))]),
            },
            || Some(OWN),
        );
        let metrics = ShardMetrics::for_shard();
        let checks = join(
            waiting.run(&metadata, &metrics),
            asked.run(&metadata, &metrics),
        );
        let request = async {
            until(|| dc_c_status.get().serves_polls()).await;
            StoreIdentity { cluster_uid: OTHER }
                .replace(&*dc_c_root, &layout())
                .await
                .unwrap();
            dc_c_status.request_check();
            until(|| !dc_c_status.get().serves_polls()).await;
        };

        let request = pin!(request);
        let checks = pin!(checks);
        let served = compio::time::timeout(LOOK_WAIT * 5, select(request, checks)).await;

        assert!(
            matches!(served, Ok(Either::Left(_))),
            "the check of dc-c waited for the one of dc-b"
        );
        assert_eq!(
            dc_c_status.get().identity,
            PeerIdentity::Changed {
                cluster_uid: DC_C,
                found: Some(OTHER),
            }
        );
        assert_eq!(
            waiting_status.get(),
            PeerStatus::default(),
            "the check of dc-b still waits for its recovery record"
        );
    }

    /// One read of committed metadata serves the looks of every peer for a
    /// [`LOOK_WAIT`].
    #[test]
    fn given_looks_of_several_peers_within_a_look_wait_when_reading_metadata_should_read_it_once() {
        let reads = Cell::new(0);
        let metadata = MetadataView::new(
            || {
                reads.set(reads.get() + 1);
                Looks::default()
            },
            || None,
        );
        let now = Instant::now();

        for at in [now, now, now + LOOK_WAIT / 2] {
            let _ = metadata.looks(at);
        }
        assert_eq!(reads.get(), 1);
        let _ = metadata.looks(now + LOOK_WAIT);

        assert_eq!(reads.get(), 2);
    }
    /// An S3 store on a thread of its own that answers one request with the
    /// identity object of one cluster, keeps the headers of that request,
    /// and then stops listening.
    struct FakeS3 {
        endpoint: String,
        requests: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
        worker: JoinHandle<()>,
    }

    impl FakeS3 {
        fn start(cluster_uid: u128) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::<Mutex<Vec<_>>>::default();
            let kept = Arc::clone(&requests);
            let body = StoreIdentity { cluster_uid }.to_string();
            let worker = std::thread::spawn(move || {
                let Ok((mut connection, _)) = listener.accept() else {
                    return;
                };
                kept.lock().unwrap().push(read_headers(&mut connection));
                let _ = write!(
                    connection,
                    "HTTP/1.1 200 OK\r\netag: \"identity\"\r\ncontent-length: {}\r\n\
                     connection: close\r\n\r\n{body}",
                    body.len()
                );
            });
            Self {
                endpoint,
                requests,
                worker,
            }
        }

        /// The requests so far.
        fn requests(&self) -> Vec<BTreeMap<String, String>> {
            self.requests.lock().unwrap().clone()
        }

        /// The one request the store answered, once its thread has ended
        /// and its socket is closed.
        fn finish(self) -> BTreeMap<String, String> {
            self.worker.join().expect("the fake store panicked");
            let mut requests = self.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "one read per check: {requests:?}");
            requests.remove(0)
        }
    }

    /// The headers of the request on `connection`, by lowercase name. The
    /// reads of a check carry no body.
    fn read_headers(connection: &mut TcpStream) -> BTreeMap<String, String> {
        let mut head = Vec::new();
        let mut buffer = [0_u8; 4096];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            match connection.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => head.extend_from_slice(&buffer[..read]),
            }
        }
        String::from_utf8_lossy(&head)
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect()
    }

    #[compio::test]
    async fn given_an_s3_peer_with_static_keys_when_checking_should_sign_with_them_and_send_no_session_token()
     {
        let store = FakeS3::start(DC_B);
        let config = geo(vec![s3_peer("dc-b", &store.endpoint)]);
        let statuses = statuses(&config);
        let tiered = TieredStorageConfig::default();
        let auth = auth(&config, &tiered, &mut RoleCredentials::default()).unwrap();
        let metrics = ShardMetrics::for_shard();

        for mut checker in checkers(&config, &tiered, &statuses, &auth) {
            checker.step(OWN, &metrics).await;
        }

        assert!(matches!(
            auth.of(0),
            Some(StoreAuth::S3(credentials)) if !credentials.refreshes()
        ));
        assert_eq!(
            statuses[0].get().identity,
            PeerIdentity::Verified { cluster_uid: DC_B }
        );
        let request = store.finish();
        let authorization = &request["authorization"];
        assert!(
            authorization.contains("Credential=AKIDPEEREXAMPLE/"),
            "{authorization}"
        );
        assert!(!request.contains_key("x-amz-security-token"), "{request:?}");
    }

    /// An `s3` peer without keys reads with what the node's refresher
    /// fetches. Before the first fetch a check finds the peer unreachable and
    /// sends nothing. The check after the fetch signs with the fetched keys
    /// and verifies the peer, without a restart.
    #[compio::test]
    async fn given_an_s3_peer_on_the_node_role_when_checking_should_be_unreachable_until_the_first_fetch_then_sign_with_it()
     {
        let store = FakeS3::start(DC_B);
        let peer = GeoPeerConfig {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            ..s3_peer("dc-b", &store.endpoint)
        };
        let config = geo(vec![peer]);
        let statuses = statuses(&config);
        let credentials = SharedCredentials::refreshed();
        let auth = PeerAuth(vec![StoreAuth::S3(credentials.clone())]);
        let metrics = ShardMetrics::for_shard();
        let mut checkers = checkers(&config, &TieredStorageConfig::default(), &statuses, &auth);
        let checker = &mut checkers[0];

        let before = checker.check(OWN).await;
        checker.record(&before, &metrics);

        assert_eq!(before.read, PeerRead::Unreachable);
        assert!(
            matches!(&before.error, Some(ObjectStoreError::Unavailable(reason)) if reason.contains("no credentials")),
            "{before:?}"
        );
        assert_eq!(
            statuses[0].get(),
            PeerStatus {
                identity: PeerIdentity::Unverified,
                unreachable: true,
                retired: false,
            }
        );
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (false, true));
        assert!(store.requests().is_empty(), "a read left without keys");

        credentials.replace(Credentials {
            access_key_id: "ASIAFETCHED".to_owned(),
            secret_access_key: "fetchedSecret".to_owned(),
            session_token: Some("fetched-session".to_owned()),
        });
        checker.step(OWN, &metrics).await;

        assert_eq!(
            statuses[0].get().identity,
            PeerIdentity::Verified { cluster_uid: DC_B }
        );
        assert_eq!(metrics.geo_peer_status_value("dc-b"), (true, false));
        let request = store.finish();
        let authorization = &request["authorization"];
        assert!(
            authorization.contains("Credential=ASIAFETCHED/"),
            "{authorization}"
        );
        assert!(
            authorization.contains("x-amz-security-token"),
            "the signature covers the session token: {authorization}"
        );
        assert_eq!(
            request.get("x-amz-security-token").map(String::as_str),
            Some("fetched-session")
        );
    }
}
