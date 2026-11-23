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
//! shards start. Shard 0 reads each peer's store identity object at once,
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
//! found for that claim. Any other claim leaves the peer mismatched, and the
//! node reads nothing from it, see [`PeerIdentity`]. The node remembers what
//! it read in memory only, so a restarted node judges the claim again.
//!
//! A peer whose origin committed metadata retired is marked so, and checked
//! only until a check in this process accepts the claim on its root. Its
//! polls wait for that one read, and no read of its identity object goes out
//! after it.

use std::collections::BTreeSet;
use std::pin::pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use configs::geo::{GeoConfig, GeoPeerConfig};
use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use consensus::{MetadataHandle, PartitionsHandle};
use futures::future::{Either, join_all, select};
use iggy_object_store::gcs::{GcsAuth, GcsStore};
use iggy_object_store::s3::{Credentials, S3Store};
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
/// shard asked for. A look sends no request.
const LOOK_WAIT: Duration = Duration::from_secs(1);
/// The most recovery records a check follows from a recorded cluster to the
/// claim it found. Each recovery of a cluster that lost every disk writes
/// one.
const RECOVERY_HOPS_MAX: usize = 8;

/// How the stores of each peer authorize their requests, in the order of
/// the configuration: the access token of a `gcs` peer, `None` for a peer
/// on another backend. Built once per node, so every shard's stores of a
/// peer send the token of one refresher.
#[derive(Clone, Default)]
pub struct PeerAuth(Vec<Option<GcsAuth>>);

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
        store(peer, Some(&GcsAuth::Anonymous))?;
    }
    Ok(())
}

/// The access tokens of the `gcs` peers, each from a refresher of its own
/// that starts here, after [`preflight`]. The boot never waits for a peer's
/// token.
///
/// # Errors
/// [`ServerError::GeoPeer`] for a key file that cannot sign.
pub fn auth(config: &GeoConfig) -> Result<PeerAuth, ServerError> {
    config
        .peers
        .iter()
        .map(|peer| match peer.backend {
            TieredStorageBackend::Gcs => {
                crate::gcs_credentials::peer_auth(&peer.origin, &peer.gcs).map(Some)
            }
            TieredStorageBackend::None
            | TieredStorageBackend::Fs
            | TieredStorageBackend::S3
            | TieredStorageBackend::MockS3 => Ok(None),
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

/// Check each peer whenever its check is due, until `stop` fires. A peer
/// whose origin retired is marked so, and checked no more once a check
/// accepted the claim on its root.
async fn run<S: ObjectStore>(
    shard: Rc<ServerShard>,
    mut checkers: Vec<PeerChecker<S>>,
    stop: Receiver<()>,
) {
    loop {
        let streams = shard.plane.metadata().mux_stm.streams();
        let retired = streams.retired_origins();
        for checker in &mut checkers {
            if retired.contains_key(&checker.origin) {
                checker.retire(shard.metrics());
            }
            let expected = streams.read(|inner| inner.recorded_cluster_uids(&checker.origin));
            checker.look(expected, Instant::now());
        }
        let wait = match own_cluster_uid(&shard) {
            None => CLUSTER_UID_WAIT,
            Some(own_cluster_uid) => {
                let now = Instant::now();
                let metrics = shard.metrics();
                let due = checkers
                    .iter_mut()
                    .filter(|checker| checker.wants_check(now))
                    .map(|checker| checker.step(own_cluster_uid, metrics));
                match select(pin!(join_all(due)), pin!(stop.recv())).await {
                    Either::Left(_) => {}
                    Either::Right(_) => return,
                }
                let now = Instant::now();
                checkers
                    .iter()
                    .filter(|checker| checker.checks())
                    .map(|checker| checker.due.saturating_duration_since(now))
                    .min()
                    .unwrap_or(RECHECK_INTERVAL)
            }
        };
        // `Ok`: the stop fired, or its sender is gone. `Err`: the wait ran
        // out.
        if compio::time::timeout(wait.min(LOOK_WAIT), stop.recv())
            .await
            .is_ok()
        {
            return;
        }
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
    /// What the recovery records under the root proved of the last claim
    /// that the expected clusters lacked.
    proof: Option<Proof>,
}

/// What the recovery records under a peer's root prove of the claim of
/// `found`, from the clusters `expected`: `of`, the one they lead from, or
/// `None` when they lead to `found` from none.
struct Proof {
    found: u128,
    expected: BTreeSet<u128>,
    of: Option<u128>,
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

    /// Take the clusters that committed metadata now expects of the peer,
    /// and any check that a shard asked for. Either makes the next check
    /// due at `now`.
    fn look(&mut self, expected: BTreeSet<u128>, now: Instant) {
        if self.status.take_check_request() {
            self.due = now;
        }
        if expected != self.expected {
            self.expected = expected;
            self.due = now;
        }
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
    /// one it expects, or one it does not know.
    async fn judge(&mut self, found: u128) -> PeerCheck {
        let Some(&recorded) = self.expected.first() else {
            return PeerCheck::read(PeerRead::Found(found));
        };
        if self.expected.contains(&found) {
            return PeerCheck::read(PeerRead::Found(found));
        }
        let known = self
            .proof
            .as_ref()
            .filter(|proof| proof.found == found && proof.expected == self.expected)
            .map(|proof| proof.of);
        let of = match known {
            Some(of) => of,
            None => match self.successor_of(found).await {
                Ok(of) => {
                    self.proof = Some(Proof {
                        found,
                        expected: self.expected.clone(),
                        of,
                    });
                    of
                }
                Err(error) => {
                    return PeerCheck {
                        read: PeerRead::Unreachable,
                        error: Some(error),
                    };
                }
            },
        };
        PeerCheck::read(of.map_or(PeerRead::Unknown { recorded, found }, |of| {
            PeerRead::Successor {
                of,
                cluster_uid: found,
            }
        }))
    }

    /// The expected cluster from which the recovery records under the root
    /// lead to `found`, in at most [`RECOVERY_HOPS_MAX`] records.
    async fn successor_of(&self, found: u128) -> Result<Option<u128>, ObjectStoreError> {
        for &start in &self.expected {
            let mut retired = start;
            let mut followed = BTreeSet::new();
            while followed.len() < RECOVERY_HOPS_MAX && followed.insert(retired) {
                let read = RecoveryRecord::read(&self.store, &self.layout, retired);
                let RecoveryRead::Found(record) = compio::time::timeout(self.timeout, read)
                    .await
                    .map_err(|_| self.no_answer("a read of a recovery record"))??
                else {
                    break;
                };
                if record.successor_cluster_uid == found {
                    return Ok(Some(start));
                }
                retired = record.successor_cluster_uid;
            }
        }
        Ok(None)
    }

    fn no_answer(&self, what: &str) -> ObjectStoreError {
        ObjectStoreError::Transport(format!("{what} got no answer within {:?}", self.timeout))
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
        let delay = next_check(after);
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
            Err(error) => {
                let read = match error.class() {
                    ErrorClass::Transient => PeerRead::Unreachable,
                    ErrorClass::NotFound
                    | ErrorClass::PreconditionFailed
                    | ErrorClass::Permanent => PeerRead::Refused,
                };
                return Self {
                    read,
                    error: Some(error),
                };
            }
        };
        Self { read, error: None }
    }
}

/// How long to wait before the next check of a peer with `status`.
const fn next_check(status: PeerStatus) -> Duration {
    match status.identity {
        PeerIdentity::Verified { .. } if !status.unreachable => RECHECK_INTERVAL,
        PeerIdentity::Changed { .. } | PeerIdentity::Mismatched { .. } => RECHECK_INTERVAL,
        PeerIdentity::Verified { .. }
        | PeerIdentity::Unverified
        | PeerIdentity::Unconfirmed { .. } => RETRY_INTERVAL,
    }
}

/// A `cluster_uid` as the store identity object writes it.
fn uid(cluster_uid: u128) -> String {
    format!("{cluster_uid:032x}")
}

impl PeerAuth {
    fn of(&self, index: usize) -> Option<&GcsAuth> {
        self.0.get(index).and_then(Option::as_ref)
    }
}

/// The store of `peer`, which only reads. A `gcs` peer's store sends the
/// token of `gcs`.
fn store(peer: &GeoPeerConfig, gcs: Option<&GcsAuth>) -> Result<ReadOnlyStore, ServerError> {
    let backend = match peer.backend {
        TieredStorageBackend::Fs => Backend::Fs(FsStore::foreign(&peer.path)),
        TieredStorageBackend::S3 => {
            let credentials = Credentials {
                access_key_id: peer.access_key_id.clone(),
                secret_access_key: peer.secret_access_key.clone(),
                session_token: None,
            };
            let store = S3Store::new(S3Location::from(peer).config(credentials.into()))
                .map_err(|error| refuse(peer, error.to_string()))?;
            Backend::S3(store)
        }
        TieredStorageBackend::Gcs => {
            let auth = gcs
                .cloned()
                .ok_or_else(|| refuse(peer, "has no gcs credentials".to_owned()))?;
            let store = GcsStore::new(gcs_config(&peer.gcs, auth))
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
    use std::future::{Future, pending};
    use std::ops::Range;
    use std::path::Path;
    use std::time::SystemTime;

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

        for mut checker in checkers(
            &config,
            &TieredStorageConfig::default(),
            &statuses,
            &PeerAuth::default(),
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

        let auth = auth(&config).unwrap();

        assert!(auth.of(0).is_none());
        assert!(matches!(auth.of(1), Some(GcsAuth::Anonymous)));
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
        RecordingStore::new(store)
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
        checker.look(expected.iter().copied().collect(), Instant::now());
        (checker, status, calls)
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

        checker.look(BTreeSet::new(), Instant::now());
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

        checker.look(BTreeSet::new(), Instant::now());
        assert_eq!(checker.due, later);
        status.request_check();
        checker.look(BTreeSet::new(), Instant::now());

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

        checker.step(OWN, &ShardMetrics::for_shard()).await;
        checker.look(BTreeSet::from([DC_B, DC_C]), Instant::now());

        assert!(status.get().serves_polls());
        assert!(!status.get().serves_jobs());
        assert!(
            !checker.wants_check(Instant::now() + RECHECK_INTERVAL * 10),
            "no check once the claim was accepted"
        );
        assert_eq!(calls.all().len(), 1, "one read of the identity object");
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
}
