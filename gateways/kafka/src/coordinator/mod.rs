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

//! Consumer groups shared by several gateways, through a coordinator log in Iggy.
//!
//! Each group belongs to one of [`PARTITIONS`] log partitions, by a hash of its id. An Iggy
//! consumer group spreads the partitions over the gateways. The gateway that Iggy gives a
//! partition claims it in the log, and while its claim is the newest it coordinates the groups
//! of that partition: it answers their requests and appends each completed generation, each
//! offset commit and each deletion to the partition. When it stops, the next gateway reads the
//! partition and takes the groups over with their members and offsets.
//!
//! Every gateway announces the partitions it owns on a second topic and reads the others'
//! announcements, so any gateway can name the coordinator of a group and the controller.
//!
//! Each logged offset names its topic's incarnation, the op of Iggy's metadata log that created the
//! topic. The gateway learns of new and deleted topics from Iggy's metadata change log, so it
//! needs the bridge to read that log. An answer skips each offset of an older incarnation than the
//! change log shows. So a topic created again under a deleted one's name never starts at the
//! deleted topic's offsets, also when no gateway heard of the deletion.

mod partition;
pub mod record;
pub mod replay;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kafka_protocol::protocol::StrBytes;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{Notify, OwnedMutexGuard, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{
    Instant, MissedTickBehavior, interval, interval_at, sleep, sleep_until, timeout_at,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::bridge::{
    BridgeError, CoordinatorLink, DeletedTopic, IggyBridge, OffsetCalls, OffsetGroupListing,
    TopicShapes,
};
use crate::coordinator::partition::{LoadError, PartitionLog, Stop, Stopped, TAIL_WAIT};
use crate::coordinator::record::{Announcement, Body, CommittedOffset};
use crate::coordinator::replay::{GroupOffsets, PartitionState, TopicPartition};
use crate::group::{GroupCoordinator, GroupHome, GroupRemoval, GroupSnapshot};
use crate::observability::CoordinatorCensus;
use crate::protocol::api::{ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_NOT_COORDINATOR};

/// Partitions of the coordinator log.
///
/// Never changes: a new count would move nearly every group to another partition. Kafka uses 50
/// and Redpanda 16. 16 spreads evenly over 2, 4 and 8 gateways, and close to evenly over 3 to 7.
pub const PARTITIONS: u32 = 16;

/// How often a gateway reads its share of the partitions from Iggy.
const LEASE_POLL: Duration = Duration::from_secs(1);

/// How long that read may fail before the gateway stops serving its partitions. Iggy may have
/// given them to another gateway meanwhile.
const LEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a partition rests after it stopped or a load failed, before a load starts again.
const RELOAD_BACKOFF: Duration = Duration::from_secs(2);

/// How long a partition that a newer gateway wrote rests before a load tries it again.
const UNREADABLE_BACKOFF: Duration = Duration::from_mins(1);

/// How often groups whose members are all gone are dropped, and their state removed from the
/// log.
const SWEEP_EVERY: Duration = Duration::from_secs(5);

/// How often an owner announces its partitions again. Well inside the announcements' retention.
const ANNOUNCE_EVERY: Duration = Duration::from_mins(10);

/// The first pause, and the longest, before a failed announcement goes again. Until one lands,
/// the other gateways send the partition's groups to its last owner.
const ANNOUNCE_RETRY_FIRST: Duration = Duration::from_secs(1);
const ANNOUNCE_RETRY_MAX: Duration = Duration::from_secs(30);

/// Kafka's `offsets.retention.minutes` default. See [`CoordinationConfig::offsets_retention`].
const DEFAULT_OFFSETS_RETENTION: Duration = Duration::from_hours(24 * 7);

/// How often an owner looks for groups whose offsets expired. Kafka's
/// `offsets.retention.check.interval.ms` default.
const DEFAULT_RETENTION_CHECK: Duration = Duration::from_mins(10);

/// The records of one append that drops groups or their offsets, at most, so a sweep of many
/// groups stays under Iggy's message size.
const DROPS_PER_APPEND: usize = 1_000;

/// How long the listing of an expiry check, and each deletion of expired offsets in Iggy, may
/// take. A deletion cut off there can still land, and the next check finds nothing left to delete.
const EXPIRY_DEADLINE: Duration = Duration::from_secs(30);

/// The longest wait between expiry checks after checks that could not list the Iggy groups.
const EXPIRY_BACKOFF_MAX: Duration = Duration::from_hours(1);

/// How long a start waits for the bridge's first read of Iggy's metadata change log.
const CHANGE_LOG_WAIT: Duration = Duration::from_secs(10);

/// Clients that append to the log. A client sends one append at a time.
const WRITERS: usize = 4;

/// How long a load and each sweep still remove the offsets of a topic Iggy deleted. A partition
/// that changes owner meanwhile has no owner to remove them, and a takeover after a crash waits
/// for Iggy's session timeout, 36 s by default. A sweep also takes a commit that was in flight at
/// the deletion, and a removal that failed.
const DELETION_MEMORY: Duration = Duration::from_mins(2);

/// How the coordinator log behaves. Only `offsets_retention` has an environment variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinationConfig {
    /// Records after which an owner writes a checkpoint, so a load reads at most about this many
    /// records past the state, or as many as the state holds when that is more. An owner also
    /// writes one when the last is 24 hours old.
    pub checkpoint_records: u64,
    /// How long the offsets of a group without members stay after its last commit, or after it
    /// lost its last member when that came later (`IGGY_KAFKA_OFFSETS_RETENTION_MINUTES`). The
    /// owner then deletes them in Iggy and in the log. `None` takes Kafka's default, 7 days.
    pub offsets_retention: Option<Duration>,
    /// How often an owner looks for groups whose offsets expired.
    pub retention_check: Duration,
    /// The groups whose offsets one check deletes, at most. The rest wait for the next check, so
    /// a backlog never keeps a check running for long.
    pub expiries_per_check: usize,
    /// The groups whose offsets one deletion in Iggy takes. Their joins and commits wait for it.
    pub expiries_per_deletion: usize,
}

impl CoordinationConfig {
    /// [`Self::offsets_retention`], or Kafka's default.
    #[must_use]
    pub fn retention(&self) -> Duration {
        self.offsets_retention.unwrap_or(DEFAULT_OFFSETS_RETENTION)
    }
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            checkpoint_records: 10_000,
            offsets_retention: None,
            retention_check: DEFAULT_RETENTION_CHECK,
            expiries_per_check: 1_000,
            expiries_per_deletion: 100,
        }
    }
}

/// FNV-1a: every gateway and every version maps a key to the same partition.
fn fnv1a(parts: &[&[u8]]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for part in parts {
        for byte in *part {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x0100_0193);
        }
    }
    hash
}

/// The log partition that holds `group_id`.
#[must_use]
pub fn partition_of_group(group_id: &str) -> u32 {
    fnv1a(&[group_id.as_bytes()]) % PARTITIONS
}

/// Who owns each log partition at one moment, for one answer that names the controller and the
/// live gateways.
#[derive(Debug, Clone)]
pub struct Placement {
    own: u16,
    /// The owner of each partition, `None` where none is known. `None` as a whole without the
    /// coordinator log: then this gateway is the controller and every listed gateway is live.
    owners: Option<Vec<Option<u16>>>,
}

impl Placement {
    /// This gateway alone is the controller, and every listed gateway counts as live.
    #[must_use]
    pub const fn alone(own: u16) -> Self {
        Self { own, owners: None }
    }

    /// A placement with `owners` as the owner of each partition, for a test of the answers.
    #[cfg(test)]
    pub(crate) const fn with_owners(own: u16, owners: Vec<Option<u16>>) -> Self {
        Self {
            own,
            owners: Some(owners),
        }
    }

    /// The controller, this gateway where none is known.
    #[must_use]
    pub fn controller(&self) -> u16 {
        self.owner(0)
    }

    /// Whether `node_id` owns a partition, or is this gateway. Every gateway is live without the
    /// coordinator log.
    #[must_use]
    pub fn is_live(&self, node_id: u16) -> bool {
        node_id == self.own
            || self
                .owners
                .as_ref()
                .is_none_or(|owners| owners.contains(&Some(node_id)))
    }

    fn owner(&self, partition: u32) -> u16 {
        self.owners
            .as_ref()
            .and_then(|owners| owners.get(partition as usize).copied().flatten())
            .unwrap_or(self.own)
    }
}

/// This gateway's part in coordinating groups with the other gateways.
pub struct Coordination {
    node_id: u16,
    config: CoordinationConfig,
    link: Arc<CoordinatorLink>,
    /// Deletes the expired offsets in Iggy.
    bridge: Arc<IggyBridge>,
    groups: GroupCoordinator,
    /// The partitions this gateway serves, each with its log.
    served: RwLock<HashMap<u32, Arc<PartitionLog>>>,
    /// The partitions a load runs for.
    loading: Mutex<HashSet<u32>>,
    /// Until when each partition rests after it stopped here or its load failed.
    rested: Mutex<HashMap<u32, Instant>>,
    /// The newest announced claim and owner of each partition.
    owners: RwLock<HashMap<u32, (u64, u16)>>,
    /// The announcements of this gateway still to send, the newest per partition.
    announcements: Mutex<HashMap<u32, Announcement>>,
    /// Wakes the task that sends them.
    announce: Notify,
    /// The topics Iggy deleted within [`DELETION_MEMORY`], with when this gateway learned it.
    deleted_topics: Mutex<VecDeque<(Instant, DeletedTopic)>>,
    /// When this gateway last missed topic deletions, so loads look for them in `topics`.
    missed_deletions: Mutex<Option<Instant>>,
    /// The Kafka topics Iggy holds, as its change log shows them.
    topics: watch::Receiver<Arc<TopicShapes>>,
    /// The owners that a listener of this gateway does not list, each warned of once.
    unlisted: Mutex<HashSet<u16>>,
    stopped: mpsc::UnboundedSender<Stopped>,
    /// Stops the lease loop and the loads. A child of `cancel`, and the first step of a shutdown.
    leasing: CancellationToken,
    /// The lease loop, which a shutdown waits for.
    lease_loop: Mutex<Option<JoinHandle<()>>>,
    /// Stops the expiry checks. A child of `cancel`, cancelled first by a shutdown.
    expiry: CancellationToken,
    /// Held by each deletion of expired offsets, from its fence to its append. A shutdown waits
    /// for it before it hands the partitions over, so the next owner's log matches Iggy.
    expiring: tokio::sync::Mutex<()>,
    /// Each expired group whose deletion in Iggy failed, and since when.
    failing: Mutex<HashMap<String, Instant>>,
    holds: ExpiryHolds,
    /// When the lease loop last read this gateway's share of the partitions from Iggy.
    lease_read: Mutex<Instant>,
    cancel: CancellationToken,
}

/// A group whose offsets expired, as one check found it.
struct Expiry {
    log: Arc<PartitionLog>,
    group_id: String,
    /// Whether the check deletes the group's offsets in Iggy before the log drops them: the log
    /// holds a commit of the group, so Iggy holds offsets of it too, and deleting them there has
    /// not failed for a whole retention. A group of which the log holds only deletions has none
    /// left there.
    in_iggy: bool,
}

/// Why an expiry check deleted nothing in Iggy.
#[derive(Debug, thiserror::Error)]
enum ListingFailed {
    #[error(transparent)]
    Bridge(#[from] BridgeError),
    #[error("the bridge user may not list the Iggy groups of the Kafka topics {0:?}")]
    Unlisted(Vec<String>),
}

/// A step of an expiry check that a test can hold it at. See [`Coordination::hold_expiry`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryStep {
    /// The check found the expired groups and listed their Iggy groups. No group is fenced yet.
    Found,
    /// A deletion fenced its groups, and has not checked them again or deleted anything.
    Fenced,
}

/// Where tests hold expiry checks.
#[derive(Debug, Default)]
struct ExpiryHolds {
    found: Arc<tokio::sync::Mutex<()>>,
    fenced: Arc<tokio::sync::Mutex<()>>,
    /// The step a check waits at.
    waiting: watch::Sender<Option<ExpiryStep>>,
}

impl ExpiryHolds {
    const fn of(&self, step: ExpiryStep) -> &Arc<tokio::sync::Mutex<()>> {
        match step {
            ExpiryStep::Found => &self.found,
            ExpiryStep::Fenced => &self.fenced,
        }
    }

    /// Waits while a test holds `step`.
    async fn pass(&self, step: ExpiryStep) {
        let hold = self.of(step);
        if hold.try_lock().is_ok() {
            return;
        }
        self.waiting.send_replace(Some(step));
        drop(hold.lock().await);
        self.waiting.send_replace(None);
    }
}

impl Coordination {
    /// Joins the lease group and starts serving the partitions Iggy gives this gateway. Makes
    /// itself the home of `groups`.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidConfig`] when the bridge cannot read Iggy's metadata change log, or
    /// `config` sets a zero retention or check interval, and [`BridgeError::Timeout`] when the
    /// first read of the change log does not end in time. Another [`BridgeError`] when the log
    /// cannot be prepared in Iggy or the lease group not joined.
    pub async fn start(
        bridge: &Arc<IggyBridge>,
        groups: GroupCoordinator,
        node_id: u16,
        config: CoordinationConfig,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, BridgeError> {
        if config.retention().is_zero()
            || config.retention_check.is_zero()
            || config.expiries_per_check == 0
            || config.expiries_per_deletion == 0
        {
            return Err(BridgeError::InvalidConfig(
                "the offsets retention, the interval of its check and the expiries of a check \
                 must be above zero"
                    .to_string(),
            ));
        }
        // Before the join, so no deletion slips between a load and this.
        let deletions = bridge.topic_deletions();
        let topics = bridge.topic_shapes();
        match bridge
            .follows_change_log(Instant::now() + CHANGE_LOG_WAIT)
            .await
        {
            Some(true) => {}
            Some(false) => {
                return Err(BridgeError::InvalidConfig(
                    "the coordinator log needs Iggy's metadata change log, which the bridge \
                     cannot read: without it a topic created again under a deleted one's name \
                     starts at the deleted topic's offsets. Grant the bridge's Iggy user \
                     (IGGY_KAFKA_IGGY_USERNAME) the manage_servers permission, or set \
                     IGGY_KAFKA_COORDINATOR_LOG=false"
                        .to_string(),
                ));
            }
            None => return Err(BridgeError::Timeout),
        }
        let link = Arc::new(bridge.coordinator_link(PARTITIONS, WRITERS).await?);
        if let Err(error) = link.join_lease().await {
            link.close().await;
            return Err(error);
        }
        let (stopped, stops) = mpsc::unbounded_channel();
        let this = Arc::new(Self {
            node_id,
            config,
            link,
            bridge: Arc::clone(bridge),
            groups: groups.clone(),
            served: RwLock::new(HashMap::new()),
            loading: Mutex::new(HashSet::new()),
            rested: Mutex::new(HashMap::new()),
            owners: RwLock::new(HashMap::new()),
            announcements: Mutex::new(HashMap::new()),
            announce: Notify::new(),
            deleted_topics: Mutex::new(VecDeque::new()),
            missed_deletions: Mutex::new(None),
            topics,
            unlisted: Mutex::new(HashSet::new()),
            stopped,
            leasing: cancel.child_token(),
            lease_loop: Mutex::new(None),
            expiry: cancel.child_token(),
            expiring: tokio::sync::Mutex::new(()),
            failing: Mutex::new(HashMap::new()),
            holds: ExpiryHolds::default(),
            lease_read: Mutex::new(Instant::now()),
            cancel,
        });
        groups.set_home(Arc::clone(&this) as Arc<dyn GroupHome>);
        let lease_loop = tokio::spawn(Arc::clone(&this).lease_on(stops));
        *this
            .lease_loop
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(lease_loop);
        tokio::spawn(Arc::clone(&this).follow_owners());
        tokio::spawn(Arc::clone(&this).announce_on());
        tokio::spawn(Arc::clone(&this).sweep_on());
        tokio::spawn(Arc::clone(&this).expire_on());
        tokio::spawn(Arc::clone(&this).forget_deleted_topics(deletions));
        tokio::spawn(warn_when_lost(
            bridge.change_log_lost(),
            this.cancel.clone(),
        ));
        info!(
            node_id,
            "coordinating consumer groups with the other gateways"
        );
        Ok(this)
    }

    /// Leaves the lease group, so Iggy hands this gateway's partitions to the others without
    /// waiting for its session to end, then stops serving them. Their groups move with their
    /// members, so they do not rebalance. Gives up on Iggy at `until`: the others then take over
    /// when the session ends.
    pub async fn shutdown(&self, until: Instant) {
        // A deletion of expired offsets in Iggy appends to the log before the partitions go.
        self.expiry.cancel();
        let expiring = timeout_at(until, self.expiring.lock()).await;
        drop(expiring);
        if timeout_at(until, self.leave()).await.is_err() {
            warn!(
                "could not leave the coordinator lease group in time; the other gateways take over when the session ends"
            );
        }
        self.release_all(Stop::Released).await;
        self.cancel.cancel();
    }

    /// Signs the log's clients out of Iggy, after [`Self::shutdown`].
    pub async fn close(&self) {
        self.link.close().await;
    }

    /// Leaves the lease group. The lease loop stops first, and its read in flight ends: a read
    /// that finds this gateway no member would join the group again, and Iggy would hand
    /// partitions back to it.
    async fn leave(&self) {
        self.leasing.cancel();
        let lease_loop = self
            .lease_loop
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(lease_loop) = lease_loop
            && let Err(error) = lease_loop.await
        {
            warn!(%error, "the coordinator lease loop did not end cleanly");
        }
        if let Err(error) = self.link.leave_lease().await {
            warn!(%error, "could not leave the coordinator lease group; the other gateways take over when the session ends");
        }
    }

    /// The gateway that coordinates `group_id`, or `None` while no gateway is known to.
    #[must_use]
    pub fn coordinator_of(&self, group_id: &str) -> Option<u16> {
        self.owner_of(partition_of_group(group_id))
    }

    /// The controller: the owner of log partition 0, or `None` while unknown.
    #[must_use]
    pub fn controller(&self) -> Option<u16> {
        self.owner_of(0)
    }

    /// Warns, once per gateway, that `node_id` owns partitions but a listener of this gateway does
    /// not list it. Its groups get `COORDINATOR_NOT_AVAILABLE` here until every list names it.
    pub fn warn_unlisted(&self, node_id: u16) {
        let first = self
            .unlisted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node_id);
        if first {
            warn!(
                node_id,
                "a gateway that IGGY_KAFKA_BROKERS does not list coordinates groups; \
                 FindCoordinator answers COORDINATOR_NOT_AVAILABLE (15) for them until this \
                 gateway restarts with a list that names it"
            );
        }
    }

    /// The partitions this gateway serves, and the age of its lease, for the metrics.
    #[must_use]
    pub fn census(&self) -> CoordinatorCensus {
        let mut served: Vec<u32> = self.served().keys().copied().collect();
        served.sort_unstable();
        let lease_read = *self
            .lease_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        CoordinatorCensus {
            served,
            partitions: PARTITIONS,
            lease_age: lease_read.elapsed(),
        }
    }

    /// Who owns each partition now.
    #[must_use]
    pub fn placement(&self) -> Placement {
        Placement {
            own: self.node_id,
            owners: Some(
                (0..PARTITIONS)
                    .map(|partition| self.owner_of(partition))
                    .collect(),
            ),
        }
    }

    /// Appends the offsets of `group_id`, each with its metadata, and waits until they count, at
    /// most until `deadline`: a heartbeat on the connection waits behind the commit. A commit
    /// equal to the logged one writes nothing (`PartitionLog::commit`).
    ///
    /// # Errors
    ///
    /// The Kafka code for every partition of the commit: `COORDINATOR_LOAD_IN_PROGRESS` when the
    /// log does not confirm it by `deadline`, else as `Self::serving` and
    /// `PartitionLog::append_by`.
    pub async fn commit_offsets(
        &self,
        group_id: &str,
        offsets: Vec<(String, i32, CommittedOffset)>,
        deadline: Instant,
    ) -> Result<(), i16> {
        let log = self.serving(group_id)?;
        log.commit(
            group_id,
            offsets,
            deadline,
            ERROR_COORDINATOR_LOAD_IN_PROGRESS,
        )
        .await
    }

    /// Deletes the offsets of `group_id` for each of `keys`.
    ///
    /// # Errors
    ///
    /// The Kafka code that refuses the deletion. See `Self::serving`.
    pub async fn delete_offsets(
        &self,
        group_id: &str,
        keys: Vec<(String, i32)>,
    ) -> Result<(), i16> {
        if keys.is_empty() {
            return Ok(());
        }
        let log = self.serving(group_id)?;
        let bodies = keys
            .into_iter()
            .map(|(topic, partition)| Body::Offset {
                group_id: group_id.to_string(),
                topic,
                partition,
                committed: None,
            })
            .collect();
        log.append(bodies).await.map(drop)
    }

    /// Deletes `group_id` with every offset it holds.
    ///
    /// # Errors
    ///
    /// The Kafka code that refuses the deletion. See `Self::serving`.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), i16> {
        let log = self.serving(group_id)?;
        log.append(vec![
            Body::GroupOffsetsDeleted {
                group_id: group_id.to_string(),
            },
            Body::Group {
                group_id: group_id.to_string(),
                snapshot: None,
            },
        ])
        .await
        .map(drop)
    }

    /// The offsets the log holds of `group_id` for the partitions of each topic of `partitions`,
    /// or for every partition with `None`. `None` when the log holds none of the group. Leaves
    /// out each offset of an older incarnation of its topic than the change log shows.
    ///
    /// # Errors
    ///
    /// The Kafka code that sends the client elsewhere. See `Self::serving`.
    pub async fn offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(&str, &[i32])]>,
    ) -> Result<Option<GroupOffsets>, i16> {
        let log = self.serving(group_id)?;
        // A newer incarnation that the change log shows proves the older one deleted, however
        // far the change log is behind the log.
        let topics = Arc::clone(&self.topics.borrow());
        let offsets = log
            .with_state(|state| {
                let offsets = state.offsets.get(group_id)?;
                let Some(partitions) = partitions else {
                    return Some(offsets.clone());
                };
                let mut entries = BTreeMap::new();
                for (topic, indexes) in partitions {
                    let mut key: TopicPartition = ((*topic).to_owned(), 0);
                    for index in *indexes {
                        key.1 = *index;
                        if let Some(entry) = offsets.entries.get(&key) {
                            entries.insert(key.clone(), entry.clone());
                        }
                    }
                }
                Some(GroupOffsets {
                    complete: offsets.complete,
                    entries,
                })
            })
            .await;
        Ok(offsets.map(|mut offsets| {
            offsets
                .drop_older_incarnations(|topic| topics.get(topic).map(|shape| shape.incarnation));
            offsets
        }))
    }

    /// Every group the log holds state or offsets for, in the partitions this gateway serves.
    pub async fn logged_groups(&self) -> BTreeSet<String> {
        let logs: Vec<Arc<PartitionLog>> = self.served().values().cloned().collect();
        let mut groups = BTreeSet::new();
        for log in logs {
            log.with_state(|state| {
                groups.extend(
                    state
                        .groups
                        .keys()
                        .chain(state.offsets.keys())
                        .filter(|group_id| logs_group(state, group_id))
                        .cloned(),
                );
            })
            .await;
        }
        groups
    }

    /// Whether the log holds state or offsets for `group_id`, in a partition this gateway serves.
    pub async fn holds(&self, group_id: &str) -> bool {
        let log = self.served().get(&partition_of_group(group_id)).cloned();
        match log {
            Some(log) => log.with_state(|state| logs_group(state, group_id)).await,
            None => false,
        }
    }

    /// The log of the partition that holds `group_id`, when this gateway serves it.
    ///
    /// # Errors
    ///
    /// `COORDINATOR_LOAD_IN_PROGRESS` while this gateway loads the partition, `NOT_COORDINATOR`
    /// when another gateway owns it.
    fn serving(&self, group_id: &str) -> Result<Arc<PartitionLog>, i16> {
        let partition = partition_of_group(group_id);
        if let Some(log) = self.served().get(&partition).filter(|log| log.is_serving()) {
            return Ok(Arc::clone(log));
        }
        if self.loading().contains(&partition) {
            return Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
        }
        Err(ERROR_NOT_COORDINATOR)
    }

    fn owner_of(&self, partition: u32) -> Option<u16> {
        if self
            .served()
            .get(&partition)
            .is_some_and(|log| log.is_serving())
        {
            return Some(self.node_id);
        }
        // An announcement of this gateway that it no longer serves is stale.
        self.owners()
            .get(&partition)
            .map(|(_, node_id)| *node_id)
            .filter(|node_id| *node_id != self.node_id)
    }

    /// Reads this gateway's share of the partitions every [`LEASE_POLL`], and loads or releases
    /// partitions to match it.
    async fn lease_on(self: Arc<Self>, mut stops: mpsc::UnboundedReceiver<Stopped>) {
        let mut tick = interval(LEASE_POLL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_read = Instant::now();
        loop {
            tokio::select! {
                () = self.leasing.cancelled() => return,
                Some(stopped) = stops.recv() => self.take_stop(stopped).await,
                _ = tick.tick() => {
                    let read = tokio::select! {
                        () = self.leasing.cancelled() => return,
                        read = self.link.assignment() => read,
                    };
                    match read {
                        Ok(Some(assigned)) => {
                            last_read = self.read_lease_now();
                            self.reconcile(&assigned);
                        }
                        Ok(None) => {
                            last_read = self.read_lease_now();
                            self.release_all(Stop::Released).await;
                            if self.leasing.is_cancelled() {
                                return;
                            }
                            warn!("this gateway is no member of the coordinator lease group; joining again");
                            if let Err(error) = self.link.join_lease().await {
                                warn!(%error, "could not join the coordinator lease group");
                            }
                        }
                        Err(error) if last_read.elapsed() >= LEASE_TIMEOUT => {
                            if !self.served().is_empty() {
                                warn!(%error, "could not read the coordinator lease for {LEASE_TIMEOUT:?}; stopping every coordinator partition");
                            }
                            self.release_all(Stop::Released).await;
                        }
                        Err(error) => debug!(%error, "could not read the coordinator lease"),
                    }
                }
            }
        }
    }

    /// Loads the partitions Iggy gives this gateway. A partition that leaves its share stays served
    /// until another gateway claims it: Iggy can take until the lease group's rebalancing timeout
    /// to hand it over, and its groups keep a coordinator meanwhile.
    fn reconcile(self: &Arc<Self>, assigned: &[u32]) {
        let now = Instant::now();
        for partition in assigned
            .iter()
            .copied()
            .filter(|partition| *partition < PARTITIONS)
        {
            // A log that stopped on its own stays until `take_stop` releases it, which evicts the
            // partition's groups. A load before that would lose the groups it restored.
            let held = self.served().contains_key(&partition);
            let resting = self
                .rested()
                .get(&partition)
                .is_some_and(|until| *until > now);
            if held || resting || !self.loading().insert(partition) {
                continue;
            }
            tokio::spawn(Arc::clone(self).load(partition));
        }
    }

    /// Claims `partition`, restores its groups and serves it.
    async fn load(self: Arc<Self>, partition: u32) {
        // Groups of this partition left from an earlier claim would shadow the log's.
        self.groups
            .evict(|group_id| partition_of_group(group_id) == partition)
            .await;
        let claimed = tokio::select! {
            () = self.leasing.cancelled() => {
                self.loading().remove(&partition);
                return;
            }
            claimed = partition::claim(&self.link, partition, self.node_id) => claimed,
        };
        match claimed {
            Ok(Some(claimed)) => {
                let snapshots: Vec<(String, GroupSnapshot)> = claimed
                    .state
                    .groups
                    .iter()
                    .map(|(group_id, snapshot)| (group_id.clone(), snapshot.clone()))
                    .collect();
                self.groups.restore(&snapshots).await;
                let log = PartitionLog::serve(
                    Arc::clone(&self.link),
                    partition,
                    self.node_id,
                    self.config,
                    claimed,
                    self.stopped.clone(),
                    &self.cancel,
                );
                let claim = log.claim;
                let served = {
                    let mut served = self.served_mut();
                    // Under the lock that a shutdown's release takes after it stops the leasing: a
                    // load either serves before the release, or sees the shutdown and does not.
                    (!self.leasing.is_cancelled())
                        .then(|| served.insert(partition, Arc::clone(&log)))
                };
                let Some(previous) = served else {
                    log.stop();
                    self.groups
                        .evict(|group_id| partition_of_group(group_id) == partition)
                        .await;
                    self.loading().remove(&partition);
                    return;
                };
                if let Some(previous) = previous {
                    previous.stop();
                }
                self.loading().remove(&partition);
                info!(
                    partition,
                    claim,
                    groups = snapshots.len(),
                    "took the coordinator log partition over"
                );
                self.announce(partition, claim);
                self.forget_recent_deletions(&log).await;
                if self.missed_recently() {
                    self.forget_missed_topics(&log).await;
                } else {
                    self.forget_older_incarnations(&log).await;
                }
            }
            Ok(None) => {
                info!(
                    partition,
                    "another gateway claimed the coordinator log partition first"
                );
                self.rest(partition);
                self.loading().remove(&partition);
            }
            Err(LoadError::Unreadable { offset, source }) => {
                error!(
                    partition,
                    offset,
                    %source,
                    "a newer gateway wrote the coordinator log partition, so this gateway does \
                     not serve it: its groups wait for a gateway of that version"
                );
                self.rest_for(partition, UNREADABLE_BACKOFF);
                self.loading().remove(&partition);
            }
            Err(error) => {
                warn!(partition, %error, "could not load the coordinator log partition");
                self.rest(partition);
                self.loading().remove(&partition);
            }
        }
    }

    /// A served partition stopped on its own: drop it, unless a later load serves it already.
    async fn take_stop(&self, stopped: Stopped) {
        let current = self
            .served()
            .get(&stopped.partition)
            .is_some_and(|log| log.claim == stopped.claim);
        if current {
            info!(
                partition = stopped.partition,
                stop = ?stopped.stop,
                "stopped serving the coordinator log partition"
            );
            self.release(stopped.partition, stopped.stop).await;
        }
    }

    async fn release(&self, partition: u32, stop: Stop) {
        let log = self.served_mut().remove(&partition);
        if let Some(log) = log {
            log.stop();
            if stop == Stop::Released {
                debug!(partition, "released the coordinator log partition");
            }
        }
        let evicted = self
            .groups
            .evict(|group_id| partition_of_group(group_id) == partition)
            .await;
        if evicted > 0 {
            debug!(partition, evicted, "moved the partition's groups away");
        }
        self.rest(partition);
    }

    async fn release_all(&self, stop: Stop) {
        let served: Vec<u32> = self.served().keys().copied().collect();
        for partition in served {
            self.release(partition, stop).await;
        }
    }

    fn rest(&self, partition: u32) {
        self.rest_for(partition, RELOAD_BACKOFF);
    }

    fn rest_for(&self, partition: u32, pause: Duration) {
        self.rested().insert(partition, Instant::now() + pause);
    }

    /// Queues an announcement that this gateway owns `partition` under `claim`, for
    /// [`Self::announce_on`]. This gateway routes by it at once.
    fn announce(&self, partition: u32, claim: u64) {
        let announcement = Announcement {
            partition,
            claim,
            node_id: self.node_id,
        };
        self.take_announcement(announcement);
        self.announcements().insert(partition, announcement);
        self.announce.notify_one();
    }

    /// Sends the queued announcements, and those of every served partition again each
    /// [`ANNOUNCE_EVERY`]. A send that fails goes again after a pause that doubles up to
    /// [`ANNOUNCE_RETRY_MAX`]. A task of its own, so no send holds up the lease loop.
    async fn announce_on(self: Arc<Self>) {
        let mut every = interval_at(Instant::now() + ANNOUNCE_EVERY, ANNOUNCE_EVERY);
        every.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pause = ANNOUNCE_RETRY_FIRST;
        let mut retry = None;
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => return,
                () = self.announce.notified() => {}
                _ = every.tick() => self.queue_served(),
                () = wake_at(retry) => {}
            }
            let queued: Vec<Announcement> = self
                .announcements()
                .drain()
                .map(|(_, announcement)| announcement)
                .collect();
            let mut failed = false;
            for announcement in queued {
                // Another gateway owns it now, and its own announcement says so.
                if !self.serves_claim(announcement.partition, announcement.claim) {
                    continue;
                }
                if let Err(error) = self.link.announce(announcement.encode()).await {
                    let partition = announcement.partition;
                    if retry.is_none() {
                        warn!(partition, %error, "could not announce the coordinator log partition; retrying");
                    } else {
                        debug!(partition, %error, "could not announce the coordinator log partition");
                    }
                    self.announcements()
                        .entry(partition)
                        .or_insert(announcement);
                    failed = true;
                }
            }
            if failed {
                retry = Some(Instant::now() + pause);
                pause = pause.saturating_mul(2).min(ANNOUNCE_RETRY_MAX);
            } else {
                retry = None;
                pause = ANNOUNCE_RETRY_FIRST;
            }
        }
    }

    fn queue_served(&self) {
        let served: Vec<Announcement> = self
            .served()
            .iter()
            .filter(|(_, log)| log.is_serving())
            .map(|(partition, log)| Announcement {
                partition: *partition,
                claim: log.claim,
                node_id: self.node_id,
            })
            .collect();
        self.announcements().extend(
            served
                .into_iter()
                .map(|announcement| (announcement.partition, announcement)),
        );
    }

    fn serves_claim(&self, partition: u32, claim: u64) -> bool {
        self.served()
            .get(&partition)
            .is_some_and(|log| log.claim == claim && log.is_serving())
    }

    fn take_announcement(&self, announcement: Announcement) {
        if announcement.partition >= PARTITIONS {
            return;
        }
        let mut owners = self.owners.write().unwrap_or_else(PoisonError::into_inner);
        let newer = owners
            .get(&announcement.partition)
            .is_none_or(|(claim, _)| announcement.claim >= *claim);
        if newer {
            owners.insert(
                announcement.partition,
                (announcement.claim, announcement.node_id),
            );
        }
    }

    /// Reads every announcement, from the oldest Iggy keeps, as it lands.
    async fn follow_owners(self: Arc<Self>) {
        let mut next = 0;
        loop {
            let read = tokio::select! {
                () = self.cancel.cancelled() => return,
                read = self.link.read_announcements(next, TAIL_WAIT) => read,
            };
            match read {
                Ok(records) => {
                    for logged in records {
                        next = logged.offset + 1;
                        match Announcement::decode(&logged.payload) {
                            Ok(Some(announcement)) => self.take_announcement(announcement),
                            Ok(None) => {}
                            Err(error) => {
                                debug!(%error, "skipping an announcement that does not decode");
                            }
                        }
                    }
                }
                Err(error) => {
                    debug!(%error, "could not read the coordinator announcements; retrying");
                    tokio::select! {
                        () = self.cancel.cancelled() => return,
                        () = tokio::time::sleep(LEASE_POLL) => {}
                    }
                }
            }
        }
    }

    /// Every [`SWEEP_EVERY`], drops the groups whose members are all gone, and removes the
    /// state the log keeps for them. Without it a member restored from the log that never comes
    /// back would stay listed.
    async fn sweep_on(self: Arc<Self>) {
        let mut tick = interval(SWEEP_EVERY);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            self.groups.sweep().await;
            let logs: Vec<Arc<PartitionLog>> = self.served().values().cloned().collect();
            for log in logs {
                self.forget_gone_groups(&log).await;
                self.forget_recent_deletions(&log).await;
            }
        }
    }

    /// Every `retention_check`, deletes the offsets that expired. Apart from the sweep, since a
    /// deletion in Iggy can take seconds. A check that cannot list the Iggy groups stops, and the
    /// wait before the next doubles, up to [`EXPIRY_BACKOFF_MAX`]. It warns once until a check
    /// lists them again.
    async fn expire_on(self: Arc<Self>) {
        let every = self.config.retention_check;
        let longest = every.max(EXPIRY_BACKOFF_MAX);
        let (mut wait, mut failing) = (every, false);
        loop {
            tokio::select! {
                () = self.expiry.cancelled() => return,
                () = sleep(wait) => {}
            }
            // Not cancelled here: a deletion in Iggy appends to the log before it ends.
            match self.expire_offsets().await {
                Ok(()) => {
                    if failing {
                        info!("the gateway deletes the expired offsets of Kafka groups again");
                    }
                    (wait, failing) = (every, false);
                }
                Err(error) => {
                    if failing {
                        debug!(%error, "could not look for expired Kafka group offsets in Iggy");
                    } else {
                        warn!(
                            %error,
                            "could not look for expired Kafka group offsets in Iggy, so none \
                             expire; trying again later, less often while this lasts"
                        );
                    }
                    (wait, failing) = (wait.saturating_mul(2).min(longest), true);
                }
            }
        }
    }

    async fn forget_gone_groups(&self, log: &PartitionLog) {
        let logged: Vec<String> = log
            .with_state(|state| state.groups.keys().cloned().collect())
            .await;
        // Read now, not before the appends of the other partitions: a group that formed
        // meanwhile must keep its state.
        let gone = self.groups.absent(logged).await;
        let bodies = gone
            .into_iter()
            .map(|group_id| Body::Group {
                group_id,
                snapshot: None,
            })
            .collect();
        append_drops(log, bodies, "emptied groups").await;
    }

    /// Deletes the offsets of each group whose offsets expired (`PartitionState::is_expired`),
    /// so neither Iggy nor the log grows with every group that ever committed. Takes at most
    /// `expiries_per_check` groups, `expiries_per_deletion` at a time, and lists their Iggy groups
    /// once.
    ///
    /// A group whose deletion in Iggy failed for a whole retention leaves the log without it, and
    /// Iggy keeps its offsets, so a deletion that keeps failing does not grow the log.
    ///
    /// # Errors
    ///
    /// [`ListingFailed`] when the Iggy groups cannot all be listed. Nothing is deleted in Iggy
    /// then, and only the groups that failed for a whole retention leave the log.
    async fn expire_offsets(&self) -> Result<(), ListingFailed> {
        let retention = self.config.retention();
        let mut expired = self.expired_groups(millis(retention)).await;
        let overdue: HashSet<String> = {
            let mut failing = self.failing();
            let found: HashSet<&str> = expired
                .iter()
                .map(|expiry| expiry.group_id.as_str())
                .collect();
            failing.retain(|group_id, _| found.contains(group_id.as_str()));
            failing
                .iter()
                .filter(|(_, since)| since.elapsed() >= retention)
                .map(|(group_id, _)| group_id.clone())
                .collect()
        };
        expired.truncate(self.config.expiries_per_check);
        for expiry in &mut expired {
            expiry.in_iggy &= !overdue.contains(&expiry.group_id);
        }
        let in_iggy: Vec<&str> = expired
            .iter()
            .filter(|expiry| expiry.in_iggy)
            .map(|expiry| expiry.group_id.as_str())
            .collect();
        let listing = if in_iggy.is_empty() {
            Ok(OffsetGroupListing::default())
        } else {
            self.list_in_iggy(&in_iggy).await
        };
        let listing = match listing {
            Ok(listing) => listing,
            Err(error) => {
                self.note_failures(&in_iggy);
                let out_of_iggy: Vec<Expiry> = expired
                    .into_iter()
                    .filter(|expiry| !expiry.in_iggy)
                    .collect();
                self.expire_all(&out_of_iggy, &OffsetGroupListing::default())
                    .await;
                return Err(error);
            }
        };
        self.holds.pass(ExpiryStep::Found).await;
        self.expire_all(&expired, &listing).await;
        Ok(())
    }

    /// The groups whose offsets expired on the partitions this gateway serves.
    async fn expired_groups(&self, retention_ms: i64) -> Vec<Expiry> {
        let now = unix_millis();
        let logs: Vec<Arc<PartitionLog>> = self
            .served()
            .values()
            .filter(|log| log.is_serving())
            .cloned()
            .collect();
        let mut expired = Vec::new();
        for log in logs {
            // An expired group holds no state, so `logs_group` asks whether it holds a commit.
            let found: Vec<(String, bool)> = log
                .with_state(|state| {
                    state
                        .expired_groups(retention_ms, now)
                        .into_iter()
                        .map(|group_id| {
                            let in_iggy = logs_group(state, &group_id);
                            (group_id, in_iggy)
                        })
                        .collect()
                })
                .await;
            expired.extend(found.into_iter().map(|(group_id, in_iggy)| Expiry {
                log: Arc::clone(&log),
                group_id,
                in_iggy,
            }));
        }
        expired
    }

    /// The Iggy groups that hold the offsets of `groups`.
    async fn list_in_iggy(&self, groups: &[&str]) -> Result<OffsetGroupListing, ListingFailed> {
        let listing = timeout_at(
            Instant::now() + EXPIRY_DEADLINE,
            self.bridge.list_offset_groups(groups),
        )
        .await
        .map_err(|_elapsed| BridgeError::Timeout)??;
        if listing.unlisted.is_empty() {
            Ok(listing)
        } else {
            Err(ListingFailed::Unlisted(listing.unlisted))
        }
    }

    /// [`Self::expire`] for `expired`, `expiries_per_deletion` at a time, until a shutdown.
    async fn expire_all(&self, expired: &[Expiry], listing: &OffsetGroupListing) {
        let retention = millis(self.config.retention());
        for batch in expired.chunks(self.config.expiries_per_deletion) {
            let _expiring = self.expiring.lock().await;
            if self.expiry.is_cancelled() {
                return;
            }
            self.expire(batch, listing, retention).await;
        }
    }

    /// Deletes the offsets of each group of `expired` while the groups take no join and no
    /// commit: in Iggy on each topic `listing` found, as `DeleteGroups` does, then in the log. A
    /// group that a member joined, or that a commit reached, since it expired keeps its offsets.
    /// A group whose deletion in Iggy fails stays in the log, and the next check tries it again.
    async fn expire(&self, expired: &[Expiry], listing: &OffsetGroupListing, retention: i64) {
        let names: Vec<StrBytes> = expired
            .iter()
            .map(|expiry| StrBytes::from_string(expiry.group_id.clone()))
            .collect();
        // Until this drops, the groups take no join and no commit.
        let (removals, deletion) = self.groups.group_deletion(&names).await;
        let deadline = Instant::now() + EXPIRY_DEADLINE;
        if !deletion.commits_ended(deadline).await {
            debug!(
                "a commit of a group whose offsets expired still runs; trying at the next check"
            );
            return;
        }
        self.holds.pass(ExpiryStep::Fenced).await;
        // Checked again just before the deletion, so a group that came back keeps its offsets,
        // and a partition that moved meanwhile deletes nothing.
        let now = unix_millis();
        let mut still: Vec<&Expiry> = Vec::with_capacity(expired.len());
        for (expiry, removal) in expired.iter().zip(removals) {
            let gone = removal == GroupRemoval::Absent
                && expiry.log.is_serving()
                && expiry
                    .log
                    .with_state(|state| state.is_expired(&expiry.group_id, retention, now))
                    .await;
            if gone {
                still.push(expiry);
            }
        }
        let in_iggy: Vec<&str> = still
            .iter()
            .filter(|expiry| expiry.in_iggy)
            .map(|expiry| expiry.group_id.as_str())
            .collect();
        // Each deletion keeps the groups fenced until it can no longer land, also past the
        // deadline, so no commit lands while a deletion can.
        let calls = OffsetCalls::new(deadline).keeping(deletion);
        let failed = self.delete_in_iggy(&in_iggy, listing, &calls).await;
        let mut drops: HashMap<u32, (&PartitionLog, Vec<Body>)> = HashMap::new();
        let mut kept_in_iggy = 0_usize;
        for expiry in still
            .into_iter()
            .filter(|expiry| !failed.contains(expiry.group_id.as_str()))
        {
            kept_in_iggy +=
                usize::from(!expiry.in_iggy && self.failing().remove(&expiry.group_id).is_some());
            drops
                .entry(expiry.log.partition)
                .or_insert_with(|| (expiry.log.as_ref(), Vec::new()))
                .1
                .push(Body::GroupOffsetsExpired {
                    group_id: expiry.group_id.clone(),
                });
        }
        if kept_in_iggy > 0 {
            warn!(
                groups = kept_in_iggy,
                "the expired offsets of Kafka groups could not be deleted in Iggy for a whole \
                 retention, so only the coordinator log drops them; Iggy keeps them until \
                 DeleteGroups deletes the groups"
            );
        }
        // Still fenced by `calls`, so no commit lands between the deletion in Iggy and the one in
        // the log.
        for (log, bodies) in drops.into_values() {
            append_drops(log, bodies, "expired offsets").await;
        }
    }

    /// Deletes the offsets of `groups` in Iggy, on each topic `listing` found. The groups whose
    /// deletion failed, which count as failing from their first failure on.
    async fn delete_in_iggy<'g>(
        &self,
        groups: &[&'g str],
        listing: &OffsetGroupListing,
        calls: &OffsetCalls,
    ) -> HashSet<&'g str> {
        if groups.is_empty() {
            return HashSet::new();
        }
        let results = self
            .bridge
            .delete_listed_offset_groups(groups, listing, calls)
            .await;
        let mut failed = HashSet::new();
        let mut first_failure = None;
        for (&group, result) in groups.iter().zip(results) {
            match result {
                Ok(_) => {
                    self.failing().remove(group);
                }
                Err(error) => {
                    failed.insert(group);
                    first_failure.get_or_insert((group, error));
                }
            }
        }
        let failing: Vec<&str> = failed.iter().copied().collect();
        let new = self.note_failures(&failing);
        if let Some((group, error)) = first_failure {
            if new > 0 {
                warn!(
                    group,
                    failed = failed.len(),
                    %error,
                    "could not delete the expired offsets of Kafka groups in Iggy; trying again \
                     at the next check"
                );
            } else {
                debug!(
                    group,
                    failed = failed.len(),
                    %error,
                    "could not delete the expired offsets of Kafka groups in Iggy again"
                );
            }
        }
        failed
    }

    /// Counts each of `groups` as failing from now, unless it already does. How many did not.
    fn note_failures(&self, groups: &[&str]) -> usize {
        let now = Instant::now();
        let mut failing = self.failing();
        groups
            .iter()
            .filter(|group| {
                if failing.contains_key(**group) {
                    return false;
                }
                failing.insert((**group).to_owned(), now);
                true
            })
            .count()
    }

    /// Runs one expiry check now, as each `retention_check` does. Whether it could list the Iggy
    /// groups. For tests, which set a long check interval.
    #[doc(hidden)]
    pub async fn expire_now(&self) -> bool {
        self.expire_offsets().await.is_ok()
    }

    /// Holds expiry checks at `step` until the guard drops. For tests that land a join or a commit
    /// there. [`Self::expiry_waiting_at`] tells when a check waits.
    #[doc(hidden)]
    pub async fn hold_expiry(&self, step: ExpiryStep) -> OwnedMutexGuard<()> {
        Arc::clone(self.holds.of(step)).lock_owned().await
    }

    /// Ends once an expiry check waits at `step`, which [`Self::hold_expiry`] holds.
    #[doc(hidden)]
    pub async fn expiry_waiting_at(&self, step: ExpiryStep) {
        let mut waiting = self.holds.waiting.subscribe();
        let _ = waiting.wait_for(|at| *at == Some(step)).await;
    }

    fn failing(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instant>> {
        self.failing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Removes the offsets of each topic Iggy deletes from the partitions this gateway serves, so
    /// a new topic of that name starts with none, as in Kafka. A receiver that lags matches the
    /// logs to Iggy's topics instead.
    async fn forget_deleted_topics(
        self: Arc<Self>,
        mut deletions: broadcast::Receiver<DeletedTopic>,
    ) {
        loop {
            let deleted = tokio::select! {
                () = self.cancel.cancelled() => return,
                deleted = deletions.recv() => match deleted {
                    Ok(deleted) => deleted,
                    Err(RecvError::Lagged(missed)) => {
                        warn!(missed, "missed topic deletions; matching the coordinator log to Iggy's topics instead");
                        self.note_missed_deletions();
                        let logs: Vec<Arc<PartitionLog>> = self.served().values().cloned().collect();
                        for log in logs {
                            self.forget_missed_topics(&log).await;
                        }
                        continue;
                    }
                    Err(RecvError::Closed) => return,
                },
            };
            self.remember_deletion(&deleted);
            let logs: Vec<Arc<PartitionLog>> = self.served().values().cloned().collect();
            for log in logs {
                if let Err(error) = forget_topic(&log, &deleted).await {
                    warn!(
                        partition = log.partition,
                        topic = deleted.name,
                        error,
                        "could not remove a deleted topic's offsets from the coordinator log; the next sweep tries again"
                    );
                }
            }
        }
    }

    fn remember_deletion(&self, deleted: &DeletedTopic) {
        let mut recent = self
            .deleted_topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        recent.retain(|(at, _)| at.elapsed() < DELETION_MEMORY);
        recent.push_back((Instant::now(), deleted.clone()));
    }

    /// Remembers that deletions were missed, so loads within [`DELETION_MEMORY`] look for them.
    fn note_missed_deletions(&self) {
        *self
            .missed_deletions
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
    }

    fn missed_recently(&self) -> bool {
        self.missed_deletions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|at| at.elapsed() < DELETION_MEMORY)
    }

    /// Removes from `log` the offsets of each topic incarnation that Iggy no longer holds. The
    /// change log shows a read's topics before it reports their deletions, so this finds the
    /// deletions that a lagged receiver missed.
    async fn forget_missed_topics(&self, log: &PartitionLog) {
        let bodies = log
            .with_state(|state| {
                // Read under the state's lock, so each commit it holds saw these topics or older.
                let topics = Arc::clone(&self.topics.borrow());
                state.deletions_against(|topic| topics.get(topic).map(|shape| shape.incarnation))
            })
            .await;
        append_drops(log, bodies, "missed topic deletions").await;
    }

    /// Removes from `log` the offsets of each topic incarnation older than the one the change log
    /// shows, so a deletion that no gateway heard of, as while every gateway was down, still
    /// reaches the log.
    async fn forget_older_incarnations(&self, log: &PartitionLog) {
        let bodies = log
            .with_state(|state| {
                let topics = Arc::clone(&self.topics.borrow());
                state.older_incarnations(|topic| topics.get(topic).map(|shape| shape.incarnation))
            })
            .await;
        append_drops(log, bodies, "older topic incarnations").await;
    }

    /// Removes from `log` the offsets of each topic Iggy deleted within [`DELETION_MEMORY`].
    async fn forget_recent_deletions(&self, log: &PartitionLog) {
        for deleted in self.recent_deletions() {
            if let Err(error) = forget_topic(log, &deleted).await {
                debug!(
                    partition = log.partition,
                    topic = deleted.name,
                    error,
                    "could not remove a deleted topic's offsets from the coordinator log"
                );
            }
        }
    }

    fn recent_deletions(&self) -> Vec<DeletedTopic> {
        self.deleted_topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(at, _)| at.elapsed() < DELETION_MEMORY)
            .map(|(_, deleted)| deleted.clone())
            .collect()
    }

    /// Notes that the lease loop read the lease now, and returns now.
    fn read_lease_now(&self) -> Instant {
        let now = Instant::now();
        *self
            .lease_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = now;
        now
    }

    fn served(&self) -> std::sync::RwLockReadGuard<'_, HashMap<u32, Arc<PartitionLog>>> {
        self.served.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn served_mut(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<u32, Arc<PartitionLog>>> {
        self.served.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn loading(&self) -> std::sync::MutexGuard<'_, HashSet<u32>> {
        self.loading.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn rested(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Instant>> {
        self.rested.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn owners(&self) -> std::sync::RwLockReadGuard<'_, HashMap<u32, (u64, u16)>> {
        self.owners.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn announcements(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Announcement>> {
        self.announcements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Milliseconds since the Unix epoch, as Kafka stamps a commit.
#[must_use]
pub fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, millis)
}

/// `duration` in milliseconds, the unit of a commit's time.
fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Ends at `at`, or never without one.
async fn wake_at(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Warns once `lost` ends, unless `cancel` comes first.
async fn warn_when_lost(lost: impl Future<Output = ()>, cancel: CancellationToken) {
    tokio::select! {
        () = cancel.cancelled() => {}
        () = lost => warn!(
            "the bridge can no longer read Iggy's metadata change log, so the coordinator log \
             keeps the offsets of the topics that Iggy deletes from now on, and a topic created \
             again under such a name starts at them. Grant manage_servers to the bridge's Iggy \
             user again and restart the gateway"
        ),
    }
}

/// Appends `bodies`, records that drop groups or their offsets, a bounded number at a time.
async fn append_drops(log: &PartitionLog, mut bodies: Vec<Body>, what: &str) {
    while !bodies.is_empty() {
        let rest = bodies.split_off(bodies.len().min(DROPS_PER_APPEND));
        if let Err(error) = log.append(bodies).await {
            debug!(
                partition = log.partition,
                error, what, "could not drop from the log"
            );
            return;
        }
        bodies = rest;
    }
}

/// Whether `state` holds the state of `group_id`, or an offset of it.
fn logs_group(state: &PartitionState, group_id: &str) -> bool {
    state.groups.contains_key(group_id)
        || state
            .offsets
            .get(group_id)
            .is_some_and(|offsets| offsets.entries.values().any(Option::is_some))
}

/// Removes from `log` the offsets that `deleted` takes, when it holds any.
///
/// # Errors
///
/// The Kafka code of the failed append.
async fn forget_topic(log: &PartitionLog, deleted: &DeletedTopic) -> Result<(), i16> {
    let holds = log
        .with_state(|state| state.holds_topic(&deleted.name, deleted.incarnation))
        .await;
    if !holds {
        return Ok(());
    }
    let body = Body::TopicOffsetsDeleted {
        topic: deleted.name.clone(),
        incarnation: deleted.incarnation,
    };
    log.append(vec![body]).await.map(drop)
}

#[async_trait]
impl GroupHome for Coordination {
    fn serves(&self, group_id: &str) -> Result<(), i16> {
        self.serving(group_id).map(drop)
    }

    async fn store(&self, group_id: &str, snapshot: GroupSnapshot) -> Result<(), i16> {
        let log = self.serving(group_id)?;
        log.append(vec![Body::Group {
            group_id: group_id.to_string(),
            snapshot: Some(snapshot),
        }])
        .await
        .map(drop)
    }
}

impl std::fmt::Debug for Coordination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordination")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_group_id_should_map_it_to_the_same_partition_every_time() {
        // Pinned, so a change of the hash, which moves every group, fails here.
        assert_eq!(partition_of_group("orders"), partition_of_group("orders"));
        assert_eq!(fnv1a(&[b"a"]), 0xe40c_292c);
        assert!((0..1_000).all(|n| partition_of_group(&format!("group-{n}")) < PARTITIONS));
    }

    #[test]
    fn given_many_groups_should_spread_them_over_every_partition() {
        let mut counts = [0_u32; PARTITIONS as usize];
        for n in 0..16_000 {
            counts[partition_of_group(&format!("group-{n}")) as usize] += 1;
        }
        assert!(
            counts.iter().all(|count| (800..1_200).contains(count)),
            "{counts:?}"
        );
    }
}
