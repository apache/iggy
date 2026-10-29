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
//! announcements, so any gateway can name the coordinator of a group, the leader of a Kafka
//! partition, and the controller.

mod partition;
pub mod record;
pub mod replay;

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::bridge::{BridgeError, CoordinatorLink, IggyBridge};
use crate::coordinator::partition::{PartitionLog, Stop, Stopped, TAIL_WAIT};
use crate::coordinator::record::{Announcement, Body, CommittedOffset};
use crate::coordinator::replay::{GroupOffsets, PartitionState, TopicPartition};
use crate::group::{GroupCoordinator, GroupHome, GroupSnapshot};
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

/// How often groups whose members are all gone are dropped, and their state removed from the
/// log.
const SWEEP_EVERY: Duration = Duration::from_secs(5);

/// How often an owner announces its partitions again. Well inside the announcements' retention.
const ANNOUNCE_EVERY: Duration = Duration::from_mins(10);

/// Clients that append to the log. A client sends one append at a time.
const WRITERS: usize = 4;

/// How long a load still removes the offsets of a topic Iggy deleted. A partition that changes
/// owner meanwhile has no owner to remove them, and a takeover after a crash waits for Iggy's
/// session timeout, 36 s by default.
const DELETION_MEMORY: Duration = Duration::from_mins(2);

/// How the coordinator log behaves. No environment variable maps onto it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinationConfig {
    /// Records after which an owner writes a checkpoint, so a load reads at most about this many
    /// records past the state, or as many as the state holds when that is more. An owner also
    /// writes one when the last is 24 hours old.
    pub checkpoint_records: u64,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            checkpoint_records: 10_000,
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

/// The log partition whose owner leads Kafka partition `partition` of `topic`.
#[must_use]
pub fn partition_of_topic(topic: &str, partition: i32) -> u32 {
    fnv1a(&[topic.as_bytes(), &partition.to_le_bytes()]) % PARTITIONS
}

/// Who owns each log partition at one moment, for one answer that names leaders.
#[derive(Debug, Clone)]
pub struct Placement {
    own: u16,
    /// The owner of each partition, `None` where none is known. `None` as a whole without the
    /// coordinator log: then this gateway leads everything and every listed gateway is live.
    owners: Option<Vec<Option<u16>>>,
}

impl Placement {
    /// This gateway alone leads every partition, and every listed gateway counts as live.
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

    /// The gateway that leads partition `partition` of `topic`, this one where none is known.
    #[must_use]
    pub fn leader_of(&self, topic: &str, partition: i32) -> u16 {
        self.owner(partition_of_topic(topic, partition))
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
    groups: GroupCoordinator,
    /// The partitions this gateway serves, each with its log.
    served: RwLock<HashMap<u32, Arc<PartitionLog>>>,
    /// The partitions a load runs for.
    loading: Mutex<HashSet<u32>>,
    /// When each partition last stopped here, so a load waits [`RELOAD_BACKOFF`].
    rested: Mutex<HashMap<u32, Instant>>,
    /// The newest announced claim and owner of each partition.
    owners: RwLock<HashMap<u32, (u64, u16)>>,
    /// The topics Iggy deleted within [`DELETION_MEMORY`], with when this gateway learned it.
    deleted_topics: Mutex<VecDeque<(Instant, String)>>,
    /// The owners that a listener of this gateway does not list, each warned of once.
    unlisted: Mutex<HashSet<u16>>,
    stopped: mpsc::UnboundedSender<Stopped>,
    cancel: CancellationToken,
}

impl Coordination {
    /// Joins the lease group and starts serving the partitions Iggy gives this gateway. Makes
    /// itself the home of `groups`.
    ///
    /// # Errors
    ///
    /// [`BridgeError`] when the log cannot be prepared in Iggy or the lease group not joined.
    pub async fn start(
        bridge: &IggyBridge,
        groups: GroupCoordinator,
        node_id: u16,
        config: CoordinationConfig,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, BridgeError> {
        let link = Arc::new(bridge.coordinator_link(PARTITIONS, WRITERS).await?);
        // Before the join, so no deletion slips between a load and this.
        let deletions = bridge.topic_deletions();
        link.join_lease().await?;
        let (stopped, stops) = mpsc::unbounded_channel();
        let this = Arc::new(Self {
            node_id,
            config,
            link,
            groups: groups.clone(),
            served: RwLock::new(HashMap::new()),
            loading: Mutex::new(HashSet::new()),
            rested: Mutex::new(HashMap::new()),
            owners: RwLock::new(HashMap::new()),
            deleted_topics: Mutex::new(VecDeque::new()),
            unlisted: Mutex::new(HashSet::new()),
            stopped,
            cancel,
        });
        groups.set_home(Arc::clone(&this) as Arc<dyn GroupHome>);
        tokio::spawn(Arc::clone(&this).lease_on(stops));
        tokio::spawn(Arc::clone(&this).follow_owners());
        tokio::spawn(Arc::clone(&this).sweep_on());
        tokio::spawn(Arc::clone(&this).forget_deleted_topics(deletions));
        info!(
            node_id,
            "coordinating consumer groups with the other gateways"
        );
        Ok(this)
    }

    /// Leaves the lease group, so Iggy hands this gateway's partitions to the others at once,
    /// then stops serving them. Their groups move with their members, so they do not rebalance.
    pub async fn shutdown(&self) {
        if let Err(error) = self.link.leave_lease().await {
            warn!(%error, "could not leave the coordinator lease group; the other gateways take over when the session ends");
        }
        self.release_all(Stop::Released).await;
        self.cancel.cancel();
        self.link.close().await;
    }

    #[must_use]
    pub const fn node_id(&self) -> u16 {
        self.node_id
    }

    /// The gateway that coordinates `group_id`, or `None` while no gateway is known to.
    #[must_use]
    pub fn coordinator_of(&self, group_id: &str) -> Option<u16> {
        self.owner_of(partition_of_group(group_id))
    }

    /// The gateway that leads partition `partition` of `topic`, or `None` while unknown.
    #[must_use]
    pub fn leader_of(&self, topic: &str, partition: i32) -> Option<u16> {
        self.owner_of(partition_of_topic(topic, partition))
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

    /// The gateways that own a partition, and this one.
    #[must_use]
    pub fn live_nodes(&self) -> BTreeSet<u16> {
        let mut nodes: BTreeSet<u16> = self
            .owners()
            .values()
            .map(|(_, node_id)| *node_id)
            .collect();
        nodes.insert(self.node_id);
        nodes
    }

    /// Appends the offsets of `group_id`, each with its metadata, and waits until they count, at
    /// most until `deadline`: a heartbeat on the connection waits behind the commit.
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
        let bodies = offsets
            .into_iter()
            .map(|(topic, partition, committed)| Body::Offset {
                group_id: group_id.to_string(),
                topic,
                partition,
                committed: Some(committed),
            })
            .collect();
        log.append_by(bodies, deadline, ERROR_COORDINATOR_LOAD_IN_PROGRESS)
            .await
            .map(drop)
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

    /// The offsets the log holds of `group_id` for `partitions`, or for every partition with
    /// `None`. `None` when the log holds none of the group.
    ///
    /// # Errors
    ///
    /// The Kafka code that sends the client elsewhere. See `Self::serving`.
    pub async fn offsets(
        &self,
        group_id: &str,
        partitions: Option<&[TopicPartition]>,
    ) -> Result<Option<GroupOffsets>, i16> {
        let log = self.serving(group_id)?;
        Ok(log
            .with_state(|state| {
                let offsets = state.offsets.get(group_id)?;
                let Some(partitions) = partitions else {
                    return Some(offsets.clone());
                };
                let entries = partitions
                    .iter()
                    .filter_map(|key| Some((key.clone(), offsets.entries.get(key)?.clone())))
                    .collect();
                Some(GroupOffsets {
                    complete: offsets.complete,
                    entries,
                })
            })
            .await)
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
        let mut announced = Instant::now();
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => return,
                Some(stopped) = stops.recv() => self.take_stop(stopped).await,
                _ = tick.tick() => {
                    match self.link.assignment().await {
                        Ok(Some(assigned)) => {
                            last_read = Instant::now();
                            self.reconcile(&assigned).await;
                        }
                        Ok(None) => {
                            last_read = Instant::now();
                            warn!("this gateway is no member of the coordinator lease group; joining again");
                            self.release_all(Stop::Released).await;
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
                    if announced.elapsed() >= ANNOUNCE_EVERY {
                        announced = Instant::now();
                        self.announce_served().await;
                    }
                }
            }
        }
    }

    /// Releases the partitions Iggy took away and loads the ones it gave.
    async fn reconcile(self: &Arc<Self>, assigned: &[u32]) {
        let assigned: HashSet<u32> = assigned
            .iter()
            .copied()
            .filter(|partition| *partition < PARTITIONS)
            .collect();
        let taken: Vec<u32> = self
            .served()
            .keys()
            .copied()
            .filter(|partition| !assigned.contains(partition))
            .collect();
        for partition in taken {
            self.release(partition, Stop::Released).await;
        }
        for partition in assigned {
            let served = self
                .served()
                .get(&partition)
                .is_some_and(|log| log.is_serving());
            let resting = self
                .rested()
                .get(&partition)
                .is_some_and(|at| at.elapsed() < RELOAD_BACKOFF);
            if served || resting || !self.loading().insert(partition) {
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
            () = self.cancel.cancelled() => {
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
                    self.config.checkpoint_records,
                    claimed,
                    self.stopped.clone(),
                    &self.cancel,
                );
                let claim = log.claim;
                let previous = self.served_mut().insert(partition, Arc::clone(&log));
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
                self.announce(partition, claim).await;
                for topic in self.recent_deletions() {
                    forget_topic(&log, &topic).await;
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
        self.rested().insert(partition, Instant::now());
    }

    async fn announce(&self, partition: u32, claim: u64) {
        let announcement = Announcement {
            partition,
            claim,
            node_id: self.node_id,
        };
        self.take_announcement(announcement);
        if let Err(error) = self.link.announce(announcement.encode()).await {
            warn!(partition, %error, "could not announce the coordinator log partition");
        }
    }

    async fn announce_served(&self) {
        let served: Vec<(u32, u64)> = self
            .served()
            .iter()
            .filter(|(_, log)| log.is_serving())
            .map(|(partition, log)| (*partition, log.claim))
            .collect();
        for (partition, claim) in served {
            self.announce(partition, claim).await;
        }
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
                let logged: Vec<String> = log
                    .with_state(|state| state.groups.keys().cloned().collect())
                    .await;
                // Read now, not before the appends of the other partitions: a group that formed
                // meanwhile must keep its state.
                let gone = self.groups.absent(logged).await;
                if gone.is_empty() {
                    continue;
                }
                let bodies = gone
                    .into_iter()
                    .map(|group_id| Body::Group {
                        group_id,
                        snapshot: None,
                    })
                    .collect();
                if let Err(error) = log.append(bodies).await {
                    debug!(
                        partition = log.partition,
                        error, "could not remove emptied groups from the log"
                    );
                }
            }
        }
    }

    /// Removes the offsets of each topic Iggy deletes from the partitions this gateway serves, so
    /// a new topic of that name starts with none, as in Kafka.
    async fn forget_deleted_topics(self: Arc<Self>, mut deletions: broadcast::Receiver<String>) {
        loop {
            let topic = tokio::select! {
                () = self.cancel.cancelled() => return,
                deleted = deletions.recv() => match deleted {
                    Ok(topic) => topic,
                    Err(RecvError::Lagged(missed)) => {
                        warn!(missed, "missed topic deletions; the coordinator log keeps their offsets");
                        continue;
                    }
                    Err(RecvError::Closed) => return,
                },
            };
            self.remember_deletion(&topic);
            let logs: Vec<Arc<PartitionLog>> = self.served().values().cloned().collect();
            for log in logs {
                forget_topic(&log, &topic).await;
            }
        }
    }

    fn remember_deletion(&self, topic: &str) {
        let mut deleted = self
            .deleted_topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        deleted.retain(|(at, _)| at.elapsed() < DELETION_MEMORY);
        deleted.push_back((Instant::now(), topic.to_string()));
    }

    fn recent_deletions(&self) -> Vec<String> {
        self.deleted_topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(at, _)| at.elapsed() < DELETION_MEMORY)
            .map(|(_, topic)| topic.clone())
            .collect()
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
}

/// Whether `state` holds the state of `group_id`, or an offset of it.
fn logs_group(state: &PartitionState, group_id: &str) -> bool {
    state.groups.contains_key(group_id)
        || state
            .offsets
            .get(group_id)
            .is_some_and(|offsets| offsets.entries.values().any(Option::is_some))
}

/// Removes the offsets of `topic` from `log`, when it holds any.
async fn forget_topic(log: &PartitionLog, topic: &str) {
    let holds = log
        .with_state(|state| {
            state
                .offsets
                .values()
                .any(|offsets| offsets.entries.keys().any(|(name, _)| name == topic))
        })
        .await;
    if !holds {
        return;
    }
    let partition = log.partition;
    if let Err(error) = log
        .append(vec![Body::TopicOffsetsDeleted {
            topic: topic.to_string(),
        }])
        .await
    {
        warn!(
            partition,
            topic, error, "could not remove a deleted topic's offsets from the coordinator log"
        );
    }
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

    #[test]
    fn given_a_topic_partition_should_map_it_by_name_and_index() {
        let spread: HashSet<u32> = (0..64)
            .map(|index| partition_of_topic("orders", index))
            .collect();
        assert!(spread.len() > 8, "{spread:?}");
    }
}
