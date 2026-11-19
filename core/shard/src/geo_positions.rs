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

//! The consumer group positions of the geo and shadow partitions here, which
//! travel between datacenters through the buckets.
//!
//! Each partition keeps its groups' positions in a state machine, see
//! [`partitions::geo_positions`]. This module moves them:
//!
//! - **Fold and publish.** Shard 0 starts a round every publish interval: it
//!   asks every shard for the positions of the partitions whose primary is
//!   here. Before it answers, a shard has those primaries fold what their
//!   groups committed, see [`IggyPartition::geo_notes`]. Shard 0 then stores
//!   one object per log in the node's store, [`PositionsObject`], when the
//!   positions of a partition it leads changed since the last one it stored.
//!   The backups commit the same positions after their primary, so only the
//!   primary publishes them, as only the primary uploads the messages.
//! - **Adopt.** Shard 0 lists the positions that each geo peer publishes and
//!   hands every shard those of the partitions it owns, see
//!   [`PeerPositions`]. The primaries here commit the ones that win over
//!   their own, see [`IggyPartition::geo_adoptions`].
//! - **Resume.** After the node starts, the group polls of geo and shadow
//!   partitions wait until every peer's positions were handed out once and
//!   the adoptions they made committed, or until a timeout. A group created
//!   here after a peer's position for its name arrived waits until the
//!   position is bound to it.
//!
//! [`IggyPartition::geo_notes`]: partitions::IggyPartition::geo_notes
//! [`IggyPartition::geo_adoptions`]: partitions::IggyPartition::geo_adoptions

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;

use consensus::{MetadataHandle, PartitionsHandle};
use futures::channel::oneshot;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{IggyError, IggyTimestamp};
use iggy_object_store::{
    GeoLog, GeoPosition, PositionsKey, PositionsObject, TierScope, TierScopeToken,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::{StreamsInner, Topic};
use partitions::geo_positions::{
    ADOPT_GEO_POSITIONS, LocalGroup, NOTE_GEO_POSITIONS, encode_adoptions, encode_notes,
};
use partitions::{PollingConsumer, TopicObjects};
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::shards_table::ShardsTable;
use crate::{
    IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, TaggedSender, bus_timeout,
    channel,
};

/// How long a round may wait for the answers of the shards and the store
/// before the next one replaces it.
const ROUND_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a shard waits for the reply of a command it proposed before it
/// proposes again for that partition.
const COMMAND_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// What a node needs to carry the positions of its groups.
#[derive(Debug, Clone)]
pub struct GeoPositionSettings {
    /// This cluster's `[geo] origin`.
    pub origin: String,
    /// The origins of the peers whose positions this node reads.
    pub peers: Vec<String>,
    pub publish_interval: Duration,
    /// How long group polls wait for the peers' positions after a start.
    pub resume_timeout: Duration,
}

/// A log that a topic of this cluster holds: one of its own geo topics, or a
/// shadow topic of a peer's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalLog {
    pub log: GeoLog,
    pub stream_id: usize,
    pub topic_id: usize,
    /// `(partition id, namespace, created revision)` of each partition here.
    pub partitions: Vec<(u32, IggyNamespace, u64)>,
}

/// The positions of one partition whose primary is on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPositions {
    pub log: GeoLog,
    pub partition_id: u32,
    pub positions: BTreeMap<String, GeoPosition>,
    /// The names of local groups whose committed offset the partition has
    /// not folded yet.
    pub unfolded: BTreeSet<String>,
}

/// A shard's answer to a round.
#[derive(Debug, Clone)]
pub struct GeoPositionsReport {
    pub round: u64,
    pub shard: u16,
    /// The partitions the shard leads.
    pub partitions: Vec<PartitionPositions>,
    /// The partitions the shard holds a replica of without leading them, by
    /// log and partition id.
    pub not_led: Vec<(GeoLog, u32)>,
}

/// What the store took of a round.
#[derive(Debug)]
pub struct FinishedPositionsStore {
    pub round: u64,
    /// The keys it stored, with what each object holds.
    pub stored: Vec<(PositionsKey, LogPositions)>,
    /// The keys whose put failed.
    pub failed: Vec<PositionsKey>,
    /// The keys of logs this cluster no longer holds, which it deleted.
    pub deleted: Vec<PositionsKey>,
}

/// The positions of one log by partition id, then by group name.
pub type LogPositions = BTreeMap<u32, BTreeMap<String, GeoPosition>>;

/// What a round found and stored of one log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogRound {
    /// The partitions this node leads.
    pub partitions: BTreeMap<u32, PartitionPositions>,
    /// The partitions this node holds a replica of without leading them.
    pub not_led: BTreeSet<u32>,
    /// Whether the node's store holds what the round found of the partitions
    /// it leads.
    pub stored: bool,
}

/// What a whole round found and stored, by log.
#[derive(Debug, Clone, Default)]
pub struct RoundSummary {
    pub logs: BTreeMap<GeoLog, LogRound>,
}

/// The positions that a geo peer publishes, for the partitions of one shard.
#[derive(Debug, Clone, Default)]
pub struct PeerPositions {
    pub origin: String,
    /// `(namespace, created revision, positions)` of each partition, the
    /// highest of the peer's nodes for each group.
    pub partitions: Vec<(IggyNamespace, u64, BTreeMap<String, GeoPosition>)>,
}

/// What a shard does with the positions of its partitions.
#[derive(Default)]
pub struct ShardGeoPositions {
    settings: RefCell<Option<Rc<GeoPositionSettings>>>,
    /// When group polls stop waiting for the peers, on the bus clock.
    resume_by: Cell<u64>,
    gate: RefCell<ResumeGate>,
    /// Partitions with a command in flight, which get no other until it ends.
    in_flight: Rc<RefCell<BTreeSet<IggyNamespace>>>,
    /// This shard's numbers for the commands it proposes.
    requests: Cell<u64>,
    /// The rounds that shard 0 runs.
    publisher: RefCell<Publisher>,
}

/// What the group polls of the partitions here wait for after the node
/// started.
#[derive(Default)]
struct ResumeGate {
    /// The wait ended for every partition: its timeout passed.
    over: bool,
    /// Per partition, the peers whose positions the partition weighed as
    /// their primary since the node started.
    weighed: BTreeMap<IggyNamespace, BTreeSet<String>>,
    /// The partitions whose wait ended: every peer weighed and the
    /// adoptions committed.
    resumed: BTreeSet<IggyNamespace>,
}

#[derive(Default)]
struct Publisher {
    rounds: u64,
    round: Option<Round>,
    /// When the next periodic round may start, in microseconds since the Unix
    /// epoch.
    next_round_at: u64,
    /// Callers that wait for a round that starts after they asked.
    waiting: Vec<oneshot::Sender<RoundSummary>>,
    /// What the node's store holds of each log, as this node stored it.
    stored: BTreeMap<PositionsKey, LogPositions>,
    /// The logs this node may have an object of, until it deletes it: those
    /// it stored and those a round found held, whose object an earlier run
    /// may have stored.
    seen: BTreeSet<GeoLog>,
    /// Cancels the store task in flight when dropped.
    scope: Option<TierScope>,
}

struct Round {
    id: u64,
    now: IggyTimestamp,
    reports: Vec<Option<GeoPositionsReport>>,
    waiters: Vec<oneshot::Sender<RoundSummary>>,
    /// What the shards reported, once all did and the store task runs.
    found: Option<RoundSummary>,
}

/// Every log that a topic of this cluster holds, with its partitions.
///
/// A detached shadow topic takes no new data, so it is left out, and so is a
/// geo topic whose partitions came at different revisions, which a geo topic
/// never has.
#[must_use]
pub fn local_logs(inner: &StreamsInner, origin: &str) -> Vec<LocalLog> {
    let mut logs = Vec::new();
    for (stream_id, stream) in &inner.items {
        for (topic_id, topic) in &stream.topics {
            let log = match (&stream.shadow, &topic.shadow) {
                (Some(source), Some(shadow)) if !shadow.is_detached() => GeoLog {
                    origin: source.origin.clone(),
                    stream_id: source.stream_id,
                    topic_id: topic_id as u64,
                    created_revision: shadow.created_revision,
                },
                (None, None) if topic.is_geo() && !origin.is_empty() => {
                    let Some(first) = topic.partitions.first() else {
                        continue;
                    };
                    if topic
                        .partitions
                        .iter()
                        .any(|partition| partition.created_revision != first.created_revision)
                    {
                        continue;
                    }
                    GeoLog {
                        origin: origin.to_owned(),
                        stream_id: stream_id as u64,
                        topic_id: topic_id as u64,
                        created_revision: first.created_revision,
                    }
                }
                _ => continue,
            };
            let partitions = topic
                .partitions
                .iter()
                .filter_map(|partition| {
                    Some((
                        u32::try_from(partition.id).ok()?,
                        IggyNamespace::try_new(stream_id, topic_id, partition.id)?,
                        partition.created_revision,
                    ))
                })
                .collect();
            logs.push(LocalLog {
                log,
                stream_id,
                topic_id,
                partitions,
            });
        }
    }
    logs
}

/// The consumer groups of `topic` whose ids fit a partition's offset key.
#[must_use]
pub fn topic_groups(topic: &Topic) -> Vec<LocalGroup> {
    topic
        .consumer_groups
        .iter()
        .filter_map(|(group_id, group)| {
            Some(LocalGroup {
                group_id: u32::try_from(*group_id).ok()?,
                name: group.name.to_string(),
                consuming: !group.members.is_empty(),
            })
        })
        .collect()
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard what it needs to carry its groups' positions, or
    /// `None` on a node without an origin. Starts the wait of group polls
    /// for the peers' positions.
    pub fn set_geo_positions(&self, settings: Option<GeoPositionSettings>) {
        let state = &self.geo_positions;
        if let Some(settings) = &settings {
            let timeout = u64::try_from(settings.resume_timeout.as_micros()).unwrap_or(u64::MAX);
            state
                .resume_by
                .set(self.bus.monotonic_micros().saturating_add(timeout));
            let mut gate = state.gate.borrow_mut();
            gate.over = settings.peers.is_empty();
            self.metrics.set_positions_resume_waiting(!gate.over);
        }
        *state.settings.borrow_mut() = settings.map(Rc::new);
    }

    fn geo_settings(&self) -> Option<Rc<GeoPositionSettings>> {
        self.geo_positions.settings.borrow().clone()
    }

    /// Whether the group polls of the partition of `namespace` still wait
    /// for the peers' positions: until it weighed those of every peer as
    /// their primary and the adoptions they gave committed, or until the
    /// timeout after the node started.
    #[must_use]
    pub fn geo_positions_resume_waiting(&self, namespace: IggyNamespace) -> bool {
        let Some(settings) = self.geo_settings() else {
            return false;
        };
        let mut gate = self.geo_positions.gate.borrow_mut();
        if gate.over || gate.resumed.contains(&namespace) {
            return false;
        }
        if self.bus.monotonic_micros() >= self.geo_positions.resume_by.get() {
            gate.over = true;
            gate.weighed.clear();
            gate.resumed.clear();
            self.metrics.set_positions_resume_waiting(false);
            tracing::info!(
                shard = self.id,
                "the wait of group polls for the geo peers' positions timed out"
            );
            return false;
        }
        let weighed_all = gate
            .weighed
            .get(&namespace)
            .is_some_and(|weighed| settings.peers.iter().all(|peer| weighed.contains(peer)));
        if weighed_all && !self.geo_positions.in_flight.borrow().contains(&namespace) {
            gate.weighed.remove(&namespace);
            gate.resumed.insert(namespace);
            return false;
        }
        true
    }

    /// Why a poll of `consumer` on the partition of `namespace` must wait,
    /// if it must: it is a group poll of a geo or shadow partition while the
    /// node waits for its peers' positions, or while a position that a peer
    /// published for the group's name is not bound to the group yet. The
    /// latter asks the pump for the bind.
    pub(crate) fn geo_position_refusal(
        &self,
        namespace: IggyNamespace,
        consumer: PollingConsumer,
    ) -> Option<IggyError>
    where
        M: StreamsFrontend,
    {
        let PollingConsumer::ConsumerGroup(group_id, _) = consumer else {
            return None;
        };
        self.geo_settings()?;
        let group_id = u32::try_from(group_id).ok()?;
        let (geo, unbound) = self
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let geo = partition.places_batches() || partition.peer_log().is_some();
                let unbound = partition
                    .geo_positions()
                    .filter(|positions| positions.bound_to(group_id).is_none())
                    .and_then(|positions| {
                        let name = self.group_name(namespace, group_id)?;
                        positions.get(&name).map(|_| ())
                    })
                    .is_some();
                (geo, unbound)
            })?;
        if !geo {
            return None;
        }
        if self.geo_positions_resume_waiting(namespace) {
            return Some(IggyError::TransientNotAccepted);
        }
        if unbound {
            if let Some(sender) = self.senders.get(self.id as usize) {
                let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::NoteGeoPositions {
                    namespace,
                }));
            }
            return Some(IggyError::TransientNotAccepted);
        }
        None
    }

    fn group_name(&self, namespace: IggyNamespace, group_id: u32) -> Option<String>
    where
        M: StreamsFrontend,
    {
        self.plane.metadata().mux_stm.streams().read(|inner| {
            inner
                .items
                .get(namespace.stream_id())?
                .topics
                .get(namespace.topic_id())?
                .consumer_groups
                .get(&u64::from(group_id))
                .map(|group| group.name.to_string())
        })
    }

    /// The pump's tick: start a round on shard 0 when one is due.
    pub(crate) fn tick_geo_positions(&self) {
        if self.id == 0 && self.geo_positions.settings.borrow().is_some() {
            self.start_geo_positions_round(IggyTimestamp::from(self.bus.realtime_micros()));
        }
    }

    /// Start a round on shard 0 when the publish interval passed since the
    /// last one and none runs. A round that waited too long for the shards
    /// is dropped, and its waiters see their channel close.
    pub fn start_geo_positions_round(&self, now: IggyTimestamp) {
        if self.id != 0 {
            return;
        }
        let Some(settings) = self.geo_settings() else {
            return;
        };
        let mut publisher = self.geo_positions.publisher.borrow_mut();
        if let Some(round) = &publisher.round {
            let timeout = u64::try_from(ROUND_TIMEOUT.as_micros()).unwrap_or(u64::MAX);
            if now.as_micros() < round.now.as_micros().saturating_add(timeout) {
                return;
            }
            publisher.round = None;
            publisher.scope = None;
        }
        if now.as_micros() < publisher.next_round_at && publisher.waiting.is_empty() {
            return;
        }
        let interval = u64::try_from(settings.publish_interval.as_micros()).unwrap_or(u64::MAX);
        publisher.next_round_at = now.as_micros().saturating_add(interval);
        publisher.rounds += 1;
        let id = publisher.rounds;
        let waiters = std::mem::take(&mut publisher.waiting);
        publisher.round = Some(Round {
            id,
            now,
            reports: vec![None; self.senders.len()],
            waiters,
            found: None,
        });
        drop(publisher);
        for sender in &self.senders {
            if sender
                .try_send(ShardFrame::lifecycle(LifecycleFrame::ReportGeoPositions {
                    round: id,
                }))
                .is_err()
            {
                // A shard that was not asked never answers.
                self.geo_positions.publisher.borrow_mut().round = None;
                return;
            }
        }
    }

    /// Wait for a round that starts after this call, on shard 0, and for the
    /// store's answer to what it found. Starts the round at once when none
    /// runs. `None` on another shard or one without settings.
    pub fn geo_positions_round_now(
        &self,
        now: IggyTimestamp,
    ) -> Option<oneshot::Receiver<RoundSummary>> {
        if self.id != 0 || self.geo_settings().is_none() {
            return None;
        }
        let (sender, receiver) = oneshot::channel();
        self.geo_positions
            .publisher
            .borrow_mut()
            .waiting
            .push(sender);
        self.start_geo_positions_round(now);
        Some(receiver)
    }

    /// Stop the rounds of shard 0: drop the one in flight, its store task
    /// and the callers that wait for a round.
    pub fn stop_geo_positions(&self) {
        let mut publisher = self.geo_positions.publisher.borrow_mut();
        publisher.round = None;
        publisher.scope = None;
        publisher.waiting.clear();
    }

    /// Hand shard `shard` the positions that the geo peer publishes for its
    /// partitions. A refused frame is picked up by the next listing.
    pub fn send_peer_positions(&self, shard: u16, positions: PeerPositions) {
        let Some(sender) = self.senders.get(usize::from(shard)) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PeerPositionsListed(
            Box::new(positions),
        )));
    }

    /// Answer a round of shard 0 with the positions of the partitions here.
    /// When a primary here has commits to fold, the shard proposes the folds
    /// first and answers once they ended, so the round stores what the
    /// groups committed before it started.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_report_geo_positions(&self, round: u64)
    where
        B: Clone,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let Some(settings) = self.geo_settings() else {
            return;
        };
        let (led, not_led, proposals) = self.collect_geo_positions(&settings);
        if proposals.is_empty() {
            self.send_geo_positions_report(round, led, not_led);
            return;
        }
        let folded = ShardFrame::lifecycle(LifecycleFrame::ReportGeoPositionsFolded { round });
        self.propose_geo_commands(proposals, Some(folded)).await;
    }

    /// Answer a round once the folds this shard proposed for it ended.
    pub(crate) fn on_report_geo_positions_folded(&self, round: u64)
    where
        M: RestorableMetadataStm,
    {
        let Some(settings) = self.geo_settings() else {
            return;
        };
        let (led, not_led, _) = self.collect_geo_positions(&settings);
        self.send_geo_positions_report(round, led, not_led);
    }

    fn send_geo_positions_report(
        &self,
        round: u64,
        partitions: Vec<PartitionPositions>,
        not_led: Vec<(GeoLog, u32)>,
    ) {
        if let Some(sender) = self.senders.first() {
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::GeoPositionsReported(
                Box::new(GeoPositionsReport {
                    round,
                    shard: self.id,
                    partitions,
                    not_led,
                }),
            )));
        }
    }

    /// The positions of the geo and shadow partitions that this shard leads
    /// as a current primary, the other geo and shadow partitions it holds,
    /// and the folds that its primaries owe.
    fn collect_geo_positions(
        &self,
        settings: &GeoPositionSettings,
    ) -> (
        Vec<PartitionPositions>,
        Vec<(GeoLog, u32)>,
        Vec<GeoProposal>,
    )
    where
        M: RestorableMetadataStm,
    {
        let (logs, groups) = self.plane.metadata().mux_stm.streams().read(|inner| {
            let logs = local_logs(inner, &settings.origin);
            let groups: BTreeMap<(usize, usize), Vec<LocalGroup>> = logs
                .iter()
                .filter_map(|local| {
                    let topic = inner
                        .items
                        .get(local.stream_id)?
                        .topics
                        .get(local.topic_id)?;
                    Some(((local.stream_id, local.topic_id), topic_groups(topic)))
                })
                .collect();
            (logs, groups)
        });
        let mut led = Vec::new();
        let mut not_led = Vec::new();
        let mut proposals = Vec::new();
        let partitions = self.plane.partitions();
        let in_flight = self.geo_positions.in_flight.borrow();
        for local in &logs {
            let local_groups = groups
                .get(&(local.stream_id, local.topic_id))
                .map_or(&[][..], Vec::as_slice);
            for (partition_id, namespace, created_revision) in &local.partitions {
                let Some(partition) = partitions
                    .get_by_ns(namespace)
                    .filter(|partition| partition.created_revision() == *created_revision)
                else {
                    continue;
                };
                let Some(positions) = partition.geo_positions() else {
                    continue;
                };
                if !partition.state_machines_current() {
                    not_led.push((local.log.clone(), *partition_id));
                    continue;
                }
                if !in_flight.contains(namespace) {
                    let notes = partition.geo_notes(local_groups);
                    if !notes.is_empty() {
                        proposals.push((*namespace, NOTE_GEO_POSITIONS, encode_notes(&notes)));
                    }
                }
                let unfolded = local_groups
                    .iter()
                    .filter(|group| !partition.geo_positions_folded(std::slice::from_ref(group)))
                    .map(|group| group.name.clone())
                    .collect();
                led.push(PartitionPositions {
                    log: local.log.clone(),
                    partition_id: *partition_id,
                    positions: positions
                        .entries()
                        .iter()
                        .map(|(name, entry)| (name.clone(), entry.position))
                        .collect(),
                    unfolded,
                });
            }
        }
        (led, not_led, proposals)
    }

    /// Fold what the groups of the partition of `namespace` committed, now:
    /// a group poll waits for its bind.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_note_geo_positions(&self, namespace: IggyNamespace)
    where
        B: Clone,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        if self.geo_settings().is_none()
            || self.geo_positions.in_flight.borrow().contains(&namespace)
        {
            return;
        }
        let Some(groups) = self.plane.metadata().mux_stm.streams().read(|inner| {
            inner
                .items
                .get(namespace.stream_id())?
                .topics
                .get(namespace.topic_id())
                .map(topic_groups)
        }) else {
            return;
        };
        let notes = {
            let partitions = self.plane.partitions();
            let Some(partition) = partitions
                .get_by_ns(&namespace)
                .filter(|partition| partition.state_machines_current())
            else {
                return;
            };
            partition.geo_notes(&groups)
        };
        if !notes.is_empty() {
            self.propose_geo_commands(
                vec![(namespace, NOTE_GEO_POSITIONS, encode_notes(&notes))],
                None,
            )
            .await;
        }
    }

    /// Commit the positions of a geo peer that win over the partitions'
    /// own, on the partitions whose primary is here. Each of those weighed
    /// the peer's positions for the resume wait, including one the peer
    /// published nothing for.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_peer_positions_listed(&self, found: PeerPositions)
    where
        B: Clone,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let Some(settings) = self.geo_settings() else {
            return;
        };
        let handed: BTreeMap<IggyNamespace, (u64, BTreeMap<String, GeoPosition>)> = found
            .partitions
            .into_iter()
            .map(|(namespace, created_revision, positions)| {
                (namespace, (created_revision, positions))
            })
            .collect();
        let (logs, groups) = self.plane.metadata().mux_stm.streams().read(|inner| {
            let logs = local_logs(inner, &settings.origin);
            let groups: BTreeMap<(usize, usize), Vec<LocalGroup>> = logs
                .iter()
                .filter_map(|local| {
                    let topic = inner
                        .items
                        .get(local.stream_id)?
                        .topics
                        .get(local.topic_id)?;
                    Some(((local.stream_id, local.topic_id), topic_groups(topic)))
                })
                .collect();
            (logs, groups)
        });
        let none = BTreeMap::new();
        let mut proposals = Vec::new();
        let mut weighed = Vec::new();
        let mut current = Vec::new();
        let mut adopted = 0;
        {
            let partitions = self.plane.partitions();
            for local in &logs {
                let local_groups = groups
                    .get(&(local.stream_id, local.topic_id))
                    .map_or(&[][..], Vec::as_slice);
                for (_, namespace, created_revision) in &local.partitions {
                    let Some(partition) = partitions
                        .get_by_ns(namespace)
                        .filter(|partition| partition.created_revision() == *created_revision)
                        .filter(|partition| partition.state_machines_current())
                    else {
                        continue;
                    };
                    current.push(*namespace);
                    if self.geo_positions.in_flight.borrow().contains(namespace) {
                        continue;
                    }
                    let peer = handed
                        .get(namespace)
                        .filter(|(revision, _)| revision == created_revision)
                        .map_or(&none, |(_, positions)| positions);
                    let waiting = self.geo_positions_resume_waiting(*namespace);
                    let adoptions = partition.geo_adoptions(local_groups, peer, waiting);
                    if !adoptions.is_empty() {
                        adopted += adoptions.len();
                        proposals.push((
                            *namespace,
                            ADOPT_GEO_POSITIONS,
                            encode_adoptions(&adoptions),
                        ));
                    }
                    if waiting {
                        weighed.push(*namespace);
                    }
                }
            }
        }
        if adopted > 0 {
            self.metrics
                .record_peer_positions_adopted(&found.origin, adopted);
        }
        {
            let mut gate = self.geo_positions.gate.borrow_mut();
            for namespace in weighed {
                gate.weighed
                    .entry(namespace)
                    .or_default()
                    .insert(found.origin.clone());
            }
        }
        self.propose_geo_commands(proposals, None).await;
        let waiting = current
            .into_iter()
            .any(|namespace| self.geo_positions_resume_waiting(namespace));
        self.metrics.set_positions_resume_waiting(waiting);
    }

    /// Propose the commands of each partition, one partition at a time, and
    /// keep each partition from another proposal, and its group polls from
    /// the end of their resume wait, until its replies come or time out.
    ///
    /// `then` goes to this shard's pump once every partition's replies came
    /// or timed out, at once when there was nothing to propose.
    #[allow(clippy::future_not_send)]
    async fn propose_geo_commands(&self, proposals: Vec<GeoProposal>, then: Option<ShardFrame>)
    where
        B: Clone,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let then = Rc::new(RefCell::new(then));
        // One for this loop, released after it.
        let remaining = Rc::new(Cell::new(1_usize));
        let own = self.senders.get(self.id as usize).cloned();
        for (namespace, kind, payloads) in proposals {
            let requests = {
                let partitions = self.plane.partitions();
                let Some(partition) = partitions.get_by_ns(&namespace) else {
                    continue;
                };
                let first = self.geo_positions.requests.get().wrapping_add(1);
                match partition.geo_position_requests(kind, &payloads, first) {
                    Ok(requests) => {
                        self.geo_positions
                            .requests
                            .set(first.wrapping_add(requests.len() as u64));
                        requests
                    }
                    Err(error) => {
                        tracing::warn!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            %error,
                            "cannot encode the consumer group positions of a partition"
                        );
                        continue;
                    }
                }
            };
            if requests.is_empty() {
                continue;
            }
            self.geo_positions.in_flight.borrow_mut().insert(namespace);
            let mut replies = Vec::with_capacity(requests.len());
            for request in requests {
                let (reply, receiver) = channel(1);
                self.dispatch_partition_submit(request, reply, None, None)
                    .await;
                replies.push(receiver);
            }
            let in_flight = Rc::clone(&self.geo_positions.in_flight);
            let bus = self.bus.clone();
            remaining.set(remaining.get() + 1);
            let (remaining, then, own) = (Rc::clone(&remaining), Rc::clone(&then), own.clone());
            // Through the bus, not the runtime directly: the simulator
            // supplies its own executor.
            self.bus.spawn(async move {
                for receiver in replies {
                    let _ = bus_timeout(&bus, COMMAND_REPLY_TIMEOUT, receiver.recv()).await;
                }
                in_flight.borrow_mut().remove(&namespace);
                release(&remaining, &then, own.as_ref());
            });
        }
        release(&remaining, &then, own.as_ref());
    }

    /// Take a shard's answer to a round. With the last one, store the
    /// objects whose positions changed.
    pub(crate) fn on_geo_positions_reported(&self, report: GeoPositionsReport)
    where
        M: RestorableMetadataStm,
    {
        let mut publisher = self.geo_positions.publisher.borrow_mut();
        let Some(round) = publisher
            .round
            .as_mut()
            .filter(|round| round.id == report.round && round.found.is_none())
        else {
            return;
        };
        let Some(answer) = round.reports.get_mut(usize::from(report.shard)) else {
            return;
        };
        *answer = Some(report);
        if round.reports.iter().any(Option::is_none) {
            return;
        }
        let mut summary = RoundSummary::default();
        for report in round.reports.iter_mut().filter_map(Option::take) {
            for partition in report.partitions {
                summary
                    .logs
                    .entry(partition.log.clone())
                    .or_default()
                    .partitions
                    .insert(partition.partition_id, partition);
            }
            for (log, partition_id) in report.not_led {
                summary
                    .logs
                    .entry(log)
                    .or_default()
                    .not_led
                    .insert(partition_id);
            }
        }
        let round_id = round.id;
        let now = round.now;
        let replica = self.partition_consensus.self_replica_id;
        let held: BTreeSet<GeoLog> = self.geo_settings().map_or_else(BTreeSet::new, |settings| {
            self.plane.metadata().mux_stm.streams().read(|inner| {
                local_logs(inner, &settings.origin)
                    .into_iter()
                    .map(|local| local.log)
                    .collect()
            })
        });
        publisher.seen.extend(held.iter().cloned());
        let (puts, deletes) =
            store_plan(&publisher.stored, &publisher.seen, &summary, replica, &held);
        let Some(round) = publisher.round.as_mut() else {
            return;
        };
        round.found = Some(summary);
        let objects = self.topic_objects();
        let sender = self.senders.first().cloned();
        if objects.is_none() || sender.is_none() || (puts.is_empty() && deletes.is_empty()) {
            drop(publisher);
            self.on_positions_stored(FinishedPositionsStore {
                round: round_id,
                stored: Vec::new(),
                failed: Vec::new(),
                deleted: Vec::new(),
            });
            return;
        }
        let (Some(objects), Some(sender)) = (objects, sender) else {
            return;
        };
        let scope = TierScope::new();
        let token = scope.token();
        publisher.scope = Some(scope);
        drop(publisher);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(store_positions(
            objects,
            sender,
            (round_id, now),
            puts,
            deletes,
            token,
        ));
    }

    /// Take the store's answer to a round, and answer its waiters.
    pub(crate) fn on_positions_stored(&self, finished: FinishedPositionsStore) {
        let mut publisher = self.geo_positions.publisher.borrow_mut();
        if publisher
            .round
            .as_ref()
            .is_none_or(|round| round.id != finished.round)
        {
            return;
        }
        let Some(round) = publisher.round.take() else {
            return;
        };
        publisher.scope = None;
        for (key, positions) in finished.stored {
            self.metrics.record_positions_stored(true);
            publisher.seen.insert(key.log.clone());
            publisher.stored.insert(key, positions);
        }
        for _ in &finished.failed {
            self.metrics.record_positions_stored(false);
        }
        for key in finished.deleted {
            publisher.seen.remove(&key.log);
            publisher.stored.remove(&key);
        }
        let replica = self.partition_consensus.self_replica_id;
        let mut summary = round.found.unwrap_or_default();
        for (log, found) in &mut summary.logs {
            let key = PositionsKey {
                log: log.clone(),
                replica,
            };
            found.stored = holds(publisher.stored.get(&key), found);
        }
        let rerun = !publisher.waiting.is_empty();
        drop(publisher);
        for waiter in round.waiters {
            let _ = waiter.send(summary.clone());
        }
        if rerun {
            self.start_geo_positions_round(IggyTimestamp::from(self.bus.realtime_micros()));
        }
    }
}

/// The commands of one partition: its namespace, their kind and payloads.
type GeoProposal = (
    IggyNamespace,
    partitions::state_machine::CommandKind,
    Vec<Vec<u8>>,
);

/// Count one proposal of a batch as ended, and send the batch's frame to the
/// pump with the last one.
fn release(
    remaining: &Cell<usize>,
    then: &RefCell<Option<ShardFrame>>,
    own: Option<&TaggedSender>,
) {
    remaining.set(remaining.get().saturating_sub(1));
    if remaining.get() == 0
        && let (Some(frame), Some(sender)) = (then.borrow_mut().take(), own)
    {
        let _ = sender.try_send(frame);
    }
}

/// What a round stores: the object of each log in which a partition this
/// node leads holds other positions than the node stored last, and the
/// delete of the node's object of each log in `seen` the cluster no longer
/// holds.
///
/// An object keeps the partitions the node stopped leading as it stored
/// them: positions only rise, so the new primary's win at the peers.
fn store_plan(
    stored: &BTreeMap<PositionsKey, LogPositions>,
    seen: &BTreeSet<GeoLog>,
    summary: &RoundSummary,
    replica: u8,
    held: &BTreeSet<GeoLog>,
) -> (Vec<(PositionsKey, LogPositions)>, Vec<PositionsKey>) {
    let mut puts = Vec::new();
    for (log, found) in &summary.logs {
        let key = PositionsKey {
            log: log.clone(),
            replica,
        };
        let last = stored.get(&key);
        if holds(last, found) {
            continue;
        }
        let mut positions = last.cloned().unwrap_or_default();
        positions.extend(
            found
                .partitions
                .iter()
                .map(|(partition_id, partition)| (*partition_id, partition.positions.clone())),
        );
        puts.push((key, positions));
    }
    let deletes = seen
        .iter()
        .filter(|log| !held.contains(*log))
        .map(|log| PositionsKey {
            log: log.clone(),
            replica,
        })
        .collect();
    (puts, deletes)
}

/// Whether `stored`, what the node's object of a log holds, holds what
/// `found` found of the partitions the node leads.
fn holds(stored: Option<&LogPositions>, found: &LogRound) -> bool {
    found.partitions.iter().all(|(partition_id, partition)| {
        stored.and_then(|stored| stored.get(partition_id)) == Some(&partition.positions)
    })
}

/// Store the objects of a round whose positions changed and delete those of
/// logs this cluster no longer holds, then hand the outcome to shard 0.
#[allow(
    clippy::future_not_send,
    reason = "the task runs on shard 0, which owns the store"
)]
async fn store_positions(
    objects: Rc<dyn TopicObjects>,
    sender: TaggedSender,
    (round, now): (u64, IggyTimestamp),
    puts: Vec<(PositionsKey, LogPositions)>,
    deletes: Vec<PositionsKey>,
    token: TierScopeToken,
) {
    let mut finished = FinishedPositionsStore {
        round,
        stored: Vec::new(),
        failed: Vec::new(),
        deleted: Vec::new(),
    };
    for (key, positions) in puts {
        let object = PositionsObject {
            key: key.clone(),
            published_at: now.as_micros(),
            partitions: positions,
        };
        let body = match object.encode() {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(
                    origin = key.log.origin,
                    stream_id = key.log.stream_id,
                    topic_id = key.log.topic_id,
                    %error,
                    "cannot encode the consumer group positions of a log"
                );
                finished.failed.push(key);
                continue;
            }
        };
        match objects
            .put_positions(key.clone(), body, token.clone())
            .await
        {
            Ok(Ok(())) => finished.stored.push((key, object.partitions)),
            _ => finished.failed.push(key),
        }
    }
    for key in deletes {
        if matches!(
            objects.delete_positions(key.clone(), token.clone()).await,
            Ok(Ok(()))
        ) {
            finished.deleted.push(key);
        }
    }
    let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::GeoPositionsStored(
        Box::new(finished),
    )));
}

#[cfg(test)]
mod tests {
    use iggy_object_store::PositionValue;

    use super::*;

    fn log(topic_id: u64) -> GeoLog {
        GeoLog {
            origin: "dc-a".to_owned(),
            stream_id: 1,
            topic_id,
            created_revision: 9,
        }
    }

    fn billing_at(offset: u64) -> BTreeMap<String, GeoPosition> {
        BTreeMap::from([(
            "billing".to_owned(),
            GeoPosition {
                reset: 0,
                value: PositionValue::Offset(offset),
            },
        )])
    }

    /// A log of which the node leads the partitions of `led`, each with the
    /// group at an offset, and holds those of `not_led`.
    fn found_led(topic_id: u64, led: &[(u32, u64)], not_led: &[u32]) -> (GeoLog, LogRound) {
        let partitions = led
            .iter()
            .map(|(partition_id, offset)| {
                (
                    *partition_id,
                    PartitionPositions {
                        log: log(topic_id),
                        partition_id: *partition_id,
                        positions: billing_at(*offset),
                        unfolded: BTreeSet::new(),
                    },
                )
            })
            .collect();
        (
            log(topic_id),
            LogRound {
                partitions,
                not_led: not_led.iter().copied().collect(),
                stored: false,
            },
        )
    }

    fn found(topic_id: u64, offset: u64) -> (GeoLog, LogRound) {
        found_led(topic_id, &[(0, offset)], &[])
    }

    fn key(topic_id: u64) -> PositionsKey {
        PositionsKey {
            log: log(topic_id),
            replica: 0,
        }
    }

    fn plan(
        stored: &BTreeMap<PositionsKey, LogPositions>,
        summary: &RoundSummary,
        held: &BTreeSet<GeoLog>,
    ) -> (Vec<(PositionsKey, LogPositions)>, Vec<PositionsKey>) {
        let seen = stored.keys().map(|key| key.log.clone()).collect();
        store_plan(stored, &seen, summary, 0, held)
    }

    fn stored_of(summary: &RoundSummary) -> BTreeMap<PositionsKey, LogPositions> {
        let (puts, _) = plan(&BTreeMap::new(), summary, &BTreeSet::new());
        puts.into_iter().collect()
    }

    #[test]
    fn given_unchanged_positions_when_planning_a_round_should_store_nothing() {
        let summary = RoundSummary {
            logs: BTreeMap::from([found(1, 10), found(2, 20)]),
        };
        let stored = stored_of(&summary);
        let held = BTreeSet::from([log(1), log(2)]);

        let (puts, deletes) = plan(&stored, &summary, &held);

        assert_eq!(puts, []);
        assert_eq!(deletes, []);
    }

    #[test]
    fn given_one_changed_log_when_planning_a_round_should_store_only_that_one() {
        let stored = stored_of(&RoundSummary {
            logs: BTreeMap::from([found(1, 10), found(2, 20)]),
        });
        let summary = RoundSummary {
            logs: BTreeMap::from([found(1, 10), found(2, 21)]),
        };
        let held = BTreeSet::from([log(1), log(2)]);

        let (puts, deletes) = plan(&stored, &summary, &held);

        assert_eq!(
            puts.iter()
                .map(|(key, _)| key.log.topic_id)
                .collect::<Vec<_>>(),
            [2]
        );
        assert_eq!(deletes, []);
    }

    #[test]
    fn given_a_log_this_cluster_no_longer_holds_when_planning_a_round_should_delete_its_object() {
        let stored = stored_of(&RoundSummary {
            logs: BTreeMap::from([found(1, 10), found(2, 20)]),
        });
        let summary = RoundSummary {
            logs: BTreeMap::from([found(1, 10)]),
        };

        let (puts, deletes) = plan(&stored, &summary, &BTreeSet::from([log(1)]));

        assert_eq!(puts, []);
        assert_eq!(deletes, [key(2)]);
        let (_, kept) = plan(&stored, &summary, &BTreeSet::from([log(1), log(2)]));
        assert_eq!(kept, [], "a log that still exists keeps its object");
    }

    #[test]
    fn given_a_log_the_node_held_without_storing_when_the_cluster_drops_it_should_delete_the_nodes_object()
     {
        let seen = BTreeSet::from([log(1), log(2)]);
        let summary = RoundSummary {
            logs: BTreeMap::from([found_led(1, &[], &[0])]),
        };

        let (puts, deletes) = store_plan(
            &BTreeMap::new(),
            &seen,
            &summary,
            0,
            &BTreeSet::from([log(1)]),
        );

        assert_eq!(
            puts,
            [],
            "a node that leads nothing of a log stores nothing"
        );
        assert_eq!(
            deletes,
            [key(2)],
            "an object of an earlier run goes with its log"
        );
    }

    #[test]
    fn given_partitions_the_node_only_follows_when_planning_a_round_should_store_nothing() {
        let summary = RoundSummary {
            logs: BTreeMap::from([found_led(1, &[], &[0, 1])]),
        };
        let held = BTreeSet::from([log(1)]);

        let (fresh, _) = plan(&BTreeMap::new(), &summary, &held);
        let earlier = stored_of(&RoundSummary {
            logs: BTreeMap::from([found_led(1, &[(0, 10), (1, 20)], &[])]),
        });
        let (left, _) = plan(&earlier, &summary, &held);

        assert_eq!(fresh, [], "no object for a log the node never led");
        assert_eq!(left, [], "the object of a former leader stays as it was");
    }

    #[test]
    fn given_a_partition_the_node_stopped_leading_when_another_one_changes_should_keep_it_in_the_object()
     {
        let stored = stored_of(&RoundSummary {
            logs: BTreeMap::from([found_led(1, &[(0, 10), (1, 20)], &[])]),
        });
        let summary = RoundSummary {
            logs: BTreeMap::from([found_led(1, &[(1, 21)], &[0])]),
        };

        let (puts, _) = plan(&stored, &summary, &BTreeSet::from([log(1)]));

        assert_eq!(
            puts,
            [(
                key(1),
                BTreeMap::from([(0, billing_at(10)), (1, billing_at(21))])
            )]
        );
    }

    #[test]
    fn given_a_partition_the_node_starts_leading_with_unchanged_positions_when_planning_a_round_should_store_it()
     {
        let stored = stored_of(&RoundSummary {
            logs: BTreeMap::from([found_led(1, &[(1, 20)], &[0])]),
        });
        let summary = RoundSummary {
            logs: BTreeMap::from([found_led(1, &[(0, 10), (1, 20)], &[])]),
        };
        let held = BTreeSet::from([log(1)]);

        let (puts, _) = plan(&stored, &summary, &held);
        let (first, _) = plan(&BTreeMap::new(), &summary, &held);

        assert_eq!(
            puts,
            [(
                key(1),
                BTreeMap::from([(0, billing_at(10)), (1, billing_at(20))])
            )]
        );
        assert_eq!(first.len(), 1, "a node's first round stores what it leads");
    }
}
