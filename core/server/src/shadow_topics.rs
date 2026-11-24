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

//! The shadow streams of this cluster's geo peers, which the metadata
//! primary keeps in step with what each peer publishes in its store.
//!
//! A peer stores an object for each of its geo topics, see [`TopicObject`].
//! Every few seconds the metadata primary lists the topic objects of each
//! peer that it reads, and reads the new ones and, now and then, the known
//! ones again. Then it proposes one `ShadowTopic` operation at a time until
//! committed metadata matches what it read:
//!
//! - A live object whose peer stream has no shadow stream yet creates the
//!   stream `__shadow_{origin}_{stream}`. A stream that a user already named
//!   so is reported and left alone.
//! - A live object adopts its topic into the shadow stream, with the peer's
//!   topic id and partition count. A topic id or name that an older
//!   incarnation still holds waits for that one to go.
//! - A tombstone detaches the shadow topic. The topic is removed once
//!   `[geo] tombstone_grace` passed since the peer wrote the tombstone, or
//!   when the peer's store no longer lists the object for
//!   [`ABSENCE_CONFIRM`].
//! - A shadow stream without topics whose peer stream has no live object is
//!   removed, and one whose peer stream was renamed is renamed too.
//!
//! Only the caught-up metadata primary proposes, so every replica learns the
//! shadow streams through consensus. A deposed primary that has not learned
//! of the new view yet can still propose, and each operation checks that its
//! target is still what the primary saw.
//!
//! Whether a peer's store answers is this node's view alone. While the
//! listing of a peer fails, or the node reads nothing from the peer, the
//! shadow topics of the peer are stale: they serve what they hold, and the
//! pass removes none of them but those whose grace ended.
//!
//! A peer whose origin retired is read no more. Each of its shadow topics is
//! removed once the topic's retention in the peer passed since the
//! retirement, or once the grace of its tombstone ended, and each of its
//! shadow streams once it holds no topic. A topic whose messages never
//! expire stays until a client deletes it.
//!
//! A node whose own store names another cluster creates no shadow stream:
//! that cluster may be one that lost every disk, whose streams a recovery
//! restores under their ids, and a shadow stream created first could take
//! one of them.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, CreateTopicWithAssignmentsRequest,
};
use iggy_binary_protocol::{WireIdentifier, WireName, WireOptions};
use iggy_common::{CompressionAlgorithm, GeoMode, IggyTimestamp, TopicCreateOptions};
use iggy_object_store::{
    ObjectPrefix, TierJobCancelled, TierScope, TierScopeToken, TopicCompression, TopicGeoMode,
    TopicKey, TopicObject, TopicRead, TopicState,
};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::shadow::{
    RetiredOrigin, ShadowRetention, ShadowStream, ShadowTopicRequest, ShadowTopicTarget,
    shadow_stream_name,
};
use metadata::stm::stream::{Stream, StreamsInner, Topic};
use partitions::{ExecutorTopicObjects, TopicObjects};
use server_common::sharding::IggyNamespace;
use tracing::{debug, info, warn};

use crate::shell::ServerShard;

/// The shortest time between two passes, each of which lists the topic
/// objects of every peer.
pub const PASS_INTERVAL: Duration = Duration::from_secs(5);

/// How long the pass trusts what it read of a topic object before it reads
/// the object again, to learn of a tombstone or a renamed stream.
pub const REREAD_INTERVAL: Duration = Duration::from_secs(30);

/// How long a topic object that every listing lacks stays missing before
/// the pass removes its shadow topic. A root that a failed mount leaves empty
/// lists no object, and the identity checks of the peer find its identity
/// object gone well within this time, which stops the pass from reading the
/// peer.
pub const ABSENCE_CONFIRM: Duration = Duration::from_mins(2);

/// The most operations one pass proposes. The next pass goes on.
const MAX_PROPOSALS_PER_PASS: usize = 64;

/// What the pass reads of one peer.
pub struct PeerTopics {
    /// The peer's `geo.origin`.
    pub origin: String,
    /// The cluster that owns the peer's store while the node reads it.
    pub cluster_uid: Option<u128>,
    /// The jobs on the peer's topic objects. `None` while the node reads
    /// nothing from the peer.
    pub objects: Option<Rc<dyn TopicObjects>>,
}

/// The peers of a node, read again on every pass.
type PeerSource = Box<dyn Fn(&ServerShard) -> Vec<PeerTopics>>;

/// The shadow streams that the metadata primary keeps in step with its geo
/// peers.
pub struct ShadowTopics {
    settings: Settings,
    peers: PeerSource,
    /// Shared with the task of the pass.
    state: Rc<RefCell<State>>,
}

#[derive(Clone, Copy)]
struct Settings {
    /// In microseconds.
    tombstone_grace: u64,
    /// The node reads at least one peer.
    has_peers: bool,
}

#[derive(Default)]
struct State {
    /// What the pass read of each peer, by origin.
    peers: BTreeMap<String, PeerView>,
    /// What the pass reported once, so the log does not repeat it.
    reported: BTreeSet<Report>,
    /// The task in flight with its number. Dropping the scope cancels it.
    running: Option<(u64, TierScope)>,
    /// Number of the last task this node started.
    tasks: u64,
    /// When the next pass may start, in microseconds since the Unix epoch.
    next_pass_at: u64,
}

/// What the pass read of one peer's store.
#[derive(Default)]
struct PeerView {
    /// The cluster that owned the store when the pass read it.
    cluster_uid: Option<u128>,
    /// The last listing failed, or the node reads nothing from the peer.
    stale: bool,
    /// The last listing succeeded, so a key that it lacks is gone from the
    /// store.
    complete: bool,
    objects: BTreeMap<TopicKey, Seen>,
    /// The incarnations of shadow topics that complete listings lacked, with
    /// when one first did, in microseconds since the Unix epoch.
    missing_since: BTreeMap<TopicKey, u64>,
}

/// One topic object that the pass read.
struct Seen {
    /// `None` for bytes that this build cannot read as a topic object.
    object: Option<TopicObject>,
    /// In microseconds since the Unix epoch.
    read_at: u64,
}

/// Something the pass reports once in the log.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Report {
    /// A stream of this cluster holds the name of a shadow stream.
    NameTaken { origin: String, name: String },
    /// The shadow stream's name would be longer than a stream name may be.
    NameTooLong { origin: String, stream_id: u64 },
    /// The object names another origin or cluster than the peer's.
    Foreign { origin: String, key: TopicKey },
    /// The object does not decode.
    Invalid { origin: String, key: TopicKey },
    /// The shadow streams of the peer came from another cluster than the one
    /// that owns its store now.
    OtherCluster { origin: String, cluster_uid: u128 },
    /// An operation committed as a no-op with this code.
    Refused { proposal: String, code: u32 },
}

impl ShadowTopics {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        Self::with_peers(config, Box::new(peer_topics))
    }

    /// The shadow streams of the peers that `peers` gives.
    #[must_use]
    pub fn with_peers(config: &GeoConfig, peers: PeerSource) -> Self {
        Self {
            settings: Settings {
                tombstone_grace: micros(config.tombstone_grace.get_duration()),
                has_peers: !config.peers.is_empty(),
            },
            peers,
            state: Rc::default(),
        }
    }

    /// Read the peers' stores and propose what committed metadata lacks, in
    /// a detached task. A pass while that task runs starts nothing. A node
    /// that is not the caught-up metadata primary forgets what it read and
    /// stops its task. A node without peers passes only while a shadow
    /// stream of a retired origin is left to remove.
    pub fn pass(&self, shard: &Rc<ServerShard>, now: IggyTimestamp) {
        let settings = self.settings;
        let metadata = shard.plane.metadata();
        let streams = metadata.mux_stm.streams();
        if !settings.has_peers && !streams.read(holds_retired_shadow_streams) {
            return;
        }
        if !metadata.is_caught_up_primary()
            || metadata.applied_frontier().store_claimed_by_another()
        {
            self.stop(shard);
            return;
        }
        let mut state = self.state.borrow_mut();
        if state.running.is_some() || now.as_micros() < state.next_pass_at {
            return;
        }
        state.next_pass_at = now.as_micros().saturating_add(micros(PASS_INTERVAL));
        let retired = streams.retired_origins();
        for origin in retired.keys() {
            if state.peers.remove(origin).is_some() {
                shard.metrics().set_geo_shadow_topics_stale(origin, 0);
            }
        }
        let peers: Vec<PeerTopics> = (self.peers)(shard)
            .into_iter()
            .filter(|peer| !retired.contains_key(&peer.origin))
            .collect();
        let scope = TierScope::new();
        state.tasks += 1;
        let task = Task {
            id: state.tasks,
            shard: Rc::clone(shard),
            state: Rc::clone(&self.state),
            token: scope.token(),
            now,
            settings,
        };
        state.running = Some((task.id, scope));
        drop(state);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            task.run(peers).await;
        });
    }

    /// Forget what this node read, cancel the task in flight and clear the
    /// gauges it set.
    pub fn stop(&self, shard: &ServerShard) {
        let mut state = self.state.borrow_mut();
        if state.running.is_none() && state.peers.is_empty() {
            return;
        }
        for origin in state.peers.keys() {
            shard.metrics().set_geo_shadow_topics_stale(origin, 0);
        }
        *state = State {
            tasks: state.tasks,
            ..State::default()
        };
    }
}

/// Where a partition of a shadow topic reads its log: the peer's origin, and
/// the prefix of the partition's incarnation in the peer's store. `None` for
/// a partition of this cluster's own topics.
#[must_use]
pub fn peer_log_of(
    stream: &Stream,
    topic: &Topic,
    namespace: IggyNamespace,
) -> Option<(String, ObjectPrefix)> {
    let source = stream.shadow.as_ref()?;
    let shadow = topic.shadow?;
    Some((
        source.origin.clone(),
        ObjectPrefix {
            stream_id: source.stream_id,
            topic_id: namespace.topic_id() as u64,
            partition_id: namespace.partition_id() as u64,
            created_revision: shadow.created_revision,
        },
    ))
}

/// The peers of `shard` as the identity checks left them.
fn peer_topics(shard: &ServerShard) -> Vec<PeerTopics> {
    shard
        .peer_stores()
        .iter()
        .map(|peer| PeerTopics {
            origin: peer.origin().to_owned(),
            cluster_uid: peer.cluster_uid(),
            objects: peer.service().map(|service| {
                Rc::new(ExecutorTopicObjects::new(
                    service.clone(),
                    peer.layout().clone(),
                )) as Rc<dyn TopicObjects>
            }),
        })
        .collect()
}

/// One task of a pass: it reads every peer, then proposes one operation at a
/// time.
struct Task {
    id: u64,
    shard: Rc<ServerShard>,
    state: Rc<RefCell<State>>,
    token: TierScopeToken,
    now: IggyTimestamp,
    settings: Settings,
}

impl Task {
    async fn run(self, peers: Vec<PeerTopics>) {
        let mut cancelled = false;
        for peer in &peers {
            if self.refresh(peer).await.is_err() {
                cancelled = true;
                break;
            }
        }
        if !cancelled {
            self.converge().await;
        }
        let mut state = self.state.borrow_mut();
        if state.running.as_ref().is_some_and(|(id, _)| *id == self.id) {
            state.running = None;
        }
    }

    /// Whether this task's pass is still the one in flight.
    fn current(&self) -> bool {
        self.state
            .borrow()
            .running
            .as_ref()
            .is_some_and(|(id, _)| *id == self.id)
    }

    /// List the topic objects of `peer` and read the new and the due ones.
    /// `Err` when the task's scope ended.
    async fn refresh(&self, peer: &PeerTopics) -> Result<(), TierJobCancelled> {
        let (Some(objects), Some(cluster_uid)) = (&peer.objects, peer.cluster_uid) else {
            self.mark_stale(&peer.origin, peer.cluster_uid);
            return Ok(());
        };
        let listed = match objects.list(self.token.clone()).await? {
            Ok(listed) => listed,
            Err(error) => {
                if !self.is_stale(&peer.origin) {
                    warn!(
                        origin = peer.origin,
                        %error,
                        "listing the geo topic objects of a peer failed; its shadow topics serve what they hold"
                    );
                }
                self.mark_stale(&peer.origin, Some(cluster_uid));
                return Ok(());
            }
        };
        let due: Vec<TopicKey> = {
            let mut state = self.state.borrow_mut();
            let view = state.peers.entry(peer.origin.clone()).or_default();
            if view.cluster_uid != Some(cluster_uid) {
                view.objects.clear();
            }
            let reread_at = self.now.as_micros().saturating_sub(micros(REREAD_INTERVAL));
            listed
                .iter()
                .filter(|key| {
                    view.objects
                        .get(key)
                        .is_none_or(|seen| seen.read_at <= reread_at)
                })
                .copied()
                .collect()
        };
        let mut read = Vec::with_capacity(due.len());
        for key in due {
            match objects.read(key, self.token.clone()).await? {
                Ok(found) => read.push((key, found)),
                Err(error) => {
                    if !self.is_stale(&peer.origin) {
                        warn!(
                            origin = peer.origin,
                            ?key,
                            %error,
                            "reading a geo topic object of a peer failed; its shadow topics serve what they hold"
                        );
                    }
                    self.mark_stale(&peer.origin, Some(cluster_uid));
                    return Ok(());
                }
            }
        }
        self.record(&peer.origin, cluster_uid, listed, read);
        Ok(())
    }

    /// Record what one listing of the peer `origin` and the reads after it
    /// found.
    fn record(
        &self,
        origin: &str,
        cluster_uid: u128,
        listed: Vec<TopicKey>,
        read: Vec<(TopicKey, TopicRead)>,
    ) {
        if !self.current() {
            return;
        }
        let mut state = self.state.borrow_mut();
        let state = &mut *state;
        let view = state.peers.entry(origin.to_owned()).or_default();
        if view.stale {
            info!(
                origin,
                "the store of a geo peer answers again; its shadow topics follow it"
            );
        }
        let read_at = self.now.as_micros();
        for (key, found) in read {
            match found {
                TopicRead::Found(object) => {
                    view.objects.insert(
                        key,
                        Seen {
                            object: Some(object),
                            read_at,
                        },
                    );
                }
                // Gone between the listing and the read.
                TopicRead::Missing => {
                    view.objects.remove(&key);
                }
                TopicRead::Invalid(error) => {
                    let report = Report::Invalid {
                        origin: origin.to_owned(),
                        key,
                    };
                    if state.reported.insert(report) {
                        warn!(
                            origin,
                            ?key,
                            %error,
                            "a geo topic object of a peer does not decode; its topic is not adopted"
                        );
                    }
                    view.objects.insert(
                        key,
                        Seen {
                            object: None,
                            read_at,
                        },
                    );
                }
            }
        }
        let listed: BTreeSet<TopicKey> = listed.into_iter().collect();
        view.objects.retain(|key, _| listed.contains(key));
        view.cluster_uid = Some(cluster_uid);
        view.stale = false;
        view.complete = true;
    }

    fn is_stale(&self, origin: &str) -> bool {
        self.state
            .borrow()
            .peers
            .get(origin)
            .is_some_and(|view| view.stale)
    }

    /// Record that the node cannot read the store of `origin` now. What it
    /// read before stays, but no longer says what the store lacks.
    fn mark_stale(&self, origin: &str, cluster_uid: Option<u128>) {
        if !self.current() {
            return;
        }
        let mut state = self.state.borrow_mut();
        let view = state.peers.entry(origin.to_owned()).or_default();
        if cluster_uid.is_some() && view.cluster_uid != cluster_uid {
            view.objects.clear();
            view.cluster_uid = cluster_uid;
        }
        view.stale = true;
        view.complete = false;
    }

    /// Propose one operation at a time until committed metadata matches what
    /// the pass read, or the pass proposed its share.
    async fn converge(&self) {
        let metadata = self.shard.plane.metadata();
        {
            let mut state = self.state.borrow_mut();
            let () = metadata
                .mux_stm
                .streams()
                .read(|inner| note_absences(inner, &mut state.peers, self.now.as_micros()));
        }
        for _ in 0..MAX_PROPOSALS_PER_PASS {
            if self.token.is_cancelled() || !self.current() {
                return;
            }
            let plan = {
                let state = self.state.borrow();
                metadata.mux_stm.streams().read(|inner| {
                    plan(
                        inner,
                        &state.peers,
                        self.now.as_micros(),
                        self.settings.tombstone_grace,
                    )
                })
            };
            self.report(plan.reports);
            let Some(proposal) = plan.proposal else {
                break;
            };
            let described = describe(&proposal);
            match metadata.submit_shadow_topic_in_process(proposal).await {
                Ok(0) => info!(proposal = described, "applied a shadow topic operation"),
                Ok(code) => {
                    self.report(vec![Report::Refused {
                        proposal: described,
                        code,
                    }]);
                    break;
                }
                Err(error) => {
                    debug!(
                        proposal = described,
                        %error,
                        "a shadow topic operation did not commit; the next pass proposes it again"
                    );
                    break;
                }
            }
        }
        self.set_gauges();
    }

    fn report(&self, reports: Vec<Report>) {
        let mut state = self.state.borrow_mut();
        for report in reports {
            if !state.reported.insert(report.clone()) {
                continue;
            }
            match report {
                Report::NameTaken { origin, name } => warn!(
                    origin,
                    name,
                    "a stream of this cluster holds the name of a shadow stream; the peer's topics are not adopted until it is renamed"
                ),
                Report::NameTooLong { origin, stream_id } => warn!(
                    origin,
                    stream_id,
                    "the shadow stream of a peer stream would have a name longer than 255 bytes; its topics are not adopted"
                ),
                Report::Foreign { origin, key } => warn!(
                    origin,
                    ?key,
                    "a geo topic object in a peer's store names another origin or cluster; it is not adopted"
                ),
                Report::Invalid { .. } => {}
                Report::OtherCluster {
                    origin,
                    cluster_uid,
                } => warn!(
                    origin,
                    cluster_uid = format!("{cluster_uid:032x}"),
                    "the shadow streams of a peer came from another cluster than the one that owns its store now; they stay as they are"
                ),
                Report::Refused { proposal, code } => warn!(
                    proposal,
                    code,
                    "a shadow topic operation committed as a no-op; the next pass reads metadata again"
                ),
            }
        }
    }

    /// Set the stale gauge of every peer the pass read.
    fn set_gauges(&self) {
        let state = self.state.borrow();
        let counts = self.shard.plane.metadata().mux_stm.streams().read(|inner| {
            state
                .peers
                .iter()
                .map(|(origin, view)| {
                    let topics = if view.stale {
                        shadow_streams_of(inner, origin)
                            .map(|(_, stream)| stream.topics.len())
                            .sum()
                    } else {
                        0
                    };
                    (origin.clone(), topics)
                })
                .collect::<Vec<_>>()
        });
        for (origin, stale) in counts {
            self.shard
                .metrics()
                .set_geo_shadow_topics_stale(&origin, stale);
        }
    }
}

/// The next operation and what to report, from committed metadata and what
/// the pass read of the peers.
#[derive(Default)]
struct Plan {
    proposal: Option<ShadowTopicRequest>,
    reports: Vec<Report>,
}

/// The first operation that brings committed metadata closer to the peers.
/// Removals and detaches come first, so a topic id or name an old
/// incarnation holds frees up before an adoption needs it.
fn plan(
    inner: &StreamsInner,
    peers: &BTreeMap<String, PeerView>,
    now: u64,
    tombstone_grace: u64,
) -> Plan {
    let mut plan = Plan::default();
    for (origin, retired) in &inner.retired_origins {
        if let Some(proposal) = expire_retired(inner, origin, retired, now, tombstone_grace) {
            plan.proposal = Some(proposal);
            return plan;
        }
    }
    let followed = peers
        .iter()
        .filter(|(origin, _)| !inner.retired_origins.contains_key(*origin));
    for (origin, view) in followed.clone() {
        if let Some(proposal) = retire(inner, origin, view, now, tombstone_grace) {
            plan.proposal = Some(proposal);
            return plan;
        }
    }
    for (origin, view) in followed {
        if let Some(proposal) = adopt(inner, origin, view, &mut plan.reports) {
            plan.proposal = Some(proposal);
            return plan;
        }
    }
    plan
}

/// Whether committed metadata holds a shadow stream of a retired origin.
fn holds_retired_shadow_streams(inner: &StreamsInner) -> bool {
    inner
        .retired_origins
        .keys()
        .any(|origin| shadow_streams_of(inner, origin).next().is_some())
}

/// A removal that the retirement of `origin` asks for: a shadow topic whose
/// retention in the peer passed since the retirement or whose tombstone's
/// grace ended, then a shadow stream without topics. Every message of the
/// topic is older than the retirement, so none is left by then.
fn expire_retired(
    inner: &StreamsInner,
    origin: &str,
    retired: &RetiredOrigin,
    now: u64,
    tombstone_grace: u64,
) -> Option<ShadowTopicRequest> {
    for (stream_id, stream) in shadow_streams_of(inner, origin) {
        let source = stream.shadow.as_ref()?;
        let stream_id = u32::try_from(stream_id).ok()?;
        for (topic_id, topic) in &stream.topics {
            let Some(shadow) = topic.shadow else {
                continue;
            };
            let detached_end = shadow
                .deleted_at
                .map(|deleted_at| deleted_at.saturating_add(tombstone_grace));
            let expired_end = shadow
                .retention
                .expiry_micros()
                .map(|expiry| retired.retired_at.saturating_add(expiry));
            if detached_end
                .into_iter()
                .chain(expired_end)
                .min()
                .is_some_and(|end| now >= end)
            {
                return Some(ShadowTopicRequest::RemoveTopic {
                    target: ShadowTopicTarget {
                        stream_id,
                        source: source.clone(),
                        topic_id: u32::try_from(topic_id).ok()?,
                        created_revision: shadow.created_revision,
                    },
                });
            }
        }
        if stream.topics.is_empty() {
            return Some(ShadowTopicRequest::RemoveStream {
                stream_id,
                source: source.clone(),
            });
        }
    }
    None
}

/// Record when a complete listing first lacked the object of each shadow
/// topic, and forget the ones that came back.
fn note_absences(inner: &StreamsInner, peers: &mut BTreeMap<String, PeerView>, now: u64) {
    for (origin, view) in peers.iter_mut() {
        if !view.complete {
            continue;
        }
        let mut shadowed = BTreeSet::new();
        for (_, stream) in shadow_streams_of(inner, origin) {
            let Some(source) = &stream.shadow else {
                continue;
            };
            for (topic_id, topic) in &stream.topics {
                if let Some(shadow) = topic.shadow {
                    shadowed.insert(TopicKey {
                        stream_id: source.stream_id,
                        topic_id: topic_id as u64,
                        created_revision: shadow.created_revision,
                    });
                }
            }
        }
        view.missing_since
            .retain(|key, _| shadowed.contains(key) && !view.objects.contains_key(key));
        for key in shadowed {
            if !view.objects.contains_key(&key) {
                view.missing_since.entry(key).or_insert(now);
            }
        }
    }
}

/// The shadow streams of `origin`, by their id in this cluster.
fn shadow_streams_of<'a>(
    inner: &'a StreamsInner,
    origin: &'a str,
) -> impl Iterator<Item = (usize, &'a Stream)> + 'a {
    inner
        .items
        .iter()
        .map(|(stream_id, stream)| (stream_id, stream.as_ref()))
        .filter(move |(_, stream)| {
            stream
                .shadow
                .as_ref()
                .is_some_and(|shadow| shadow.origin == origin)
        })
}

/// A detach or a removal that committed metadata lacks for `origin`.
fn retire(
    inner: &StreamsInner,
    origin: &str,
    view: &PeerView,
    now: u64,
    tombstone_grace: u64,
) -> Option<ShadowTopicRequest> {
    for (stream_id, stream) in shadow_streams_of(inner, origin) {
        let source = stream.shadow.as_ref()?;
        let follows = !view.stale && view.cluster_uid == Some(source.cluster_uid);
        for (topic_id, topic) in &stream.topics {
            let Some(shadow) = topic.shadow else {
                continue;
            };
            let target = || ShadowTopicTarget {
                stream_id: u32::try_from(stream_id).unwrap_or(u32::MAX),
                source: source.clone(),
                topic_id: u32::try_from(topic_id).unwrap_or(u32::MAX),
                created_revision: shadow.created_revision,
            };
            if let Some(deleted_at) = shadow.deleted_at {
                if now >= deleted_at.saturating_add(tombstone_grace) {
                    return Some(ShadowTopicRequest::RemoveTopic { target: target() });
                }
                continue;
            }
            if !follows {
                continue;
            }
            let key = TopicKey {
                stream_id: source.stream_id,
                topic_id: topic_id as u64,
                created_revision: shadow.created_revision,
            };
            match view.objects.get(&key).and_then(|seen| seen.object.as_ref()) {
                Some(TopicObject {
                    state: TopicState::Tombstone { deleted_at },
                    ..
                }) => {
                    return Some(ShadowTopicRequest::DetachTopic {
                        target: target(),
                        deleted_at: *deleted_at,
                    });
                }
                // The peer deleted the topic's objects after its grace, or
                // lost them. Nothing is left to read.
                None if view.complete
                    && !view.objects.contains_key(&key)
                    && view.missing_since.get(&key).is_some_and(|since| {
                        now >= since.saturating_add(micros(ABSENCE_CONFIRM))
                    }) =>
                {
                    return Some(ShadowTopicRequest::RemoveTopic { target: target() });
                }
                Some(_) | None => {}
            }
        }
        let live_stream = view.objects.values().any(|seen| {
            seen.object
                .as_ref()
                .is_some_and(|object| object.is_live() && object.key.stream_id == source.stream_id)
        });
        if follows && view.complete && stream.topics.is_empty() && !live_stream {
            return Some(ShadowTopicRequest::RemoveStream {
                stream_id: u32::try_from(stream_id).unwrap_or(u32::MAX),
                source: source.clone(),
            });
        }
    }
    None
}

/// A stream create or rename, or a topic adoption, that committed metadata
/// lacks for `origin`.
fn adopt(
    inner: &StreamsInner,
    origin: &str,
    view: &PeerView,
    reports: &mut Vec<Report>,
) -> Option<ShadowTopicRequest> {
    let cluster_uid = view.cluster_uid.filter(|_| !view.stale)?;
    if let Some((_, stream)) = shadow_streams_of(inner, origin).find(|(_, stream)| {
        stream
            .shadow
            .as_ref()
            .is_some_and(|shadow| shadow.cluster_uid != cluster_uid)
    }) && let Some(shadow) = &stream.shadow
    {
        reports.push(Report::OtherCluster {
            origin: origin.to_owned(),
            cluster_uid: shadow.cluster_uid,
        });
        return None;
    }
    // The live objects of each peer stream, newest stream name last.
    let mut streams: BTreeMap<u64, Vec<&TopicObject>> = BTreeMap::new();
    for (key, seen) in &view.objects {
        let Some(object) = seen.object.as_ref().filter(|object| object.is_live()) else {
            continue;
        };
        if object.origin != origin || object.cluster_uid != cluster_uid {
            reports.push(Report::Foreign {
                origin: origin.to_owned(),
                key: *key,
            });
            continue;
        }
        streams.entry(key.stream_id).or_default().push(object);
    }
    for (peer_stream_id, mut objects) in streams {
        objects.sort_by_key(|object| object.published_at);
        let source = ShadowStream {
            origin: origin.to_owned(),
            cluster_uid,
            stream_id: peer_stream_id,
        };
        let newest = objects.last()?;
        let Some(name) = shadow_stream_name(origin, &newest.stream_name) else {
            reports.push(Report::NameTooLong {
                origin: origin.to_owned(),
                stream_id: peer_stream_id,
            });
            continue;
        };
        let committed = inner
            .items
            .iter()
            .find(|(_, stream)| stream.shadow.as_ref() == Some(&source));
        let name_holder = inner.resolve_stream_id(&WireIdentifier::named(&name).ok()?);
        let Some((stream_id, stream)) = committed else {
            if name_holder.is_some() {
                reports.push(Report::NameTaken {
                    origin: origin.to_owned(),
                    name,
                });
                continue;
            }
            return Some(ShadowTopicRequest::CreateStream {
                source,
                name: WireName::new(&name).ok()?,
            });
        };
        if *stream.name != *name {
            if name_holder.is_none() {
                return Some(ShadowTopicRequest::RenameStream {
                    stream_id: u32::try_from(stream_id).ok()?,
                    source,
                    name: WireName::new(&name).ok()?,
                });
            }
            reports.push(Report::NameTaken {
                origin: origin.to_owned(),
                name,
            });
        }
        for object in objects {
            let topic_id = usize::try_from(object.key.topic_id).ok()?;
            // Taken by this incarnation, or by an older one that has to go
            // first.
            if stream.topics.contains(topic_id)
                || inner
                    .resolve_topic_id(stream_id, &WireIdentifier::named(&object.topic_name).ok()?)
                    .is_some()
            {
                continue;
            }
            return Some(ShadowTopicRequest::AdoptTopic {
                target: ShadowTopicTarget {
                    stream_id: u32::try_from(stream_id).ok()?,
                    source: source.clone(),
                    topic_id: u32::try_from(topic_id).ok()?,
                    created_revision: object.key.created_revision,
                },
                create: Box::new(adoption(stream_id, object)?),
                retention: retention_of(object),
                geo_mode: match object.geo_mode {
                    TopicGeoMode::Geo => GeoMode::Geo,
                    TopicGeoMode::GeoTiered => GeoMode::GeoTiered,
                },
            });
        }
    }
    None
}

/// The create of the shadow topic of `object`, without the partition
/// assignments that the metadata primary fills in.
fn adoption(stream_id: usize, object: &TopicObject) -> Option<CreateTopicWithAssignmentsRequest> {
    let options = TopicCreateOptions {
        compression_algorithm: Some(match object.compression {
            TopicCompression::None => CompressionAlgorithm::None,
            TopicCompression::Gzip => CompressionAlgorithm::Gzip,
        }),
        ..TopicCreateOptions::default()
    };
    Some(CreateTopicWithAssignmentsRequest {
        request: CreateTopicRequest {
            stream_id: WireIdentifier::numeric(u32::try_from(stream_id).ok()?),
            partitions_count: object.partition_count,
            name: WireName::new(&object.topic_name).ok()?,
            options: options.to_wire().ok()?,
        },
        derived_options: WireOptions::empty(),
        partitions: Vec::new(),
    })
}

/// The retention of the topic of `object` in the peer.
const fn retention_of(object: &TopicObject) -> ShadowRetention {
    ShadowRetention {
        message_expiry: object.message_expiry,
        max_topic_size: object.max_topic_size,
    }
}

/// `proposal` for the log.
fn describe(proposal: &ShadowTopicRequest) -> String {
    match proposal {
        ShadowTopicRequest::CreateStream { source, name } => {
            format!(
                "create stream {} for {}",
                name.as_str(),
                describe_source(source)
            )
        }
        ShadowTopicRequest::RenameStream {
            stream_id, name, ..
        } => format!("rename stream {stream_id} to {}", name.as_str()),
        ShadowTopicRequest::RemoveStream { stream_id, .. } => {
            format!("remove stream {stream_id}")
        }
        ShadowTopicRequest::AdoptTopic { target, create, .. } => format!(
            "adopt topic {} ({}) of {} into stream {}",
            target.topic_id,
            create.request.name.as_str(),
            describe_source(&target.source),
            target.stream_id
        ),
        ShadowTopicRequest::DetachTopic { target, .. } => {
            format!(
                "detach topic {} of stream {}",
                target.topic_id, target.stream_id
            )
        }
        ShadowTopicRequest::RemoveTopic { target } => {
            format!(
                "remove topic {} of stream {}",
                target.topic_id, target.stream_id
            )
        }
        ShadowTopicRequest::RestoreStream { source, name } => format!(
            "restore stream {} as {}",
            describe_source(source),
            name.as_str()
        ),
        ShadowTopicRequest::RestoreTopic { target, create } => format!(
            "restore topic {} ({}) of {} at revision {}",
            target.topic_id,
            create.request.name.as_str(),
            describe_source(&target.source),
            target.created_revision
        ),
    }
}

fn describe_source(source: &ShadowStream) -> String {
    format!("stream {} of {}", source.stream_id, source.origin)
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::system::RetireGeoOriginRequest;
    use iggy_object_store::TopicGeoMode;
    use metadata::stm::StateHandler;

    const PEER: &str = "dc-b";
    const PEER_CLUSTER: u128 = 0x0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b;
    const PEER_STREAM: u64 = 7;
    const PEER_TOPIC: u64 = 3;
    const PEER_TOPIC_SLOT: usize = 3;
    const PEER_REVISION: u64 = 41;
    const SHADOW_NAME: &str = "__shadow_dc-b_orders";
    const GRACE: u64 = 60_000_000;
    const NOW: u64 = 1_000_000_000;

    fn key() -> TopicKey {
        TopicKey {
            stream_id: PEER_STREAM,
            topic_id: PEER_TOPIC,
            created_revision: PEER_REVISION,
        }
    }

    fn object(key: TopicKey, stream_name: &str, published_at: u64) -> TopicObject {
        TopicObject {
            key,
            origin: PEER.to_owned(),
            cluster_uid: PEER_CLUSTER,
            stream_name: stream_name.to_owned(),
            topic_name: format!("topic-{}", key.topic_id),
            partition_count: 2,
            compression: TopicCompression::Gzip,
            max_message_size: 1024,
            message_expiry: u64::MAX,
            max_topic_size: u64::MAX,
            geo_mode: TopicGeoMode::Geo,
            created_at: 1,
            published_at,
            state: TopicState::Live,
            tombstone_grace: 0,
        }
    }

    fn view(objects: impl IntoIterator<Item = TopicObject>) -> BTreeMap<String, PeerView> {
        let view = PeerView {
            cluster_uid: Some(PEER_CLUSTER),
            stale: false,
            complete: true,
            missing_since: BTreeMap::new(),
            objects: objects
                .into_iter()
                .map(|object| {
                    (
                        object.key,
                        Seen {
                            object: Some(object),
                            read_at: NOW,
                        },
                    )
                })
                .collect(),
        };
        BTreeMap::from([(PEER.to_owned(), view)])
    }

    fn source() -> ShadowStream {
        ShadowStream {
            origin: PEER.to_owned(),
            cluster_uid: PEER_CLUSTER,
            stream_id: PEER_STREAM,
        }
    }

    fn apply(inner: &mut StreamsInner, request: &ShadowTopicRequest) {
        let reply = request.apply(inner, IggyTimestamp::from(NOW));
        assert_eq!(reply.code, 0, "{request:?}");
    }

    /// Apply what `plan` proposes, giving an adoption the assignments that
    /// the metadata primary would.
    fn apply_plan(inner: &mut StreamsInner, peers: &BTreeMap<String, PeerView>) {
        let mut proposal = plan(inner, peers, NOW, GRACE).proposal.expect("a proposal");
        if let ShadowTopicRequest::AdoptTopic { create, .. } = &mut proposal {
            create.partitions = (0..create.request.partitions_count)
                .map(|partition_id| CreatedPartitionAssignment {
                    partition_id,
                    consensus_group_id: 100 + u64::from(partition_id),
                    initial_view: 0,
                    members: Vec::new(),
                })
                .collect();
        }
        apply(inner, &proposal);
    }

    /// The committed state once the pass adopted the peer's topic.
    fn adopted() -> (StreamsInner, u32) {
        adopted_from(object(key(), "orders", 10))
    }

    /// The committed state once the pass adopted the topic of `object`.
    fn adopted_from(object: TopicObject) -> (StreamsInner, u32) {
        let mut inner = StreamsInner::new();
        let peers = view([object]);
        apply_plan(&mut inner, &peers);
        apply_plan(&mut inner, &peers);
        let stream_id =
            u32::try_from(inner.resolve_stream_id(&named(SHADOW_NAME)).unwrap()).unwrap();
        (inner, stream_id)
    }

    /// The peer's object of a topic whose messages live `expiry`
    /// microseconds, in at most `bytes` bytes.
    fn retained(expiry: u64, bytes: u64) -> TopicObject {
        TopicObject {
            message_expiry: expiry,
            max_topic_size: bytes,
            ..object(key(), "orders", 10)
        }
    }

    fn target(stream_id: u32) -> ShadowTopicTarget {
        ShadowTopicTarget {
            stream_id,
            source: source(),
            topic_id: u32::try_from(PEER_TOPIC).unwrap(),
            created_revision: PEER_REVISION,
        }
    }

    /// Commit the retirement of `origin` at `at`.
    fn retire_origin(inner: &mut StreamsInner, origin: &str, at: u64) {
        let retire = RetireGeoOriginRequest {
            origin: WireName::new(origin).unwrap(),
        };
        assert_eq!(retire.apply(inner, IggyTimestamp::from(at)).code, 0);
    }

    fn named(name: &str) -> WireIdentifier {
        WireIdentifier::named(name).unwrap()
    }

    fn detach(inner: &mut StreamsInner, stream_id: u32, deleted_at: u64) {
        apply(
            inner,
            &ShadowTopicRequest::DetachTopic {
                target: ShadowTopicTarget {
                    stream_id,
                    source: source(),
                    topic_id: u32::try_from(PEER_TOPIC).unwrap(),
                    created_revision: PEER_REVISION,
                },
                deleted_at,
            },
        );
    }

    fn tombstone(deleted_at: u64) -> TopicObject {
        object(key(), "orders", 10).into_tombstone(deleted_at)
    }

    #[test]
    fn given_a_live_object_of_a_new_stream_when_planning_should_create_its_shadow_stream() {
        let inner = StreamsInner::new();

        let plan = plan(&inner, &view([object(key(), "orders", 10)]), NOW, GRACE);

        assert_eq!(
            plan.proposal,
            Some(ShadowTopicRequest::CreateStream {
                source: source(),
                name: WireName::new(SHADOW_NAME).unwrap(),
            })
        );
    }

    #[test]
    fn given_a_shadow_stream_when_planning_should_adopt_the_topic_with_the_peers_ids() {
        let (inner, stream_id) = adopted();

        let topic = &inner.items[stream_id as usize].topics[PEER_TOPIC_SLOT];
        assert_eq!(&*topic.name, "topic-3");
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(
            topic.resolved.compression_algorithm,
            CompressionAlgorithm::Gzip
        );
        assert_eq!(
            plan(&inner, &view([object(key(), "orders", 10)]), NOW, GRACE).proposal,
            None,
            "nothing more to do"
        );
    }

    #[test]
    fn given_an_object_with_retention_when_adopting_should_keep_the_peers_retention() {
        let (inner, stream_id) = adopted_from(retained(3_600_000_000, 1 << 30));

        let shadow = inner.items[stream_id as usize].topics[PEER_TOPIC_SLOT]
            .shadow
            .unwrap();
        assert_eq!(
            shadow.retention,
            ShadowRetention {
                message_expiry: 3_600_000_000,
                max_topic_size: 1 << 30,
            }
        );
    }

    #[test]
    fn given_a_retired_origin_when_planning_should_follow_nothing_of_it() {
        let mut inner = StreamsInner::new();
        retire_origin(&mut inner, PEER, NOW);

        let plan = plan(&inner, &view([object(key(), "orders", 10)]), NOW, GRACE);

        assert_eq!(plan.proposal, None);
        assert_eq!(plan.reports, Vec::new());
    }

    #[test]
    fn given_a_retired_origin_when_the_topics_expiry_passed_should_remove_the_topic() {
        let expiry = 60_000_000;
        let (mut early, stream_id) = adopted_from(retained(expiry, u64::MAX));
        let (mut late, _) = adopted_from(retained(expiry, u64::MAX));
        retire_origin(&mut early, PEER, NOW - expiry + 1);
        retire_origin(&mut late, PEER, NOW - expiry);
        let no_peers = BTreeMap::new();

        assert_eq!(plan(&early, &no_peers, NOW, GRACE).proposal, None);
        assert_eq!(
            plan(&late, &no_peers, NOW, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic {
                target: target(stream_id),
            })
        );
        assert!(holds_retired_shadow_streams(&late));
    }

    #[test]
    fn given_a_retired_origin_with_a_topic_that_never_expires_should_keep_it() {
        let (mut inner, _) = adopted();
        retire_origin(&mut inner, PEER, 0);

        assert_eq!(plan(&inner, &BTreeMap::new(), NOW, GRACE).proposal, None);
    }

    #[test]
    fn given_a_detached_topic_of_a_retired_origin_when_its_grace_ended_should_remove_it() {
        let (mut inner, stream_id) = adopted();
        detach(&mut inner, stream_id, NOW - GRACE);
        retire_origin(&mut inner, PEER, NOW);

        assert_eq!(
            plan(&inner, &BTreeMap::new(), NOW, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic {
                target: target(stream_id),
            })
        );
    }

    #[test]
    fn given_an_empty_shadow_stream_of_a_retired_origin_when_planning_should_remove_it() {
        let mut inner = StreamsInner::new();
        apply_plan(&mut inner, &view([object(key(), "orders", 10)]));
        let stream_id =
            u32::try_from(inner.resolve_stream_id(&named(SHADOW_NAME)).unwrap()).unwrap();
        retire_origin(&mut inner, PEER, NOW);

        assert_eq!(
            plan(&inner, &BTreeMap::new(), NOW, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveStream {
                stream_id,
                source: source(),
            })
        );
    }

    #[test]
    fn given_a_tombstone_when_planning_should_detach_and_remove_after_the_grace() {
        let (mut inner, stream_id) = adopted();
        let deleted_at = NOW - 10;
        let peers = view([tombstone(deleted_at)]);

        assert!(matches!(
            plan(&inner, &peers, NOW, GRACE).proposal,
            Some(ShadowTopicRequest::DetachTopic { deleted_at: at, .. }) if at == deleted_at
        ));
        detach(&mut inner, stream_id, deleted_at);
        assert_eq!(
            plan(&inner, &peers, NOW, GRACE).proposal,
            None,
            "within the grace"
        );
        assert!(matches!(
            plan(&inner, &peers, deleted_at + GRACE, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic { .. })
        ));
    }

    #[test]
    fn given_a_stale_peer_when_planning_should_remove_only_topics_past_their_grace() {
        let (mut inner, stream_id) = adopted();
        let mut peers = view([]);
        let peer = peers.get_mut(PEER).unwrap();
        peer.stale = true;
        peer.complete = false;

        assert_eq!(
            plan(&inner, &peers, NOW, GRACE).proposal,
            None,
            "an object a failed listing lacks is not gone"
        );
        detach(&mut inner, stream_id, NOW - GRACE);
        assert!(matches!(
            plan(&inner, &peers, NOW, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic { .. })
        ));
    }

    #[test]
    fn given_listings_without_the_object_when_planning_should_remove_the_topic_once_confirmed() {
        let (inner, _) = adopted();
        let mut peers = view([]);
        let confirmed = NOW + micros(ABSENCE_CONFIRM);

        note_absences(&inner, &mut peers, NOW);
        assert_eq!(
            plan(&inner, &peers, NOW, GRACE).proposal,
            None,
            "an empty root lists nothing until the peer reads as unverified"
        );
        note_absences(&inner, &mut peers, confirmed);
        assert!(matches!(
            plan(&inner, &peers, confirmed, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic { .. })
        ));

        let mut back = view([object(key(), "orders", 10)]);
        back.get_mut(PEER).unwrap().missing_since = peers[PEER].missing_since.clone();
        note_absences(&inner, &mut back, confirmed);
        assert!(back[PEER].missing_since.is_empty(), "the object came back");
    }

    #[test]
    fn given_an_empty_shadow_stream_without_live_objects_when_planning_should_remove_it() {
        let mut inner = StreamsInner::new();
        apply(
            &mut inner,
            &ShadowTopicRequest::CreateStream {
                source: source(),
                name: WireName::new(SHADOW_NAME).unwrap(),
            },
        );

        assert!(matches!(
            plan(&inner, &view([]), NOW, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveStream { .. })
        ));
    }

    #[test]
    fn given_a_renamed_peer_stream_when_planning_should_rename_the_shadow_stream() {
        let (inner, stream_id) = adopted();

        let plan = plan(&inner, &view([object(key(), "billing", 20)]), NOW, GRACE);

        assert_eq!(
            plan.proposal,
            Some(ShadowTopicRequest::RenameStream {
                stream_id,
                source: source(),
                name: WireName::new("__shadow_dc-b_billing").unwrap(),
            })
        );
    }

    #[test]
    fn given_a_stream_that_holds_the_shadow_name_when_planning_should_report_it_and_adopt_nothing()
    {
        let mut inner = StreamsInner::new();
        apply(
            &mut inner,
            &ShadowTopicRequest::CreateStream {
                source: ShadowStream {
                    origin: "dc-c".to_owned(),
                    ..source()
                },
                name: WireName::new(SHADOW_NAME).unwrap(),
            },
        );

        let plan = plan(&inner, &view([object(key(), "orders", 10)]), NOW, GRACE);

        assert_eq!(plan.proposal, None);
        assert_eq!(
            plan.reports,
            [Report::NameTaken {
                origin: PEER.to_owned(),
                name: SHADOW_NAME.to_owned(),
            }]
        );
    }

    #[test]
    fn given_an_object_of_another_cluster_when_planning_should_report_it_and_adopt_nothing() {
        let inner = StreamsInner::new();
        let mut foreign = object(key(), "orders", 10);
        foreign.cluster_uid = PEER_CLUSTER + 1;

        let plan = plan(&inner, &view([foreign]), NOW, GRACE);

        assert_eq!(plan.proposal, None);
        assert_eq!(
            plan.reports,
            [Report::Foreign {
                origin: PEER.to_owned(),
                key: key(),
            }]
        );
    }

    #[test]
    fn given_a_new_incarnation_at_a_detached_topic_id_when_planning_should_wait_for_the_old_one() {
        let (mut inner, stream_id) = adopted();
        detach(&mut inner, stream_id, NOW - 10);
        let recreated = TopicKey {
            created_revision: PEER_REVISION + 5,
            ..key()
        };
        let peers = view([tombstone(NOW - 10), object(recreated, "orders", 30)]);

        assert_eq!(plan(&inner, &peers, NOW, GRACE).proposal, None);
        assert!(matches!(
            plan(&inner, &peers, NOW + GRACE, GRACE).proposal,
            Some(ShadowTopicRequest::RemoveTopic { target }) if target.created_revision == PEER_REVISION
        ));
    }

    /// The peer's topic places its batches, and a partition that does cannot
    /// follow a peer's log. The shadow topic is a topic of this cluster that
    /// is no geo topic, so its partitions take the log.
    #[test]
    fn given_the_shadow_of_a_topic_that_places_batches_when_deriving_its_runtime_options_should_place_none()
     {
        assert_eq!(object(key(), "orders", NOW).geo_mode, TopicGeoMode::Geo);
        let (inner, stream_id) = adopted();
        let topic = &inner.items[stream_id as usize].topics[PEER_TOPIC_SLOT];

        let runtime = iggy_common::TopicRuntimeOptions::from_resource_options(&topic.options);

        assert!(!runtime.places_batches);
        assert!(!runtime.geo_topic);
    }

    #[test]
    fn given_a_shadow_partition_when_naming_its_peer_log_should_use_the_peers_incarnation() {
        let (inner, stream_id) = adopted();
        let stream = &inner.items[stream_id as usize];
        let topic = &stream.topics[PEER_TOPIC_SLOT];
        let namespace = IggyNamespace::new(stream_id as usize, PEER_TOPIC_SLOT, 1);

        assert_eq!(
            peer_log_of(stream, topic, namespace),
            Some((
                PEER.to_owned(),
                ObjectPrefix {
                    stream_id: PEER_STREAM,
                    topic_id: PEER_TOPIC,
                    partition_id: 1,
                    created_revision: PEER_REVISION,
                },
            ))
        );
    }
}
