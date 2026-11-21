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

//! The follows of the geo peers' fresh objects, which keep the shadow
//! partitions of this node about a second behind their origins.
//!
//! Shard 0 of a node follows a peer while the node is the primary of one of
//! its shadow partitions, the node that lists the peer's manifests for
//! them. It finds each new fresh object of the peer, reads its index, and
//! hands every shard what the object holds of the shadow partitions it
//! owns, see `shard::PeerFreshFound`.
//!
//! The objects of one writer life sit under the prefix of its replica and
//! shard, numbered from 0. So the follower probes the next number of each
//! writer with one ranged read of its index, which finds the object and
//! reads it at once, and probes again after [`PROBE_INTERVAL`] when it is
//! not there yet. A writer that stored nothing for [`IDLE_AFTER`] is no
//! longer probed. Every [`LIST_INTERVAL`] the follower lists the keys
//! after the newest one it read under each prefix: that finds a writer that
//! restarted or one that is new, and an object past a number the writer
//! never stored. A number the listing skipped is probed for
//! [`HOLE_TTL`], as its object may still be uploading.
//!
//! A follower that starts reads the newest object of each writer only. The
//! proofs it finds then wait until the floor of a manifest reaches them,
//! since the shadow partition never fills a gap from what a writer claimed.
//!
//! An object of a cluster other than the one that owns the peer's root
//! counts only when that cluster lost its disks and the owner recovered
//! it: the recovery record names the owner as its successor, and gives the
//! cutover of each partition.
//!
//! All reads go to the peer's own tier service in a class of their own, so
//! neither a busy peer nor this node's polls hold them back. A peer whose
//! origin retired is followed no more.

#![allow(
    clippy::future_not_send,
    reason = "the follower runs on shard 0 and never crosses threads"
)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_object_store::{
    FreshIndex, FreshObjectId, KeyLayout, ListPage, ListRequest, ObjectPrefix, ObjectStore,
    ObjectStoreError, RecoveryRead, RecoveryRecord, TierExecutor, TierJobClass, TierJobRequest,
    TierScope, TierScopeToken,
};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::StreamsInner;
use partitions::{FreshIndexRead, PeerFreshRows, ProofTrust, RemoteObjects, read_fresh_index};
use server_common::sharding::IggyNamespace;
use shard::PeerFreshFound;
use shard::shards_table::calculate_shard_assignment;
use tracing::debug;

use crate::peer_manifests::NodePlace;
use crate::shell::ServerShard;

/// How long the follower waits before it probes a writer again whose next
/// object was not there.
pub const PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// How often the follower lists the writers of a peer.
pub const LIST_INTERVAL: Duration = Duration::from_secs(10);

/// How long a writer may store nothing before the follower stops probing
/// it and finds its next object by listing.
pub const IDLE_AFTER: Duration = Duration::from_mins(1);

/// How long the follower probes a number that a listing skipped. An
/// upload that takes longer is cancelled, and its proofs go into a later
/// object.
pub const HOLE_TTL: Duration = Duration::from_secs(20);

/// The most skipped numbers of one writer the follower probes.
const HOLES_MAX: usize = 8;

/// The most objects one writer gets read in a row before the others.
const READS_IN_A_ROW: usize = 64;

/// Keys one page of a listing asks for.
const LIST_PAGE: usize = 100;

/// How long a missing recovery record is taken as missing.
const RECORD_RECHECK: Duration = Duration::from_mins(1);

/// The follows of the geo peers that shard 0 runs.
pub struct PeerFreshFollower {
    enabled: bool,
    follows: RefCell<BTreeMap<String, Follow>>,
}

/// The task that follows one peer.
struct Follow {
    /// Where the rows of each partition of the peer go here.
    routes: Rc<RefCell<Routes>>,
    stopped: Rc<Cell<bool>>,
    /// Dropping it cancels the reads in flight.
    scope: TierScope,
}

/// The shadow partition here of each partition incarnation of a peer, by
/// the peer's raw namespace and incarnation.
type Routes = HashMap<(u64, u64), Route>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Route {
    namespace: IggyNamespace,
    created_revision: u64,
    prefix: ObjectPrefix,
}

impl PeerFreshFollower {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        Self {
            enabled: !config.peers.is_empty(),
            follows: RefCell::default(),
        }
    }

    /// Start a follow of each peer whose shadow partition this node leads,
    /// stop the follows of the others, and hand the running ones where the
    /// rows of each partition go. Shard 0 only.
    pub fn pass(&self, shard: &Rc<ServerShard>, place: NodePlace) {
        if !self.enabled || shard.id != 0 {
            return;
        }
        let followed = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| followed(inner, place));
        let mut follows = self.follows.borrow_mut();
        follows.retain(|origin, follow| {
            let keep = followed.contains_key(origin);
            if !keep {
                follow.stopped.set(true);
            }
            keep
        });
        for (origin, routes) in followed {
            if let Some(follow) = follows.get(&origin) {
                *follow.routes.borrow_mut() = routes;
                continue;
            }
            let scope = TierScope::new();
            let follow = Follow {
                routes: Rc::new(RefCell::new(routes)),
                stopped: Rc::new(Cell::new(false)),
                scope,
            };
            let task = FollowTask {
                shard: Rc::clone(shard),
                origin: origin.clone(),
                routes: Rc::clone(&follow.routes),
                stopped: Rc::clone(&follow.stopped),
                token: follow.scope.token(),
                total_shards: place.total_shards,
            };
            // Through the bus, not the runtime directly: the simulator
            // supplies its own executor.
            shard.bus.spawn(task.run());
            follows.insert(origin, follow);
        }
    }

    /// Stop every follow.
    pub fn stop(&self) {
        for follow in self.follows.borrow().values() {
            follow.stopped.set(true);
        }
        self.follows.borrow_mut().clear();
    }
}

/// The routes of each peer that a shadow partition here follows, for the
/// peers whose shadow partitions this node leads one of, as their last
/// reported view names it. A detached shadow topic takes no new data, and
/// nothing of a retired origin is followed.
fn followed(inner: &StreamsInner, place: NodePlace) -> BTreeMap<String, Routes> {
    let mut routes: BTreeMap<String, Routes> = BTreeMap::new();
    let mut leads: BTreeMap<String, bool> = BTreeMap::new();
    for (stream_id, stream) in &inner.items {
        let Some(source) = stream
            .shadow
            .as_ref()
            .filter(|source| !inner.retired_origins.contains_key(&source.origin))
        else {
            continue;
        };
        for (topic_id, topic) in &stream.topics {
            let Some(shadow) = topic.shadow.filter(|shadow| !shadow.is_detached()) else {
                continue;
            };
            for partition in &topic.partitions {
                let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                let view = inner
                    .stats_registry
                    .directory()
                    .get(namespace, partition.created_revision)
                    .map_or(partition.created_view, |status| status.view);
                if partition.primary_node(view, place.replica_count) == Some(place.self_replica_id)
                {
                    leads.insert(source.origin.clone(), true);
                }
                let prefix = ObjectPrefix {
                    stream_id: source.stream_id,
                    topic_id: topic_id as u64,
                    partition_id: partition.id as u64,
                    created_revision: shadow.created_revision,
                };
                let origin_namespace = IggyNamespace::new(
                    usize::try_from(source.stream_id).unwrap_or(usize::MAX),
                    topic_id,
                    partition.id,
                );
                routes.entry(source.origin.clone()).or_default().insert(
                    (origin_namespace.inner(), shadow.created_revision),
                    Route {
                        namespace,
                        created_revision: partition.created_revision,
                        prefix,
                    },
                );
            }
        }
    }
    routes.retain(|origin, _| leads.contains_key(origin));
    routes
}

/// The follow of one peer, as a task of shard 0.
struct FollowTask {
    shard: Rc<ServerShard>,
    origin: String,
    routes: Rc<RefCell<Routes>>,
    stopped: Rc<Cell<bool>>,
    token: TierScopeToken,
    total_shards: u16,
}

impl FollowTask {
    async fn run(self) {
        let mut follower = Follower::default();
        let mut outbox: Vec<(u16, PeerFreshFound)> = Vec::new();
        while !self.stopped.get() {
            outbox = self.deliver(outbox);
            let wait = self.step(&mut follower, &mut outbox).await;
            self.shard.bus.sleep(wait).await;
        }
    }

    /// One round: list and probe what is due, hand out what was read, and
    /// return how long to wait for the next round.
    async fn step(
        &self,
        follower: &mut Follower,
        outbox: &mut Vec<(u16, PeerFreshFound)>,
    ) -> Duration {
        let peers = self.shard.peer_stores();
        let Some(peer) = peers.get(&self.origin) else {
            return PROBE_INTERVAL;
        };
        let (Some(service), Some(owner)) = (peer.service(), peer.cluster_uid()) else {
            return PROBE_INTERVAL;
        };
        let reads = ServiceReads {
            service: service.clone(),
            token: self.token.clone(),
            layout: peer.layout().clone(),
        };
        let objects = peer.follow_objects();
        let metrics = self.shard.metrics();
        let record = |request: &'static str, succeeded: bool| {
            metrics.record_peer_fresh_request(&self.origin, request, succeeded);
        };
        let now = self.shard.bus.monotonic_micros();
        let found = follower
            .step(&reads, &*objects, peer.layout(), now, &record)
            .await;
        for index in found {
            let trust = follower.trust(&reads, &index, owner, now, &record).await;
            let routes = self.routes.borrow();
            for (owner_shard, rows) in route(&index, &routes, &trust, self.total_shards) {
                outbox.push((
                    owner_shard,
                    PeerFreshFound {
                        origin: self.origin.clone(),
                        rows,
                        gone: Vec::new(),
                    },
                ));
            }
        }
        follower.wait(self.shard.bus.monotonic_micros())
    }

    /// Send what waits, and keep what an inbox refused for the next round.
    fn deliver(&self, outbox: Vec<(u16, PeerFreshFound)>) -> Vec<(u16, PeerFreshFound)> {
        let mut refused = Vec::new();
        for (owner, found) in outbox {
            if let Err(found) = self.shard.send_peer_fresh_found(owner, found) {
                refused.push((owner, found));
            }
        }
        if !refused.is_empty() {
            debug!(
                origin = self.origin,
                frames = refused.len(),
                "a shard's inbox refused the rows of a geo peer's fresh objects; they go again"
            );
        }
        refused
    }
}

/// The reads of a peer's store that a follower makes besides the indexes.
trait PeerReads {
    async fn list(&self, request: ListRequest<'_>) -> Result<ListPage, ObjectStoreError>;

    async fn recovery_record(&self, retired: u128) -> Result<RecoveryRead, ObjectStoreError>;
}

/// The reads of a peer's store through its tier service.
struct ServiceReads<E> {
    service: E,
    token: TierScopeToken,
    layout: KeyLayout,
}

impl<E: TierExecutor> PeerReads for ServiceReads<E> {
    async fn list(&self, request: ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        let prefix = request.prefix.to_owned();
        let start_after = request.start_after.map(str::to_owned);
        let continuation = request.continuation.map(str::to_owned);
        let max_keys = request.max_keys;
        self.service
            .submit(
                TierJobRequest::new(TierJobClass::FreshFollow, self.token.clone()),
                move |store| async move {
                    let mut request = ListRequest::new(&prefix).with_max_keys(max_keys);
                    if let Some(key) = start_after.as_deref() {
                        request = request.starting_after(key);
                    }
                    if let Some(continuation) = continuation.as_deref() {
                        request = request.after(continuation);
                    }
                    ObjectStore::list(&*store, &request).await
                },
            )
            .await
            .map_err(|cancelled| ObjectStoreError::Transport(cancelled.to_string()))?
    }

    async fn recovery_record(&self, retired: u128) -> Result<RecoveryRead, ObjectStoreError> {
        let layout = self.layout.clone();
        self.service
            .submit(
                TierJobRequest::new(TierJobClass::FreshFollow, self.token.clone()),
                move |store| async move { RecoveryRecord::read(&*store, &layout, retired).await },
            )
            .await
            .map_err(|cancelled| ObjectStoreError::Transport(cancelled.to_string()))?
    }
}

/// What a follower knows of the writers of one peer, see the module docs.
#[derive(Debug, Default)]
struct Follower {
    /// By the replica and the shard whose prefix a writer writes under.
    writers: BTreeMap<(u8, u16), Writer>,
    /// A listing ran: the writers it found then were there before.
    listed: bool,
    /// When the next listing is due, in microseconds of the monotonic clock.
    list_at: u64,
    /// What the recovery record of each other cluster said, and when.
    records: HashMap<u128, (Option<RecoveryRecord>, u64)>,
}

/// One writer prefix: the newest life written under it.
#[derive(Debug, Clone)]
struct Writer {
    /// The newest object read under the prefix, which a listing starts
    /// after.
    newest: FreshObjectId,
    started_at: u64,
    next_sequence: u64,
    /// Numbers a listing skipped, with when.
    holes: BTreeMap<u64, u64>,
    probe_at: u64,
    found_at: u64,
    index_len: u64,
}

impl Writer {
    const fn of(id: FreshObjectId, now: u64) -> Self {
        Self {
            newest: id,
            started_at: id.started_at(),
            next_sequence: id.sequence().saturating_add(1),
            holes: BTreeMap::new(),
            probe_at: now,
            found_at: now,
            index_len: 0,
        }
    }

    const fn next(&self) -> Option<FreshObjectId> {
        FreshObjectId::new(
            self.newest.replica(),
            self.newest.shard(),
            self.started_at,
            self.next_sequence,
        )
    }

    fn is_idle(&self, now: u64) -> bool {
        now.saturating_sub(self.found_at) >= micros(IDLE_AFTER)
    }
}

const fn prefix_of(id: FreshObjectId) -> (u8, u16) {
    (id.replica(), id.shard())
}

impl Follower {
    /// List when due and probe what is due, and return the indexes read,
    /// each once, in the order they were read.
    async fn step<R: PeerReads>(
        &mut self,
        reads: &R,
        objects: &dyn RemoteObjects,
        layout: &KeyLayout,
        now: u64,
        record: &dyn Fn(&'static str, bool),
    ) -> Vec<FreshIndex> {
        let mut found = Vec::new();
        if now >= self.list_at {
            let listed = self.list(reads, layout, now, record).await;
            for id in listed {
                self.read(objects, layout, id, "index", now, record, &mut found)
                    .await;
            }
        }
        let due: Vec<FreshObjectId> = self.due(now);
        for id in due {
            let mut next = Some(id);
            let mut in_a_row = 0;
            while let Some(id) = next.take() {
                let hit = self
                    .read(objects, layout, id, "probe", now, record, &mut found)
                    .await;
                in_a_row += 1;
                let writer = self.writers.get(&prefix_of(id));
                if hit && in_a_row < READS_IN_A_ROW {
                    next = writer
                        .filter(|writer| writer.started_at == id.started_at())
                        .and_then(Writer::next)
                        .filter(|next| next.sequence() == id.sequence() + 1);
                }
            }
        }
        found
    }

    /// Read the index of `id` and note what the read found. Returns whether
    /// the object was there.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        &mut self,
        objects: &dyn RemoteObjects,
        layout: &KeyLayout,
        id: FreshObjectId,
        request: &'static str,
        now: u64,
        record: &dyn Fn(&'static str, bool),
        found: &mut Vec<FreshIndex>,
    ) -> bool {
        let guess = self.writers.get(&prefix_of(id)).map_or(0, |writer| {
            writer.index_len.saturating_add(writer.index_len / 4)
        });
        let read = read_fresh_index(objects, layout, id, guess).await;
        record(request, read.is_ok());
        match read {
            Ok(FreshIndexRead::Found(index)) => {
                self.found(id, index.index_len(), now);
                found.push(index);
                true
            }
            // An object of the format before carries no proofs, and one
            // that does not decode never will.
            Ok(FreshIndexRead::Unreadable(_)) => {
                self.found(id, 0, now);
                true
            }
            Ok(FreshIndexRead::Missing) | Err(_) => {
                self.missing(id, now);
                false
            }
        }
    }

    /// The objects due for a probe: the next one of each writer that is not
    /// idle, and the numbers a listing skipped that may still come.
    fn due(&mut self, now: u64) -> Vec<FreshObjectId> {
        let mut due = Vec::new();
        for writer in self.writers.values_mut() {
            writer
                .holes
                .retain(|_, noticed| now.saturating_sub(*noticed) < micros(HOLE_TTL));
            if writer.probe_at > now || writer.is_idle(now) {
                continue;
            }
            due.extend(writer.next());
            due.extend(writer.holes.keys().filter_map(|sequence| {
                FreshObjectId::new(
                    writer.newest.replica(),
                    writer.newest.shard(),
                    writer.started_at,
                    *sequence,
                )
            }));
        }
        due
    }

    /// How long until the next probe or listing is due, at most a probe
    /// interval.
    fn wait(&self, now: u64) -> Duration {
        let next = self
            .writers
            .values()
            .filter(|writer| !writer.is_idle(now))
            .map(|writer| writer.probe_at)
            .chain([self.list_at, now.saturating_add(micros(PROBE_INTERVAL))])
            .min()
            .unwrap_or(now);
        Duration::from_micros(next.saturating_sub(now))
    }

    /// The object `id` is there.
    fn found(&mut self, id: FreshObjectId, index_len: u64, now: u64) {
        let writer = self
            .writers
            .entry(prefix_of(id))
            .or_insert_with(|| Writer::of(id, now));
        if id.started_at() < writer.started_at {
            return;
        }
        if id.started_at() > writer.started_at {
            *writer = Writer::of(id, now);
        } else if id.sequence() >= writer.next_sequence {
            for skipped in writer.next_sequence..id.sequence() {
                writer.holes.insert(skipped, now);
            }
            while writer.holes.len() > HOLES_MAX {
                writer.holes.pop_first();
            }
            writer.next_sequence = id.sequence().saturating_add(1);
        } else {
            writer.holes.remove(&id.sequence());
        }
        writer.newest = writer.newest.max(id);
        writer.found_at = now;
        writer.probe_at = now;
        if index_len > 0 {
            writer.index_len = index_len;
        }
    }

    /// The object `id` is not there yet.
    fn missing(&mut self, id: FreshObjectId, now: u64) {
        if let Some(writer) = self.writers.get_mut(&prefix_of(id))
            && writer.next() == Some(id)
        {
            writer.probe_at = now.saturating_add(micros(PROBE_INTERVAL));
        }
    }

    /// Whether a listing that found `id` under a prefix the follower knows
    /// should read it: an object of a newer life, past the next number, or
    /// one a listing skipped before.
    fn wants(&self, id: FreshObjectId) -> bool {
        self.writers.get(&prefix_of(id)).is_none_or(|writer| {
            match id.started_at().cmp(&writer.started_at) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => {
                    id.sequence() >= writer.next_sequence
                        || writer.holes.contains_key(&id.sequence())
                }
            }
        })
    }

    /// One walk over the keys of the peer's fresh objects, from the newest
    /// one read under each prefix on. Returns what to read: every new
    /// object of a known writer, and of a writer found later on, and only
    /// the newest object of each writer the first walk found.
    async fn list<R: PeerReads>(
        &mut self,
        reads: &R,
        layout: &KeyLayout,
        now: u64,
        record: &dyn Fn(&'static str, bool),
    ) -> Vec<FreshObjectId> {
        let root = layout.fresh_prefix();
        let mut wanted = Vec::new();
        let mut start_after: Option<String> = None;
        let complete = loop {
            let mut request = ListRequest::new(&root).with_max_keys(LIST_PAGE);
            if let Some(key) = start_after.as_deref() {
                request = request.starting_after(key);
            }
            let page = reads.list(request).await;
            record("list", page.is_ok());
            let Ok(page) = page else {
                break false;
            };
            let mut next_start = None;
            for listed in &page.objects {
                let Some(id) = layout
                    .parse_fresh_key(&listed.key)
                    .map(FreshObjectId::from_raw)
                else {
                    continue;
                };
                let prefix = prefix_of(id);
                match self.writers.get(&prefix) {
                    Some(_) if self.wants(id) => wanted.push(id),
                    // The old keys of a known writer: go on after the newest
                    // one read.
                    Some(writer) => {
                        let newest = layout.fresh_key(writer.newest.get());
                        if newest.as_str() > listed.key.as_str() {
                            next_start = Some(newest);
                            break;
                        }
                    }
                    None => {
                        let Some(newest) = self
                            .list_writer(reads, layout, prefix, &mut wanted, record)
                            .await
                        else {
                            break;
                        };
                        next_start = Some(layout.fresh_key(newest.get()));
                        break;
                    }
                }
            }
            match next_start {
                Some(key) => start_after = Some(key),
                None => match page.objects.last() {
                    Some(last) if page.next.is_some() => start_after = Some(last.key.clone()),
                    _ => break true,
                },
            }
        };
        if complete {
            self.listed = true;
        }
        self.list_at = now.saturating_add(micros(LIST_INTERVAL));
        wanted
    }

    /// Every key under the prefix of a writer the follower did not know,
    /// page by page. Puts what to read into `wanted`, and returns the
    /// newest object listed, `None` when the listing failed.
    async fn list_writer<R: PeerReads>(
        &self,
        reads: &R,
        layout: &KeyLayout,
        (replica, shard): (u8, u16),
        wanted: &mut Vec<FreshObjectId>,
        record: &dyn Fn(&'static str, bool),
    ) -> Option<FreshObjectId> {
        let prefix = layout.fresh_shard_prefix(replica, shard);
        let mut ids = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut request = ListRequest::new(&prefix);
            if let Some(after) = continuation.as_deref() {
                request = request.after(after);
            }
            let page = reads.list(request).await;
            record("list", page.is_ok());
            let page = page.ok()?;
            ids.extend(
                page.objects
                    .iter()
                    .filter_map(|listed| layout.parse_fresh_key(&listed.key))
                    .map(FreshObjectId::from_raw),
            );
            match page.next {
                Some(next) => continuation = Some(next),
                None => break,
            }
        }
        let newest = ids.iter().copied().max()?;
        if self.listed {
            wanted.extend(ids);
        } else {
            wanted.push(newest);
        }
        Some(newest)
    }

    /// Which proof rows of `index` count, by the cluster that wrote it.
    async fn trust<R: PeerReads>(
        &mut self,
        reads: &R,
        index: &FreshIndex,
        owner: u128,
        now: u64,
        record: &dyn Fn(&'static str, bool),
    ) -> Trust {
        let writer = index.header().cluster_uid;
        if writer == owner {
            return Trust::Owner;
        }
        let known = self
            .records
            .get(&writer)
            .filter(|(record, checked_at)| {
                record.is_some() || now.saturating_sub(*checked_at) < micros(RECORD_RECHECK)
            })
            .map(|(record, _)| record.clone());
        let retired = if let Some(retired) = known {
            retired
        } else {
            let read = reads.recovery_record(writer).await;
            record("record", read.is_ok());
            let retired = match read {
                Ok(RecoveryRead::Found(found)) => Some(found),
                Ok(RecoveryRead::Missing | RecoveryRead::Invalid(_)) => None,
                Err(_) => return Trust::Other,
            };
            self.records.insert(writer, (retired.clone(), now));
            retired
        };
        match retired {
            Some(record) if record.successor_cluster_uid == owner => Trust::Retired(record),
            _ => Trust::Other,
        }
    }
}

/// Which proof rows of an object count, see the module docs.
#[derive(Debug, Clone)]
enum Trust {
    Owner,
    Retired(RecoveryRecord),
    Other,
}

impl Trust {
    fn of(&self, prefix: &ObjectPrefix) -> ProofTrust {
        match self {
            Self::Owner => ProofTrust::Owner,
            Self::Retired(record) => {
                record
                    .cutover(prefix)
                    .map_or(ProofTrust::None, |cutover| ProofTrust::Retired {
                        cutover: cutover.cutover,
                    })
            }
            Self::Other => ProofTrust::None,
        }
    }
}

/// What `index` holds of each shadow partition here, by the shard that owns
/// it.
fn route(
    index: &FreshIndex,
    routes: &Routes,
    trust: &Trust,
    total_shards: u16,
) -> BTreeMap<u16, Vec<(IggyNamespace, u64, PeerFreshRows)>> {
    let mut by_shard: BTreeMap<u16, Vec<_>> = BTreeMap::new();
    for row in index.directory() {
        let Some(route) = routes.get(&(row.namespace, row.created_revision)) else {
            continue;
        };
        let owner = calculate_shard_assignment(&route.namespace, u32::from(total_shards));
        by_shard.entry(owner).or_default().push((
            route.namespace,
            route.created_revision,
            PeerFreshRows::of(index, row, trust.of(&route.prefix)),
        ));
    }
    by_shard
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use futures::FutureExt;
    use iggy_object_store::{
        FreshBatch, FreshObjectHeader, FreshPartition, FreshProofRow, MemoryStore, PutBody,
        PutOptions, RecoveredCutover, SealedFreshObject,
    };

    use super::*;

    const OWNER: u128 = 0xa;
    const RETIRED: u128 = 0xb;
    const STARTED_AT: u64 = 1_700_000_000_000_000;
    const PROBE: u64 = 250_000;

    fn layout() -> KeyLayout {
        KeyLayout::new("dc-a").unwrap()
    }

    fn namespace() -> IggyNamespace {
        IggyNamespace::new(3, 1, 0)
    }

    fn id(shard: u16, started_at: u64, sequence: u64) -> FreshObjectId {
        FreshObjectId::new(0, shard, started_at, sequence).unwrap()
    }

    /// The reads of a test store, with the recovery records it holds.
    struct TestReads(MemoryStore);

    impl PeerReads for TestReads {
        async fn list(&self, request: ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
            ObjectStore::list(&self.0, &request).await
        }

        async fn recovery_record(&self, retired: u128) -> Result<RecoveryRead, ObjectStoreError> {
            RecoveryRecord::read(&self.0, &layout(), retired).await
        }
    }

    /// The requests the follower sent, by kind: `(sent, failed)`.
    #[derive(Default)]
    struct Counts(RefCell<BTreeMap<&'static str, (u64, u64)>>);

    impl Counts {
        fn record(&self, request: &'static str, succeeded: bool) {
            let mut counts = self.0.borrow_mut();
            let entry = counts.entry(request).or_default();
            entry.0 += 1;
            if !succeeded {
                entry.1 += 1;
            }
        }

        fn sent(&self, request: &'static str) -> u64 {
            self.0.borrow().get(request).map_or(0, |(sent, _)| *sent)
        }
    }

    /// Store object `id` of `cluster_uid` with one batch of the partition
    /// and `proofs`.
    fn put(store: &MemoryStore, id: FreshObjectId, cluster_uid: u128, proofs: Vec<FreshProofRow>) {
        let sealed = SealedFreshObject::seal(
            FreshObjectHeader {
                object_id: id,
                cluster_uid,
                sealed_at: STARTED_AT,
            },
            &[FreshPartition {
                namespace: namespace().inner(),
                created_revision: 9,
                view: 1,
                claimed_first_offset: 0,
                proven_below: 0,
                batches: vec![FreshBatch {
                    message_count: 1,
                    origin_timestamp: 1,
                    blob_checksum: 0,
                    producer: None,
                    blob: b"blob".to_vec(),
                }],
                proofs,
            }],
        )
        .unwrap();
        store
            .put(
                &layout().fresh_key(id.get()),
                PutBody::from_bytes(sealed.bytes),
                &PutOptions::default(),
            )
            .now_or_never()
            .unwrap()
            .unwrap();
    }

    fn step(
        follower: &mut Follower,
        reads: &TestReads,
        counts: &Counts,
        now: u64,
    ) -> Vec<FreshObjectId> {
        let record = |request, succeeded| counts.record(request, succeeded);
        follower
            .step(reads, &reads.0, &layout(), now, &record)
            .now_or_never()
            .unwrap()
            .iter()
            .map(|index| index.header().object_id)
            .collect()
    }

    #[test]
    fn given_a_writer_whose_objects_land_one_by_one_when_following_should_find_each_with_one_read_and_probe_a_miss_again_after_the_interval()
     {
        let reads = TestReads(MemoryStore::new());
        let counts = Counts::default();
        put(&reads.0, id(1, STARTED_AT, 0), OWNER, Vec::new());
        put(&reads.0, id(1, STARTED_AT, 1), OWNER, Vec::new());
        let mut follower = Follower::default();

        let first = step(&mut follower, &reads, &counts, 0);

        assert_eq!(
            first,
            [id(1, STARTED_AT, 1)],
            "a follower starts at the newest"
        );
        assert_eq!(counts.sent("probe"), 1, "the next number is probed once");
        assert_eq!(follower.wait(0), Duration::from_micros(PROBE));
        let probes = counts.sent("probe");
        assert_eq!(step(&mut follower, &reads, &counts, PROBE / 2), []);
        assert_eq!(counts.sent("probe"), probes, "no probe before the interval");

        put(&reads.0, id(1, STARTED_AT, 2), OWNER, Vec::new());
        let second = step(&mut follower, &reads, &counts, PROBE);

        assert_eq!(second, [id(1, STARTED_AT, 2)]);
        assert_eq!(
            counts.sent("probe"),
            probes + 2,
            "the probe that finds the object reads it, and the next one misses"
        );
        assert_eq!(
            counts.sent("list"),
            3,
            "a walk of two pages, and one listing of the writer it found"
        );
        assert_eq!(counts.sent("index"), 1);
    }

    #[test]
    fn given_a_number_the_writer_never_stored_when_listing_should_skip_it_and_probe_it_while_it_may_still_land()
     {
        let reads = TestReads(MemoryStore::new());
        let counts = Counts::default();
        put(&reads.0, id(1, STARTED_AT, 0), OWNER, Vec::new());
        let mut follower = Follower::default();
        step(&mut follower, &reads, &counts, 0);
        put(&reads.0, id(1, STARTED_AT, 2), OWNER, Vec::new());
        put(&reads.0, id(1, STARTED_AT, 3), OWNER, Vec::new());

        let probed = step(&mut follower, &reads, &counts, PROBE);
        let listed = step(&mut follower, &reads, &counts, micros(LIST_INTERVAL));

        assert_eq!(probed, [], "the probe of the missing number misses");
        assert_eq!(listed, [id(1, STARTED_AT, 2), id(1, STARTED_AT, 3)]);
        put(&reads.0, id(1, STARTED_AT, 1), OWNER, Vec::new());
        let late = step(
            &mut follower,
            &reads,
            &counts,
            micros(LIST_INTERVAL) + PROBE,
        );
        assert_eq!(late, [id(1, STARTED_AT, 1)], "a late upload is still found");
        put(&reads.0, id(1, STARTED_AT, 4), OWNER, Vec::new());
        assert_eq!(
            step(
                &mut follower,
                &reads,
                &counts,
                micros(LIST_INTERVAL) + 2 * PROBE
            ),
            [id(1, STARTED_AT, 4)]
        );
    }

    #[test]
    fn given_a_writer_that_restarts_and_a_new_writer_when_listing_should_follow_both_from_their_first_object()
     {
        let reads = TestReads(MemoryStore::new());
        let counts = Counts::default();
        put(&reads.0, id(1, STARTED_AT, 5), OWNER, Vec::new());
        let mut follower = Follower::default();
        step(&mut follower, &reads, &counts, 0);
        let restarted = STARTED_AT + 1_000;
        put(&reads.0, id(1, restarted, 0), OWNER, Vec::new());
        put(&reads.0, id(1, restarted, 1), OWNER, Vec::new());
        put(&reads.0, id(2, STARTED_AT, 0), OWNER, Vec::new());
        put(&reads.0, id(2, STARTED_AT, 1), OWNER, Vec::new());

        let listed = step(&mut follower, &reads, &counts, micros(LIST_INTERVAL));

        assert_eq!(
            listed,
            [
                id(1, restarted, 0),
                id(1, restarted, 1),
                id(2, STARTED_AT, 0),
                id(2, STARTED_AT, 1)
            ]
        );
        put(&reads.0, id(1, restarted, 2), OWNER, Vec::new());
        put(&reads.0, id(2, STARTED_AT, 2), OWNER, Vec::new());
        assert_eq!(
            step(
                &mut follower,
                &reads,
                &counts,
                micros(LIST_INTERVAL) + PROBE
            ),
            [id(1, restarted, 2), id(2, STARTED_AT, 2)],
            "both writers are probed"
        );
    }

    #[test]
    fn given_writers_idle_for_a_minute_when_following_should_list_them_only_every_interval() {
        let reads = TestReads(MemoryStore::new());
        let counts = Counts::default();
        put(&reads.0, id(1, STARTED_AT, 0), OWNER, Vec::new());
        put(&reads.0, id(2, STARTED_AT, 0), OWNER, Vec::new());
        let mut follower = Follower::default();
        let minute = micros(IDLE_AFTER);
        let mut now = 0;
        while now < minute {
            step(&mut follower, &reads, &counts, now);
            now += PROBE;
        }
        let (probes, lists) = (counts.sent("probe"), counts.sent("list"));

        while now < 2 * minute {
            step(&mut follower, &reads, &counts, now);
            now += PROBE;
        }

        assert_eq!(
            probes,
            2 * 240,
            "a miss per writer and probe interval while it may still write"
        );
        assert_eq!(counts.sent("probe"), probes, "an idle writer is not probed");
        assert_eq!(
            counts.sent("list") - lists,
            6,
            "two walks of three pages a minute"
        );
        put(&reads.0, id(2, STARTED_AT, 1), OWNER, Vec::new());
        let walk = now.next_multiple_of(micros(LIST_INTERVAL));
        assert_eq!(
            step(&mut follower, &reads, &counts, walk),
            [id(2, STARTED_AT, 1)],
            "a listing finds the next object of an idle writer"
        );
        assert_eq!(
            counts.sent("probe"),
            probes + 1,
            "and it is probed again at once"
        );
    }

    fn proof(base_offset: u64) -> FreshProofRow {
        FreshProofRow {
            object_id: id(7, STARTED_AT, 0),
            position: 1_000,
            base_offset,
            base_timestamp: 1,
            message_count: 1,
            placed_checksum: 1,
        }
    }

    #[test]
    fn given_objects_of_each_cluster_when_routing_should_take_the_owners_rows_a_retired_ones_up_to_its_cutover_and_others_as_sources()
     {
        let reads = TestReads(MemoryStore::new());
        let counts = Counts::default();
        let prefix = ObjectPrefix {
            stream_id: 3,
            topic_id: 1,
            partition_id: 0,
            created_revision: 9,
        };
        let shadow = IggyNamespace::new(11, 1, 0);
        let routes: Routes = HashMap::from([(
            (namespace().inner(), 9),
            Route {
                namespace: shadow,
                created_revision: 44,
                prefix,
            },
        )]);
        let record = RecoveryRecord {
            retired_cluster_uid: RETIRED,
            successor_cluster_uid: OWNER,
            recovered_at: 1,
            adopted_unproven: true,
            partitions: vec![RecoveredCutover {
                prefix,
                published_end: 0,
                proven_end: 3,
                cutover: 5,
            }],
        };
        assert!(
            record
                .create(&reads.0, &layout())
                .now_or_never()
                .unwrap()
                .unwrap()
        );
        put(&reads.0, id(1, STARTED_AT, 0), OWNER, vec![proof(0)]);
        put(&reads.0, id(1, STARTED_AT, 1), RETIRED, vec![proof(0)]);
        put(&reads.0, id(1, STARTED_AT, 2), 0xc, vec![proof(0)]);
        let mut follower = Follower::default();
        let index = |sequence| {
            let key = layout().fresh_key(id(1, STARTED_AT, sequence).get());
            let bytes = ObjectStore::get_range(&reads.0, &key, 0..u64::MAX)
                .now_or_never()
                .unwrap()
                .unwrap()
                .bytes;
            FreshIndex::decode(&bytes, id(1, STARTED_AT, sequence)).unwrap()
        };
        let trust_of = |follower: &mut Follower, sequence| {
            let record = |request, succeeded| counts.record(request, succeeded);
            let index = index(sequence);
            let trust = follower
                .trust(&reads, &index, OWNER, 0, &record)
                .now_or_never()
                .unwrap();
            let by_shard = route(&index, &routes, &trust, 4);
            let (shard, rows) = by_shard.into_iter().next().unwrap();
            assert_eq!(
                shard,
                calculate_shard_assignment(&shadow, 4),
                "the rows go to the shard that owns the shadow partition"
            );
            assert_eq!((rows[0].0, rows[0].1), (shadow, 44));
            rows[0].2.trust
        };

        assert_eq!(trust_of(&mut follower, 0), ProofTrust::Owner);
        assert_eq!(
            trust_of(&mut follower, 1),
            ProofTrust::Retired { cutover: 5 }
        );
        assert_eq!(trust_of(&mut follower, 2), ProofTrust::None);
        let other_prefix = ObjectPrefix {
            partition_id: 1,
            ..prefix
        };
        assert_eq!(
            Trust::Retired(record).of(&other_prefix),
            ProofTrust::None,
            "a partition the recovery did not take"
        );
        assert_eq!(counts.sent("record"), 2, "one read per other cluster");
        trust_of(&mut follower, 2);
        assert_eq!(counts.sent("record"), 2, "a missing record is kept a while");
    }
}
