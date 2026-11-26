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

//! The objects of this cluster's geo topics in its own store, which the
//! metadata primary keeps in step with committed metadata.
//!
//! Another cluster adopts a geo topic from its object, see
//! [`TopicObject`], and the primary of each of the topic's partitions takes
//! no messages before it reads the object back, see
//! [`partitions::TopicObjects`]. So the metadata primary stores the object of
//! a new geo topic on its next pass after the topic commits. It stores the
//! object again whenever what the object holds changes, as when the stream
//! is renamed.
//!
//! Once a minute, a sweep lists every topic object under the root:
//!
//! - It reads the object of each topic that committed metadata holds, and
//!   stores it again when the store holds a tombstone or other content.
//! - It turns the live object of each topic that committed metadata no
//!   longer holds into a tombstone, which records when it was written.
//! - It deletes the segments and manifests of each topic whose tombstone is
//!   older than its grace, and then the tombstone. The grace is the topic's
//!   own `tombstone_grace`, which its object records because the topic is no
//!   longer in metadata then, or else `[geo] tombstone_grace`.
//!
//! The store holds everything that the sweep acts on, so nothing is lost
//! when the node restarts or another node becomes the metadata primary. The
//! deletes of removed incarnations, see [`crate::incarnation_deletes`], leave
//! the partitions of geo topics to the sweep.
//!
//! Only the caught-up metadata primary writes. A deposed primary that has
//! not learned of the new view yet can still write for a moment, so a peer
//! can see the tombstone of a live topic until the next sweep repairs it.
//! The node reads the stores of its peers through read-only stores, see
//! [`crate::geo_peers`], and never writes to them.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;

use configs::geo::GeoConfig;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{CompressionAlgorithm, GeoMode, IggyExpiry, IggyTimestamp, MaxTopicSize};
use iggy_object_store::{
    TierJobCancelled, TierScope, TierScopeToken, TopicCompression, TopicGeoMode, TopicKey,
    TopicObject, TopicRead, TopicState,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::{Stream, StreamsInner, Topic};
use partitions::TopicObjects;
use server_common::Message;
use tracing::{debug, info, warn};

use crate::shell::{ShellBus, ShellShard};
use crate::store_identity::own_cluster_uid;

/// How often the metadata primary lists the topic objects of its store.
pub const SWEEP_INTERVAL: Duration = Duration::from_mins(1);

/// The shortest time between two passes that read committed metadata. The
/// partition reconciler wakes on every commit, and a new geo topic waits at
/// most this long for its object.
const PASS_INTERVAL: Duration = Duration::from_secs(1);

/// The topic objects that the metadata primary keeps in its store.
pub struct GeoTopics {
    /// `None` on a node without an origin, which has no geo topics to
    /// publish.
    settings: Option<Settings>,
    /// Shared with the task of the pass, which records what it stored.
    state: Rc<RefCell<State>>,
}

struct Settings {
    origin: String,
    /// In microseconds.
    tombstone_grace: u64,
}

#[derive(Default)]
struct State {
    /// What this node stored of each topic, or found stored, since it became
    /// the metadata primary, with [`unstamped`] times. A pass stores only
    /// the objects that differ from it.
    stored: BTreeMap<TopicKey, TopicObject>,
    /// The task in flight with its number. Dropping the scope cancels it.
    running: Option<(u64, TierScope)>,
    /// Number of the last task this node started.
    tasks: u64,
    /// When the next pass may read committed metadata, in microseconds since
    /// the Unix epoch.
    next_pass_at: u64,
    /// When the next sweep is due, in microseconds since the Unix epoch. The
    /// first pass of a primary sweeps.
    next_sweep_at: u64,
}

impl GeoTopics {
    #[must_use]
    pub fn new(config: &GeoConfig) -> Self {
        let settings = (!config.origin.is_empty()).then(|| Settings {
            origin: config.origin.clone(),
            tombstone_grace: micros(config.tombstone_grace.get_duration()),
        });
        Self {
            settings,
            state: Rc::default(),
        }
    }

    /// Store the objects that changed, and sweep when a sweep is due, in a
    /// detached task. A pass while that task runs starts nothing. A node
    /// that is not the caught-up metadata primary forgets what it stored and
    /// stops its task: the primary that took over stores every object again.
    pub fn pass<B, MJ, S, SB>(&self, shard: &Rc<ShellShard<B, MJ, S, SB>>, now: IggyTimestamp)
    where
        B: ShellBus + Unpin,
        MJ: JournalHandle + 'static,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        S: 'static,
        SB: SuperblockStore + 'static,
    {
        let Some(settings) = &self.settings else {
            return;
        };
        if !shard.plane.metadata().is_caught_up_primary() {
            self.stop();
            return;
        }
        let (Some(objects), Some(cluster_uid)) = (shard.topic_objects(), own_cluster_uid(shard))
        else {
            return;
        };
        let mut state = self.state.borrow_mut();
        if state.running.is_some() || now.as_micros() < state.next_pass_at {
            return;
        }
        state.next_pass_at = now.as_micros().saturating_add(micros(PASS_INTERVAL));
        let max_message_size = u64::try_from(shard.bus_max_message_size()).unwrap_or(u64::MAX);
        let committed = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| Committed::of(inner, settings, cluster_uid, max_message_size, now));
        let puts: Vec<TopicObject> = committed
            .objects
            .values()
            .filter(|object| state.stored.get(&object.key) != Some(&unstamped(object)))
            .cloned()
            .collect();
        let sweep = now.as_micros() >= state.next_sweep_at;
        if puts.is_empty() && !sweep {
            return;
        }
        if sweep {
            state.next_sweep_at = now.as_micros().saturating_add(micros(SWEEP_INTERVAL));
        }
        let scope = TierScope::new();
        let token = scope.token();
        state.tasks += 1;
        let task = Task {
            id: state.tasks,
            objects,
            state: Rc::clone(&self.state),
            token,
            now,
        };
        state.running = Some((task.id, scope));
        drop(state);
        let tombstone_grace = settings.tombstone_grace;
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            task.run(puts, sweep.then_some((committed, tombstone_grace)))
                .await;
        });
    }

    /// Forget what this node stored and cancel the task in flight.
    pub fn stop(&self) {
        let mut state = self.state.borrow_mut();
        *state = State {
            tasks: state.tasks,
            ..State::default()
        };
    }

    /// Whether a task of a pass is in flight.
    #[cfg(test)]
    pub fn running(&self) -> bool {
        self.state.borrow().running.is_some()
    }
}

/// One task of a pass, which stores and sweeps one object at a time.
struct Task {
    id: u64,
    objects: Rc<dyn TopicObjects>,
    state: Rc<RefCell<State>>,
    token: TierScopeToken,
    now: IggyTimestamp,
}

impl Task {
    async fn run(self, puts: Vec<TopicObject>, sweep: Option<(Committed, u64)>) {
        let mut stopped = false;
        for object in puts {
            if self.put(object).await.is_err() {
                stopped = true;
                break;
            }
        }
        if !stopped && let Some((committed, tombstone_grace)) = sweep {
            let _ = self.sweep(&committed, tombstone_grace).await;
        }
        let mut state = self.state.borrow_mut();
        if state.running.as_ref().is_some_and(|(id, _)| *id == self.id) {
            state.running = None;
        }
    }

    /// Store `object` over what the store holds under its key. `Err` when
    /// the task's scope ended.
    async fn put(&self, object: TopicObject) -> Result<(), TierJobCancelled> {
        let key = object.key;
        match self.objects.put(object.clone(), self.token.clone()).await? {
            Ok(()) => {
                if object.is_live() {
                    debug!(?key, "stored the object of a geo topic");
                    self.record(unstamped(&object));
                } else {
                    info!(
                        ?key,
                        "stored the tombstone of a deleted geo topic; its objects stay for the tombstone grace"
                    );
                }
            }
            Err(error) => warn!(
                ?key,
                %error,
                "storing the object of a geo topic failed; a later pass stores it again"
            ),
        }
        Ok(())
    }

    /// List the topic objects of the store, and bring each one in line with
    /// what committed metadata holds. `Err` when the task's scope ended.
    async fn sweep(
        &self,
        committed: &Committed,
        tombstone_grace: u64,
    ) -> Result<(), TierJobCancelled> {
        let listed = match self.objects.list(self.token.clone()).await? {
            Ok(listed) => listed,
            Err(error) => {
                warn!(%error, "listing the geo topic objects of the store failed; the next sweep lists them again");
                return Ok(());
            }
        };
        let listed_keys: BTreeSet<TopicKey> = listed.iter().copied().collect();
        for key in listed {
            let read = match self.objects.read(key, self.token.clone()).await? {
                Ok(read) => read,
                Err(error) => {
                    warn!(?key, %error, "reading a geo topic object failed; the next sweep reads it again");
                    continue;
                }
            };
            match (committed.objects.get(&key), read) {
                (Some(object), TopicRead::Found(found))
                    if unstamped(&found) == unstamped(object) =>
                {
                    self.record(unstamped(&found));
                }
                (Some(object), _) => self.put(object.clone()).await?,
                // A topic that metadata holds and the node cannot describe
                // keeps what the store holds of it.
                (None, _) if committed.incarnations.contains(&key) => {}
                (None, TopicRead::Found(found)) => self.retire(found, tombstone_grace).await?,
                (None, TopicRead::Missing) => {}
                (None, TopicRead::Invalid(error)) => warn!(
                    ?key,
                    %error,
                    "a geo topic object of the store does not decode; the sweep leaves it in place"
                ),
            }
        }
        // An object can go missing behind the node's back, as when an operator
        // restores the store.
        for object in committed.objects.values() {
            if !listed_keys.contains(&object.key) {
                self.put(object.clone()).await?;
            }
        }
        Ok(())
    }

    /// Act on the object of a topic that committed metadata no longer holds:
    /// write its tombstone, or delete the topic once the tombstone is older
    /// than the grace that the object records, or else `tombstone_grace`.
    async fn retire(
        &self,
        found: TopicObject,
        tombstone_grace: u64,
    ) -> Result<(), TierJobCancelled> {
        let key = found.key;
        let deleted_at = match found.state {
            TopicState::Live => {
                return self
                    .put(found.into_tombstone(self.now.as_micros().max(1)))
                    .await;
            }
            TopicState::Tombstone { deleted_at } => deleted_at,
        };
        let grace = match found.tombstone_grace {
            0 => tombstone_grace,
            topic_grace => topic_grace,
        };
        if self.now.as_micros() < deleted_at.saturating_add(grace) {
            return Ok(());
        }
        match self
            .objects
            .delete_topic(key, found.partition_count, self.token.clone())
            .await?
        {
            Ok(0) => info!(
                ?key,
                "deleted the objects and the tombstone of a geo topic whose tombstone grace ended"
            ),
            Ok(left) => warn!(
                ?key,
                left,
                "some objects of a deleted geo topic were not deleted; the next sweep tries again"
            ),
            Err(error) => warn!(
                ?key,
                %error,
                "listing the objects of a deleted geo topic failed; the next sweep tries again"
            ),
        }
        Ok(())
    }

    /// Record `object` as stored, unless the task's pass is no longer the
    /// one in flight, as after the node stopped being the primary.
    fn record(&self, object: TopicObject) {
        let mut state = self.state.borrow_mut();
        if state.running.as_ref().is_some_and(|(id, _)| *id == self.id) {
            state.stored.insert(object.key, object);
        }
    }
}

/// What committed metadata holds of the geo topics.
struct Committed {
    /// The live object of each geo topic that the node can describe, by its
    /// key.
    objects: BTreeMap<TopicKey, TopicObject>,
    /// Every incarnation that a partition of a geo topic names, which the
    /// sweep never turns into a tombstone.
    incarnations: BTreeSet<TopicKey>,
}

impl Committed {
    fn of(
        inner: &StreamsInner,
        settings: &Settings,
        cluster_uid: u128,
        max_message_size: u64,
        now: IggyTimestamp,
    ) -> Self {
        let mut objects = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        for (stream_id, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                if !topic.is_geo() {
                    continue;
                }
                incarnations.extend(topic.partitions.iter().map(|partition| TopicKey {
                    stream_id: stream_id as u64,
                    topic_id: topic_id as u64,
                    created_revision: partition.created_revision,
                }));
                if let Some(object) = topic_object(
                    (stream_id, stream),
                    (topic_id, topic),
                    settings,
                    cluster_uid,
                    max_message_size,
                    now,
                ) {
                    objects.insert(object.key, object);
                }
            }
        }
        Self {
            objects,
            incarnations,
        }
    }
}

/// The live object of one geo topic. `None` for a topic without partitions,
/// whose incarnation no partition names, and for one whose partitions came
/// at different revisions, which a geo topic never has.
fn topic_object(
    (stream_id, stream): (usize, &Stream),
    (topic_id, topic): (usize, &Topic),
    settings: &Settings,
    cluster_uid: u128,
    max_message_size: u64,
    now: IggyTimestamp,
) -> Option<TopicObject> {
    let created_revision = topic.partitions.first()?.created_revision;
    if topic
        .partitions
        .iter()
        .any(|partition| partition.created_revision != created_revision)
    {
        return None;
    }
    Some(TopicObject {
        key: TopicKey {
            stream_id: stream_id as u64,
            topic_id: topic_id as u64,
            created_revision,
        },
        origin: settings.origin.clone(),
        cluster_uid,
        stream_name: stream.name.to_string(),
        topic_name: topic.name.to_string(),
        partition_count: u32::try_from(topic.partitions.len()).ok()?,
        compression: match topic.resolved.compression_algorithm {
            CompressionAlgorithm::None => TopicCompression::None,
            CompressionAlgorithm::Gzip => TopicCompression::Gzip,
        },
        max_message_size,
        message_expiry: match topic.resolved.message_expiry {
            IggyExpiry::ExpireDuration(expiry) => expiry.as_micros(),
            IggyExpiry::ServerDefault | IggyExpiry::NeverExpire => u64::MAX,
        },
        max_topic_size: match topic.resolved.max_topic_size {
            MaxTopicSize::Custom(size) => size.as_bytes_u64(),
            MaxTopicSize::ServerDefault | MaxTopicSize::Unlimited => u64::MAX,
        },
        geo_mode: match topic.resolved.geo_mode.unwrap_or_default() {
            GeoMode::Geo => TopicGeoMode::Geo,
            GeoMode::GeoTiered => TopicGeoMode::GeoTiered,
        },
        created_at: topic.created_at.as_micros(),
        published_at: now.as_micros(),
        state: TopicState::Live,
        tombstone_grace: topic
            .resolved
            .geo_durations
            .tombstone_grace
            .map_or(0, |grace| grace.as_micros()),
    })
}

/// `object` without the moment it was built, which two objects of the same
/// content can differ in.
fn unstamped(object: &TopicObject) -> TopicObject {
    TopicObject {
        published_at: 0,
        ..object.clone()
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
