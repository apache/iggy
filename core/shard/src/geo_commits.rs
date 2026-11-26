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

//! The offsets that consumers of geo and shadow partitions store themselves,
//! which are answered only once the node's store holds their positions.
//!
//! The consumers are consumer groups, the external offsets of consumer
//! groups, as the Kafka gateway sends them, and plain consumers.
//!
//! Such a `StoreConsumerOffset` commits through the partition's consensus
//! as any other. Its reply then waits on the shard, and the shard asks
//! shard 0 for a round of the commit's topic, see
//! [`crate::geo_positions`]. In the round the primary folds the commit into
//! the consumer's position, and shard 0 stores the node's object of the log.
//! The reply goes out once the store took an object that holds the
//! position. Only one round runs at a time, and a round of held commits
//! starts at least `[geo] consumer_offset_upload_interval` after the last
//! one started, so the commits that arrive during a round or within the
//! interval go into the next one: a busy topic costs one put an interval,
//! not one a commit. Only the replies wait. The partition and the shard go
//! on.
//!
//! A position that the store did not take is tried in a later round, which
//! waits longer after each failed store. A commit that waited
//! [`COMMIT_HOLD_TIMEOUT`], or whose partition stopped being led here, is
//! answered `TransientNotCommitted`. Its commit stands, and the client sends
//! the same request again: the partition answers it from its dedup table,
//! and the reply waits again, on the new primary. A commit whose group was
//! deleted meanwhile has no position to carry and gets its reply. A plain
//! consumer is never deleted.
//!
//! Only the stores on a log that the rounds store wait, see
//! [`crate::geo_positions::topic_log`]. A store on a detached shadow topic
//! is answered at its commit, and a held one once its shadow topic detaches.
//!
//! A poll's `auto_commit` is the server's own store, and is not held. The
//! publish interval carries it.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use consensus::{MetadataHandle, PartitionsHandle, build_deny_reply_from_request_header};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::{
    AckLevel, GenericHeader, KIND_CONSUMER, KIND_CONSUMER_GROUP, KIND_EXTERNAL_GROUP, Operation,
    ReplyHeader, RoutedRequestHeader, WireDecode, WireIdentifier,
};
use iggy_common::{ConsumerKind, GeoTopicDurations, IggyError, calculate_32};
use iggy_object_store::GeoLog;
use journal::superblock::SuperblockStore;
use message_bus::{MessageBus, is_auto_commit_client};
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::geo_positions::{PartitionPositions, partition_log};
use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, Sender, ShardFrame, TaggedSender};

/// How long a commit's reply waits for its position to be stored before it
/// is answered as uncertain.
///
/// The relay that waits for a write's reply gives up after 20 s, and the
/// client replays the request within its own deadline.
pub const COMMIT_HOLD_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest wait after a failed store before the next round of held
/// commits.
const COMMIT_RETRY_DELAY_MAX: Duration = Duration::from_secs(1);

/// The submit channel a write arrived with.
type SubmitReply = Sender<Option<Message<GenericHeader>>>;

/// What a held commit is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitAnswer {
    /// Its committed reply: the store holds its position, or its group is
    /// gone and has none to carry.
    Committed,
    /// `TransientNotCommitted`: the commit stands, and the store may not
    /// hold its position.
    Uncertain,
}

/// Where a held commit stands, as the shard finds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitCheck {
    /// Its partition is led here and its group exists.
    Waiting,
    /// Its partition is gone, another incarnation, or not led here.
    NotLed,
    /// Its group was deleted.
    GroupGone,
    /// No round stores its log any more: its shadow topic detached.
    LogGone,
}

/// The commits whose replies wait for their positions to be stored, by
/// partition, each in the order it was held. `R` is the reply.
pub struct HeldCommits<R> {
    partitions: BTreeMap<IggyNamespace, Vec<Held<R>>>,
}

impl<R> Default for HeldCommits<R> {
    fn default() -> Self {
        Self {
            partitions: BTreeMap::new(),
        }
    }
}

/// The local reader whose offset a held commit stored: the kind of its
/// offsets, and the id of its group or of the plain consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldReader {
    pub kind: ConsumerKind,
    pub id: u32,
}

struct Held<R> {
    reply: R,
    reader: HeldReader,
    created_revision: u64,
    /// The bus's monotonic clock when the commit was held.
    since: u64,
    /// The round whose report carries the commit's position, and the log
    /// whose object takes it.
    covered: Option<(u64, GeoLog)>,
}

impl<R> HeldCommits<R> {
    /// Hold the reply of a commit of `reader` on the partition of
    /// `namespace`, in its incarnation `created_revision`.
    pub fn hold(
        &mut self,
        namespace: IggyNamespace,
        reader: HeldReader,
        created_revision: u64,
        since: u64,
        reply: R,
    ) {
        self.partitions.entry(namespace).or_default().push(Held {
            reply,
            reader,
            created_revision,
            since,
            covered: None,
        });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.partitions.values().map(Vec::len).sum()
    }

    #[must_use]
    pub fn holds(&self, namespace: &IggyNamespace) -> bool {
        self.partitions.contains_key(namespace)
    }

    /// The partitions with a commit that `round` does not cover, which wait
    /// for another round.
    #[must_use]
    pub fn uncovered(&self, round: u64) -> BTreeSet<IggyNamespace> {
        self.partitions
            .iter()
            .filter(|(_, held)| {
                held.iter().any(|commit| {
                    commit
                        .covered
                        .as_ref()
                        .is_none_or(|(covered, _)| *covered != round)
                })
            })
            .map(|(namespace, _)| *namespace)
            .collect()
    }

    /// Mark the commits of the partition of `namespace` whose readers'
    /// positions folded what they committed as covered by `round`, whose
    /// report carries those positions to the object of `log`. A commit that
    /// an earlier round covered and that got no answer from it is weighed
    /// again, as that round ended without one. Returns how many commits
    /// `round` covers.
    pub fn cover(
        &mut self,
        namespace: IggyNamespace,
        created_revision: u64,
        round: u64,
        log: &GeoLog,
        folded: impl Fn(HeldReader) -> bool,
    ) -> usize {
        let Some(held) = self.partitions.get_mut(&namespace) else {
            return 0;
        };
        let mut covered = 0;
        for commit in held
            .iter_mut()
            .filter(|commit| commit.created_revision == created_revision)
        {
            if commit.covered.as_ref().is_some_and(|(at, _)| *at >= round) {
                continue;
            }
            commit.covered = folded(commit.reader).then(|| (round, log.clone()));
            covered += usize::from(commit.covered.is_some());
        }
        covered
    }

    /// The store's answer to `round`, which stored the objects of the logs
    /// in `stored`: the replies of the commits it covered whose log it
    /// stored, and the partitions of those whose log it did not store, whose
    /// commits wait for another round.
    pub fn published(
        &mut self,
        round: u64,
        stored: &BTreeSet<GeoLog>,
    ) -> (Vec<R>, BTreeSet<IggyNamespace>) {
        let mut released = Vec::new();
        let mut again = BTreeSet::new();
        self.partitions.retain(|namespace, held| {
            let mut kept = Vec::with_capacity(held.len());
            for mut commit in held.drain(..) {
                match &commit.covered {
                    Some((covered, log)) if *covered == round => {
                        if stored.contains(log) {
                            released.push(commit.reply);
                            continue;
                        }
                        commit.covered = None;
                        again.insert(*namespace);
                    }
                    _ => {}
                }
                kept.push(commit);
            }
            *held = kept;
            !held.is_empty()
        });
        (released, again)
    }

    /// Take the commits that wait no longer: those held for `timeout` or
    /// more, and those that `check` finds not led here, of a deleted group
    /// or of a log no round stores, with their answers.
    pub fn expire(
        &mut self,
        now: u64,
        timeout: u64,
        mut check: impl FnMut(IggyNamespace, u64, HeldReader) -> CommitCheck,
    ) -> Vec<(R, CommitAnswer)> {
        let mut answers = Vec::new();
        self.partitions.retain(|namespace, held| {
            let mut kept = Vec::with_capacity(held.len());
            for commit in held.drain(..) {
                let answer = match check(*namespace, commit.created_revision, commit.reader) {
                    CommitCheck::NotLed => Some(CommitAnswer::Uncertain),
                    CommitCheck::GroupGone | CommitCheck::LogGone => Some(CommitAnswer::Committed),
                    CommitCheck::Waiting if now.saturating_sub(commit.since) >= timeout => {
                        Some(CommitAnswer::Uncertain)
                    }
                    CommitCheck::Waiting => None,
                };
                match answer {
                    Some(answer) => answers.push((commit.reply, answer)),
                    None => kept.push(commit),
                }
            }
            *held = kept;
            !held.is_empty()
        });
        answers
    }
}

/// The pace that a geo topic sets for the rounds of its logs. A field that
/// is `None` follows the node's setting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicPace {
    /// The topic's own publish interval, in microseconds.
    pub publish_interval: Option<u64>,
    /// The topic's own offset upload interval in microseconds, `Some(None)`
    /// when the topic answers the offsets its groups store at their commit.
    pub offset_upload_interval: Option<Option<u64>>,
}

impl TopicPace {
    /// The pace of a topic with `durations`, `None` for a topic that follows
    /// the node's settings.
    #[must_use]
    pub fn of(durations: &GeoTopicDurations) -> Option<Self> {
        let pace = Self {
            publish_interval: durations
                .positions_publish_interval
                .map(|interval| interval.as_micros()),
            offset_upload_interval: durations
                .consumer_offset_upload_interval
                .map(|interval| (!interval.is_zero()).then(|| interval.as_micros())),
        };
        (pace != Self::default()).then_some(pace)
    }
}

/// A round that [`RoundPacing::start`] starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundStart {
    /// The topics whose logs the round stores, by stream and topic id,
    /// `None` for every log.
    pub scope: Option<Vec<(usize, usize)>>,
    /// The topics whose logs a round of every log does not store: each sets
    /// its own publish interval, which has not passed, and none of its held
    /// commits is due.
    pub skipped: BTreeSet<(usize, usize)>,
}

/// When shard 0 starts the rounds that store positions.
///
/// One of every log each publish interval, and between those, one of the
/// topics with held commits once no round runs, the offset upload interval
/// passed since the last round started, and the wait after a failed store
/// ended. A topic that sets its own publish interval gets a round at that
/// interval, and the rounds of every log leave it out until it is due. A
/// topic that sets its own offset upload interval spaces the rounds of its
/// held commits by it, apart from the other topics.
#[derive(Debug, Clone, Default)]
pub struct RoundPacing {
    /// When the next round of every log is due.
    next_round_at: u64,
    /// When the last round started that carried the held commits of the
    /// topics that follow the node's offset upload interval.
    last_started_at: Option<u64>,
    /// The topics, by stream and topic id, whose held commits wait for a
    /// round.
    waiting: BTreeSet<(usize, usize)>,
    /// No round of held commits starts before it, after a failed store.
    retry_at: u64,
    /// The rounds in a row whose store failed.
    failed_rounds: u32,
    /// The topics that set their own pace, as of the last round of every
    /// log.
    paces: BTreeMap<(usize, usize), TopicPace>,
    /// Of each topic with its own publish interval, when its next round is
    /// due.
    publish_at: BTreeMap<(usize, usize), u64>,
    /// Of each topic with its own offset upload interval, when the last
    /// round that carried its held commits started.
    held_started_at: BTreeMap<(usize, usize), u64>,
}

impl RoundPacing {
    /// The held commits of `topics` wait for a round.
    pub fn wait(&mut self, topics: impl IntoIterator<Item = (usize, usize)>) {
        self.waiting.extend(topics);
    }

    /// Whether the next round of every log is due at `now`, when shard 0
    /// reads the paces of the topics again.
    #[must_use]
    pub const fn every_log_due(&self, now: u64) -> bool {
        now >= self.next_round_at
    }

    /// Take the paces that the topics set, at `now`, in place of the ones
    /// taken before.
    pub fn set_paces(&mut self, paces: BTreeMap<(usize, usize), TopicPace>, now: u64) {
        let gone: Vec<(usize, usize)> = self
            .paces
            .keys()
            .filter(|topic| !paces.contains_key(*topic))
            .copied()
            .collect();
        for topic in gone {
            self.set_pace(topic, None, now);
        }
        for (topic, pace) in paces {
            self.set_pace(topic, Some(pace), now);
        }
    }

    /// Take the pace that `topic` sets at `now`, `None` when it follows the
    /// node. A topic that sets its own publish interval is due one interval
    /// later, or sooner when it was due sooner.
    pub fn set_pace(&mut self, topic: (usize, usize), pace: Option<TopicPace>, now: u64) {
        let pace = pace.unwrap_or_default();
        match pace.publish_interval {
            Some(interval) => {
                let due_at = now.saturating_add(interval);
                self.publish_at
                    .entry(topic)
                    .and_modify(|at| *at = (*at).min(due_at))
                    .or_insert(due_at);
            }
            None => {
                self.publish_at.remove(&topic);
            }
        }
        if !matches!(pace.offset_upload_interval, Some(Some(_))) {
            self.held_started_at.remove(&topic);
        }
        if pace == TopicPace::default() {
            self.paces.remove(&topic);
        } else {
            self.paces.insert(topic, pace);
        }
    }

    /// The round to start at `now`, while no round runs. Times are
    /// microseconds. `publish_interval` and `offset_upload_interval` are the
    /// node's, and `offset_upload_interval` is `None` when the node does not
    /// hold commits.
    pub fn start(
        &mut self,
        now: u64,
        publish_interval: u64,
        offset_upload_interval: Option<u64>,
    ) -> Option<RoundStart> {
        let held = self.due_held(now, offset_upload_interval);
        if now >= self.next_round_at {
            self.next_round_at = now.saturating_add(publish_interval);
            let skipped: BTreeSet<(usize, usize)> = self
                .publish_at
                .iter()
                .filter(|(topic, due_at)| now < **due_at && !held.contains(*topic))
                .map(|(topic, _)| *topic)
                .collect();
            let carried: Vec<(usize, usize)> = self
                .waiting
                .iter()
                .filter(|topic| !skipped.contains(*topic))
                .copied()
                .collect();
            self.carry(now, &carried);
            self.last_started_at = Some(now);
            let published: Vec<(usize, usize)> = self
                .publish_at
                .keys()
                .filter(|topic| !skipped.contains(*topic))
                .copied()
                .collect();
            self.published(now, &published);
            return Some(RoundStart {
                scope: None,
                skipped,
            });
        }
        let mut topics: BTreeSet<(usize, usize)> = self
            .publish_at
            .iter()
            .filter(|(_, due_at)| now >= **due_at)
            .map(|(topic, _)| *topic)
            .collect();
        topics.extend(held.iter().copied());
        if topics.is_empty() {
            return None;
        }
        let held: Vec<(usize, usize)> = held.into_iter().collect();
        self.carry(now, &held);
        let topics: Vec<(usize, usize)> = topics.into_iter().collect();
        self.published(now, &topics);
        Some(RoundStart {
            scope: Some(topics),
            skipped: BTreeSet::new(),
        })
    }

    /// A round ended at `now`, `failed` when the store refused one of its
    /// objects. Each failed round in a row doubles the wait before the next
    /// round of held commits, from twice the offset upload interval up to a
    /// second. The node's interval is the base, or the shortest a topic sets
    /// when the node holds no commits.
    pub fn finished(&mut self, now: u64, failed: bool, offset_upload_interval: Option<u64>) {
        if !failed {
            self.failed_rounds = 0;
            self.retry_at = 0;
            return;
        }
        self.failed_rounds = self.failed_rounds.saturating_add(1);
        let base = offset_upload_interval.or_else(|| {
            self.paces
                .values()
                .filter_map(|pace| pace.offset_upload_interval.flatten())
                .min()
        });
        let delay = base
            .unwrap_or(0)
            .saturating_mul(1 << self.failed_rounds.min(16))
            .min(micros(COMMIT_RETRY_DELAY_MAX));
        self.retry_at = now.saturating_add(delay);
    }

    /// The waiting topics whose round of held commits may start at `now`.
    fn due_held(&self, now: u64, offset_upload_interval: Option<u64>) -> BTreeSet<(usize, usize)> {
        if now < self.retry_at {
            return BTreeSet::new();
        }
        let spaced = |started: Option<u64>, interval: u64| {
            started.is_none_or(|started| now >= started.saturating_add(interval))
        };
        self.waiting
            .iter()
            .filter(|topic| {
                match self
                    .paces
                    .get(*topic)
                    .and_then(|pace| pace.offset_upload_interval)
                {
                    Some(Some(own)) => spaced(self.held_started_at.get(*topic).copied(), own),
                    // Commits held before the topic stopped holding them go at once.
                    Some(None) => true,
                    None => offset_upload_interval
                        .is_some_and(|interval| spaced(self.last_started_at, interval)),
                }
            })
            .copied()
            .collect()
    }

    /// A round that starts at `now` carries the held commits of `topics`.
    fn carry(&mut self, now: u64, topics: &[(usize, usize)]) {
        for topic in topics {
            self.waiting.remove(topic);
            match self
                .paces
                .get(topic)
                .and_then(|pace| pace.offset_upload_interval)
            {
                Some(Some(_)) => {
                    self.held_started_at.insert(*topic, now);
                }
                Some(None) => {}
                None => self.last_started_at = Some(now),
            }
        }
    }

    /// A round that starts at `now` stores the logs of `topics`: the next
    /// round of each that sets its own publish interval is one interval on.
    fn published(&mut self, now: u64, topics: &[(usize, usize)]) {
        for topic in topics {
            if let Some(interval) = self.paces.get(topic).and_then(|pace| pace.publish_interval) {
                self.publish_at.insert(*topic, now.saturating_add(interval));
            }
        }
    }
}

/// A committed reply that waits for its position to be stored, and what
/// answers it otherwise.
pub struct HeldReply {
    request: RoutedRequestHeader,
    committed: Message<GenericHeader>,
    reply: SubmitReply,
}

impl HeldReply {
    fn answer(self, answer: CommitAnswer) {
        let message = match answer {
            CommitAnswer::Committed => self.committed,
            CommitAnswer::Uncertain => build_deny_reply_from_request_header(
                &self.request,
                IggyError::TransientNotCommitted.as_code(),
            )
            .into_generic(),
        };
        let _ = self.reply.try_send(Some(message));
    }
}

/// A committed offset store that the shard's pump holds, see
/// [`LifecycleFrame::GeoCommitHeld`].
pub struct HeldGeoCommit {
    namespace: IggyNamespace,
    reader: HeldReader,
    created_revision: u64,
    reply: HeldReply,
}

/// An offset store whose reply waits for its position, decided before the
/// store reaches its partition.
pub struct GeoCommitHold {
    namespace: IggyNamespace,
    reader: HeldReader,
    created_revision: u64,
    request: RoutedRequestHeader,
    own: TaggedSender,
}

impl GeoCommitHold {
    /// The reader whose store the hold waits for.
    #[cfg(test)]
    pub(crate) const fn reader(&self) -> HeldReader {
        self.reader
    }

    /// Hand the committed reply of the store to the shard's pump to hold. A
    /// pump that takes no frame cannot hold it, so the store is answered as
    /// uncertain, and the client replays it.
    pub fn keep(self, committed: Message<ReplyHeader>, reply: SubmitReply) {
        if committed.header().status != 0 {
            let _ = reply.try_send(Some(committed.into_generic()));
            return;
        }
        let refused = reply.clone();
        let frame = ShardFrame::lifecycle(LifecycleFrame::GeoCommitHeld(Box::new(HeldGeoCommit {
            namespace: self.namespace,
            reader: self.reader,
            created_revision: self.created_revision,
            reply: HeldReply {
                request: self.request,
                committed: committed.into_generic(),
                reply,
            },
        })));
        if self.own.try_send(frame).is_err() {
            let uncertain = build_deny_reply_from_request_header(
                &self.request,
                IggyError::TransientNotCommitted.as_code(),
            );
            let _ = refused.try_send(Some(uncertain.into_generic()));
        }
    }
}

/// What a shard keeps of the commits it holds.
#[derive(Default)]
pub struct ShardGeoCommits {
    held: RefCell<HeldCommits<HeldReply>>,
    /// The topics the shard asked shard 0 for a round of, until it reports
    /// to a round of them: more asks would find them waiting there.
    asked: RefCell<BTreeSet<(usize, usize)>>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Whether the reply of `request` waits for the position it stores: a
    /// `StoreConsumerOffset` that a consumer sent itself, with a quorum ack,
    /// on a partition here of a log that the rounds store, while commits are
    /// held and this node stores positions. The consumer is a group, the
    /// external offsets of a group or a plain consumer.
    pub(crate) fn geo_commit_hold(
        &self,
        request: &Message<RoutedRequestHeader>,
    ) -> Option<GeoCommitHold>
    where
        M: StreamsFrontend,
    {
        let header = request.header();
        if header.operation != Operation::StoreConsumerOffset
            || is_auto_commit_client(header.client)
        {
            return None;
        }
        let settings = self.geo_settings()?;
        let namespace = IggyNamespace::from_raw(header.group);
        // The topic's own interval wins over the node's, and zero holds none.
        match self
            .geo_durations(namespace)
            .consumer_offset_upload_interval
        {
            Some(interval) if interval.is_zero() => return None,
            Some(_) => {}
            None => {
                settings.offset_upload_interval?;
            }
        }
        self.topic_objects()?;
        let store = StoreConsumerOffsetRequest::decode_from(request.body()).ok()?;
        if store.ack != AckLevel::Quorum {
            return None;
        }
        // The server gave a group's store the group's numeric id. A plain
        // consumer keeps its own, and a name stands for its hash.
        let reader = match (store.consumer.kind, &store.consumer.id) {
            (KIND_CONSUMER_GROUP, WireIdentifier::Numeric(id)) => HeldReader {
                kind: ConsumerKind::ConsumerGroup,
                id: *id,
            },
            (KIND_EXTERNAL_GROUP, WireIdentifier::Numeric(id)) => HeldReader {
                kind: ConsumerKind::ExternalGroup,
                id: *id,
            },
            (KIND_CONSUMER, WireIdentifier::Numeric(id)) => HeldReader {
                kind: ConsumerKind::Consumer,
                id: *id,
            },
            (KIND_CONSUMER, WireIdentifier::String(name)) => HeldReader {
                kind: ConsumerKind::Consumer,
                id: calculate_32(name.as_str().as_bytes()),
            },
            _ => return None,
        };
        self.plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| partition_log(inner, namespace, &settings.origin))?;
        let created_revision = self
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                partition
                    .geo_positions()
                    .map(|_| partition.created_revision())
            })
            .flatten()?;
        Some(GeoCommitHold {
            namespace,
            reader,
            created_revision,
            request: *header,
            own: self.senders.get(self.id as usize)?.clone(),
        })
    }

    /// Hold a committed store until a round stores its position, and ask
    /// shard 0 for the round. A partition no longer led here cannot store
    /// it, so its store is answered as uncertain at once.
    pub(crate) fn on_geo_commit_held(&self, commit: HeldGeoCommit) {
        let HeldGeoCommit {
            namespace,
            reader,
            created_revision,
            reply,
        } = commit;
        let led = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| {
                partition.created_revision() == created_revision
                    && partition.state_machines_current()
            });
        if !led {
            self.answer_geo_commit(reply, CommitAnswer::Uncertain);
            return;
        }
        self.geo_commits.held.borrow_mut().hold(
            namespace,
            reader,
            created_revision,
            self.bus.monotonic_micros(),
            reply,
        );
        self.ask_for_commit_round(BTreeSet::from([(
            namespace.stream_id(),
            namespace.topic_id(),
        )]));
    }

    /// Mark the held commits that this shard's report to `round`, of the
    /// topics in `scope` or of every log, carries, from `led`, the
    /// partitions it reports, as the report reads them. A commit the report
    /// does not carry, whose group has not folded it yet, asks for another
    /// round. Returns whether the report carries one.
    pub(crate) fn cover_geo_commits(
        &self,
        round: u64,
        scope: Option<&[(usize, usize)]>,
        led: &[PartitionPositions],
    ) -> bool {
        {
            let mut asked = self.geo_commits.asked.borrow_mut();
            match scope {
                Some(topics) => topics.iter().for_each(|topic| {
                    asked.remove(topic);
                }),
                None => asked.clear(),
            }
        }
        let mut held = self.geo_commits.held.borrow_mut();
        if held.is_empty() {
            return false;
        }
        let partitions = self.plane.partitions();
        let mut covered = 0;
        for reported in led {
            if !held.holds(&reported.namespace) {
                continue;
            }
            let Some(partition) = partitions.get_by_ns(&reported.namespace) else {
                continue;
            };
            covered += held.cover(
                reported.namespace,
                partition.created_revision(),
                round,
                &reported.log,
                |reader| partition.geo_folded(reader.kind, reader.id),
            );
        }
        let uncovered = held.uncovered(round);
        drop(held);
        if !uncovered.is_empty() {
            self.ask_for_commit_round(
                uncovered
                    .iter()
                    .map(|namespace| (namespace.stream_id(), namespace.topic_id()))
                    .collect(),
            );
        }
        covered > 0
    }

    /// Answer the commits that `round` covered and whose log's object the
    /// store took, `stored`. The others wait for another round.
    pub(crate) fn on_geo_positions_published(&self, round: u64, stored: Vec<GeoLog>) {
        let stored: BTreeSet<GeoLog> = stored.into_iter().collect();
        let (released, again) = self.geo_commits.held.borrow_mut().published(round, &stored);
        for reply in released {
            self.answer_geo_commit(reply, CommitAnswer::Committed);
        }
        if !again.is_empty() {
            self.ask_for_commit_round(
                again
                    .iter()
                    .map(|namespace| (namespace.stream_id(), namespace.topic_id()))
                    .collect(),
            );
        }
    }

    /// Answer the held commits that wait no longer, on the pump's tick.
    pub(crate) fn expire_geo_commits(&self)
    where
        M: StreamsFrontend,
    {
        if self.geo_commits.held.borrow().is_empty() {
            return;
        }
        let now = self.bus.monotonic_micros();
        let settings = self.geo_settings();
        let origin = settings
            .as_deref()
            .map_or("", |settings| settings.origin.as_str());
        let answers = {
            let partitions = self.plane.partitions();
            let mut led = BTreeMap::new();
            let mut readers = BTreeMap::new();
            self.geo_commits.held.borrow_mut().expire(
                now,
                micros(COMMIT_HOLD_TIMEOUT),
                |namespace, created_revision, reader| {
                    let led = *led.entry((namespace, created_revision)).or_insert_with(|| {
                        partitions.get_by_ns(&namespace).is_some_and(|partition| {
                            partition.created_revision() == created_revision
                                && partition.state_machines_current()
                        })
                    });
                    if !led {
                        return CommitCheck::NotLed;
                    }
                    *readers
                        .entry((
                            namespace.stream_id(),
                            namespace.topic_id(),
                            reader.kind.index(),
                            reader.id,
                        ))
                        .or_insert_with(|| self.geo_commit_check(namespace, reader, origin))
                },
            )
        };
        let uncertain = answers
            .iter()
            .filter(|(_, answer)| *answer == CommitAnswer::Uncertain)
            .count();
        if uncertain > 0 {
            tracing::warn!(
                shard = self.id,
                commits = uncertain,
                "consumer offset stores whose positions were not stored in time, or whose \
                 partitions are no longer led here, are answered as uncertain"
            );
        }
        for (reply, answer) in answers {
            self.answer_geo_commit(reply, answer);
        }
    }

    /// Where a held commit of `reader` on the partition of `namespace` stands
    /// in the metadata.
    fn geo_commit_check(
        &self,
        namespace: IggyNamespace,
        reader: HeldReader,
        origin: &str,
    ) -> CommitCheck
    where
        M: StreamsFrontend,
    {
        self.plane.metadata().mux_stm.streams().read(|inner| {
            let reader_exists = reader.kind == ConsumerKind::Consumer
                || inner
                    .items
                    .get(namespace.stream_id())
                    .and_then(|stream| stream.topics.get(namespace.topic_id()))
                    .is_some_and(|topic| topic.consumer_groups.contains_key(&u64::from(reader.id)));
            if !reader_exists {
                CommitCheck::GroupGone
            } else if partition_log(inner, namespace, origin).is_none() {
                CommitCheck::LogGone
            } else {
                CommitCheck::Waiting
            }
        })
    }

    fn answer_geo_commit(&self, reply: HeldReply, answer: CommitAnswer) {
        reply.answer(answer);
        self.metrics.record_geo_commit_answered(answer);
    }

    /// Ask shard 0 for a round of `topics`, those not asked for already. A
    /// refused frame leaves the commits to the next round of every log.
    fn ask_for_commit_round(&self, topics: BTreeSet<(usize, usize)>) {
        let mut asked = self.geo_commits.asked.borrow_mut();
        let topics: Vec<(usize, usize)> = topics
            .into_iter()
            .filter(|topic| !asked.contains(topic))
            .collect();
        if topics.is_empty() {
            return;
        }
        let Some(sender) = self.senders.first() else {
            return;
        };
        if sender
            .try_send(ShardFrame::lifecycle(LifecycleFrame::GeoCommitsWaiting {
                topics: topics.clone(),
            }))
            .is_ok()
        {
            asked.extend(topics);
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROUND_ONE: u64 = 1;
    const ROUND_TWO: u64 = 2;
    const REVISION: u64 = 7;
    const BILLING: u32 = 3;
    const AUDIT: u32 = 4;
    const MS: u64 = 1_000;

    fn log(topic_id: u64) -> GeoLog {
        GeoLog {
            origin: "dc-a".to_owned(),
            stream_id: 1,
            topic_id,
            created_revision: REVISION,
        }
    }

    fn partition(partition_id: usize) -> IggyNamespace {
        IggyNamespace::new(1, 1, partition_id)
    }

    fn group(id: u32) -> HeldReader {
        HeldReader {
            kind: ConsumerKind::ConsumerGroup,
            id,
        }
    }

    fn held(commits: &[(usize, u32, u64, &'static str)]) -> HeldCommits<&'static str> {
        let mut held = HeldCommits::default();
        for (partition_id, group_id, held_at, reply) in commits {
            held.hold(
                partition(*partition_id),
                group(*group_id),
                REVISION,
                *held_at,
                *reply,
            );
        }
        held
    }

    fn all_folded(_: HeldReader) -> bool {
        true
    }

    #[test]
    fn given_several_commits_of_one_log_when_one_round_stores_it_should_answer_them_all_from_one_put()
     {
        let mut held = held(&[
            (0, BILLING, 0, "first"),
            (0, BILLING, 1, "second"),
            (1, AUDIT, 2, "third"),
        ]);

        let covered = held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded)
            + held.cover(partition(1), REVISION, ROUND_ONE, &log(1), all_folded);
        let (released, again) = held.published(ROUND_ONE, &BTreeSet::from([log(1)]));

        assert_eq!(covered, 3);
        assert_eq!(released, ["first", "second", "third"]);
        assert_eq!(again, BTreeSet::new());
        assert_eq!(held.len(), 0);
    }

    #[test]
    fn given_a_covered_commit_when_its_round_has_not_stored_yet_should_hold_its_reply() {
        let mut held = held(&[(0, BILLING, 0, "commit")]);
        held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded);

        let expired = held.expire(5 * MS, micros(COMMIT_HOLD_TIMEOUT), |_, _, _| {
            CommitCheck::Waiting
        });
        let (from_another_round, _) = held.published(ROUND_TWO, &BTreeSet::from([log(1)]));

        assert_eq!(expired, Vec::new(), "covered is not stored");
        assert_eq!(
            from_another_round,
            Vec::<&str>::new(),
            "only the round that carried the position answers it"
        );
        assert_eq!(held.len(), 1);
    }

    #[test]
    fn given_a_commit_its_group_has_not_folded_when_a_round_stores_should_wait_for_the_next() {
        let mut held = held(&[(0, BILLING, 0, "late"), (0, AUDIT, 0, "folded")]);

        let covered = held.cover(partition(0), REVISION, ROUND_ONE, &log(1), |reader| {
            reader == group(AUDIT)
        });
        let (released, _) = held.published(ROUND_ONE, &BTreeSet::from([log(1)]));
        held.cover(partition(0), REVISION, ROUND_TWO, &log(1), all_folded);
        let (next, _) = held.published(ROUND_TWO, &BTreeSet::from([log(1)]));

        assert_eq!(covered, 1);
        assert_eq!(released, ["folded"]);
        assert_eq!(next, ["late"]);
    }

    #[test]
    fn given_a_commit_a_round_does_not_carry_when_the_report_goes_should_ask_for_another_round() {
        let mut held = held(&[(0, BILLING, 0, "folded"), (1, AUDIT, 0, "late")]);

        held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded);
        held.cover(partition(1), REVISION, ROUND_ONE, &log(1), |_| false);

        assert_eq!(held.uncovered(ROUND_ONE), BTreeSet::from([partition(1)]));
    }

    #[test]
    fn given_a_put_that_failed_when_the_round_answers_should_keep_the_commits_for_another_round() {
        let mut held = held(&[(0, BILLING, 0, "commit")]);
        held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded);

        let (released, again) = held.published(ROUND_ONE, &BTreeSet::from([log(2)]));
        let covered = held.cover(partition(0), REVISION, ROUND_TWO, &log(1), all_folded);
        let (retried, _) = held.published(ROUND_TWO, &BTreeSet::from([log(1)]));

        assert_eq!(released, Vec::<&str>::new(), "a failed put answers nothing");
        assert_eq!(again, BTreeSet::from([partition(0)]));
        assert_eq!(covered, 1);
        assert_eq!(retried, ["commit"]);
    }

    #[test]
    fn given_a_round_that_ended_without_an_answer_when_another_covers_should_answer_from_that_one()
    {
        let mut held = held(&[(0, BILLING, 0, "commit")]);
        held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded);

        held.cover(partition(0), REVISION, ROUND_TWO, &log(1), all_folded);
        let (stale, _) = held.published(ROUND_ONE, &BTreeSet::from([log(1)]));
        let (released, _) = held.published(ROUND_TWO, &BTreeSet::from([log(1)]));

        assert_eq!(stale, Vec::<&str>::new());
        assert_eq!(released, ["commit"]);
    }

    #[test]
    fn given_a_partition_no_longer_led_here_when_expiring_should_answer_its_commits_as_uncertain() {
        let mut held = held(&[(0, BILLING, 0, "moved"), (1, BILLING, 0, "stays")]);
        held.cover(partition(0), REVISION, ROUND_ONE, &log(1), all_folded);

        let answers = held.expire(MS, micros(COMMIT_HOLD_TIMEOUT), |namespace, _, _| {
            if namespace == partition(0) {
                CommitCheck::NotLed
            } else {
                CommitCheck::Waiting
            }
        });

        assert_eq!(answers, [("moved", CommitAnswer::Uncertain)]);
        assert_eq!(held.len(), 1);
        let (released, _) = held.published(ROUND_ONE, &BTreeSet::from([log(1)]));
        assert_eq!(
            released,
            Vec::<&str>::new(),
            "an answered commit is not answered twice"
        );
    }

    #[test]
    fn given_a_deleted_group_when_expiring_should_answer_its_commit_with_the_reply() {
        let mut held = held(&[(0, BILLING, 0, "gone")]);

        let answers = held.expire(MS, micros(COMMIT_HOLD_TIMEOUT), |_, _, _| {
            CommitCheck::GroupGone
        });

        assert_eq!(answers, [("gone", CommitAnswer::Committed)]);
        assert_eq!(held.len(), 0);
    }

    #[test]
    fn given_a_commit_held_past_the_timeout_when_expiring_should_answer_it_as_uncertain() {
        let timeout = micros(COMMIT_HOLD_TIMEOUT);
        let mut held = held(&[(0, BILLING, 0, "old"), (0, BILLING, timeout, "young")]);

        let answers = held.expire(timeout, timeout, |_, _, _| CommitCheck::Waiting);

        assert_eq!(answers, [("old", CommitAnswer::Uncertain)]);
        assert_eq!(held.len(), 1);
    }

    #[test]
    fn given_another_incarnation_of_the_partition_when_covering_should_leave_the_commit() {
        let mut held = held(&[(0, BILLING, 0, "commit")]);

        let covered = held.cover(partition(0), REVISION + 1, ROUND_ONE, &log(1), all_folded);

        assert_eq!(covered, 0);
    }

    #[test]
    fn given_commits_that_arrive_together_when_pacing_should_start_one_round_for_their_topics() {
        let mut pacing = RoundPacing::default();
        assert_eq!(
            pacing.start(0, 1_000 * MS, Some(50 * MS)),
            Some(every_log())
        );

        pacing.wait([(1, 1)]);
        pacing.wait([(1, 1), (1, 2)]);
        let round = pacing.start(60 * MS, 1_000 * MS, Some(50 * MS));
        let next = pacing.start(200 * MS, 1_000 * MS, Some(50 * MS));

        assert_eq!(round, Some(topics(vec![(1, 1), (1, 2)])));
        assert_eq!(next, None, "nothing waits");
    }

    #[test]
    fn given_commits_within_the_interval_when_pacing_should_start_their_round_once_it_passed() {
        let mut pacing = RoundPacing::default();
        pacing.start(0, 1_000 * MS, Some(50 * MS));
        pacing.wait([(1, 1)]);

        assert_eq!(pacing.start(20 * MS, 1_000 * MS, Some(50 * MS)), None);
        assert_eq!(
            pacing.start(50 * MS, 1_000 * MS, Some(50 * MS)),
            Some(topics(vec![(1, 1)]))
        );
        pacing.wait([(1, 1)]);
        assert_eq!(pacing.start(70 * MS, 1_000 * MS, Some(50 * MS)), None);
        assert_eq!(
            pacing.start(100 * MS, 1_000 * MS, Some(50 * MS)),
            Some(topics(vec![(1, 1)]))
        );
    }

    #[test]
    fn given_a_due_round_of_every_log_when_commits_wait_should_start_it_and_cover_them() {
        let mut pacing = RoundPacing::default();
        pacing.start(0, 1_000 * MS, Some(50 * MS));
        pacing.wait([(1, 1)]);

        assert_eq!(
            pacing.start(1_000 * MS, 1_000 * MS, Some(50 * MS)),
            Some(every_log())
        );
        assert_eq!(
            pacing.start(1_100 * MS, 1_000 * MS, Some(50 * MS)),
            None,
            "the round of every log took the waiting topics"
        );
    }

    #[test]
    fn given_held_commits_turned_off_when_pacing_should_start_only_the_rounds_of_every_log() {
        let mut pacing = RoundPacing::default();
        pacing.start(0, 1_000 * MS, None);
        pacing.wait([(1, 1)]);

        assert_eq!(pacing.start(500 * MS, 1_000 * MS, None), None);
    }

    #[test]
    fn given_failed_stores_when_pacing_should_back_off_up_to_a_second_and_reset_on_success() {
        let mut pacing = RoundPacing::default();
        pacing.start(0, 60_000 * MS, Some(50 * MS));
        let mut now = 0;
        let mut waits = Vec::new();
        for _ in 0..6 {
            pacing.finished(now, true, Some(50 * MS));
            pacing.wait([(1, 1)]);
            let started_at = (0..2_000)
                .map(|ms| now + ms * MS)
                .find(|at| starts(&pacing, *at))
                .expect("a round starts");
            waits.push((started_at - now) / MS);
            pacing.start(started_at, 60_000 * MS, Some(50 * MS));
            now = started_at;
        }
        pacing.finished(now, false, Some(50 * MS));
        pacing.wait([(1, 1)]);

        assert_eq!(waits, [100, 200, 400, 800, 1_000, 1_000]);
        assert!(starts(&pacing, now + 50 * MS), "a success ends the wait");
    }

    #[test]
    fn given_topics_with_their_own_offset_upload_intervals_when_commits_keep_coming_should_space_each_by_its_own()
     {
        let (quick, slow, node) = ((1, 1), (1, 2), (1, 3));
        let own = |interval: u64| TopicPace {
            offset_upload_interval: Some(Some(interval * MS)),
            ..TopicPace::default()
        };
        let mut pacing = RoundPacing::default();
        pacing.set_paces(BTreeMap::from([(quick, own(20)), (slow, own(200))]), 0);
        assert_eq!(
            pacing.start(0, 60_000 * MS, Some(50 * MS)),
            Some(every_log())
        );

        let mut carried: BTreeMap<(usize, usize), Vec<u64>> = BTreeMap::new();
        for ms in 1..=400 {
            pacing.wait([quick, slow, node]);
            if let Some(RoundStart {
                scope: Some(topics),
                ..
            }) = pacing.start(ms * MS, 60_000 * MS, Some(50 * MS))
            {
                for topic in topics {
                    carried.entry(topic).or_default().push(ms);
                }
            }
        }

        assert_eq!(carried[&quick], (1..=400).step_by(20).collect::<Vec<_>>());
        assert_eq!(carried[&slow], [1, 201]);
        // The round of every log at 0 started the node's interval.
        assert_eq!(carried[&node], (50..=400).step_by(50).collect::<Vec<_>>());
    }

    #[test]
    fn given_topics_with_their_own_publish_intervals_when_pacing_should_give_each_its_rounds_and_skip_it_in_rounds_of_every_log()
     {
        let (slow, quick) = ((1, 1), (1, 2));
        let own = |interval: u64| TopicPace {
            publish_interval: Some(interval * MS),
            ..TopicPace::default()
        };
        let mut pacing = RoundPacing::default();
        pacing.set_paces(BTreeMap::from([(slow, own(5_000)), (quick, own(250))]), 0);
        let first = pacing.start(0, 1_000 * MS, Some(50 * MS));
        assert_eq!(
            first,
            Some(RoundStart {
                scope: None,
                skipped: BTreeSet::from([slow, quick]),
            })
        );

        let mut every_log = Vec::new();
        let mut own_rounds = Vec::new();
        for ms in 1..=5_000 {
            match pacing.start(ms * MS, 1_000 * MS, Some(50 * MS)) {
                Some(RoundStart {
                    scope: None,
                    skipped,
                }) => every_log.push((ms, skipped)),
                Some(RoundStart {
                    scope: Some(topics),
                    ..
                }) => own_rounds.push((ms, topics)),
                None => {}
            }
        }

        assert_eq!(
            every_log,
            [
                (1_000, BTreeSet::from([slow])),
                (2_000, BTreeSet::from([slow])),
                (3_000, BTreeSet::from([slow])),
                (4_000, BTreeSet::from([slow])),
                (5_000, BTreeSet::new()),
            ]
        );
        let expected: Vec<(u64, Vec<(usize, usize)>)> = (250..5_000)
            .step_by(250)
            .filter(|ms| ms % 1_000 != 0)
            .map(|ms| (ms, vec![quick]))
            .collect();
        assert_eq!(own_rounds, expected);
    }

    #[test]
    fn given_a_node_that_holds_no_commits_when_a_topic_holds_them_and_its_store_fails_should_back_off_from_its_interval()
     {
        let mut pacing = RoundPacing::default();
        pacing.set_paces(
            BTreeMap::from([(
                (1, 1),
                TopicPace {
                    offset_upload_interval: Some(Some(20 * MS)),
                    ..TopicPace::default()
                },
            )]),
            0,
        );
        pacing.start(0, 60_000 * MS, None);

        pacing.finished(0, true, None);
        pacing.wait([(1, 1)]);

        assert_eq!(pacing.clone().start(39 * MS, 60_000 * MS, None), None);
        assert_eq!(
            pacing.start(40 * MS, 60_000 * MS, None),
            Some(topics(vec![(1, 1)]))
        );
    }

    /// A round of every log that leaves no topic out.
    fn every_log() -> RoundStart {
        RoundStart {
            scope: None,
            skipped: BTreeSet::new(),
        }
    }

    /// A round of the logs of `topics`.
    fn topics(topics: Vec<(usize, usize)>) -> RoundStart {
        RoundStart {
            scope: Some(topics),
            skipped: BTreeSet::new(),
        }
    }

    /// Whether `pacing` starts a round at `now`, left as it was.
    fn starts(pacing: &RoundPacing, now: u64) -> bool {
        pacing
            .clone()
            .start(now, 60_000 * MS, Some(50 * MS))
            .is_some()
    }
}
