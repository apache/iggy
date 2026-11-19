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

//! The offsets that consumer groups of geo and shadow partitions store
//! themselves, which are answered only once the node's store holds their
//! positions.
//!
//! Such a `StoreConsumerOffset` commits through the partition's consensus
//! as any other. Its reply then waits on the shard, and the shard asks
//! shard 0 for a round of the commit's topic, see
//! [`crate::geo_positions`]. In the round the primary folds the commit into
//! the group's position, and shard 0 stores the node's object of the log.
//! The reply goes out once the store took an object that holds the
//! position. Only one round runs at a time, and a round of held commits
//! starts at least `[geo] positions_commit_interval` after the last one
//! started, so the commits that arrive during a round or within the interval
//! go into the next one: a busy topic costs one put an interval, not one a
//! commit. Only the replies wait. The partition and the shard go on.
//!
//! A position that the store did not take is tried in a later round, which
//! waits longer after each failed store. A commit that waited
//! [`COMMIT_HOLD_TIMEOUT`], or whose partition stopped being led here, is
//! answered `TransientNotCommitted`. Its commit stands, and the client sends
//! the same request again: the partition answers it from its dedup table,
//! and the reply waits again, on the new primary. A commit whose group was
//! deleted meanwhile has no position to carry and gets its reply.
//!
//! A poll's `auto_commit` is the server's own store, and is not held. The
//! publish interval carries it.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use consensus::{MetadataHandle, PartitionsHandle, build_deny_reply_from_request_header};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::{
    AckLevel, GenericHeader, KIND_CONSUMER_GROUP, Operation, ReplyHeader, RoutedRequestHeader,
    WireDecode, WireIdentifier,
};
use iggy_common::IggyError;
use iggy_object_store::GeoLog;
use journal::superblock::SuperblockStore;
use message_bus::{MessageBus, is_auto_commit_client};
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::geo_positions::PartitionPositions;
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

struct Held<R> {
    reply: R,
    group_id: u32,
    created_revision: u64,
    /// The bus's monotonic clock when the commit was held.
    since: u64,
    /// The round whose report carries the commit's position, and the log
    /// whose object takes it.
    covered: Option<(u64, GeoLog)>,
}

impl<R> HeldCommits<R> {
    /// Hold the reply of a commit of the group `group_id` on the partition
    /// of `namespace`, in its incarnation `created_revision`.
    pub fn hold(
        &mut self,
        namespace: IggyNamespace,
        group_id: u32,
        created_revision: u64,
        since: u64,
        reply: R,
    ) {
        self.partitions.entry(namespace).or_default().push(Held {
            reply,
            group_id,
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

    /// Mark the commits of the partition of `namespace` whose groups'
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
        folded: impl Fn(u32) -> bool,
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
            commit.covered = folded(commit.group_id).then(|| (round, log.clone()));
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
    /// more, and those that `check` finds not led here or of a deleted
    /// group, with their answers.
    pub fn expire(
        &mut self,
        now: u64,
        timeout: u64,
        mut check: impl FnMut(IggyNamespace, u64, u32) -> CommitCheck,
    ) -> Vec<(R, CommitAnswer)> {
        let mut answers = Vec::new();
        self.partitions.retain(|namespace, held| {
            let mut kept = Vec::with_capacity(held.len());
            for commit in held.drain(..) {
                let answer = match check(*namespace, commit.created_revision, commit.group_id) {
                    CommitCheck::NotLed => Some(CommitAnswer::Uncertain),
                    CommitCheck::GroupGone => Some(CommitAnswer::Committed),
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

/// When shard 0 starts the rounds that store positions.
///
/// One of every log each publish interval, and between those, one of the
/// topics with held commits once no round runs, the commit interval passed
/// since the last round started, and the wait after a failed store ended.
#[derive(Debug, Clone, Default)]
pub struct RoundPacing {
    /// When the next round of every log is due.
    next_round_at: u64,
    last_started_at: Option<u64>,
    /// The topics, by stream and topic id, whose held commits wait for a
    /// round.
    waiting: BTreeSet<(usize, usize)>,
    /// No round of held commits starts before it, after a failed store.
    retry_at: u64,
    /// The rounds in a row whose store failed.
    failed_rounds: u32,
}

impl RoundPacing {
    /// The held commits of `topics` wait for a round.
    pub fn wait(&mut self, topics: impl IntoIterator<Item = (usize, usize)>) {
        self.waiting.extend(topics);
    }

    /// The round to start at `now`, while no round runs: `Some(None)` for
    /// every log, `Some(Some(topics))` for the topics with held commits.
    /// Times are microseconds, `commit_interval` is `None` when commits are
    /// not held.
    pub fn start(
        &mut self,
        now: u64,
        publish_interval: u64,
        commit_interval: Option<u64>,
    ) -> Option<Option<Vec<(usize, usize)>>> {
        if now >= self.next_round_at {
            self.next_round_at = now.saturating_add(publish_interval);
            self.last_started_at = Some(now);
            self.waiting.clear();
            return Some(None);
        }
        let commit_interval = commit_interval?;
        let spaced = self
            .last_started_at
            .is_none_or(|started| now >= started.saturating_add(commit_interval));
        if self.waiting.is_empty() || !spaced || now < self.retry_at {
            return None;
        }
        self.last_started_at = Some(now);
        Some(Some(
            std::mem::take(&mut self.waiting).into_iter().collect(),
        ))
    }

    /// A round ended at `now`, `failed` when the store refused one of its
    /// objects. Each failed round in a row doubles the wait before the next
    /// round of held commits, from twice the commit interval up to a
    /// second.
    pub fn finished(&mut self, now: u64, failed: bool, commit_interval: Option<u64>) {
        if !failed {
            self.failed_rounds = 0;
            self.retry_at = 0;
            return;
        }
        self.failed_rounds = self.failed_rounds.saturating_add(1);
        let delay = commit_interval
            .unwrap_or(0)
            .saturating_mul(1 << self.failed_rounds.min(16))
            .min(micros(COMMIT_RETRY_DELAY_MAX));
        self.retry_at = now.saturating_add(delay);
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

/// A committed group offset store that the shard's pump holds, see
/// [`LifecycleFrame::GeoCommitHeld`].
pub struct HeldGeoCommit {
    namespace: IggyNamespace,
    group_id: u32,
    created_revision: u64,
    reply: HeldReply,
}

/// A group offset store whose reply waits for its position, decided before
/// the store reaches its partition.
pub struct GeoCommitHold {
    namespace: IggyNamespace,
    group_id: u32,
    created_revision: u64,
    request: RoutedRequestHeader,
    own: TaggedSender,
}

impl GeoCommitHold {
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
            group_id: self.group_id,
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
    /// `StoreConsumerOffset` that a consumer group sent itself, with a
    /// quorum ack, on a geo or shadow partition here, while commits are held
    /// and this node stores positions.
    pub(crate) fn geo_commit_hold(
        &self,
        request: &Message<RoutedRequestHeader>,
    ) -> Option<GeoCommitHold> {
        let header = request.header();
        if header.operation != Operation::StoreConsumerOffset
            || is_auto_commit_client(header.client)
        {
            return None;
        }
        self.geo_settings()?.commit_interval?;
        self.topic_objects()?;
        let store = StoreConsumerOffsetRequest::decode_from(request.body()).ok()?;
        if store.consumer.kind != KIND_CONSUMER_GROUP || store.ack != AckLevel::Quorum {
            return None;
        }
        let WireIdentifier::Numeric(group_id) = store.consumer.id else {
            return None;
        };
        let namespace = IggyNamespace::from_raw(header.group);
        let created_revision = self
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let geo = partition.places_batches() || partition.peer_log().is_some();
                (geo && partition.geo_positions().is_some()).then(|| partition.created_revision())
            })
            .flatten()?;
        Some(GeoCommitHold {
            namespace,
            group_id,
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
            group_id,
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
            group_id,
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
                |group_id| partition.geo_group_folded(group_id),
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
        let answers = {
            let partitions = self.plane.partitions();
            let mut led = BTreeMap::new();
            let mut groups = BTreeMap::new();
            self.geo_commits.held.borrow_mut().expire(
                now,
                micros(COMMIT_HOLD_TIMEOUT),
                |namespace, created_revision, group_id| {
                    let led = *led.entry((namespace, created_revision)).or_insert_with(|| {
                        partitions.get_by_ns(&namespace).is_some_and(|partition| {
                            partition.created_revision() == created_revision
                                && partition.state_machines_current()
                        })
                    });
                    if !led {
                        return CommitCheck::NotLed;
                    }
                    let exists = *groups
                        .entry((namespace.stream_id(), namespace.topic_id(), group_id))
                        .or_insert_with(|| self.geo_group_exists(namespace, group_id));
                    if exists {
                        CommitCheck::Waiting
                    } else {
                        CommitCheck::GroupGone
                    }
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
                "consumer group offset stores whose positions were not stored in time, or whose \
                 partitions are no longer led here, are answered as uncertain"
            );
        }
        for (reply, answer) in answers {
            self.answer_geo_commit(reply, answer);
        }
    }

    fn geo_group_exists(&self, namespace: IggyNamespace, group_id: u32) -> bool
    where
        M: StreamsFrontend,
    {
        self.plane.metadata().mux_stm.streams().read(|inner| {
            inner
                .items
                .get(namespace.stream_id())
                .and_then(|stream| stream.topics.get(namespace.topic_id()))
                .is_some_and(|topic| topic.consumer_groups.contains_key(&u64::from(group_id)))
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

    fn held(commits: &[(usize, u32, u64, &'static str)]) -> HeldCommits<&'static str> {
        let mut held = HeldCommits::default();
        for (partition_id, group_id, held_at, reply) in commits {
            held.hold(
                partition(*partition_id),
                *group_id,
                REVISION,
                *held_at,
                *reply,
            );
        }
        held
    }

    fn all_folded(_: u32) -> bool {
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

        let covered = held.cover(partition(0), REVISION, ROUND_ONE, &log(1), |group_id| {
            group_id == AUDIT
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
        assert_eq!(pacing.start(0, 1_000 * MS, Some(50 * MS)), Some(None));

        pacing.wait([(1, 1)]);
        pacing.wait([(1, 1), (1, 2)]);
        let round = pacing.start(60 * MS, 1_000 * MS, Some(50 * MS));
        let next = pacing.start(200 * MS, 1_000 * MS, Some(50 * MS));

        assert_eq!(round, Some(Some(vec![(1, 1), (1, 2)])));
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
            Some(Some(vec![(1, 1)]))
        );
        pacing.wait([(1, 1)]);
        assert_eq!(pacing.start(70 * MS, 1_000 * MS, Some(50 * MS)), None);
        assert_eq!(
            pacing.start(100 * MS, 1_000 * MS, Some(50 * MS)),
            Some(Some(vec![(1, 1)]))
        );
    }

    #[test]
    fn given_a_due_round_of_every_log_when_commits_wait_should_start_it_and_cover_them() {
        let mut pacing = RoundPacing::default();
        pacing.start(0, 1_000 * MS, Some(50 * MS));
        pacing.wait([(1, 1)]);

        assert_eq!(
            pacing.start(1_000 * MS, 1_000 * MS, Some(50 * MS)),
            Some(None)
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

    /// Whether `pacing` starts a round at `now`, left as it was.
    fn starts(pacing: &RoundPacing, now: u64) -> bool {
        pacing
            .clone()
            .start(now, 60_000 * MS, Some(50 * MS))
            .is_some()
    }
}
