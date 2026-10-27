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

//! The Kafka groups that hold offsets in Iggy, for the group admin APIs. Finding them costs one
//! Iggy call per topic, so one scan answers every request for a while.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use super::TopicTarget;
use crate::bridge::error::BridgeError;

/// How long the groups of a scan answer, counted from the end of the scan, before a caller starts
/// a new one. The requests of one `kafka-consumer-groups.sh` run, a `ListGroups` and then a
/// `DescribeGroups`, then share a scan, which costs one Iggy call per topic.
const SCAN_MAX_AGE: Duration = Duration::from_secs(10);

/// How long the groups of a scan still answer at once while a newer scan runs. So kafka-ui, which
/// lists groups every 30 s, never waits for a scan. A caller that finds only older groups waits
/// for the newer scan, until its deadline.
const SCAN_STALE_LIMIT: Duration = Duration::from_mins(1);

/// How long `DeleteGroups` also deletes on a topic that a commit through this gateway stored
/// offsets to, counted from the end of the commit. It lists the groups on the scan client, which
/// can read a replica that missed the commit.
const STORED_TOPICS_MAX_AGE: Duration = Duration::from_mins(1);

/// How long `DeleteGroups` answers 0, once, for a group that this gateway deleted but no longer
/// finds. A deletion can end after its request answered 14, and the Java admin client retries
/// that request for up to 60 s (`default.api.timeout.ms`).
const DELETED_MAX_AGE: Duration = Duration::from_mins(1);

/// The Kafka groups one scan found holding offsets in Iggy.
#[derive(Debug, Clone)]
pub struct OffsetGroups {
    /// When the scan ended.
    ended: Instant,
    groups: BTreeSet<String>,
}

impl OffsetGroups {
    #[must_use]
    pub fn contains(&self, group_id: &str) -> bool {
        self.groups.contains(group_id)
    }

    /// The group ids, in order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.groups.iter().map(String::as_str)
    }

    fn age(&self) -> Duration {
        self.ended.elapsed()
    }
}

/// What a caller learns about the Kafka groups that hold offsets in Iggy.
#[derive(Debug, Clone)]
pub enum ScannedGroups {
    /// The groups of the last scan that found them.
    Found(Arc<OffsetGroups>),
    /// The first scan has not ended yet.
    Pending,
    /// Every scan that ended failed, so no scan found the groups.
    Failed,
}

/// What one scan found.
pub(super) struct Scanned {
    pub(super) groups: BTreeSet<String>,
    /// `false` when the scan stopped at its time limit before it listed every topic.
    pub(super) whole: bool,
}

/// The groups of the last scan that ended, and at most one scan that runs.
pub(super) struct GroupIndex {
    published: Arc<watch::Sender<Published>>,
    /// One permit, held by the scan that runs.
    scanning: Arc<Semaphore>,
    stored: StoredTopics,
    deleted: DeletedGroups,
}

/// Per group, the topics that commits through this gateway stored offsets to. See
/// `STORED_TOPICS_MAX_AGE`.
#[derive(Clone)]
pub(super) struct StoredTopics(Arc<Mutex<Stores>>);

struct Stores {
    groups: HashMap<String, HashMap<TopicTarget, Stored>>,
    /// When the topics past `STORED_TOPICS_MAX_AGE` last went.
    pruned: Instant,
}

/// The last commit that stored offsets of a group to a topic.
#[derive(Clone, Copy)]
struct Stored {
    ended: Instant,
    /// The numeric id of the Iggy group there, when a commit through this gateway created it.
    id: Option<u32>,
}

/// The commits of one request that store offsets of `group`. The topics they stored to count
/// when this drops, once none of those commits can land.
pub(super) struct Storing {
    stored: StoredTopics,
    group: String,
    /// Each topic that a commit stored to, or may have, with the numeric id of the Iggy group that
    /// a commit created there.
    topics: Mutex<HashMap<TopicTarget, Option<u32>>>,
}

/// The groups whose offsets this gateway deleted, and when. See `DELETED_MAX_AGE`.
#[derive(Clone, Default)]
pub(super) struct DeletedGroups(Arc<Mutex<HashMap<String, Instant>>>);

#[derive(Default)]
struct Published {
    /// Scans that ended, with groups or without. A caller waits for this count to move.
    ended: u64,
    /// The groups of the last scan that found them.
    groups: Option<Arc<OffsetGroups>>,
    /// The last change of each group while a scan runs: this gateway stored offsets for it
    /// (`true`) or deleted them (`false`). `None` while none runs. The scan can list a topic
    /// before such a change, so the changes go on top of what it finds.
    changes: Option<HashMap<String, bool>>,
}

impl GroupIndex {
    pub(super) fn new() -> Self {
        Self {
            published: Arc::new(watch::Sender::new(Published::default())),
            scanning: Arc::new(Semaphore::new(1)),
            stored: StoredTopics(Arc::new(Mutex::new(Stores {
                groups: HashMap::new(),
                pruned: Instant::now(),
            }))),
            deleted: DeletedGroups::default(),
        }
    }

    /// The groups of the last scan, at once while that scan ended less than `SCAN_MAX_AGE` ago.
    /// An older scan starts a new one with `scan`, unless one runs. The groups of a scan that ended
    /// less than `SCAN_STALE_LIMIT` ago still answer at once. Otherwise the caller waits for the
    /// scan that runs, until `deadline`.
    ///
    /// A scan that fails, or that does not end by `deadline`, leaves the groups of the last scan
    /// that found them, and those are the answer.
    pub(super) async fn groups<F, S>(&self, deadline: Instant, scan: F) -> ScannedGroups
    where
        F: FnOnce() -> S,
        S: Future<Output = Result<Scanned, BridgeError>> + Send + 'static,
    {
        let mut published = self.published.subscribe();
        let (last, ended) = {
            let current = published.borrow_and_update();
            (current.groups.clone(), current.ended)
        };
        match last {
            Some(groups) if groups.age() < SCAN_MAX_AGE => return ScannedGroups::Found(groups),
            Some(groups) if groups.age() < SCAN_STALE_LIMIT => {
                self.start(scan);
                return ScannedGroups::Found(groups);
            }
            _ => self.start(scan),
        }
        // A timeout leaves the groups of the last scan as the answer.
        let _ = timeout_at(
            deadline,
            published.wait_for(|current| current.ended > ended),
        )
        .await;
        let current = published.borrow();
        match &current.groups {
            Some(groups) => ScannedGroups::Found(Arc::clone(groups)),
            None if current.ended == 0 => ScannedGroups::Pending,
            None => ScannedGroups::Failed,
        }
    }

    /// The groups of the last scan that found them, without a scan.
    pub(super) fn last(&self) -> Option<Arc<OffsetGroups>> {
        self.published.borrow().groups.clone()
    }

    /// Adds `group` to the groups of the last scan. A commit of this gateway created its offsets,
    /// so it shows before the next scan.
    pub(super) fn note(&self, group: &str) {
        self.change(group, true);
    }

    /// The commits of one request that store offsets of `group`. See [`Storing`].
    pub(super) fn storing(&self, group: &str) -> Storing {
        Storing {
            stored: self.stored.clone(),
            group: group.to_owned(),
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// The topics that commits through this gateway stored offsets of `group` to, each with the
    /// numeric id of the Iggy group there when one of those commits created it. Only those of
    /// commits that ended in the last `STORED_TOPICS_MAX_AGE`.
    pub(super) fn stored_topics(&self, group: &str) -> Vec<(TopicTarget, Option<u32>)> {
        lock(&self.stored.0)
            .groups
            .get(group)
            .map_or_else(Vec::new, |topics| {
                topics
                    .iter()
                    .filter(|(_, stored)| stored.ended.elapsed() < STORED_TOPICS_MAX_AGE)
                    .map(|(topic, stored)| (topic.clone(), stored.id))
                    .collect()
            })
    }

    /// Removes `group` from the groups of the last scan, with the topics it was stored to. This
    /// gateway deleted its offsets, so it goes before the next scan.
    pub(super) fn forget(&self, group: &str) {
        self.change(group, false);
        lock(&self.stored.0).groups.remove(group);
    }

    pub(super) const fn deleted(&self) -> &DeletedGroups {
        &self.deleted
    }

    fn change(&self, group: &str, holds: bool) {
        self.published.send_if_modified(|current| {
            if let Some(changes) = current.changes.as_mut() {
                if let Some(held) = changes.get_mut(group) {
                    *held = holds;
                } else {
                    changes.insert(group.to_owned(), holds);
                }
            }
            let Some(groups) = current.groups.as_mut() else {
                return false;
            };
            if groups.contains(group) == holds {
                return false;
            }
            apply(&mut Arc::make_mut(groups).groups, group.to_owned(), holds);
            true
        });
    }

    /// Runs `scan` in a task of its own, so it ends even when no caller waits for it any more.
    /// Does nothing while another scan runs.
    fn start<F, S>(&self, scan: F)
    where
        F: FnOnce() -> S,
        S: Future<Output = Result<Scanned, BridgeError>> + Send + 'static,
    {
        let Ok(permit) = Arc::clone(&self.scanning).try_acquire_owned() else {
            return;
        };
        let published = Arc::clone(&self.published);
        published.send_if_modified(|current| {
            current.changes = Some(HashMap::new());
            false
        });
        let scanning = scan();
        tokio::spawn(async move {
            let found = scanning.await;
            if let Err(error) = &found {
                warn!(%error, "could not list the Kafka groups that hold offsets in Iggy");
            }
            published.send_modify(|current| {
                current.ended += 1;
                let changes = current.changes.take().unwrap_or_default();
                let Ok(Scanned { mut groups, whole }) = found else {
                    return;
                };
                if !whole && let Some(last) = &current.groups {
                    // The topics that the scan did not reach keep the groups of the last scan.
                    groups.extend(last.groups.iter().cloned());
                }
                for (group, holds) in changes {
                    apply(&mut groups, group, holds);
                }
                current.groups = Some(Arc::new(OffsetGroups {
                    ended: Instant::now(),
                    groups,
                }));
            });
            drop(permit);
        });
    }
}

impl StoredTopics {
    /// Notes that commits that ended now stored offsets of `group` to each topic of `topics`. A
    /// topic without an id keeps the id noted before.
    fn record(&self, group: &str, topics: Vec<(TopicTarget, Option<u32>)>) {
        if topics.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut noted = lock(&self.0);
        noted.prune(now);
        if let Some(stored) = noted.groups.get_mut(group) {
            refresh(stored, topics, now);
        } else {
            let mut stored = HashMap::with_capacity(topics.len());
            refresh(&mut stored, topics, now);
            noted.groups.insert(group.to_owned(), stored);
        }
    }
}

impl Stores {
    /// Drops the topics past `STORED_TOPICS_MAX_AGE`, at most once in that time.
    fn prune(&mut self, now: Instant) {
        if now.duration_since(self.pruned) < STORED_TOPICS_MAX_AGE {
            return;
        }
        self.pruned = now;
        self.groups.retain(|_, topics| {
            topics.retain(|_, stored| now.duration_since(stored.ended) < STORED_TOPICS_MAX_AGE);
            !topics.is_empty()
        });
    }
}

impl Storing {
    /// Notes that a commit stored offsets to `topic`, or may have, and the id of the Iggy group
    /// there when it `created` that group.
    pub(super) fn stored(&self, topic: &TopicTarget, created: Option<u32>) {
        let mut topics = lock(&self.topics);
        if let Some(id) = topics.get_mut(topic) {
            *id = created.or(*id);
        } else {
            topics.insert(topic.clone(), created);
        }
    }
}

impl Drop for Storing {
    fn drop(&mut self) {
        let topics = std::mem::take(
            self.topics
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner),
        );
        self.stored
            .record(&self.group, topics.into_iter().collect());
    }
}

impl DeletedGroups {
    /// Notes that the offsets of `group` went from a topic.
    pub(super) fn record(&self, group: &str) {
        let now = Instant::now();
        let mut deleted = lock(&self.0);
        deleted.retain(|_, at| now.duration_since(*at) < DELETED_MAX_AGE);
        deleted.insert(group.to_owned(), now);
    }

    /// Whether the offsets of `group` went in the last `DELETED_MAX_AGE`. Forgets that they did.
    pub(super) fn take(&self, group: &str) -> bool {
        lock(&self.0)
            .remove(group)
            .is_some_and(|at| at.elapsed() < DELETED_MAX_AGE)
    }
}

fn apply(groups: &mut BTreeSet<String>, group: String, holds: bool) {
    if holds {
        groups.insert(group);
    } else {
        groups.remove(&group);
    }
}

fn refresh(
    stored: &mut HashMap<TopicTarget, Stored>,
    topics: Vec<(TopicTarget, Option<u32>)>,
    ended: Instant,
) {
    for (topic, id) in topics {
        match stored.get_mut(&topic) {
            Some(last) => {
                last.ended = ended;
                last.id = id.or(last.id);
            }
            None => {
                stored.insert(topic, Stored { ended, id });
            }
        }
    }
}

/// Each update is whole, so the state stays sound after a panic elsewhere.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iggy::prelude::{Identifier, IggyError};
    use tokio::sync::oneshot;

    use super::*;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn target(topic: &str) -> TopicTarget {
        TopicTarget {
            stream_id: Identifier::named("kafka").unwrap(),
            topic_id: Identifier::named(topic).unwrap(),
        }
    }

    fn whole(groups: &[&str]) -> Scanned {
        Scanned {
            groups: groups.iter().map(|group| (*group).to_owned()).collect(),
            whole: true,
        }
    }

    fn names(groups: ScannedGroups) -> Vec<String> {
        let ScannedGroups::Found(groups) = groups else {
            panic!("a scan found groups: {groups:?}");
        };
        groups.iter().map(str::to_owned).collect()
    }

    /// A scan that counts its runs and finds `groups`.
    fn counted(
        runs: &Arc<AtomicUsize>,
        groups: &'static [&'static str],
    ) -> impl FnOnce() -> std::future::Ready<Result<Scanned, BridgeError>> {
        let runs = Arc::clone(runs);
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(whole(groups)))
        }
    }

    fn failing() -> std::future::Ready<Result<Scanned, BridgeError>> {
        std::future::ready(Err(BridgeError::Iggy(IggyError::CannotEstablishConnection)))
    }

    /// Lets the task of a scan that already ended publish its groups.
    async fn settle() {
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fresh_scan_should_answer_from_it_without_scanning_again() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));

        let first = index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_MAX_AGE.saturating_sub(Duration::from_millis(1))).await;
        let second = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(first), ["orders"]);
        assert_eq!(names(second), ["orders"]);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// Groups older than `SCAN_MAX_AGE` still answer at once, and the scan they start serves the
    /// next caller.
    #[tokio::test(start_paused = true)]
    async fn given_an_old_scan_should_answer_from_it_at_once_and_scan_again() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_MAX_AGE).await;

        let old = index
            .groups(Instant::now(), counted(&runs, &["payments"]))
            .await;
        settle().await;
        let new = index.groups(later(), counted(&runs, &["audit"])).await;

        assert_eq!(names(old), ["orders"], "no wait for the new scan");
        assert_eq!(names(new), ["payments"]);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_scan_older_than_the_stale_limit_should_wait_for_a_new_one() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_STALE_LIMIT).await;

        let second = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(second), ["payments"]);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    /// A slow scan's groups are as new as its end, so they answer for `SCAN_MAX_AGE` after it.
    #[tokio::test(start_paused = true)]
    async fn given_a_scan_slower_than_its_max_age_should_count_its_age_from_its_end() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let slow = || async {
            tokio::time::sleep(SCAN_MAX_AGE * 2).await;
            Ok(whole(&["orders"]))
        };

        let first = index.groups(later() + SCAN_MAX_AGE * 2, slow).await;
        let second = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(first), ["orders"]);
        assert_eq!(names(second), ["orders"]);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    /// Callers that come while a scan runs wait for that scan rather than start their own.
    #[tokio::test(start_paused = true)]
    async fn given_callers_during_a_scan_should_share_that_scan() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let (release, released) = oneshot::channel::<()>();
        let first_runs = Arc::clone(&runs);
        let blocked = move || {
            first_runs.fetch_add(1, Ordering::SeqCst);
            async move {
                let _ = released.await;
                Ok(whole(&["orders"]))
            }
        };

        let (first, second, ()) = tokio::join!(
            index.groups(later(), blocked),
            index.groups(later(), counted(&runs, &["payments"])),
            async {
                tokio::task::yield_now().await;
                release.send(()).expect("the scan waits for its release");
            },
        );

        assert_eq!(names(first), ["orders"]);
        assert_eq!(names(second), ["orders"]);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_failed_scan_should_answer_the_groups_of_the_last_scan_that_found_them() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_STALE_LIMIT).await;

        let failed = index.groups(later(), failing).await;

        assert_eq!(names(failed), ["orders"]);
    }

    /// With Iggy down, every scan fails. The callers then answer from the live groups instead of
    /// waiting for a scan that may never find the groups.
    #[tokio::test(start_paused = true)]
    async fn given_no_scan_that_found_groups_when_a_scan_fails_should_answer_failed() {
        let index = GroupIndex::new();

        let failed = index.groups(later(), failing).await;

        assert!(matches!(failed, ScannedGroups::Failed), "{failed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_first_scan_past_the_deadline_should_answer_pending() {
        let index = GroupIndex::new();

        let pending = index
            .groups(Instant::now() + Duration::from_millis(20), || {
                std::future::pending::<Result<Scanned, BridgeError>>()
            })
            .await;

        assert!(matches!(pending, ScannedGroups::Pending), "{pending:?}");
    }

    /// A scan past the deadline of its caller keeps running, and the next caller gets its groups
    /// without another scan.
    #[tokio::test(start_paused = true)]
    async fn given_a_scan_past_the_deadline_should_finish_for_the_next_caller() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let (release, released) = oneshot::channel::<()>();

        let late = index
            .groups(
                Instant::now() + Duration::from_millis(20),
                move || async move {
                    let _ = released.await;
                    Ok(whole(&["orders"]))
                },
            )
            .await;
        assert!(matches!(late, ScannedGroups::Pending), "{late:?}");

        release.send(()).expect("the scan still runs");
        settle().await;
        let next = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(next), ["orders"]);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "the scan that ended is fresh"
        );
    }

    /// A scan that stops at its time limit did not list some topics. Those keep the groups that
    /// the last scan found there.
    #[tokio::test(start_paused = true)]
    async fn given_a_partial_scan_should_keep_the_groups_of_the_last_scan_too() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_STALE_LIMIT).await;

        let partial = index
            .groups(later(), || {
                std::future::ready(Ok(Scanned {
                    groups: BTreeSet::from(["payments".to_owned()]),
                    whole: false,
                }))
            })
            .await;

        assert_eq!(names(partial), ["orders", "payments"]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_forgotten_group_should_leave_it_out_before_the_next_scan() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index
            .groups(later(), counted(&runs, &["orders", "payments"]))
            .await;

        index.forget("orders");
        index.forget("never-scanned");
        let groups = index.groups(later(), counted(&runs, &[])).await;

        assert_eq!(names(groups), ["payments"]);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// A scan can list a topic before this gateway deletes or stores offsets there. What it finds
    /// must not undo those changes.
    #[tokio::test(start_paused = true)]
    async fn given_changes_while_a_scan_runs_should_apply_them_to_what_it_finds() {
        let index = GroupIndex::new();
        let (release, released) = oneshot::channel::<()>();
        let stale = move || async move {
            let _ = released.await;
            Ok(whole(&["deleted", "kept", "flipped"]))
        };

        let (groups, ()) = tokio::join!(index.groups(later(), stale), async {
            tokio::task::yield_now().await;
            index.forget("deleted");
            index.note("stored");
            index.note("flipped");
            index.forget("flipped");
            release.send(()).expect("the scan waits for its release");
        });

        assert_eq!(names(groups), ["kept", "stored"]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_noted_group_should_answer_it_before_the_next_scan() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));
        index.note("before-any-scan");
        index.groups(later(), counted(&runs, &["orders"])).await;

        index.note("payments");
        index.note("orders");
        let groups = index.groups(later(), counted(&runs, &[])).await;

        assert_eq!(names(groups), ["orders", "payments"]);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_scan_should_have_no_last_groups() {
        let index = GroupIndex::new();
        assert!(index.last().is_none());

        index
            .groups(later(), || std::future::ready(Ok(whole(&["orders"]))))
            .await;

        let last = index.last().expect("the scan found groups");
        assert!(last.contains("orders"));
    }

    /// The commits of one request that ended now stored offsets of `group` to `topics`.
    fn stored(index: &GroupIndex, group: &str, topics: &[&str]) {
        let storing = index.storing(group);
        for topic in topics {
            storing.stored(&target(topic), None);
        }
    }

    /// The listing of `DeleteGroups` can miss a recent commit, so it also deletes on the topics
    /// that commits through this gateway stored to lately.
    #[tokio::test(start_paused = true)]
    async fn given_stored_topics_should_name_them_for_their_group_until_they_are_old() {
        let index = GroupIndex::new();
        stored(&index, "orders-app", &["orders", "payments"]);
        stored(&index, "audit-app", &["audit"]);

        let named: HashSet<(TopicTarget, Option<u32>)> =
            index.stored_topics("orders-app").into_iter().collect();
        assert_eq!(
            named,
            HashSet::from([(target("orders"), None), (target("payments"), None)])
        );
        assert_eq!(index.stored_topics("audit-app"), [(target("audit"), None)]);
        assert_eq!(index.stored_topics("never-stored"), []);

        tokio::time::advance(STORED_TOPICS_MAX_AGE).await;
        assert_eq!(index.stored_topics("orders-app"), []);
        stored(&index, "billing-app", &["billing"]);
        assert_eq!(
            lock(&index.stored.0).groups.len(),
            1,
            "a later commit drops the old topics"
        );
    }

    /// A commit can still land until its task ends, so its age counts from then.
    #[tokio::test(start_paused = true)]
    async fn given_a_commit_that_still_runs_should_count_its_topics_from_its_end() {
        let index = GroupIndex::new();
        let storing = index.storing("orders-app");
        storing.stored(&target("orders"), None);
        assert_eq!(index.stored_topics("orders-app"), []);

        tokio::time::advance(STORED_TOPICS_MAX_AGE).await;
        drop(storing);
        tokio::time::advance(STORED_TOPICS_MAX_AGE / 2).await;

        assert_eq!(
            index.stored_topics("orders-app"),
            [(target("orders"), None)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_topic_stored_to_again_should_count_its_age_from_the_last_commit() {
        let index = GroupIndex::new();
        stored(&index, "orders-app", &["orders"]);
        tokio::time::advance(STORED_TOPICS_MAX_AGE / 2).await;

        stored(&index, "orders-app", &["orders"]);
        tokio::time::advance(STORED_TOPICS_MAX_AGE / 2).await;

        assert_eq!(
            index.stored_topics("orders-app"),
            [(target("orders"), None)]
        );
    }

    /// A read can miss a group that a commit just created, so the id from the create counts.
    /// A later commit that only stores there keeps it, and a later create replaces it.
    #[tokio::test(start_paused = true)]
    async fn given_a_commit_that_created_the_iggy_group_should_keep_its_id() {
        let index = GroupIndex::new();
        let storing = index.storing("orders-app");
        storing.stored(&target("orders"), Some(7));
        storing.stored(&target("payments"), None);
        storing.stored(&target("orders"), None);
        drop(storing);

        let named: HashSet<(TopicTarget, Option<u32>)> =
            index.stored_topics("orders-app").into_iter().collect();
        assert_eq!(
            named,
            HashSet::from([(target("orders"), Some(7)), (target("payments"), None)])
        );

        stored(&index, "orders-app", &["orders"]);
        assert!(
            index
                .stored_topics("orders-app")
                .contains(&(target("orders"), Some(7)))
        );
        let storing = index.storing("orders-app");
        storing.stored(&target("orders"), Some(8));
        drop(storing);
        assert!(
            index
                .stored_topics("orders-app")
                .contains(&(target("orders"), Some(8)))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_forgotten_group_should_drop_its_stored_topics() {
        let index = GroupIndex::new();
        stored(&index, "orders-app", &["orders"]);

        index.forget("orders-app");

        assert_eq!(index.stored_topics("orders-app"), []);
    }

    /// A deletion can end after its request answered 14. The retry then finds no offsets, and
    /// answers for that deletion once.
    #[tokio::test(start_paused = true)]
    async fn given_a_recorded_deletion_should_answer_once_while_it_is_fresh() {
        let deleted = DeletedGroups::default();
        deleted.record("orders-app");
        deleted.record("payments-app");

        assert!(deleted.take("orders-app"));
        assert!(!deleted.take("orders-app"), "only once");
        tokio::time::advance(DELETED_MAX_AGE).await;
        assert!(!deleted.take("payments-app"), "too old");
    }
}
