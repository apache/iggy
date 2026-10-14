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

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::bridge::error::BridgeError;

/// How long one scan answers for. kafka-ui lists groups every 30 s, and a scan costs one Iggy call
/// per topic.
const SCAN_MAX_AGE: Duration = Duration::from_secs(10);

/// The Kafka groups one scan found holding offsets in Iggy.
#[derive(Debug, Clone)]
pub struct OffsetGroups {
    /// When the scan began.
    started: Instant,
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

    fn is_fresh(&self) -> bool {
        self.started.elapsed() < SCAN_MAX_AGE
    }
}

/// The groups of the last scan that ended, and at most one scan that runs.
pub(super) struct GroupIndex {
    published: Arc<watch::Sender<Published>>,
    /// One permit, held by the scan that runs.
    scanning: Arc<Semaphore>,
}

#[derive(Default)]
struct Published {
    /// Scans that ended, with groups or without. A caller waits for this count to move.
    ended: u64,
    /// The groups of the last scan that found them.
    groups: Option<Arc<OffsetGroups>>,
    /// The groups this gateway stored offsets for (`true`) or deleted (`false`) while a scan
    /// runs, in order. `None` while none runs. The scan can list a topic before such a change,
    /// so the changes go on top of what it finds.
    changes: Option<Vec<(String, bool)>>,
}

impl GroupIndex {
    pub(super) fn new() -> Self {
        Self {
            published: Arc::new(watch::Sender::new(Published::default())),
            scanning: Arc::new(Semaphore::new(1)),
        }
    }

    /// The groups of a scan that began less than `SCAN_MAX_AGE` ago. Otherwise starts a scan with
    /// `scan`, unless one runs, and waits for the scan that runs until `deadline`.
    ///
    /// A scan that fails, or that does not end by `deadline`, leaves the groups of the last scan
    /// that found them, and those are the answer. `None` when no scan found groups yet.
    pub(super) async fn groups<F, S>(&self, deadline: Instant, scan: F) -> Option<Arc<OffsetGroups>>
    where
        F: FnOnce() -> S,
        S: Future<Output = Result<BTreeSet<String>, BridgeError>> + Send + 'static,
    {
        let mut published = self.published.subscribe();
        let ended = {
            let current = published.borrow_and_update();
            if let Some(groups) = current.groups.as_ref().filter(|groups| groups.is_fresh()) {
                return Some(Arc::clone(groups));
            }
            current.ended
        };
        self.start(scan);
        // A timeout leaves the groups of the last scan as the answer.
        let _ = timeout_at(
            deadline,
            published.wait_for(|current| current.ended > ended),
        )
        .await;
        published.borrow().groups.clone()
    }

    /// Adds `group` to the groups of the last scan. A commit of this gateway created its offsets,
    /// so it shows before the next scan.
    pub(super) fn note(&self, group: &str) {
        self.change(group, true);
    }

    /// Removes `group` from the groups of the last scan. This gateway deleted its offsets, so it
    /// goes before the next scan.
    pub(super) fn forget(&self, group: &str) {
        self.change(group, false);
    }

    fn change(&self, group: &str, holds: bool) {
        self.published.send_if_modified(|current| {
            if let Some(changes) = current.changes.as_mut() {
                changes.push((group.to_owned(), holds));
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
        S: Future<Output = Result<BTreeSet<String>, BridgeError>> + Send + 'static,
    {
        let Ok(permit) = Arc::clone(&self.scanning).try_acquire_owned() else {
            return;
        };
        let published = Arc::clone(&self.published);
        published.send_if_modified(|current| {
            current.changes = Some(Vec::new());
            false
        });
        let started = Instant::now();
        let scanning = scan();
        tokio::spawn(async move {
            let found = scanning.await;
            if let Err(error) = &found {
                warn!(%error, "could not list the Kafka groups that hold offsets in Iggy");
            }
            published.send_modify(|current| {
                current.ended += 1;
                let changes = current.changes.take().unwrap_or_default();
                if let Ok(mut groups) = found {
                    for (group, holds) in changes {
                        apply(&mut groups, group, holds);
                    }
                    current.groups = Some(Arc::new(OffsetGroups { started, groups }));
                }
            });
            drop(permit);
        });
    }
}

fn apply(groups: &mut BTreeSet<String>, group: String, holds: bool) {
    if holds {
        groups.insert(group);
    } else {
        groups.remove(&group);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iggy::prelude::IggyError;
    use tokio::sync::oneshot;

    use super::*;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn found(groups: &[&str]) -> BTreeSet<String> {
        groups.iter().map(|group| (*group).to_owned()).collect()
    }

    fn names(groups: Option<Arc<OffsetGroups>>) -> Vec<String> {
        groups
            .expect("a scan found groups")
            .iter()
            .map(str::to_owned)
            .collect()
    }

    /// A scan that counts its runs and finds `groups`.
    fn counted(
        runs: &Arc<AtomicUsize>,
        groups: &'static [&'static str],
    ) -> impl FnOnce() -> std::future::Ready<Result<BTreeSet<String>, BridgeError>> {
        let runs = Arc::clone(runs);
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(found(groups)))
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

    #[tokio::test(start_paused = true)]
    async fn given_a_scan_older_than_its_max_age_should_scan_again() {
        let index = GroupIndex::new();
        let runs = Arc::new(AtomicUsize::new(0));

        index.groups(later(), counted(&runs, &["orders"])).await;
        tokio::time::advance(SCAN_MAX_AGE).await;
        let second = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(second), ["payments"]);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
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
                Ok(found(&["orders"]))
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
        tokio::time::advance(SCAN_MAX_AGE).await;

        let failed = index
            .groups(later(), || async {
                Err(BridgeError::Iggy(IggyError::CannotEstablishConnection))
            })
            .await;

        assert_eq!(names(failed), ["orders"]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_scan_that_found_groups_when_a_scan_fails_should_answer_none() {
        let index = GroupIndex::new();

        let failed = index
            .groups(later(), || async {
                Err(BridgeError::Iggy(IggyError::CannotEstablishConnection))
            })
            .await;

        assert!(failed.is_none());
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
                    Ok(found(&["orders"]))
                },
            )
            .await;
        assert!(late.is_none(), "no scan found groups by the deadline");

        release.send(()).expect("the scan still runs");
        tokio::task::yield_now().await;
        let next = index.groups(later(), counted(&runs, &["payments"])).await;

        assert_eq!(names(next), ["orders"]);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "the scan that ended is fresh"
        );
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
            Ok(found(&["deleted", "kept"]))
        };

        let (groups, ()) = tokio::join!(index.groups(later(), stale), async {
            tokio::task::yield_now().await;
            index.forget("deleted");
            index.note("stored");
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
}
