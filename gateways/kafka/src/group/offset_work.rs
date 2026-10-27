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

//! Keeps each deletion of a group's offsets apart from the group's joins and commits.
//!
//! Kafka's coordinator takes the requests of one group one at a time. Here a deletion makes many
//! Iggy calls, so a join or a commit of the group could land in the middle. While a deletion runs,
//! the coordinator refuses the group's joins and commits, and the deletion waits for each commit
//! that passed its check before the deletion began. So a commit lands whole before the deletion or
//! whole after it, and no member subscribes to a topic between the check of `OffsetDelete` and its
//! deletions.
//!
//! A commit or a deletion runs until none of its Iggy calls can land, not until its request is
//! answered. A call that its request stopped waiting for goes on and can still land, and so can a
//! call that ended in doubt until a replay proves its outcome. So the call keeps its
//! [`RunningCommit`] or [`RunningDeletion`] until then.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kafka_protocol::protocol::StrBytes;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

/// The commits and the offset deletions that run for each group.
pub(super) struct OffsetWork {
    /// A group with no commit and no deletion that runs has no entry. Updated under the
    /// coordinator's lock, and by a drop, which cannot wait for that lock.
    running: Mutex<HashMap<StrBytes, Running>>,
}

#[derive(Default)]
struct Running {
    commits: usize,
    /// Each deletion of the group's offsets that runs, as the count of commits it waits for. A
    /// commit that ends counts down each of them, since each began while the commit ran.
    deletions: Vec<Arc<watch::Sender<usize>>>,
}

impl OffsetWork {
    pub(super) fn new() -> Self {
        Self {
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Whether a deletion of the offsets of `group_id` runs.
    pub(super) fn deleting(&self, group_id: &StrBytes) -> bool {
        self.lock()
            .get(group_id)
            .is_some_and(|running| !running.deletions.is_empty())
    }

    /// Counts a commit of `group_id` until the returned value drops. `None` while a deletion of
    /// the group's offsets runs.
    pub(super) fn begin_commit(self: &Arc<Self>, group_id: &StrBytes) -> Option<RunningCommit> {
        let group_id = super::owned_str(group_id);
        let mut running = self.lock();
        let group = running.entry(group_id.clone()).or_default();
        if !group.deletions.is_empty() {
            return None;
        }
        group.commits += 1;
        drop(running);
        Some(RunningCommit {
            work: Arc::clone(self),
            group_id,
        })
    }

    /// Counts a deletion of the offsets of each group of `group_ids` until the returned value
    /// drops. The deletion waits for the commits of those groups that run now.
    pub(super) fn begin_deletion<'g>(
        self: &Arc<Self>,
        group_ids: impl IntoIterator<Item = &'g StrBytes>,
    ) -> RunningDeletion {
        let group_ids: Vec<StrBytes> = group_ids.into_iter().map(super::owned_str).collect();
        let waited = Arc::new(watch::Sender::new(0));
        let mut running = self.lock();
        let mut commits = 0;
        for group_id in &group_ids {
            let group = running.entry(group_id.clone()).or_default();
            commits += group.commits;
            group.deletions.push(Arc::clone(&waited));
        }
        // Under the lock, so no commit ends before it is counted.
        waited.send_replace(commits);
        drop(running);
        RunningDeletion {
            work: Arc::clone(self),
            group_ids,
            waited,
        }
    }

    /// Each update is whole, so the map stays sound after a panic elsewhere.
    fn lock(&self) -> MutexGuard<'_, HashMap<StrBytes, Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A commit of one group, counted until it drops. Keep it until no Iggy call of the commit can
/// land.
pub struct RunningCommit {
    work: Arc<OffsetWork>,
    group_id: StrBytes,
}

impl Drop for RunningCommit {
    fn drop(&mut self) {
        let mut running = self.work.lock();
        let Some(group) = running.get_mut(&self.group_id) else {
            return;
        };
        group.commits = group.commits.saturating_sub(1);
        for waited in &group.deletions {
            waited.send_modify(|commits| *commits = commits.saturating_sub(1));
        }
        if group.commits == 0 && group.deletions.is_empty() {
            running.remove(&self.group_id);
        }
    }
}

/// A deletion of the offsets of some groups, counted until it drops. Keep it until no Iggy call
/// of the deletion can land.
pub struct RunningDeletion {
    work: Arc<OffsetWork>,
    group_ids: Vec<StrBytes>,
    /// The commits of those groups that ran when the deletion began and still run.
    waited: Arc<watch::Sender<usize>>,
}

impl RunningDeletion {
    /// Waits until no commit of the groups runs. `false` when one still runs at `deadline`.
    pub async fn commits_ended(&self, deadline: Instant) -> bool {
        let mut waited = self.waited.subscribe();
        matches!(
            timeout_at(deadline, waited.wait_for(|commits| *commits == 0)).await,
            Ok(Ok(_))
        )
    }
}

impl Drop for RunningDeletion {
    fn drop(&mut self) {
        let mut running = self.work.lock();
        for group_id in &self.group_ids {
            let Some(group) = running.get_mut(group_id) else {
                continue;
            };
            if let Some(at) = group
                .deletions
                .iter()
                .position(|waited| Arc::ptr_eq(waited, &self.waited))
            {
                group.deletions.swap_remove(at);
            }
            if group.commits == 0 && group.deletions.is_empty() {
                running.remove(group_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn id(group: &str) -> StrBytes {
        StrBytes::from_string(group.to_owned())
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn work() -> Arc<OffsetWork> {
        Arc::new(OffsetWork::new())
    }

    #[test]
    fn given_a_deletion_should_refuse_commits_of_its_groups_until_it_ends() {
        let work = work();
        let deletion = work.begin_deletion(&[id("orders"), id("payments")]);

        assert!(work.deleting(&id("orders")));
        assert!(work.begin_commit(&id("orders")).is_none());
        assert!(work.begin_commit(&id("payments")).is_none());
        assert!(
            work.begin_commit(&id("audit")).is_some(),
            "another group commits"
        );

        drop(deletion);
        assert!(!work.deleting(&id("orders")));
        assert!(work.begin_commit(&id("orders")).is_some());
    }

    #[test]
    fn given_two_deletions_of_a_group_should_refuse_commits_until_both_end() {
        let work = work();
        let first = work.begin_deletion(&[id("orders")]);
        let second = work.begin_deletion(&[id("orders")]);

        drop(first);
        assert!(work.begin_commit(&id("orders")).is_none());
        drop(second);
        assert!(work.begin_commit(&id("orders")).is_some());
    }

    #[test]
    fn given_every_commit_and_deletion_ended_should_keep_no_entry() {
        let work = work();
        let commits = [
            work.begin_commit(&id("orders")),
            work.begin_commit(&id("orders")),
        ];
        let deletion = work.begin_deletion(&[id("orders"), id("payments")]);

        drop(commits);
        drop(deletion);

        assert!(work.lock().is_empty());
    }

    /// A commit that passed its check before the deletion began lands whole before it.
    #[tokio::test(start_paused = true)]
    async fn given_a_commit_that_runs_should_start_the_deletion_after_it_ends() {
        let work = work();
        let commit = work.begin_commit(&id("orders"));
        let deletion = work.begin_deletion(&[id("orders")]);

        let (ended, ()) = tokio::join!(deletion.commits_ended(later()), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(commit);
        });

        assert!(ended);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_commit_past_the_deadline_should_report_that_it_still_runs() {
        let work = work();
        let _commit = work.begin_commit(&id("orders"));
        let deletion = work.begin_deletion(&[id("orders")]);

        assert!(
            !deletion
                .commits_ended(Instant::now() + Duration::from_millis(20))
                .await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_commits_of_other_groups_should_not_wait_for_them() {
        let work = work();
        let _commit = work.begin_commit(&id("payments"));
        let deletion = work.begin_deletion(&[id("orders")]);

        assert!(deletion.commits_ended(later()).await);
    }

    /// Each deletion waits only for the commits of its own groups, so one with many groups costs
    /// the commits of other groups nothing.
    #[tokio::test(start_paused = true)]
    async fn given_two_deletions_when_a_commit_of_one_ends_should_free_only_that_one() {
        let work = work();
        let orders = work.begin_commit(&id("orders"));
        let _payments = work.begin_commit(&id("payments"));
        let of_orders = work.begin_deletion(&[id("orders")]);
        let of_both = work.begin_deletion(&[id("orders"), id("payments")]);

        drop(orders);

        assert!(of_orders.commits_ended(later()).await);
        assert!(
            !of_both
                .commits_ended(Instant::now() + Duration::from_millis(20))
                .await,
            "the commit of payments still runs"
        );
    }

    /// A commit moved into a task counts until the task ends, as a commit that its request stopped
    /// waiting for does.
    #[tokio::test(start_paused = true)]
    async fn given_a_commit_held_by_a_task_should_count_until_the_task_ends() {
        let work = work();
        let commit = work.begin_commit(&id("orders")).expect("no deletion runs");
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = released.await;
            drop(commit);
        });
        let deletion = work.begin_deletion(&[id("orders")]);

        assert!(
            !deletion
                .commits_ended(Instant::now() + Duration::from_millis(20))
                .await
        );
        release.send(()).expect("the task waits for its release");
        task.await.expect("the task ends");
        assert!(deletion.commits_ended(later()).await);
    }
}
