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

use iggy_binary_protocol::primitives::partition_status::PartitionStatusEntry;
use server_common::sharding::IggyNamespace;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// What each partition's primary last reported, as this node heard it.
///
/// Node-local and never replicated: the metadata primary collects it from the
/// other nodes' metadata replies and hands it back out in its probes, so a
/// node answers for partitions it does not store.
#[derive(Debug)]
pub struct PartitionDirectory {
    entries: Mutex<BTreeMap<u64, Recorded>>,
    /// Nodes the metadata primary hears from, bit `n` for node `n`, or every
    /// bit without that evidence. Reports leave the reads with their in-sync
    /// nodes narrowed to these, so a primary that died does not keep claiming
    /// its members in sync with its last report.
    ready_nodes: AtomicU32,
    /// Locked after `entries` whenever both are held.
    changes: Mutex<Changes>,
}

#[derive(Debug, Clone, Copy)]
struct Recorded {
    report: PartitionStatusEntry,
    received_at: Instant,
    /// The metadata primary found no copy newer than its stale cutoff, so the
    /// reads list no member in sync until the primary reports again.
    stale: bool,
}

/// The order in which reports last changed what a route or an in-sync list
/// reads, so a node hears a new primary first rather than after a whole
/// cycle through every partition.
#[derive(Debug, Default)]
struct Changes {
    last: u64,
    by_seq: BTreeMap<u64, u64>,
    by_namespace: HashMap<u64, u64>,
}

impl Changes {
    fn mark(&mut self, namespace: u64) {
        self.last += 1;
        if let Some(previous) = self.by_namespace.insert(namespace, self.last) {
            self.by_seq.remove(&previous);
        }
        self.by_seq.insert(self.last, namespace);
    }

    fn forget(&mut self, namespace: u64) {
        if let Some(seq) = self.by_namespace.remove(&namespace) {
            self.by_seq.remove(&seq);
        }
    }
}

impl Default for PartitionDirectory {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
            ready_nodes: AtomicU32::new(u32::MAX),
            changes: Mutex::default(),
        }
    }
}

impl PartitionDirectory {
    /// Narrow the in-sync nodes of every report read from here on to
    /// `ready_nodes`, or stop narrowing them with `u32::MAX`.
    pub fn set_ready_nodes(&self, ready_nodes: u32) {
        self.ready_nodes.store(ready_nodes, Ordering::Relaxed);
    }

    fn narrowed(&self, recorded: &Recorded) -> PartitionStatusEntry {
        let in_sync_nodes = if recorded.stale {
            0
        } else {
            recorded.report.in_sync_nodes & self.ready_nodes.load(Ordering::Relaxed)
        };
        PartitionStatusEntry {
            in_sync_nodes,
            ..recorded.report
        }
    }

    /// Keep the newest report of each partition, ordered by incarnation, then
    /// view, then the primary's commit, received at `now`. Returns the reports
    /// it kept.
    pub fn record(
        &self,
        reports: impl IntoIterator<Item = PartitionStatusEntry>,
        now: Instant,
    ) -> Vec<PartitionStatusEntry> {
        let mut entries = self.lock();
        let mut changes = self.changes();
        let mut accepted = Vec::new();
        for report in reports {
            let known = entries.get(&report.namespace);
            let replace = known.is_none_or(|known| {
                (report.created_revision, report.view, report.commit)
                    >= (
                        known.report.created_revision,
                        known.report.view,
                        known.report.commit,
                    )
            });
            if replace {
                let moved = known.is_none_or(|known| {
                    known.stale
                        || (
                            known.report.created_revision,
                            known.report.view,
                            known.report.in_sync_nodes,
                        ) != (report.created_revision, report.view, report.in_sync_nodes)
                });
                entries.insert(
                    report.namespace,
                    Recorded {
                        report,
                        received_at: now,
                        stale: false,
                    },
                );
                if moved {
                    changes.mark(report.namespace);
                }
                accepted.push(report);
            }
        }
        drop(changes);
        drop(entries);
        accepted
    }

    /// The newest change, 0 before the first.
    #[must_use]
    pub fn last_change(&self) -> u64 {
        self.changes().last
    }

    /// Count the held reports of `namespaces` as the newest changes.
    pub fn mark_changed(&self, namespaces: impl IntoIterator<Item = u64>) {
        let entries = self.lock();
        let mut changes = self.changes();
        for namespace in namespaces {
            if entries.contains_key(&namespace) {
                changes.mark(namespace);
            }
        }
        drop(changes);
        drop(entries);
    }

    /// How many reports this node holds, of every incarnation not yet swept.
    #[must_use]
    pub fn report_count(&self) -> usize {
        self.lock().len()
    }

    /// Up to `limit` reports whose incarnation, view or in-sync nodes changed
    /// after change `after`, oldest change first, with the last change they
    /// reach, the next call's `after`.
    #[must_use]
    pub fn changed_after(
        &self,
        after: u64,
        limit: usize,
        mut include: impl FnMut(&PartitionStatusEntry) -> bool,
    ) -> (Vec<PartitionStatusEntry>, u64) {
        let entries = self.lock();
        let changes = self.changes();
        let mut last = after;
        let mut batch = Vec::new();
        for (&seq, namespace) in changes.by_seq.range(after.saturating_add(1)..) {
            if batch.len() == limit {
                break;
            }
            last = seq;
            if let Some(recorded) = entries
                .get(namespace)
                .filter(|recorded| include(&recorded.report))
            {
                batch.push(self.narrowed(recorded));
            }
        }
        drop(changes);
        drop(entries);
        (batch, last)
    }

    /// The latest report of `namespace`'s `created_revision` incarnation.
    #[must_use]
    pub fn get(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
    ) -> Option<PartitionStatusEntry> {
        self.lock()
            .get(&namespace.inner())
            .filter(|recorded| recorded.report.created_revision == created_revision)
            .map(|recorded| self.narrowed(recorded))
    }

    /// Like [`Self::get`], after marking the report stale when it was received
    /// before `stale_before`. Only a newer report clears the mark. A report
    /// turning stale counts as a change, so the other nodes hear of it first.
    pub fn expire_and_get(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        stale_before: Option<Instant>,
    ) -> Option<PartitionStatusEntry> {
        let mut entries = self.lock();
        let recorded = entries
            .get_mut(&namespace.inner())
            .filter(|recorded| recorded.report.created_revision == created_revision)?;
        if !recorded.stale && stale_before.is_some_and(|before| recorded.received_at < before) {
            recorded.stale = true;
            self.changes().mark(namespace.inner());
        }
        let status = self.narrowed(recorded);
        drop(entries);
        Some(status)
    }

    /// Up to `limit` reports in namespace order after `after`, wrapping to the
    /// start, so repeated calls cycle through every partition.
    #[must_use]
    pub fn batch(&self, after: Option<u64>, limit: usize) -> Vec<PartitionStatusEntry> {
        self.scan(after, limit, usize::MAX, |_| true).0
    }

    /// Up to `limit` reports `include` accepts, visiting at most `scan_max`
    /// reports in namespace order after `after` and wrapping to the start once.
    /// Returns them with the last report visited, the next call's `after`.
    pub fn scan(
        &self,
        after: Option<u64>,
        limit: usize,
        scan_max: usize,
        mut include: impl FnMut(&PartitionStatusEntry) -> bool,
    ) -> (Vec<PartitionStatusEntry>, Option<u64>) {
        let entries = self.lock();
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let end = after.map_or(Bound::Excluded(0), Bound::Included);
        let mut batch = Vec::new();
        let mut last = after;
        for (namespace, recorded) in entries
            .range((start, Bound::Unbounded))
            .chain(entries.range((Bound::Unbounded, end)))
            .take(scan_max.min(entries.len()))
        {
            if batch.len() == limit {
                break;
            }
            last = Some(*namespace);
            if include(&recorded.report) {
                batch.push(self.narrowed(recorded));
            }
        }
        drop(entries);
        (batch, last)
    }

    /// Drop the reports `keep` rejects among at most `limit` after `after`.
    /// Returns where the next chunk starts, `None` once the sweep reached the
    /// end, so repeated calls eventually visit every report.
    pub fn retain_chunk(
        &self,
        after: Option<u64>,
        limit: usize,
        mut keep: impl FnMut(&PartitionStatusEntry) -> bool,
    ) -> Option<u64> {
        let mut entries = self.lock();
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut visited = 0;
        let mut last = None;
        let mut rejected = Vec::new();
        for (namespace, recorded) in entries.range((start, Bound::Unbounded)).take(limit) {
            visited += 1;
            last = Some(*namespace);
            if !keep(&recorded.report) {
                rejected.push(*namespace);
            }
        }
        let mut changes = self.changes();
        for namespace in rejected {
            entries.remove(&namespace);
            changes.forget(namespace);
        }
        drop(changes);
        drop(entries);
        (visited == limit).then_some(last).flatten()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, Recorded>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn changes(&self) -> std::sync::MutexGuard<'_, Changes> {
        self.changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn report(namespace: u64, created_revision: u64, view: u32) -> PartitionStatusEntry {
        PartitionStatusEntry {
            namespace,
            created_revision,
            view,
            commit: 0,
            current_offset: u64::from(view),
            messages_count: 0,
            size_bytes: 0,
            segments_count: 1,
            in_sync_nodes: 0,
        }
    }

    #[test]
    fn given_reports_when_their_view_or_in_sync_nodes_change_should_list_them_in_change_order() {
        let directory = PartitionDirectory::default();
        directory.record(
            [report(1, 1, 0), report(2, 1, 0), report(3, 1, 0)],
            Instant::now(),
        );
        let (all, seen) = directory.changed_after(0, 8, |_| true);
        assert_eq!(all.len(), 3);

        let mut counters_only = report(1, 1, 0);
        counters_only.messages_count = 5;
        counters_only.commit = 2;
        directory.record([report(3, 1, 1), counters_only], Instant::now());
        let mut in_sync = report(2, 1, 0);
        in_sync.in_sync_nodes = 0b11;
        directory.record([in_sync], Instant::now());
        let (changed, last) = directory.changed_after(seen, 8, |_| true);
        assert_eq!(
            changed
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>(),
            [3, 2],
            "a new view or in-sync set is a change, new counters are not"
        );
        assert_eq!(directory.changed_after(last, 8, |_| true).0, []);

        directory.mark_changed([1, 99]);
        assert_eq!(
            directory
                .changed_after(last, 8, |_| true)
                .0
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>(),
            [1],
            "a marked report goes out again, a namespace without one is no change"
        );
        assert_eq!(directory.last_change(), last + 1);

        directory.retain_chunk(None, 8, |entry| entry.namespace != 3);
        assert_eq!(
            directory
                .changed_after(0, 8, |_| true)
                .0
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>(),
            [2, 1],
            "a retired report leaves the change log"
        );
    }

    #[test]
    fn given_reports_when_recording_should_keep_the_newest_incarnation_and_view() {
        let directory = PartitionDirectory::default();
        let namespace = IggyNamespace::from_raw(7);
        directory.record([report(7, 1, 4), report(7, 1, 2)], Instant::now());
        assert_eq!(directory.get(namespace, 1).unwrap().view, 4);

        let mut refreshed = report(7, 1, 4);
        refreshed.messages_count = 9;
        refreshed.commit = 3;
        assert_eq!(directory.record([refreshed], Instant::now()), [refreshed]);
        assert_eq!(directory.get(namespace, 1).unwrap().messages_count, 9);
        let mut reordered = refreshed;
        reordered.commit = 2;
        reordered.messages_count = 1;
        assert_eq!(
            directory.record([reordered], Instant::now()),
            [],
            "an older commit of the same view must not regress the counters"
        );
        assert_eq!(directory.get(namespace, 1).unwrap().messages_count, 9);

        directory.record([report(7, 2, 0)], Instant::now());
        assert!(
            directory.get(namespace, 1).is_none(),
            "a newer incarnation replaces it"
        );
        directory.record([report(7, 1, 9)], Instant::now());
        assert_eq!(directory.get(namespace, 2).unwrap().view, 0);
    }

    #[test]
    fn given_a_cursor_when_batching_should_cycle_through_every_report() {
        let directory = PartitionDirectory::default();
        directory.record(
            (1..=5).map(|namespace| report(namespace, 1, 0)),
            Instant::now(),
        );
        let namespaces = |batch: Vec<PartitionStatusEntry>| {
            batch
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>()
        };
        assert_eq!(namespaces(directory.batch(None, 2)), [1, 2]);
        assert_eq!(namespaces(directory.batch(Some(2), 2)), [3, 4]);
        assert_eq!(namespaces(directory.batch(Some(4), 2)), [5, 1]);
        assert_eq!(namespaces(directory.batch(Some(5), 10)), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn given_a_chunked_retain_when_cycling_should_visit_every_report_eventually() {
        let directory = PartitionDirectory::default();
        directory.record(
            (1..=5).map(|namespace| report(namespace, 1, 0)),
            Instant::now(),
        );
        let odd = |entry: &PartitionStatusEntry| entry.namespace % 2 == 1;
        assert_eq!(directory.retain_chunk(None, 2, odd), Some(2));
        assert_eq!(directory.retain_chunk(Some(2), 2, odd), Some(4));
        assert_eq!(directory.retain_chunk(Some(4), 2, odd), None);
        let namespaces = |batch: Vec<PartitionStatusEntry>| {
            batch
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>()
        };
        assert_eq!(namespaces(directory.batch(None, 10)), [1, 3, 5]);
        assert_eq!(
            directory.scan(Some(1), 1, 10, |entry| entry.namespace != 3),
            (vec![report(5, 1, 0)], Some(5)),
            "a filtered scan still advances past what it skips"
        );
        assert_eq!(directory.scan(Some(5), 10, 1, |_| true).1, Some(1));
    }

    #[test]
    fn given_a_report_received_before_the_cutoff_when_expiring_should_list_no_member_in_sync_until_the_next_one()
     {
        let directory = PartitionDirectory::default();
        let namespace = IggyNamespace::from_raw(7);
        let received = Instant::now();
        let later = received + Duration::from_secs(1);
        let mut healthy = report(7, 1, 0);
        healthy.in_sync_nodes = 0b11;
        directory.record([healthy], received);
        let seen = directory.last_change();
        let in_sync = |stale_before| {
            directory
                .expire_and_get(namespace, 1, stale_before)
                .unwrap()
                .in_sync_nodes
        };
        assert_eq!(in_sync(Some(received)), 0b11, "received at the cutoff");
        assert_eq!(in_sync(None), 0b11);
        assert_eq!(in_sync(Some(later)), 0);
        assert_eq!(directory.get(namespace, 1).unwrap().in_sync_nodes, 0);
        assert_eq!(
            directory.changed_after(seen, 8, |_| true).0,
            [PartitionStatusEntry {
                in_sync_nodes: 0,
                ..healthy
            }],
            "the other nodes hear that no member is in sync"
        );
        let seen = directory.last_change();
        assert_eq!(in_sync(None), 0, "only a new report clears the mark");

        directory.record([healthy], later);
        assert_eq!(directory.get(namespace, 1).unwrap().in_sync_nodes, 0b11);
        assert_eq!(
            directory.changed_after(seen, 8, |_| true).0,
            [healthy],
            "the same report arriving again is a change once it was stale"
        );
    }
}
