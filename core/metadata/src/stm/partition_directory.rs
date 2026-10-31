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

/// What each partition's primary last reported, as this node heard it.
///
/// Node-local and never replicated: the metadata primary collects it from the
/// other nodes' metadata replies and hands it back out in its probes, so a
/// node answers for partitions it does not store.
#[derive(Debug)]
pub struct PartitionDirectory {
    entries: Mutex<BTreeMap<u64, PartitionStatusEntry>>,
    /// Nodes the metadata primary hears from, bit `n` for node `n`, or every
    /// bit without that evidence. Reports leave the reads with their in-sync
    /// nodes narrowed to these, so a primary that died does not keep claiming
    /// its members in sync with its last report.
    ready_nodes: AtomicU32,
    /// Locked after `entries` whenever both are held.
    changes: Mutex<Changes>,
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

    fn narrowed(&self, entry: &PartitionStatusEntry) -> PartitionStatusEntry {
        PartitionStatusEntry {
            in_sync_nodes: entry.in_sync_nodes & self.ready_nodes.load(Ordering::Relaxed),
            ..*entry
        }
    }

    /// Keep the newest report of each partition, ordered by incarnation, then
    /// view, then the primary's commit. Returns the reports it kept.
    pub fn record(
        &self,
        reports: impl IntoIterator<Item = PartitionStatusEntry>,
    ) -> Vec<PartitionStatusEntry> {
        let mut entries = self.lock();
        let mut changes = self.changes();
        let mut accepted = Vec::new();
        for report in reports {
            let known = entries.get(&report.namespace);
            let replace = known.is_none_or(|known| {
                (report.created_revision, report.view, report.commit)
                    >= (known.created_revision, known.view, known.commit)
            });
            if replace {
                let moved = known.is_none_or(|known| {
                    (known.created_revision, known.view, known.in_sync_nodes)
                        != (report.created_revision, report.view, report.in_sync_nodes)
                });
                entries.insert(report.namespace, report);
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
            if let Some(entry) = entries.get(namespace).filter(|entry| include(entry)) {
                batch.push(self.narrowed(entry));
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
            .filter(|entry| entry.created_revision == created_revision)
            .map(|entry| self.narrowed(entry))
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
        for (namespace, entry) in entries
            .range((start, Bound::Unbounded))
            .chain(entries.range((Bound::Unbounded, end)))
            .take(scan_max.min(entries.len()))
        {
            if batch.len() == limit {
                break;
            }
            last = Some(*namespace);
            if include(entry) {
                batch.push(self.narrowed(entry));
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
        for (namespace, entry) in entries.range((start, Bound::Unbounded)).take(limit) {
            visited += 1;
            last = Some(*namespace);
            if !keep(entry) {
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

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, PartitionStatusEntry>> {
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
        directory.record([report(1, 1, 0), report(2, 1, 0), report(3, 1, 0)]);
        let (all, seen) = directory.changed_after(0, 8, |_| true);
        assert_eq!(all.len(), 3);

        let mut counters_only = report(1, 1, 0);
        counters_only.messages_count = 5;
        counters_only.commit = 2;
        directory.record([report(3, 1, 1), counters_only]);
        let mut in_sync = report(2, 1, 0);
        in_sync.in_sync_nodes = 0b11;
        directory.record([in_sync]);
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

        directory.retain_chunk(None, 8, |entry| entry.namespace != 3);
        assert_eq!(
            directory
                .changed_after(0, 8, |_| true)
                .0
                .iter()
                .map(|entry| entry.namespace)
                .collect::<Vec<_>>(),
            [1, 2],
            "a retired report leaves the change log"
        );
    }

    #[test]
    fn given_reports_when_recording_should_keep_the_newest_incarnation_and_view() {
        let directory = PartitionDirectory::default();
        let namespace = IggyNamespace::from_raw(7);
        directory.record([report(7, 1, 4), report(7, 1, 2)]);
        assert_eq!(directory.get(namespace, 1).unwrap().view, 4);

        let mut refreshed = report(7, 1, 4);
        refreshed.messages_count = 9;
        refreshed.commit = 3;
        assert_eq!(directory.record([refreshed]), [refreshed]);
        assert_eq!(directory.get(namespace, 1).unwrap().messages_count, 9);
        let mut reordered = refreshed;
        reordered.commit = 2;
        reordered.messages_count = 1;
        assert_eq!(
            directory.record([reordered]),
            [],
            "an older commit of the same view must not regress the counters"
        );
        assert_eq!(directory.get(namespace, 1).unwrap().messages_count, 9);

        directory.record([report(7, 2, 0)]);
        assert!(
            directory.get(namespace, 1).is_none(),
            "a newer incarnation replaces it"
        );
        directory.record([report(7, 1, 9)]);
        assert_eq!(directory.get(namespace, 2).unwrap().view, 0);
    }

    #[test]
    fn given_a_cursor_when_batching_should_cycle_through_every_report() {
        let directory = PartitionDirectory::default();
        directory.record((1..=5).map(|namespace| report(namespace, 1, 0)));
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
        directory.record((1..=5).map(|namespace| report(namespace, 1, 0)));
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
}
