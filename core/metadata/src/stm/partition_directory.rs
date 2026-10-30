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
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;

/// What each partition's primary last reported, as this node heard it.
///
/// Node-local and never replicated: the metadata primary collects it from the
/// other nodes' metadata replies and hands it back out in its probes, so a
/// node answers for partitions it does not store.
#[derive(Debug, Default)]
pub struct PartitionDirectory {
    entries: Mutex<BTreeMap<u64, PartitionStatusEntry>>,
}

impl PartitionDirectory {
    /// Keep the newest report of each partition, ordered by incarnation, then
    /// view, then the primary's commit. Returns the reports it kept.
    pub fn record(
        &self,
        reports: impl IntoIterator<Item = PartitionStatusEntry>,
    ) -> Vec<PartitionStatusEntry> {
        let mut entries = self.lock();
        let mut accepted = Vec::new();
        for report in reports {
            let replace = entries.get(&report.namespace).is_none_or(|known| {
                (report.created_revision, report.view, report.commit)
                    >= (known.created_revision, known.view, known.commit)
            });
            if replace {
                entries.insert(report.namespace, report);
                accepted.push(report);
            }
        }
        drop(entries);
        accepted
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
            .copied()
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
                batch.push(*entry);
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
        for namespace in rejected {
            entries.remove(&namespace);
        }
        drop(entries);
        (visited == limit).then_some(last).flatten()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, PartitionStatusEntry>> {
        self.entries
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
        }
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
