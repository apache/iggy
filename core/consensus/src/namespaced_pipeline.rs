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

use crate::Pipeline;
use crate::impls::{PIPELINE_PREPARE_QUEUE_MAX, PipelineEntry};
use iggy_common::header::PrepareHeader;
use iggy_common::message::Message;
use std::collections::{HashMap, VecDeque};

/// Pipeline that partitions entries by namespace for independent commit draining.
///
/// A single global op sequence and hash chain spans all namespaces, but entries
/// are stored in per-namespace VecDeques. Each namespace tracks its own commit
/// frontier so `drain_committable_all` drains quorum'd entries per-namespace
/// without waiting for the global commit to advance past unrelated namespaces.
///
/// The global commit (on `VsrConsensus`) remains a conservative lower bound
/// for the VSR protocol (view change, follower commit piggybacking). It only
/// advances when all ops up to that point are drained. Per-namespace draining
/// can run ahead of the global commit.
///
/// An alternative (simpler) approach would drain purely by per-entry quorum
/// flag without tracking per-namespace commit numbers, relying solely on
/// `global_commit_frontier` for the protocol commit. We track per-namespace
/// commits explicitly for observability and to make the independence model
/// visible in the data structure.
#[derive(Debug)]
pub struct NamespacedPipeline {
    queues: HashMap<u64, VecDeque<PipelineEntry>>,
    /// Per-namespace commit frontier: highest drained op per namespace.
    pub(crate) ns_commits: HashMap<u64, u64>,
    pub(crate) total_count: usize,
    last_push_checksum: u128,
    last_push_op: u64,
    /// Lower bound of ops pushed to this pipeline instance.
    /// Used by `global_commit_frontier` to distinguish "never pushed" from "drained."
    first_push_op: u64,
}

impl Default for NamespacedPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl NamespacedPipeline {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            ns_commits: HashMap::new(),
            total_count: 0,
            last_push_checksum: 0,
            last_push_op: 0,
            first_push_op: 0,
        }
    }

    pub fn register_namespace(&mut self, ns: u64) {
        self.queues.entry(ns).or_default();
        self.ns_commits.entry(ns).or_insert(0);
    }

    /// Per-namespace commit frontier for the given namespace.
    pub fn ns_commit(&self, ns: u64) -> Option<u64> {
        self.ns_commits.get(&ns).copied()
    }

    /// Drain entries that have achieved quorum, independently per namespace.
    ///
    /// For each namespace queue, drains from the front while entries have
    /// `ok_quorum_received == true`. Returns entries sorted by global op
    /// for deterministic processing.
    pub fn drain_committable_all(&mut self) -> Vec<PipelineEntry> {
        let mut drained = Vec::new();

        let Self {
            queues,
            ns_commits,
            total_count,
            ..
        } = self;

        for (ns, queue) in queues.iter_mut() {
            while let Some(front) = queue.front() {
                if !front.ok_quorum_received {
                    break;
                }
                let entry = queue.pop_front().expect("front exists");
                *total_count -= 1;
                if let Some(ns_commit) = ns_commits.get_mut(ns) {
                    *ns_commit = entry.header.op;
                }
                drained.push(entry);
            }
        }

        drained.sort_by_key(|entry| entry.header.op);
        drained
    }

    /// Compute the global commit frontier after draining.
    ///
    /// Walks forward from `current_commit + 1`, treating any op not found
    /// in the pipeline (already drained) as committed. Stops at the first
    /// op still present in a queue or past `last_push_op`.
    pub fn global_commit_frontier(&self, current_commit: u64) -> u64 {
        let mut commit = current_commit;
        loop {
            let next = commit + 1;
            if next > self.last_push_op {
                break;
            }
            // Ops below first_push_op were never in this pipeline instance
            // and must not be mistaken for drained entries.
            if next < self.first_push_op {
                break;
            }
            // Still in a queue means not yet drained
            if self.message_by_op(next).is_some() {
                break;
            }
            commit = next;
        }
        commit
    }
}

impl Pipeline for NamespacedPipeline {
    type Message = Message<PrepareHeader>;
    type Entry = PipelineEntry;

    fn push_message(&mut self, message: Self::Message) {
        assert!(
            self.total_count < PIPELINE_PREPARE_QUEUE_MAX,
            "namespaced pipeline full"
        );

        let header = *message.header();
        let ns = header.namespace;

        if self.total_count > 0 {
            assert_eq!(
                header.op,
                self.last_push_op + 1,
                "global ops must be sequential: expected {}, got {}",
                self.last_push_op + 1,
                header.op
            );
            assert_eq!(
                header.parent, self.last_push_checksum,
                "parent must chain to previous global checksum"
            );
        } else {
            self.first_push_op = header.op;
        }

        let queue = self
            .queues
            .get_mut(&ns)
            .expect("push_message: namespace not registered");
        if let Some(tail) = queue.back() {
            assert!(
                header.op > tail.header.op,
                "op must increase within namespace queue"
            );
        }

        queue.push_back(PipelineEntry::new(header));
        self.total_count += 1;
        self.last_push_checksum = header.checksum;
        self.last_push_op = header.op;
    }

    fn pop_message(&mut self) -> Option<Self::Entry> {
        let min_ns = self
            .queues
            .iter()
            .filter_map(|(ns, q)| q.front().map(|entry| (*ns, entry.header.op)))
            .min_by_key(|(_, op)| *op)
            .map(|(ns, _)| ns)?;

        let entry = self.queues.get_mut(&min_ns)?.pop_front()?;
        self.total_count -= 1;
        Some(entry)
    }

    fn clear(&mut self) {
        for queue in self.queues.values_mut() {
            queue.clear();
        }
        self.total_count = 0;
        self.last_push_checksum = 0;
        self.last_push_op = 0;
        self.first_push_op = 0;
    }

    /// Linear scan all queues. Ops are globally unique; max 8 entries total.
    fn message_by_op(&self, op: u64) -> Option<&Self::Entry> {
        for queue in self.queues.values() {
            for entry in queue {
                if entry.header.op == op {
                    return Some(entry);
                }
            }
        }
        None
    }

    fn message_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry> {
        for queue in self.queues.values_mut() {
            for entry in queue.iter_mut() {
                if entry.header.op == op {
                    return Some(entry);
                }
            }
        }
        None
    }

    fn message_by_op_and_checksum(&self, op: u64, checksum: u128) -> Option<&Self::Entry> {
        let entry = self.message_by_op(op)?;
        if entry.header.checksum == checksum {
            Some(entry)
        } else {
            None
        }
    }

    fn head(&self) -> Option<&Self::Entry> {
        self.queues
            .values()
            .filter_map(|q| q.front())
            .min_by_key(|entry| entry.header.op)
    }

    fn is_full(&self) -> bool {
        self.total_count >= PIPELINE_PREPARE_QUEUE_MAX
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn verify(&self) {
        assert!(self.total_count <= PIPELINE_PREPARE_QUEUE_MAX);

        let actual_count: usize = self.queues.values().map(|q| q.len()).sum();
        assert_eq!(actual_count, self.total_count, "total_count mismatch");

        // Per-namespace: ops must be monotonically increasing
        for queue in self.queues.values() {
            let mut prev_op = None;
            for entry in queue {
                if let Some(prev) = prev_op {
                    assert!(
                        entry.header.op > prev,
                        "ops must increase within namespace queue"
                    );
                }
                prev_op = Some(entry.header.op);
            }
        }

        // Global: collect all entries, sort by op, verify sequential ops and hash chain
        let mut all_entries: Vec<&PipelineEntry> =
            self.queues.values().flat_map(|q| q.iter()).collect();
        all_entries.sort_by_key(|e| e.header.op);

        for window in all_entries.windows(2) {
            let prev = &window[0].header;
            let curr = &window[1].header;
            assert_eq!(
                curr.op,
                prev.op + 1,
                "global ops must be sequential: {} -> {}",
                prev.op,
                curr.op
            );
            assert_eq!(
                curr.parent, prev.checksum,
                "global hash chain broken at op {}: parent={} expected={}",
                curr.op, curr.parent, prev.checksum
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::header::Command2;

    fn make_prepare(
        op: u64,
        parent: u128,
        checksum: u128,
        namespace: u64,
    ) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(std::mem::size_of::<PrepareHeader>()).transmute_header(
            |_, new| {
                *new = PrepareHeader {
                    command: Command2::Prepare,
                    op,
                    parent,
                    checksum,
                    namespace,
                    ..Default::default()
                };
            },
        )
    }

    fn mark_quorum(pipeline: &mut NamespacedPipeline, op: u64) {
        pipeline
            .message_by_op_mut(op)
            .expect("mark_quorum: op not in pipeline")
            .ok_quorum_received = true;
    }

    #[test]
    fn multi_namespace_push_pop() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);

        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 100));
        pipeline.push_message(make_prepare(4, 30, 40, 200));

        assert_eq!(pipeline.total_count, 4);
        assert!(!pipeline.is_empty());

        // head is the entry with the smallest op
        assert_eq!(pipeline.head().unwrap().header.op, 1);

        // pop returns entries in global op order
        assert_eq!(pipeline.pop_message().unwrap().header.op, 1);
        assert_eq!(pipeline.pop_message().unwrap().header.op, 2);
        assert_eq!(pipeline.pop_message().unwrap().header.op, 3);
        assert_eq!(pipeline.pop_message().unwrap().header.op, 4);
        assert!(pipeline.is_empty());
    }

    #[test]
    fn drain_committable_all() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);

        // Interleaved ops across two namespaces: [ns_a:1, ns_b:2, ns_a:3, ns_b:4]
        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 100));
        pipeline.push_message(make_prepare(4, 30, 40, 200));

        // Mark ops 1,2,3 as quorum'd (not 4)
        mark_quorum(&mut pipeline, 1);
        mark_quorum(&mut pipeline, 2);
        mark_quorum(&mut pipeline, 3);

        // ns_100 drains [1,3], ns_200 drains [2] (stops at non-quorum'd 4)
        let drained = pipeline.drain_committable_all();
        let drained_ops: Vec<_> = drained.iter().map(|e| e.header.op).collect();
        assert_eq!(drained_ops, vec![1, 2, 3]);

        assert_eq!(pipeline.total_count, 1);
        assert_eq!(pipeline.head().unwrap().header.op, 4);

        // Per-namespace commits track highest drained op
        assert_eq!(pipeline.ns_commit(100), Some(3));
        assert_eq!(pipeline.ns_commit(200), Some(2));

        // Global commit advances past contiguously drained ops
        assert_eq!(pipeline.global_commit_frontier(0), 3);
    }

    #[test]
    fn drain_committable_all_full() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);

        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 100));
        pipeline.push_message(make_prepare(4, 30, 40, 200));

        mark_quorum(&mut pipeline, 1);
        mark_quorum(&mut pipeline, 2);
        mark_quorum(&mut pipeline, 3);
        mark_quorum(&mut pipeline, 4);

        let drained = pipeline.drain_committable_all();
        assert_eq!(drained.len(), 4);
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.global_commit_frontier(0), 4);
    }

    #[test]
    fn independent_namespace_progress() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);

        // [ns_a:1, ns_b:2, ns_a:3, ns_b:4]
        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 100));
        pipeline.push_message(make_prepare(4, 30, 40, 200));

        // Only ns_a (ops 1,3) gets quorum, ns_b (ops 2,4) does not
        mark_quorum(&mut pipeline, 1);
        mark_quorum(&mut pipeline, 3);

        let drained = pipeline.drain_committable_all();
        let drained_ops: Vec<_> = drained.iter().map(|e| e.header.op).collect();
        assert_eq!(drained_ops, vec![1, 3]);

        // ns_a progressed independently, ns_b untouched
        assert_eq!(pipeline.ns_commit(100), Some(3));
        assert_eq!(pipeline.ns_commit(200), Some(0));
        assert_eq!(pipeline.total_count, 2);

        // Global commit only advances to 1 (can't skip ns_b's op 2)
        assert_eq!(pipeline.global_commit_frontier(0), 1);

        // Now ns_b gets quorum
        mark_quorum(&mut pipeline, 2);
        mark_quorum(&mut pipeline, 4);

        let drained = pipeline.drain_committable_all();
        let drained_ops: Vec<_> = drained.iter().map(|e| e.header.op).collect();
        assert_eq!(drained_ops, vec![2, 4]);

        // Global commit jumps to 4 (ops 1,3 already drained, 2,4 just drained)
        assert_eq!(pipeline.global_commit_frontier(1), 4);
        assert_eq!(pipeline.ns_commit(200), Some(4));
    }

    #[test]
    fn message_by_op_cross_namespace() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);
        pipeline.register_namespace(300);

        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 300));

        assert_eq!(pipeline.message_by_op(1).unwrap().header.namespace, 100);
        assert_eq!(pipeline.message_by_op(2).unwrap().header.namespace, 200);
        assert_eq!(pipeline.message_by_op(3).unwrap().header.namespace, 300);
        assert!(pipeline.message_by_op(4).is_none());
    }

    #[test]
    fn message_by_op_and_checksum() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.push_message(make_prepare(1, 0, 10, 100));

        assert!(pipeline.message_by_op_and_checksum(1, 10).is_some());
        assert!(pipeline.message_by_op_and_checksum(1, 99).is_none());
        assert!(pipeline.message_by_op_and_checksum(2, 10).is_none());
    }

    #[test]
    fn verify_passes() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);
        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));
        pipeline.push_message(make_prepare(3, 20, 30, 100));
        pipeline.verify();
    }

    #[test]
    fn is_full() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(0);
        pipeline.register_namespace(1);
        for i in 0..PIPELINE_PREPARE_QUEUE_MAX as u128 {
            let parent = if i == 0 { 0 } else { i * 10 };
            let checksum = (i + 1) * 10;
            pipeline.push_message(make_prepare(i as u64 + 1, parent, checksum, i as u64 % 2));
        }
        assert!(pipeline.is_full());
    }

    #[test]
    #[should_panic(expected = "namespaced pipeline full")]
    fn push_when_full_panics() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(0);
        for i in 0..PIPELINE_PREPARE_QUEUE_MAX as u128 {
            let parent = if i == 0 { 0 } else { i * 10 };
            let checksum = (i + 1) * 10;
            pipeline.push_message(make_prepare(i as u64 + 1, parent, checksum, 0));
        }
        pipeline.push_message(make_prepare(100, 80, 1000, 0));
    }

    #[test]
    fn clear_preserves_ns_commits() {
        let mut pipeline = NamespacedPipeline::new();
        pipeline.register_namespace(100);
        pipeline.register_namespace(200);
        pipeline.push_message(make_prepare(1, 0, 10, 100));
        pipeline.push_message(make_prepare(2, 10, 20, 200));

        // Mark op 1 as committed in ns 100 before clearing
        pipeline.ns_commits.insert(100, 1);

        pipeline.clear();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.total_count, 0);

        // ns_commits must survive clear -- they represent durable knowledge
        // about already-drained ops, not pipeline state
        assert_eq!(pipeline.ns_commits.get(&100), Some(&1));
        assert_eq!(pipeline.ns_commits.get(&200), Some(&0));
    }
}
