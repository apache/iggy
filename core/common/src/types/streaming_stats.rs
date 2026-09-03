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

use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
};
use tracing::warn;

/// Number of rollup decrements that could not be covered by the counter they
/// were subtracted from.
///
/// A decrement bigger than the total it targets means the tree lost
/// `parent >= sum(children)` somewhere upstream. The counters are unsigned, so
/// without a clamp that subtraction wraps to ~1.8e19 and every reader
/// (`get_stream`, `get_topic`, `/stats`, `/metrics`) serves the wrapped value
/// until the process restarts. Clamping keeps the total merely low, which the
/// next write corrects; this counter plus the `warn!` below is what tells an
/// operator the divergence happened at all.
static ROLLUP_UNDERFLOWS: AtomicU64 = AtomicU64::new(0);

/// Monotonic count of clamped rollup decrements. Surfaced on `/metrics` so the
/// clamp is alertable rather than log-grep-able.
#[must_use]
pub fn rollup_underflows() -> u64 {
    ROLLUP_UNDERFLOWS.load(Ordering::Relaxed)
}

fn report_rollup_underflow(scope: &'static str, counter: &'static str, shortfall: u64) {
    let total = ROLLUP_UNDERFLOWS.fetch_add(1, Ordering::Relaxed) + 1;
    // One line for the first, then at powers of two. A skewed tree emits up to
    // three of these per counter per rollback, and a bulk delete would turn a
    // diagnostic into a log flood that buries the first one. The counter above
    // stays exact, and it is what an alert reads.
    if total.is_power_of_two() {
        warn!(
            scope,
            counter,
            shortfall,
            total,
            "rollup decrement exceeded the total it was subtracted from; clamped at zero"
        );
    }
}

/// Subtract `amount`, clamping at zero instead of wrapping. Returns what was
/// actually taken, which is what the caller passes on to its parent.
///
/// `fetch_update` rather than a load followed by a subtract: the counters are
/// written from the metadata shard and from whichever shard owns the partition,
/// so a separate load leaves a window where the clamp reads one value and
/// subtracts from another.
fn clamped_sub_u64(
    counter: &AtomicU64,
    amount: u64,
    scope: &'static str,
    name: &'static str,
) -> u64 {
    // The closure always yields `Some`, so `Err` carries the same previous
    // value `Ok` would; recovering it keeps the result exact either way.
    let previous = counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(amount))
        })
        .unwrap_or_else(|previous| previous);
    if previous < amount {
        report_rollup_underflow(scope, name, amount - previous);
    }
    previous.min(amount)
}

fn clamped_sub_u32(
    counter: &AtomicU32,
    amount: u32,
    scope: &'static str,
    name: &'static str,
) -> u32 {
    let previous = counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(amount))
        })
        .unwrap_or_else(|previous| previous);
    if previous < amount {
        report_rollup_underflow(scope, name, u64::from(amount - previous));
    }
    previous.min(amount)
}

#[derive(Default, Debug)]
pub struct StreamStats {
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU32,
}

impl StreamStats {
    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
    }

    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        let _ = clamped_sub_u64(&self.size_bytes, size_bytes, "stream", "size_bytes");
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        let _ = clamped_sub_u64(
            &self.messages_count,
            messages_count,
            "stream",
            "messages_count",
        );
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        let _ = clamped_sub_u32(
            &self.segments_count,
            segments_count,
            "stream",
            "segments_count",
        );
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn zero_out_size_bytes(&self) {
        self.size_bytes.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_messages_count(&self) {
        self.messages_count.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_segments_count(&self) {
        self.segments_count.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
    }

    pub fn load_for_snapshot(&self) -> (u64, u64, u32) {
        (
            self.size_bytes.load(Ordering::Relaxed),
            self.messages_count.load(Ordering::Relaxed),
            self.segments_count.load(Ordering::Relaxed),
        )
    }

    pub fn store_from_snapshot(&self, size_bytes: u64, messages_count: u64, segments_count: u32) {
        self.size_bytes.store(size_bytes, Ordering::Relaxed);
        self.messages_count.store(messages_count, Ordering::Relaxed);
        self.segments_count.store(segments_count, Ordering::Relaxed);
    }
}

#[derive(Default, Debug)]
pub struct TopicStats {
    parent: Arc<StreamStats>,
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU32,
}

impl TopicStats {
    pub fn new(parent: Arc<StreamStats>) -> Self {
        Self {
            parent,
            size_bytes: AtomicU64::new(0),
            messages_count: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
        }
    }

    pub fn parent(&self) -> Arc<StreamStats> {
        self.parent.clone()
    }

    pub fn increment_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_parent_messages_count(&self, messages_count: u64) {
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_parent_segments_count(&self, segments_count: u32) {
        self.parent.increment_segments_count(segments_count);
    }

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.increment_parent_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.increment_parent_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.increment_parent_segments_count(segments_count);
    }

    pub fn decrement_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.decrement_size_bytes(size_bytes);
    }

    pub fn decrement_parent_messages_count(&self, messages_count: u64) {
        self.parent.decrement_messages_count(messages_count);
    }

    pub fn decrement_parent_segments_count(&self, segments_count: u32) {
        self.parent.decrement_segments_count(segments_count);
    }

    // Forward what this level actually gave up, not what was asked for. A
    // decrement bigger than the counter holds means the bytes were never in
    // this subtree, so the ancestors do not hold them either -- passing the
    // full amount up would take them out of a sibling's live data instead.
    // Under `parent == sum(children)` the two are equal and nothing changes.
    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        let taken = clamped_sub_u64(&self.size_bytes, size_bytes, "topic", "size_bytes");
        self.decrement_parent_size_bytes(taken);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        let taken = clamped_sub_u64(
            &self.messages_count,
            messages_count,
            "topic",
            "messages_count",
        );
        self.decrement_parent_messages_count(taken);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        let taken = clamped_sub_u32(
            &self.segments_count,
            segments_count,
            "topic",
            "segments_count",
        );
        self.decrement_parent_segments_count(taken);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn zero_out_size_bytes(&self) {
        let prev = self.size_bytes.swap(0, Ordering::AcqRel);
        self.parent.decrement_size_bytes(prev);
    }

    pub fn zero_out_messages_count(&self) {
        let prev = self.messages_count.swap(0, Ordering::AcqRel);
        self.parent.decrement_messages_count(prev);
    }

    pub fn zero_out_segments_count(&self) {
        let prev = self.segments_count.swap(0, Ordering::AcqRel);
        self.parent.decrement_segments_count(prev);
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
    }

    pub fn load_for_snapshot(&self) -> (u64, u64, u32) {
        (
            self.size_bytes.load(Ordering::Relaxed),
            self.messages_count.load(Ordering::Relaxed),
            self.segments_count.load(Ordering::Relaxed),
        )
    }

    pub fn store_from_snapshot(&self, size_bytes: u64, messages_count: u64, segments_count: u32) {
        self.size_bytes.store(size_bytes, Ordering::Relaxed);
        self.messages_count.store(messages_count, Ordering::Relaxed);
        self.segments_count.store(segments_count, Ordering::Relaxed);
    }
}

#[derive(Default, Debug)]
pub struct PartitionStats {
    parent: Arc<TopicStats>,
    messages_count: AtomicU64,
    size_bytes: AtomicU64,
    segments_count: AtomicU32,
    current_offset: AtomicU64,
}

impl PartitionStats {
    pub fn new(parent_stats: Arc<TopicStats>) -> Self {
        Self {
            parent: parent_stats,
            messages_count: AtomicU64::new(0),
            size_bytes: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
            current_offset: AtomicU64::new(0),
        }
    }

    pub fn parent(&self) -> Arc<TopicStats> {
        self.parent.clone()
    }

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.increment_parent_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.increment_parent_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.increment_parent_segments_count(segments_count);
    }

    pub fn increment_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_parent_messages_count(&self, messages_count: u64) {
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_parent_segments_count(&self, segments_count: u32) {
        self.parent.increment_segments_count(segments_count);
    }

    // Forward what this level actually gave up, not what was asked for. A
    // decrement bigger than the counter holds means the bytes were never in
    // this subtree, so the ancestors do not hold them either -- passing the
    // full amount up would take them out of a sibling's live data instead.
    // Under `parent == sum(children)` the two are equal and nothing changes.
    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        let taken = clamped_sub_u64(&self.size_bytes, size_bytes, "partition", "size_bytes");
        self.decrement_parent_size_bytes(taken);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        let taken = clamped_sub_u64(
            &self.messages_count,
            messages_count,
            "partition",
            "messages_count",
        );
        self.decrement_parent_messages_count(taken);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        let taken = clamped_sub_u32(
            &self.segments_count,
            segments_count,
            "partition",
            "segments_count",
        );
        self.decrement_parent_segments_count(taken);
    }

    pub fn decrement_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.decrement_size_bytes(size_bytes);
    }

    pub fn decrement_parent_messages_count(&self, messages_count: u64) {
        self.parent.decrement_messages_count(messages_count);
    }

    pub fn decrement_parent_segments_count(&self, segments_count: u32) {
        self.parent.decrement_segments_count(segments_count);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::Relaxed)
    }

    pub fn set_current_offset(&self, offset: u64) {
        self.current_offset.store(offset, Ordering::Relaxed);
    }

    pub fn zero_out_size_bytes(&self) {
        let prev = self.size_bytes.swap(0, Ordering::AcqRel);
        self.parent.decrement_size_bytes(prev);
    }

    pub fn zero_out_messages_count(&self) {
        let prev = self.messages_count.swap(0, Ordering::AcqRel);
        self.parent.decrement_messages_count(prev);
    }

    pub fn zero_out_segments_count(&self) {
        let prev = self.segments_count.swap(0, Ordering::AcqRel);
        self.parent.decrement_segments_count(prev);
    }

    pub fn zero_out_current_offset(&self) {
        self.current_offset.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
        self.zero_out_current_offset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> (Arc<StreamStats>, Arc<TopicStats>, Arc<PartitionStats>) {
        let stream = Arc::new(StreamStats::default());
        let topic = Arc::new(TopicStats::new(stream.clone()));
        let partition = Arc::new(PartitionStats::new(topic.clone()));
        (stream, topic, partition)
    }

    #[test]
    fn given_a_parent_short_of_its_child_when_rolling_back_should_clamp_instead_of_wrapping() {
        let (stream, topic, partition) = tree();
        partition.increment_size_bytes(512);
        // A snapshot restore stores absolute parent totals, so a parent can end
        // up holding less than its children do.
        topic.store_from_snapshot(0, 0, 0);
        stream.store_from_snapshot(0, 0, 0);

        partition.zero_out_all();

        assert_eq!(topic.size_bytes_inconsistent(), 0);
        assert_eq!(stream.size_bytes_inconsistent(), 0);
    }

    /// A deleted partition keeps its handle until the reconciler tears it down,
    /// so a retention sweep can decrement counters the delete already rolled
    /// back. Those bytes are not in the parents any more, and taking them again
    /// would take a live sibling's instead.
    #[test]
    fn given_a_late_decrement_on_a_rolled_back_partition_should_leave_siblings_alone() {
        let stream = Arc::new(StreamStats::default());
        let topic = Arc::new(TopicStats::new(stream.clone()));
        let survivor = Arc::new(PartitionStats::new(topic.clone()));
        let deleted = Arc::new(PartitionStats::new(topic.clone()));
        survivor.increment_size_bytes(488);
        deleted.increment_size_bytes(512);

        deleted.zero_out_all();
        assert_eq!(topic.size_bytes_inconsistent(), 488);

        // Retention retiring a segment of the partition that is on its way out.
        deleted.decrement_size_bytes(100);

        assert_eq!(
            topic.size_bytes_inconsistent(),
            488,
            "the survivor's bytes are the only ones left; they are not the deleted partition's"
        );
        assert_eq!(stream.size_bytes_inconsistent(), 488);
        assert_eq!(survivor.size_bytes_inconsistent(), 488);
    }

    /// `delete_topic` settles the topic's residue on the stream, and the
    /// reconciler later settles the same partition's own handle. The second
    /// pass must find nothing left to take, or it takes it from another topic.
    #[test]
    fn given_a_topic_already_settled_when_its_partition_settles_should_leave_other_topics_alone() {
        let stream = Arc::new(StreamStats::default());
        let doomed_topic = Arc::new(TopicStats::new(stream.clone()));
        let live_topic = Arc::new(TopicStats::new(stream.clone()));
        let doomed_partition = Arc::new(PartitionStats::new(doomed_topic.clone()));
        let live_partition = Arc::new(PartitionStats::new(live_topic.clone()));
        live_partition.increment_size_bytes(900);
        // Landed through the cached handle after the delete evicted its entry.
        doomed_partition.increment_size_bytes(100);
        assert_eq!(stream.size_bytes_inconsistent(), 1000);

        doomed_topic.zero_out_all();
        assert_eq!(stream.size_bytes_inconsistent(), 900);

        doomed_partition.zero_out_all();

        assert_eq!(
            stream.size_bytes_inconsistent(),
            900,
            "the surviving topic's bytes must not pay for the deleted one's residue"
        );
        assert_eq!(live_topic.size_bytes_inconsistent(), 900);
    }

    #[test]
    fn given_a_short_u32_segments_count_when_rolling_back_should_clamp_that_counter_too() {
        let (stream, topic, partition) = tree();
        partition.increment_segments_count(3);
        topic.store_from_snapshot(0, 0, 0);
        stream.store_from_snapshot(0, 0, 0);

        partition.zero_out_all();

        // u32 wraps at a different modulus than the two u64 counters, so it
        // needs its own coverage.
        assert_eq!(topic.segments_count_inconsistent(), 0);
        assert_eq!(stream.segments_count_inconsistent(), 0);
    }

    #[test]
    fn given_a_covered_decrement_when_rolling_back_should_subtract_exactly() {
        let (stream, topic, partition) = tree();
        partition.increment_size_bytes(512);
        partition.increment_messages_count(7);

        partition.decrement_size_bytes(112);

        assert_eq!(partition.size_bytes_inconsistent(), 400);
        assert_eq!(topic.size_bytes_inconsistent(), 400);
        assert_eq!(stream.size_bytes_inconsistent(), 400);
        assert_eq!(stream.messages_count_inconsistent(), 7);
    }
}
