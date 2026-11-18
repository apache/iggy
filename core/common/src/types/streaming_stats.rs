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
    atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
};
use tracing::warn;

/// Process-wide number of rollup decrements that could not be covered by the
/// counter they were subtracted from. One static for the whole node, summed
/// across every stream, topic and partition it holds.
///
/// A decrement bigger than the total it targets means the tree lost
/// `parent >= sum(children)` somewhere upstream. The counters are unsigned, so
/// without a clamp that subtraction wraps to ~1.8e19 and every reader
/// (`get_stream`, `get_topic`, `/stats`, `/metrics`) serves the wrapped value
/// until the process restarts.
///
/// Clamping trades that for a total that is merely low, and low is where it
/// stays: increments are `fetch_add`, so every later write stacks on the
/// clamped base and carries the shortfall with it. Only a rebuild
/// (`rebuild_parent_totals`) or a restart puts the total back. This counter
/// plus the `warn!` below is what tells an operator the divergence happened,
/// and that the aggregate needs a rebuild rather than time.
static ROLLUP_UNDERFLOWS: AtomicU64 = AtomicU64::new(0);

/// Monotonic process-wide count of clamped rollup decrements. Surfaced on
/// `/metrics` so the clamp is alertable rather than log-grep-able.
///
/// Named for the root it is reached from, not for this module: `iggy_common`
/// globs the module into its crate root, so a bare `rollup_underflows` would
/// sit in the published SDK surface next to the stream and topic types saying
/// nothing about which rollup it counts.
#[must_use]
pub fn stats_rollup_underflows() -> u64 {
    ROLLUP_UNDERFLOWS.load(Ordering::Relaxed)
}

/// One scope/counter pair's clamp state: the labels an operator reads, plus the
/// count the log throttle keys on.
struct UnderflowSite {
    scope: &'static str,
    counter: &'static str,
    clamped: AtomicU64,
}

impl UnderflowSite {
    const fn new(scope: &'static str, counter: &'static str) -> Self {
        Self {
            scope,
            counter,
            clamped: AtomicU64::new(0),
        }
    }

    /// Count the clamp globally, then log this pair's first and every power of
    /// two after it.
    ///
    /// The throttle is per pair, not global: a skewed tree emits up to four
    /// lines per counter per rollback and a bulk delete turns that into a
    /// flood, so a shared count would hold a quiet pair's first-ever
    /// divergence back for up to 1023 events behind a noisy one.
    ///
    /// Both counts go on the line. `clamped` is this pair's, `total` is the
    /// process-wide one [`stats_rollup_underflows`] exports, which carries no
    /// scope or counter label -- without both an operator cannot tell which
    /// pair moved the metric.
    fn report(&self, shortfall: u64) {
        let total = ROLLUP_UNDERFLOWS.fetch_add(1, Ordering::Relaxed) + 1;
        let clamped = self.clamped.fetch_add(1, Ordering::Relaxed) + 1;
        if clamped.is_power_of_two() {
            warn!(
                target: "iggy.stats.diag",
                scope = self.scope,
                counter = self.counter,
                shortfall,
                clamped,
                total,
                "rollup decrement exceeded the total it was subtracted from; clamped at zero"
            );
        }
    }
}

static NODE_SIZE_BYTES: UnderflowSite = UnderflowSite::new("node", "size_bytes");
static NODE_MESSAGES_COUNT: UnderflowSite = UnderflowSite::new("node", "messages_count");
static NODE_SEGMENTS_COUNT: UnderflowSite = UnderflowSite::new("node", "segments_count");
static STREAM_SIZE_BYTES: UnderflowSite = UnderflowSite::new("stream", "size_bytes");
static STREAM_MESSAGES_COUNT: UnderflowSite = UnderflowSite::new("stream", "messages_count");
static STREAM_SEGMENTS_COUNT: UnderflowSite = UnderflowSite::new("stream", "segments_count");
static TOPIC_SIZE_BYTES: UnderflowSite = UnderflowSite::new("topic", "size_bytes");
static TOPIC_MESSAGES_COUNT: UnderflowSite = UnderflowSite::new("topic", "messages_count");
static TOPIC_SEGMENTS_COUNT: UnderflowSite = UnderflowSite::new("topic", "segments_count");
static PARTITION_SIZE_BYTES: UnderflowSite = UnderflowSite::new("partition", "size_bytes");
static PARTITION_MESSAGES_COUNT: UnderflowSite = UnderflowSite::new("partition", "messages_count");
static PARTITION_SEGMENTS_COUNT: UnderflowSite = UnderflowSite::new("partition", "segments_count");

/// Subtract `amount`, clamping at zero instead of wrapping. Returns the value
/// before the subtraction and what was actually taken, which is what the
/// caller passes on to its parent.
///
/// `try_update` rather than a load followed by a subtract: the counters are
/// written from the metadata shard and from whichever shard owns the partition,
/// so a separate load leaves a window where the clamp reads one value and
/// subtracts from another.
macro_rules! clamped_sub {
    ($name:ident, $counter:ty, $amount:ty) => {
        fn $name(
            counter: &$counter,
            amount: $amount,
            site: &'static UnderflowSite,
        ) -> ($amount, $amount) {
            let previous = counter
                .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    // `None` on an already-zero counter: no store, and the
                    // `Err` it returns carries that same zero, so the clamp
                    // reports identically either way. That is the steady shape
                    // for a scope whose rollback has already run.
                    (current != 0).then(|| current.saturating_sub(amount))
                })
                .unwrap_or_else(|previous| previous);
            if previous < amount {
                site.report(u64::from(amount - previous));
            }
            (previous, previous.min(amount))
        }
    };
}

clamped_sub!(clamped_sub_u64, AtomicU64, u64);
clamped_sub!(clamped_sub_u32, AtomicU32, u32);

/// Move `counter` from `sampled` to `rebuilt` by their difference. A fall
/// clamps like any other rollup decrement.
fn move_by_difference(
    counter: &AtomicU64,
    sampled: u64,
    rebuilt: u64,
    site: &'static UnderflowSite,
) {
    if rebuilt >= sampled {
        counter.fetch_add(rebuilt - sampled, Ordering::AcqRel);
    } else {
        clamped_sub_u64(counter, sampled - rebuilt, site);
    }
}

/// The totals of every stream on the node, the root of the rollup. A stats
/// read takes them without a walk over the streams.
#[derive(Default, Debug)]
pub struct NodeStats {
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU64,
    /// Partitions whose own segment count is above zero. Signed, because a
    /// thread that empties a partition can report the loss before the thread
    /// that filled it reports the gain. The sum is exact once both land.
    partitions_with_segments: AtomicI64,
}

impl NodeStats {
    fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
    }

    fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
    }

    fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(u64::from(segments_count), Ordering::AcqRel);
    }

    // The root, so a clamp here has nowhere left to be forwarded.
    fn decrement_size_bytes(&self, size_bytes: u64) {
        clamped_sub_u64(&self.size_bytes, size_bytes, &NODE_SIZE_BYTES);
    }

    fn decrement_messages_count(&self, messages_count: u64) {
        clamped_sub_u64(&self.messages_count, messages_count, &NODE_MESSAGES_COUNT);
    }

    fn decrement_segments_count(&self, segments_count: u32) {
        clamped_sub_u64(
            &self.segments_count,
            u64::from(segments_count),
            &NODE_SEGMENTS_COUNT,
        );
    }

    fn partition_gained_segments(&self) {
        self.partitions_with_segments.fetch_add(1, Ordering::AcqRel);
    }

    fn partition_lost_segments(&self) {
        self.partitions_with_segments.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u64 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn partitions_with_segments_inconsistent(&self) -> u64 {
        u64::try_from(self.partitions_with_segments.load(Ordering::Relaxed)).unwrap_or(0)
    }

    /// Read the totals that a later [`Self::apply_rebuilt`] moves from. The
    /// partition count is read raw, below zero included, because the
    /// difference has to be taken against what the counter really holds.
    pub fn sample(&self) -> NodeStatsSample {
        NodeStatsSample {
            size_bytes: self.size_bytes.load(Ordering::Relaxed),
            messages_count: self.messages_count.load(Ordering::Relaxed),
            segments_count: self.segments_count.load(Ordering::Relaxed),
            partitions_with_segments: self.partitions_with_segments.load(Ordering::Relaxed),
        }
    }

    /// Move the totals from `sampled` to the ones rebuilt from the partition
    /// counters by the difference, not by a store: a change another shard made
    /// after the sample is already in the totals, and it stays there.
    pub fn apply_rebuilt(
        &self,
        sampled: NodeStatsSample,
        size_bytes: u64,
        messages_count: u64,
        segments_count: u64,
        partitions_with_segments: u64,
    ) {
        move_by_difference(
            &self.size_bytes,
            sampled.size_bytes,
            size_bytes,
            &NODE_SIZE_BYTES,
        );
        move_by_difference(
            &self.messages_count,
            sampled.messages_count,
            messages_count,
            &NODE_MESSAGES_COUNT,
        );
        move_by_difference(
            &self.segments_count,
            sampled.segments_count,
            segments_count,
            &NODE_SEGMENTS_COUNT,
        );
        let partitions_with_segments = i64::try_from(partitions_with_segments).unwrap_or(i64::MAX);
        self.partitions_with_segments.fetch_add(
            partitions_with_segments.saturating_sub(sampled.partitions_with_segments),
            Ordering::AcqRel,
        );
    }
}

/// The node totals as one read saw them, the base [`NodeStats::apply_rebuilt`]
/// moves from.
#[derive(Debug, Clone, Copy)]
pub struct NodeStatsSample {
    size_bytes: u64,
    messages_count: u64,
    segments_count: u64,
    partitions_with_segments: i64,
}

#[derive(Default, Debug)]
pub struct StreamStats {
    parent: Arc<NodeStats>,
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU32,
}

impl StreamStats {
    pub fn new(parent: Arc<NodeStats>) -> Self {
        Self {
            parent,
            size_bytes: AtomicU64::new(0),
            messages_count: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
        }
    }

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.parent.increment_segments_count(segments_count);
    }

    // Forward what this level actually gave up, as `TopicStats` does.
    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        let (_, taken) = clamped_sub_u64(&self.size_bytes, size_bytes, &STREAM_SIZE_BYTES);
        self.parent.decrement_size_bytes(taken);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        let (_, taken) =
            clamped_sub_u64(&self.messages_count, messages_count, &STREAM_MESSAGES_COUNT);
        self.parent.decrement_messages_count(taken);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        let (_, taken) =
            clamped_sub_u32(&self.segments_count, segments_count, &STREAM_SEGMENTS_COUNT);
        self.parent.decrement_segments_count(taken);
    }

    fn partition_gained_segments(&self) {
        self.parent.partition_gained_segments();
    }

    fn partition_lost_segments(&self) {
        self.parent.partition_lost_segments();
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

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.parent.increment_segments_count(segments_count);
    }

    // Forward what this level actually gave up, not what was asked for. A
    // decrement bigger than the counter holds means the bytes were never in
    // this subtree, so the ancestors do not hold them either -- passing the
    // full amount up would take them out of a sibling's live data instead.
    // Under `parent == sum(children)` the two are equal and nothing changes.
    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        let (_, taken) = clamped_sub_u64(&self.size_bytes, size_bytes, &TOPIC_SIZE_BYTES);
        self.parent.decrement_size_bytes(taken);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        let (_, taken) =
            clamped_sub_u64(&self.messages_count, messages_count, &TOPIC_MESSAGES_COUNT);
        self.parent.decrement_messages_count(taken);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        let (_, taken) =
            clamped_sub_u32(&self.segments_count, segments_count, &TOPIC_SEGMENTS_COUNT);
        self.parent.decrement_segments_count(taken);
    }

    fn partition_gained_segments(&self) {
        self.parent.partition_gained_segments();
    }

    fn partition_lost_segments(&self) {
        self.parent.partition_lost_segments();
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
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        let previous = self
            .segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.parent.increment_segments_count(segments_count);
        if previous == 0 && segments_count > 0 {
            self.parent.partition_gained_segments();
        }
    }

    // Forward what this level actually gave up, not what was asked for. A
    // decrement bigger than the counter holds means the bytes were never in
    // this subtree, so the ancestors do not hold them either -- passing the
    // full amount up would take them out of a sibling's live data instead.
    // Under `parent == sum(children)` the two are equal and nothing changes.
    //
    // Each returns this level's shortfall -- how much of the amount it did not
    // hold, zero when the decrement was covered -- so a caller that knows the
    // ids can name where the divergence is.
    pub fn decrement_size_bytes(&self, size_bytes: u64) -> u64 {
        let (_, taken) = clamped_sub_u64(&self.size_bytes, size_bytes, &PARTITION_SIZE_BYTES);
        self.parent.decrement_size_bytes(taken);
        size_bytes - taken
    }

    pub fn decrement_messages_count(&self, messages_count: u64) -> u64 {
        let (_, taken) = clamped_sub_u64(
            &self.messages_count,
            messages_count,
            &PARTITION_MESSAGES_COUNT,
        );
        self.parent.decrement_messages_count(taken);
        messages_count - taken
    }

    pub fn decrement_segments_count(&self, segments_count: u32) -> u32 {
        let (previous, taken) = clamped_sub_u32(
            &self.segments_count,
            segments_count,
            &PARTITION_SEGMENTS_COUNT,
        );
        self.parent.decrement_segments_count(taken);
        if previous > 0 && previous <= segments_count {
            self.parent.partition_lost_segments();
        }
        segments_count - taken
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

    /// Move every counter to the values another node reported, carrying the
    /// difference into the parents.
    pub fn store_reported(
        &self,
        size_bytes: u64,
        messages_count: u64,
        segments_count: u32,
        current_offset: u64,
    ) {
        let size = self.size_bytes.load(Ordering::Relaxed);
        if size_bytes >= size {
            self.increment_size_bytes(size_bytes - size);
        } else {
            self.decrement_size_bytes(size - size_bytes);
        }
        let messages = self.messages_count.load(Ordering::Relaxed);
        if messages_count >= messages {
            self.increment_messages_count(messages_count - messages);
        } else {
            self.decrement_messages_count(messages - messages_count);
        }
        let segments = self.segments_count.load(Ordering::Relaxed);
        if segments_count >= segments {
            self.increment_segments_count(segments_count - segments);
        } else {
            self.decrement_segments_count(segments - segments_count);
        }
        self.set_current_offset(current_offset);
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
        if prev > 0 {
            self.parent.partition_lost_segments();
        }
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
    fn given_a_rolled_back_partition_when_a_late_decrement_arrives_should_leave_siblings_alone() {
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

        // `clamped_sub!` expands separately per width, so the u32 counter is
        // its own wiring: same `saturating_sub` body, its own `UnderflowSite`
        // and its own call sites at all three levels.
        assert_eq!(topic.segments_count_inconsistent(), 0);
        assert_eq!(stream.segments_count_inconsistent(), 0);
    }

    fn partition_under(node: &Arc<NodeStats>) -> Arc<PartitionStats> {
        let stream = Arc::new(StreamStats::new(node.clone()));
        let topic = Arc::new(TopicStats::new(stream));
        Arc::new(PartitionStats::new(topic))
    }

    #[test]
    fn given_partitions_of_two_streams_when_counting_should_sum_them_on_the_node() {
        let node = Arc::new(NodeStats::default());
        let first = partition_under(&node);
        let second = partition_under(&node);
        first.increment_size_bytes(100);
        first.increment_messages_count(10);
        first.increment_segments_count(2);
        second.increment_size_bytes(50);
        second.increment_messages_count(5);
        second.increment_segments_count(1);
        assert_eq!(node.size_bytes_inconsistent(), 150);
        assert_eq!(node.messages_count_inconsistent(), 15);
        assert_eq!(node.segments_count_inconsistent(), 3);

        first.decrement_size_bytes(40);
        first.decrement_messages_count(4);
        first.decrement_segments_count(1);
        assert_eq!(node.size_bytes_inconsistent(), 110);
        assert_eq!(node.messages_count_inconsistent(), 11);
        assert_eq!(node.segments_count_inconsistent(), 2);

        second.zero_out_all();
        assert_eq!(node.size_bytes_inconsistent(), 60);
        assert_eq!(node.messages_count_inconsistent(), 6);
        assert_eq!(node.segments_count_inconsistent(), 1);
    }

    /// Stats reads add the floor of one segment for each partition outside
    /// this count, so a missed transition shows up there as a wrong total.
    #[test]
    fn given_segment_counts_crossing_zero_when_counting_should_track_partitions_with_segments() {
        let node = Arc::new(NodeStats::default());
        let first = partition_under(&node);
        let second = partition_under(&node);

        first.increment_segments_count(0);
        assert_eq!(node.partitions_with_segments_inconsistent(), 0);
        first.increment_segments_count(1);
        first.increment_segments_count(1);
        second.increment_segments_count(3);
        assert_eq!(node.partitions_with_segments_inconsistent(), 2);

        first.decrement_segments_count(1);
        assert_eq!(node.partitions_with_segments_inconsistent(), 2);
        first.decrement_segments_count(1);
        assert_eq!(node.partitions_with_segments_inconsistent(), 1);
        // Nothing left to lose: an empty partition is already outside the count.
        first.decrement_segments_count(1);
        assert_eq!(node.partitions_with_segments_inconsistent(), 1);

        // A clamped decrement still empties the partition.
        second.decrement_segments_count(5);
        assert_eq!(node.partitions_with_segments_inconsistent(), 0);

        second.increment_segments_count(2);
        second.zero_out_all();
        second.zero_out_all();
        assert_eq!(node.partitions_with_segments_inconsistent(), 0);
        assert_eq!(node.segments_count_inconsistent(), 0);

        // A read clamps a negative count to zero, so only the next gain shows
        // whether zeroing the empty partition took one away.
        first.increment_segments_count(1);
        assert_eq!(node.partitions_with_segments_inconsistent(), 1);
    }

    #[test]
    fn given_a_loss_landing_before_its_gain_when_reading_should_not_wrap() {
        let node = NodeStats::default();
        node.partition_lost_segments();
        assert_eq!(node.partitions_with_segments_inconsistent(), 0);
        node.partition_gained_segments();
        node.partition_gained_segments();
        assert_eq!(node.partitions_with_segments_inconsistent(), 1);
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
