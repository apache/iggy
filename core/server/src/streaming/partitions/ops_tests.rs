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

//! Tests for the in-flight buffer visibility gap fix (issue #2715).
//!
//! These tests set up "State C" directly in memory:
//!   - in-flight holds offsets [0..N-1]  (committed journal, not yet on disk)
//!   - journal holds offsets  [N..N+M-1] (new appends after commit)
//!   - no actual disk data
//!
//! Before the fix, Cases 1-3 in get_messages_by_offset never checked
//! in-flight, causing the consumer to miss committed data and either
//! get empty results or skip directly to journal offsets.

#[cfg(test)]
mod tests {
    use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
    use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
    use crate::streaming::partitions::journal::{Inner, Journal};
    use crate::streaming::partitions::local_partition::LocalPartition;
    use crate::streaming::partitions::local_partitions::LocalPartitions;
    use crate::streaming::partitions::ops;
    use crate::streaming::polling_consumer::PollingConsumer;
    use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
    use iggy_common::sharding::IggyNamespace;
    use iggy_common::{
        IggyByteSize, IggyMessage, MemoryPool, MemoryPoolConfigOther, PollingStrategy, Sizeable,
    };
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    fn init_memory_pool() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let config = MemoryPoolConfigOther {
                enabled: false,
                size: IggyByteSize::from(64 * 1024 * 1024u64),
                bucket_capacity: 256,
            };
            MemoryPool::init_pool(&config);
        });
    }

    fn create_test_partition(current_offset: u64) -> LocalPartition {
        let stream_stats = Arc::new(StreamStats::default());
        let topic_stats = Arc::new(TopicStats::new(stream_stats));
        let partition_stats = Arc::new(PartitionStats::new(topic_stats));

        LocalPartition::new(
            partition_stats,
            Arc::new(AtomicU64::new(current_offset)),
            Arc::new(ConsumerOffsets::with_capacity(10)),
            Arc::new(ConsumerGroupOffsets::with_capacity(10)),
            None,
            iggy_common::IggyTimestamp::now(),
            1,
            true,
        )
    }

    fn create_batch(count: u32) -> iggy_common::IggyMessagesBatchMut {
        let messages: Vec<IggyMessage> = (0..count)
            .map(|_| {
                IggyMessage::builder()
                    .payload(bytes::Bytes::from("test-payload"))
                    .build()
                    .unwrap()
            })
            .collect();

        let messages_size: u32 = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        iggy_common::IggyMessagesBatchMut::from_messages(&messages, messages_size)
    }

    /// Sets up "State C": in-flight holds committed data, journal holds new
    /// appends that arrived after commit but before persist completes.
    ///
    /// Layout:
    ///   segment metadata:  [0..journal_end]   (no actual disk data)
    ///   in-flight:         [0..in_flight_count-1]
    ///   journal:           [in_flight_count..in_flight_count+journal_count-1]
    ///   partition.offset:  in_flight_count + journal_count - 1
    async fn setup_state_c(
        in_flight_count: u32,
        journal_count: u32,
    ) -> (RefCell<LocalPartitions>, IggyNamespace) {
        init_memory_pool();
        let ns = IggyNamespace::new(1, 1, 0);

        let in_flight_end = in_flight_count as u64 - 1;
        let journal_base = in_flight_end + 1;
        let journal_end = journal_base + journal_count as u64 - 1;

        let mut partition = create_test_partition(journal_end);

        let segment = iggy_common::Segment::new(0, IggyByteSize::from(1_073_741_824u64));
        let storage = iggy_common::SegmentStorage::default();
        partition.log.add_persisted_segment(segment, storage);

        let seg = &mut partition.log.segments_mut()[0];
        seg.end_offset = journal_end;
        seg.start_timestamp = 1;
        seg.end_timestamp = 2;

        let mut in_flight_batch = create_batch(in_flight_count);
        in_flight_batch.prepare_for_persistence(0, 0, 0, None).await;
        let in_flight_size = in_flight_batch.size();
        partition.log.set_in_flight(vec![in_flight_batch.freeze()]);

        let journal_inner = Inner {
            base_offset: journal_base,
            current_offset: 0,
            first_timestamp: 0,
            end_timestamp: 0,
            messages_count: 0,
            size: IggyByteSize::default(),
        };
        partition.log.journal_mut().init(journal_inner);

        let mut journal_batch = create_batch(journal_count);
        journal_batch
            .prepare_for_persistence(0, journal_base, in_flight_size, None)
            .await;
        partition.log.journal_mut().append(journal_batch).unwrap();

        let mut store = LocalPartitions::new();
        store.insert(ns, partition);
        (RefCell::new(store), ns)
    }

    // -----------------------------------------------------------------------
    // Issue #2715: In-flight buffer must be reachable when journal is non-empty
    // -----------------------------------------------------------------------

    #[compio::test]
    async fn in_flight_reachable_when_journal_non_empty() {
        let (store, ns) = setup_state_c(10, 5).await;
        let batches = ops::get_messages_by_offset(&store, &ns, 0, 5)
            .await
            .unwrap();
        assert_eq!(batches.count(), 5);
        assert_eq!(batches.first_offset(), Some(0));
    }

    #[compio::test]
    async fn spanning_in_flight_and_journal_returns_all_in_order() {
        let (store, ns) = setup_state_c(10, 5).await;
        let batches = ops::get_messages_by_offset(&store, &ns, 0, 15)
            .await
            .unwrap();
        assert_eq!(batches.count(), 15);
        assert_eq!(batches.first_offset(), Some(0));
    }

    #[compio::test]
    async fn polling_next_starts_from_in_flight_not_journal() {
        let (store, ns) = setup_state_c(10, 5).await;
        let consumer = PollingConsumer::Consumer(1, 0);
        let args =
            crate::shard::system::messages::PollingArgs::new(PollingStrategy::next(), 15, false);
        let (metadata, batches) = ops::poll_messages(&store, &ns, consumer, args)
            .await
            .unwrap();
        assert_eq!(batches.first_offset(), Some(0));
        assert!(metadata.current_offset >= 14);
    }

    #[compio::test]
    async fn single_message_at_in_flight_journal_boundary() {
        let (store, ns) = setup_state_c(10, 5).await;
        let batches = ops::get_messages_by_offset(&store, &ns, 9, 1)
            .await
            .unwrap();
        assert_eq!(batches.count(), 1);
        assert_eq!(batches.first_offset(), Some(9));
    }

    #[compio::test]
    async fn single_message_from_in_flight_at_offset_zero() {
        let (store, ns) = setup_state_c(10, 5).await;
        let batches = ops::get_messages_by_offset(&store, &ns, 0, 1)
            .await
            .unwrap();
        assert_eq!(batches.count(), 1);
        assert_eq!(batches.first_offset(), Some(0));
    }

    // -----------------------------------------------------------------------
    // Existing correct behavior must still work
    // -----------------------------------------------------------------------

    #[compio::test]
    async fn in_flight_reachable_when_journal_empty() {
        init_memory_pool();
        let ns = IggyNamespace::new(1, 1, 0);
        let mut partition = create_test_partition(9);

        let segment = iggy_common::Segment::new(0, IggyByteSize::from(1_073_741_824u64));
        partition
            .log
            .add_persisted_segment(segment, iggy_common::SegmentStorage::default());
        let seg = &mut partition.log.segments_mut()[0];
        seg.end_offset = 9;
        seg.start_timestamp = 1;
        seg.end_timestamp = 2;

        let mut batch = create_batch(10);
        batch.prepare_for_persistence(0, 0, 0, None).await;
        partition.log.set_in_flight(vec![batch.freeze()]);

        let mut store = LocalPartitions::new();
        store.insert(ns, partition);
        let store = RefCell::new(store);

        let batches = ops::get_messages_by_offset(&store, &ns, 0, 10)
            .await
            .unwrap();
        assert_eq!(batches.count(), 10);
    }

    #[compio::test]
    async fn journal_reachable_when_in_flight_empty() {
        init_memory_pool();
        let ns = IggyNamespace::new(1, 1, 0);
        let mut partition = create_test_partition(9);

        let segment = iggy_common::Segment::new(0, IggyByteSize::from(1_073_741_824u64));
        partition
            .log
            .add_persisted_segment(segment, iggy_common::SegmentStorage::default());
        let seg = &mut partition.log.segments_mut()[0];
        seg.end_offset = 9;
        seg.start_timestamp = 1;
        seg.end_timestamp = 2;

        partition.log.journal_mut().init(Inner {
            base_offset: 0,
            current_offset: 0,
            first_timestamp: 0,
            end_timestamp: 0,
            messages_count: 0,
            size: IggyByteSize::default(),
        });

        let mut batch = create_batch(10);
        batch.prepare_for_persistence(0, 0, 0, None).await;
        partition.log.journal_mut().append(batch).unwrap();

        let mut store = LocalPartitions::new();
        store.insert(ns, partition);
        let store = RefCell::new(store);

        let batches = ops::get_messages_by_offset(&store, &ns, 0, 10)
            .await
            .unwrap();
        assert_eq!(batches.count(), 10);
    }

    #[compio::test]
    async fn journal_single_message_at_specific_offset() {
        let (store, ns) = setup_state_c(10, 5).await;
        let batches = ops::get_messages_by_offset(&store, &ns, 12, 1)
            .await
            .unwrap();
        assert_eq!(batches.count(), 1);
        assert_eq!(batches.first_offset(), Some(12));
    }

    // -----------------------------------------------------------------------
    // Bug reproduction: journal base_offset=0 after restart causes offset skip
    // -----------------------------------------------------------------------

    /// Verifies that journal self-heals base_offset on first append.
    /// Without self-healing, a journal created via Default would have
    /// base_offset=0, causing incorrect offset calculations.
    #[compio::test]
    async fn journal_self_heals_base_offset_on_first_append() {
        init_memory_pool();

        let mut journal = crate::streaming::partitions::journal::MemoryMessageJournal::empty();
        assert_eq!(journal.inner().base_offset, 0);

        let mut batch = create_batch(5);
        batch.prepare_for_persistence(0, 100, 0, None).await;
        journal.append(batch).unwrap();

        assert_eq!(
            journal.inner().base_offset,
            100,
            "Journal should self-heal base_offset from batch's first offset"
        );
        assert_eq!(
            journal.inner().current_offset,
            104,
            "current_offset should be base_offset + messages_count - 1"
        );
    }

    /// Verifies that slice_by_offset returns None when start_offset is below
    /// the batch's range, instead of clamping to index 0 (the old bug).
    #[compio::test]
    async fn slice_by_offset_rejects_offset_below_range() {
        init_memory_pool();

        let mut batch = create_batch(10);
        batch.prepare_for_persistence(0, 100, 0, None).await;

        let result = batch.slice_by_offset(95, 10);

        assert!(
            result.is_none(),
            "slice_by_offset should return None when start_offset(95) < first_offset(100), \
             got {} messages at offset {:?}",
            result.as_ref().map(|r| r.count()).unwrap_or(0),
            result.as_ref().and_then(|r| r.first_offset())
        );
    }

    /// After proper journal initialization, polling across the in-flight/journal
    /// boundary returns contiguous messages with no gaps.
    #[compio::test]
    async fn post_restart_poll_with_correct_journal_init_no_skip() {
        let (store, ns) = setup_state_c(100, 10).await;

        let batches = ops::get_messages_by_offset(&store, &ns, 95, 10)
            .await
            .unwrap();

        assert_eq!(batches.count(), 10, "should return exactly 10 messages");
        assert_eq!(
            batches.first_offset(),
            Some(95),
            "first message should be at requested offset 95"
        );
        assert_eq!(
            batches.last_offset(),
            Some(104),
            "last message should be at offset 104 (contiguous)"
        );
    }
}
