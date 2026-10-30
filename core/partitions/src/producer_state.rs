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

//! Idempotent producer state of one partition: the epoch of each producer and
//! its last batches.
//!
//! Offsets are assigned when the primary stamps a prepare, so a batch that a
//! producer sends again must be caught before that. The primary checks it
//! against the committed table and the uncommitted prepares in its journal.
//! Every replica records a committed batch in op order, so the tables agree.

use iggy_binary_protocol::batch::BatchProducer;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Batches each producer keeps.
///
/// A producer can have up to five batches in flight to one partition when it
/// spreads them over several sessions, and a resend of any of them still finds
/// the offset of its first write.
pub const PRODUCER_BATCHES_RETAINED: usize = 5;

/// Producers a partition keeps, which bounds its memory. Past this, the one
/// idle the longest goes, and its next batch starts at sequence 0, as the
/// first batch of a new producer does.
pub const PRODUCERS_MAX: usize = 4096;

/// A producer idle this long goes. A day lets a producer resend after a
/// restart or a long outage. Measured in prepare timestamps, so every replica
/// drops the same producers at the same op.
pub const PRODUCER_IDLE_EXPIRY_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

/// One batch of a producer, as the partition wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    pub first_sequence: u32,
    pub last_sequence: u32,
    pub base_offset: u64,
}

/// The state of one producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: u16,
    /// Oldest first, at most [`PRODUCER_BATCHES_RETAINED`].
    pub batches: VecDeque<ProducerBatch>,
    /// Prepare timestamp of the newest batch.
    pub last_timestamp: u64,
}

/// What the primary does with a batch of a producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerAdmission {
    /// Write the batch.
    Append,
    /// The partition already holds the batch at `base_offset`.
    Duplicate { base_offset: u64 },
    /// An uncommitted prepare holds the batch. The sender retries, and gets
    /// [`Self::Duplicate`] once it commits.
    DuplicateInFlight,
    /// The batch does not follow the last batch of its producer.
    OutOfOrder,
    /// The partition holds no state for the producer, and the batch does not
    /// start at sequence 0.
    Unknown,
    /// The producer moved to a newer epoch.
    StaleEpoch,
}

/// A stamped batch of an uncommitted prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampedBatch {
    pub producer: BatchProducer,
    pub message_count: u32,
    pub base_offset: u64,
    pub timestamp: u64,
}

impl ProducerEntry {
    fn new(batch: &StampedBatch) -> Self {
        let mut entry = Self {
            epoch: batch.producer.epoch,
            batches: VecDeque::with_capacity(PRODUCER_BATCHES_RETAINED),
            last_timestamp: batch.timestamp,
        };
        entry.push(batch);
        entry
    }

    /// The rules in order: the epoch, a duplicate of a retained batch, then
    /// the sequence.
    #[must_use]
    pub fn check(&self, producer: BatchProducer, message_count: u32) -> ProducerAdmission {
        if producer.epoch < self.epoch {
            return ProducerAdmission::StaleEpoch;
        }
        if producer.epoch > self.epoch {
            // A new epoch starts its sequence again.
            return if producer.base_sequence == 0 {
                ProducerAdmission::Append
            } else {
                ProducerAdmission::OutOfOrder
            };
        }
        let last_sequence = producer.last_sequence(message_count);
        if let Some(batch) = self.batches.iter().find(|batch| {
            batch.first_sequence == producer.base_sequence && batch.last_sequence == last_sequence
        }) {
            return ProducerAdmission::Duplicate {
                base_offset: batch.base_offset,
            };
        }
        let follows = self.batches.back().is_none_or(|newest| {
            BatchProducer::advance(newest.last_sequence, 1) == producer.base_sequence
        });
        if follows {
            ProducerAdmission::Append
        } else {
            ProducerAdmission::OutOfOrder
        }
    }

    fn record(&mut self, batch: &StampedBatch) {
        if batch.producer.epoch < self.epoch {
            return;
        }
        if batch.producer.epoch > self.epoch {
            self.epoch = batch.producer.epoch;
            self.batches.clear();
        }
        self.push(batch);
        self.last_timestamp = self.last_timestamp.max(batch.timestamp);
    }

    fn push(&mut self, batch: &StampedBatch) {
        if self.batches.len() == PRODUCER_BATCHES_RETAINED {
            self.batches.pop_front();
        }
        self.batches.push_back(ProducerBatch {
            first_sequence: batch.producer.base_sequence,
            last_sequence: batch.producer.last_sequence(batch.message_count),
            base_offset: batch.base_offset,
        });
    }
}

/// The committed producer state of a partition, keyed by producer id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerTable {
    entries: BTreeMap<u64, ProducerEntry>,
    /// `(last_timestamp, producer_id)` of every entry, the longest idle first.
    by_idle: BTreeSet<(u64, u64)>,
}

impl ProducerTable {
    /// A table of entries that a checkpoint or a transfer carried.
    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = (u64, ProducerEntry)>) -> Self {
        let entries: BTreeMap<u64, ProducerEntry> = entries.into_iter().collect();
        let by_idle = entries
            .iter()
            .map(|(producer_id, entry)| (entry.last_timestamp, *producer_id))
            .collect();
        Self { entries, by_idle }
    }

    /// Every entry, ascending by producer id.
    pub fn entries(&self) -> impl Iterator<Item = (u64, &ProducerEntry)> {
        self.entries
            .iter()
            .map(|(producer_id, entry)| (*producer_id, entry))
    }

    /// Record a committed batch. Every replica calls this in op order, so
    /// every replica also drops the same idle producers.
    pub fn record(&mut self, batch: &StampedBatch) {
        let producer_id = batch.producer.producer_id;
        if let Some(entry) = self.entries.get_mut(&producer_id) {
            self.by_idle.remove(&(entry.last_timestamp, producer_id));
            entry.record(batch);
            self.by_idle.insert((entry.last_timestamp, producer_id));
        } else {
            let entry = ProducerEntry::new(batch);
            self.by_idle.insert((entry.last_timestamp, producer_id));
            self.entries.insert(producer_id, entry);
        }
        self.expire(batch.timestamp);
    }

    fn expire(&mut self, now: u64) {
        while let Some(&(last_timestamp, producer_id)) = self.by_idle.first() {
            let idle = last_timestamp.saturating_add(PRODUCER_IDLE_EXPIRY_MICROS) < now;
            if !idle && self.entries.len() <= PRODUCERS_MAX {
                break;
            }
            self.by_idle.pop_first();
            self.entries.remove(&producer_id);
        }
    }

    /// Check a batch against the committed state and then `in_flight`, the
    /// uncommitted batches of the same producer in op order.
    #[must_use]
    pub fn admit(
        &self,
        producer: BatchProducer,
        message_count: u32,
        in_flight: &[StampedBatch],
    ) -> ProducerAdmission {
        let mut entry = self.entries.get(&producer.producer_id).cloned();
        for batch in in_flight {
            match &mut entry {
                Some(entry) => entry.record(batch),
                None => entry = Some(ProducerEntry::new(batch)),
            }
        }
        // A producer starts at sequence 0. A later batch that overtook the
        // first one through another session would otherwise land ahead of it.
        let Some(entry) = entry else {
            return if producer.base_sequence == 0 {
                ProducerAdmission::Append
            } else {
                ProducerAdmission::Unknown
            };
        };
        match entry.check(producer, message_count) {
            ProducerAdmission::Duplicate { base_offset }
                if in_flight
                    .iter()
                    .any(|batch| batch.base_offset == base_offset) =>
            {
                ProducerAdmission::DuplicateInFlight
            }
            admission => admission,
        }
    }

    #[must_use]
    pub fn get(&self, producer_id: u64) -> Option<&ProducerEntry> {
        self.entries.get(&producer_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRODUCER_ID: u64 = 42;

    fn producer(epoch: u16, base_sequence: u32) -> BatchProducer {
        BatchProducer::new(PRODUCER_ID, epoch, base_sequence).unwrap()
    }

    fn stamped(epoch: u16, base_sequence: u32, count: u32, base_offset: u64) -> StampedBatch {
        StampedBatch {
            producer: producer(epoch, base_sequence),
            message_count: count,
            base_offset,
            timestamp: base_offset,
        }
    }

    fn table(batches: &[StampedBatch]) -> ProducerTable {
        let mut table = ProducerTable::default();
        for batch in batches {
            table.record(batch);
        }
        table
    }

    #[test]
    fn given_an_unknown_producer_when_admitting_should_append_only_from_sequence_zero() {
        let table = ProducerTable::default();
        assert_eq!(
            table.admit(producer(0, 0), 3, &[]),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(2, 0), 3, &[]),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(0, 17), 3, &[]),
            ProducerAdmission::Unknown
        );
    }

    #[test]
    fn given_a_first_batch_in_flight_when_its_successor_arrives_should_append_it() {
        let table = ProducerTable::default();
        let in_flight = [stamped(0, 0, 3, 100)];

        assert_eq!(
            table.admit(producer(0, 3), 2, &in_flight),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(0, 3), 2, &[]),
            ProducerAdmission::Unknown
        );
    }

    #[test]
    fn given_a_committed_batch_when_sent_again_should_answer_its_offset() {
        let table = table(&[stamped(0, 0, 3, 100), stamped(0, 3, 2, 103)]);

        assert_eq!(
            table.admit(producer(0, 0), 3, &[]),
            ProducerAdmission::Duplicate { base_offset: 100 }
        );
        assert_eq!(
            table.admit(producer(0, 3), 2, &[]),
            ProducerAdmission::Duplicate { base_offset: 103 }
        );
        assert_eq!(
            table.admit(producer(0, 5), 1, &[]),
            ProducerAdmission::Append
        );
    }

    #[test]
    fn given_a_gap_or_a_changed_split_when_admitting_should_refuse_it() {
        let table = table(&[stamped(0, 0, 3, 100)]);

        assert_eq!(
            table.admit(producer(0, 4), 1, &[]),
            ProducerAdmission::OutOfOrder
        );
        assert_eq!(
            table.admit(producer(0, 0), 2, &[]),
            ProducerAdmission::OutOfOrder
        );
    }

    #[test]
    fn given_epochs_when_admitting_should_fence_the_old_and_restart_the_new_at_zero() {
        let table = table(&[stamped(2, 0, 3, 100)]);

        assert_eq!(
            table.admit(producer(1, 3), 1, &[]),
            ProducerAdmission::StaleEpoch
        );
        assert_eq!(
            table.admit(producer(3, 0), 1, &[]),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(3, 3), 1, &[]),
            ProducerAdmission::OutOfOrder
        );
    }

    #[test]
    fn given_a_new_epoch_when_recorded_should_forget_the_old_batches() {
        let table = table(&[stamped(0, 0, 3, 100), stamped(1, 0, 2, 103)]);

        let entry = table.get(PRODUCER_ID).unwrap();
        assert_eq!(entry.epoch, 1);
        assert_eq!(entry.batches.len(), 1);
        assert_eq!(
            table.admit(producer(1, 2), 1, &[]),
            ProducerAdmission::Append
        );
    }

    #[test]
    fn given_in_flight_batches_when_admitting_should_follow_them_and_hold_their_duplicates() {
        let table = table(&[stamped(0, 0, 3, 100)]);
        let in_flight = [stamped(0, 3, 2, 103)];

        assert_eq!(
            table.admit(producer(0, 5), 1, &in_flight),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(0, 3), 2, &in_flight),
            ProducerAdmission::DuplicateInFlight
        );
        assert_eq!(
            table.admit(producer(0, 0), 3, &in_flight),
            ProducerAdmission::Duplicate { base_offset: 100 }
        );
        assert_eq!(
            table.admit(producer(0, 3), 1, &in_flight),
            ProducerAdmission::OutOfOrder
        );
    }

    #[test]
    fn given_more_than_five_batches_when_recorded_should_keep_the_newest_five() {
        let batches: Vec<StampedBatch> = (0..7u32)
            .map(|index| stamped(0, index, 1, 100 + u64::from(index)))
            .collect();
        let table = table(&batches);

        assert_eq!(
            table.get(PRODUCER_ID).unwrap().batches.len(),
            PRODUCER_BATCHES_RETAINED
        );
        assert_eq!(
            table.admit(producer(0, 1), 1, &[]),
            ProducerAdmission::OutOfOrder
        );
        assert_eq!(
            table.admit(producer(0, 2), 1, &[]),
            ProducerAdmission::Duplicate { base_offset: 102 }
        );
    }

    #[test]
    fn given_a_producer_idle_past_the_expiry_when_another_records_should_drop_it() {
        let mut table = table(&[stamped(0, 0, 1, 100)]);
        let other = StampedBatch {
            producer: BatchProducer::new(PRODUCER_ID + 1, 0, 0).unwrap(),
            message_count: 1,
            base_offset: 101,
            timestamp: 100 + PRODUCER_IDLE_EXPIRY_MICROS,
        };
        table.record(&other);
        assert!(table.get(PRODUCER_ID).is_some(), "idle exactly the expiry");

        table.record(&StampedBatch {
            timestamp: other.timestamp + 1,
            ..other
        });

        assert!(table.get(PRODUCER_ID).is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(
            table.admit(producer(0, 9), 1, &[]),
            ProducerAdmission::Unknown,
            "a dropped producer starts again at sequence 0"
        );
        assert_eq!(
            table.admit(producer(0, 0), 1, &[]),
            ProducerAdmission::Append
        );
    }

    #[test]
    fn given_a_full_table_when_a_new_producer_records_should_drop_the_longest_idle() {
        let mut table = ProducerTable::default();
        for producer_id in 0..=PRODUCERS_MAX as u64 {
            table.record(&StampedBatch {
                producer: BatchProducer::new(producer_id, 0, 0).unwrap(),
                message_count: 1,
                base_offset: producer_id,
                timestamp: producer_id,
            });
        }

        assert_eq!(table.len(), PRODUCERS_MAX);
        assert!(table.get(0).is_none(), "the longest idle producer goes");
        assert!(table.get(1).is_some());
    }

    #[test]
    fn given_entries_when_rebuilt_should_admit_and_expire_as_the_original() {
        let original = table(&[stamped(0, 0, 3, 100), stamped(0, 3, 1, 103)]);
        let rebuilt = ProducerTable::from_entries(
            original
                .entries()
                .map(|(producer_id, entry)| (producer_id, entry.clone())),
        );

        assert_eq!(rebuilt, original);
        assert_eq!(
            rebuilt.admit(producer(0, 3), 1, &[]),
            ProducerAdmission::Duplicate { base_offset: 103 }
        );
    }

    #[test]
    fn given_a_sequence_at_the_wrap_when_admitting_should_follow_with_zero() {
        let table = table(&[stamped(0, BatchProducer::SEQUENCE_MAX - 1, 2, 100)]);

        assert_eq!(
            table.admit(producer(0, 0), 1, &[]),
            ProducerAdmission::Append
        );
        assert_eq!(
            table.admit(producer(0, 1), 1, &[]),
            ProducerAdmission::OutOfOrder
        );
    }
}
