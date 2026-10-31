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

//! The idempotent mode of a producer.
//!
//! Every chunk carries the producer id, the epoch and the sequence of its
//! first message. A partition writes a chunk of one producer once, so the
//! producer can send a chunk again after an unknown outcome, also from a new
//! session. The sequence counts per partition, so the producer picks the
//! partition of a chunk itself and sends the chunks of one partition one at a
//! time.

use iggy_common::{BatchProducer, Identifier, IggyError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Sends of one chunk with the same producer after an unknown outcome,
/// before the send fails.
pub(crate) const RESENDS: usize = 3;

type PartitionSequences = HashMap<u32, Arc<tokio::sync::Mutex<Sequence>>>;

#[derive(Debug, Default)]
pub(crate) struct Idempotence {
    identity: Mutex<Identity>,
    sequences: Mutex<HashMap<Identifier, HashMap<Identifier, PartitionSequences>>>,
}

/// The producer id, its current epoch, and whether a newer copy fenced it.
#[derive(Debug, Default, Clone, Copy)]
struct Identity {
    producer_id: Option<u64>,
    epoch: u16,
    fenced: bool,
}

/// The next sequence of one partition, under the producer id and the epoch
/// that it counts for.
#[derive(Debug, Default)]
pub(crate) struct Sequence {
    producer_id: u64,
    epoch: u16,
    next: u32,
}

impl Idempotence {
    /// The producer id, once a session gave one.
    pub(crate) fn producer_id(&self) -> Option<u64> {
        self.identity().producer_id
    }

    /// Keeps `producer_id` unless a concurrent chunk already set one.
    pub(crate) fn set_producer_id(&self, producer_id: u64) {
        let mut identity = self.identity();
        if identity.producer_id.is_none() {
            identity.producer_id = Some(producer_id);
            identity.epoch = 0;
        }
    }

    /// The lock that orders the chunks of one partition. The holder sends
    /// one chunk at a time, so the sequence order is the send order.
    pub(crate) fn sequence(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partition_id: u32,
    ) -> Arc<tokio::sync::Mutex<Sequence>> {
        let mut sequences = self
            .sequences
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(sequence) = sequences
            .get(stream)
            .and_then(|topics| topics.get(topic))
            .and_then(|partitions| partitions.get(&partition_id))
        {
            return Arc::clone(sequence);
        }
        Arc::clone(
            sequences
                .entry(stream.clone())
                .or_default()
                .entry(topic.clone())
                .or_default()
                .entry(partition_id)
                .or_default(),
        )
    }

    /// The producer section of the next chunk of `sequence`. A new producer
    /// id or epoch starts the partition again at sequence 0.
    pub(crate) fn next_producer(
        &self,
        sequence: &mut Sequence,
    ) -> Result<BatchProducer, IggyError> {
        let identity = self.identity();
        if identity.fenced {
            return Err(IggyError::ProducerEpochFenced);
        }
        let producer_id = identity
            .producer_id
            .ok_or(IggyError::ProducerIdsExhausted)?;
        if sequence.producer_id != producer_id || sequence.epoch != identity.epoch {
            *sequence = Sequence {
                producer_id,
                epoch: identity.epoch,
                next: 0,
            };
        }
        BatchProducer::new(producer_id, identity.epoch, sequence.next)
            .ok_or(IggyError::InvalidCommand)
    }

    /// Records the written chunk of `count` messages that `producer` started.
    pub(crate) fn advance(sequence: &mut Sequence, producer: BatchProducer, count: u32) {
        sequence.next = producer.next(count).base_sequence();
    }

    /// Moves past `producer` to the next epoch, unless a chunk of another
    /// partition already did. After the last epoch the producer id goes, and
    /// the caller asks for a new one.
    pub(crate) fn next_epoch(&self, producer: BatchProducer) {
        let mut identity = self.identity();
        if identity.producer_id != Some(producer.producer_id())
            || identity.epoch != producer.epoch()
        {
            return;
        }
        if producer.epoch() == BatchProducer::EPOCH_MAX {
            identity.producer_id = None;
            identity.epoch = 0;
        } else {
            identity.epoch += 1;
        }
    }

    /// A newer copy of this producer wrote to a partition, so this one stops.
    pub(crate) fn fence(&self) {
        self.identity().fenced = true;
    }

    fn identity(&self) -> MutexGuard<'_, Identity> {
        self.identity.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream() -> Identifier {
        Identifier::numeric(1).unwrap()
    }

    fn topic() -> Identifier {
        Identifier::numeric(2).unwrap()
    }

    fn producer(producer_id: u64, epoch: u16, base_sequence: u32) -> BatchProducer {
        BatchProducer::new(producer_id, epoch, base_sequence).unwrap()
    }

    #[tokio::test]
    async fn given_written_chunks_when_sending_more_should_count_each_partition_on_its_own() {
        let idempotence = Idempotence::default();
        idempotence.set_producer_id(7);
        let first = idempotence.sequence(&stream(), &topic(), 0);
        let second = idempotence.sequence(&stream(), &topic(), 1);

        let mut sequence = first.lock().await;
        let sent = idempotence.next_producer(&mut sequence).unwrap();
        assert_eq!(sent, producer(7, 0, 0));
        Idempotence::advance(&mut sequence, sent, 3);
        assert_eq!(
            idempotence.next_producer(&mut sequence).unwrap(),
            producer(7, 0, 3)
        );
        drop(sequence);

        let mut other = second.lock().await;
        assert_eq!(
            idempotence.next_producer(&mut other).unwrap(),
            producer(7, 0, 0)
        );
    }

    #[tokio::test]
    async fn given_a_new_epoch_when_sending_should_start_every_partition_at_zero() {
        let idempotence = Idempotence::default();
        idempotence.set_producer_id(7);
        let lock = idempotence.sequence(&stream(), &topic(), 0);
        let mut sequence = lock.lock().await;
        let sent = idempotence.next_producer(&mut sequence).unwrap();
        Idempotence::advance(&mut sequence, sent, 5);

        idempotence.next_epoch(sent);
        idempotence.next_epoch(sent);

        assert_eq!(
            idempotence.next_producer(&mut sequence).unwrap(),
            producer(7, 1, 0),
            "a second move past the same producer is a no-op"
        );
    }

    #[tokio::test]
    async fn given_the_last_epoch_when_moving_on_should_drop_the_producer_id() {
        let idempotence = Idempotence::default();
        idempotence.set_producer_id(7);
        let lock = idempotence.sequence(&stream(), &topic(), 0);
        let mut sequence = lock.lock().await;
        for _ in 0..BatchProducer::EPOCH_MAX {
            let sent = idempotence.next_producer(&mut sequence).unwrap();
            idempotence.next_epoch(sent);
        }
        let last = idempotence.next_producer(&mut sequence).unwrap();
        assert_eq!(last.epoch(), BatchProducer::EPOCH_MAX);

        idempotence.next_epoch(last);

        assert_eq!(idempotence.producer_id(), None);
        idempotence.set_producer_id(8);
        assert_eq!(
            idempotence.next_producer(&mut sequence).unwrap(),
            producer(8, 0, 0)
        );
    }

    #[tokio::test]
    async fn given_a_fenced_producer_when_sending_should_refuse() {
        let idempotence = Idempotence::default();
        idempotence.set_producer_id(7);
        idempotence.fence();
        let lock = idempotence.sequence(&stream(), &topic(), 0);

        assert_eq!(
            idempotence.next_producer(&mut *lock.lock().await),
            Err(IggyError::ProducerEpochFenced)
        );
    }
}
