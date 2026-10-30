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

//! Exactly-once delivery for a source.
//!
//! Each batch goes to one partition with a producer section: the source's producer id, an epoch,
//! and the sequence of its first message. A partition writes a batch of one producer once, so a
//! batch sent again after a lost reply or a crash lands once.
//!
//! The runtime keeps the producer in the source's state, around the plugin's own bytes, and
//! stores the range of a batch before it sends the batch. After a restart, the plugin produces
//! that batch again from its saved state. A batch with the same message count goes with the
//! stored range and the old epoch, so the partition answers it with its first offset. New batches
//! use the next epoch, which fences an older copy of the runtime.

use std::time::Instant;

use iggy::prelude::{
    BatchProducer, Client, Identifier, IggyClient, IggyError, IggyMessage, MessageClient,
    Partitioning,
};
use iggy_common::BINARY_OPERATION_TIMEOUT;
use iggy_connector_sdk::{ConnectorState, Error as SdkError};
use tracing::{debug, error, info, warn};

use crate::state::StateStorage;

/// Starts the state of an exactly-once source. No state that a plugin writes starts with these
/// bytes, and the last one is the layout version.
const MAGIC: [u8; 8] = *b"\0IGGYEO\x01";

/// Magic, the record, and the plugin flag, before the plugin's bytes.
const HEADER_SIZE: usize = MAGIC.len() + 8 + 2 + 4 + 4 + 1 + 8 + 2 + 4 + 4 + 1;

/// Sends of one batch after an unknown outcome or a lost producer, before the batch fails.
const RESENDS: usize = 3;

/// Where an exactly-once source writes.
#[derive(Debug, Clone)]
pub(crate) struct ExactlyOnceTarget {
    pub stream: Identifier,
    pub topic: Identifier,
    pub partition_id: u32,
}

/// The producer of a source, as its state keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerRecord {
    pub producer_id: u64,
    /// The epoch of new batches.
    pub epoch: u16,
    /// The partition that `next_sequence` counts for.
    pub partition_id: u32,
    pub next_sequence: u32,
    /// The batch stored before its send, which no later save settled.
    pub in_flight: Option<InFlight>,
}

/// The producer section of one batch, and its message count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InFlight {
    pub producer_id: u64,
    pub epoch: u16,
    pub base_sequence: u32,
    pub count: u32,
}

/// A stored state, split into the plugin's bytes and the producer record.
#[derive(Debug, Default)]
pub(crate) struct StoredState {
    pub plugin: Option<ConnectorState>,
    pub record: Option<ProducerRecord>,
}

impl StoredState {
    /// Splits a stored state. A state without the marker is the plugin's own, as a source without
    /// exactly-once delivery writes it.
    pub(crate) fn split(state: Option<ConnectorState>) -> Result<Self, SdkError> {
        let Some(state) = state else {
            return Ok(Self::default());
        };
        if !state.0.starts_with(&MAGIC) {
            return Ok(Self {
                plugin: Some(state),
                record: None,
            });
        }
        let bytes = &state.0;
        if bytes.len() < HEADER_SIZE {
            return Err(SdkError::InvalidState);
        }
        let mut reader = Reader {
            bytes,
            at: MAGIC.len(),
        };
        let producer_id = reader.u64();
        let epoch = reader.u16();
        let partition_id = reader.u32();
        let next_sequence = reader.u32();
        let has_in_flight = reader.flag()?;
        let in_flight = InFlight {
            producer_id: reader.u64(),
            epoch: reader.u16(),
            base_sequence: reader.u32(),
            count: reader.u32(),
        };
        let has_plugin = reader.flag()?;
        let rest = &bytes[HEADER_SIZE..];
        if !has_plugin && !rest.is_empty() {
            return Err(SdkError::InvalidState);
        }
        Ok(Self {
            plugin: has_plugin.then(|| ConnectorState(rest.to_vec())),
            record: Some(ProducerRecord {
                producer_id,
                epoch,
                partition_id,
                next_sequence,
                in_flight: has_in_flight.then_some(in_flight),
            }),
        })
    }
}

/// The state that an exactly-once source saves: the record, then the plugin's bytes.
pub(crate) fn encode(record: &ProducerRecord, plugin: Option<&[u8]>) -> ConnectorState {
    let in_flight = record.in_flight.unwrap_or(InFlight {
        producer_id: 0,
        epoch: 0,
        base_sequence: 0,
        count: 0,
    });
    let mut bytes = Vec::with_capacity(HEADER_SIZE + plugin.map_or(0, <[u8]>::len));
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&record.producer_id.to_le_bytes());
    bytes.extend_from_slice(&record.epoch.to_le_bytes());
    bytes.extend_from_slice(&record.partition_id.to_le_bytes());
    bytes.extend_from_slice(&record.next_sequence.to_le_bytes());
    bytes.push(u8::from(record.in_flight.is_some()));
    bytes.extend_from_slice(&in_flight.producer_id.to_le_bytes());
    bytes.extend_from_slice(&in_flight.epoch.to_le_bytes());
    bytes.extend_from_slice(&in_flight.base_sequence.to_le_bytes());
    bytes.extend_from_slice(&in_flight.count.to_le_bytes());
    bytes.push(u8::from(plugin.is_some()));
    if let Some(plugin) = plugin {
        bytes.extend_from_slice(plugin);
    }
    ConnectorState(bytes)
}

/// The plugin's bytes of a stored state, without the record of an exactly-once source.
pub(crate) fn plugin_state(
    state: Option<ConnectorState>,
) -> Result<Option<ConnectorState>, SdkError> {
    StoredState::split(state).map(|stored| stored.plugin)
}

impl ProducerRecord {
    /// The record with the next epoch, whose sequences start at 0. `None` after the last epoch.
    fn next_epoch(self) -> Option<Self> {
        let epoch = self
            .epoch
            .checked_add(1)
            .filter(|epoch| *epoch <= BatchProducer::EPOCH_MAX)?;
        Some(Self {
            epoch,
            next_sequence: 0,
            ..self
        })
    }

    /// The section of a batch of `count` messages. The batch in flight goes again when the count
    /// matches, so the partition finds its first write. Any other batch is a new one.
    fn batch(&self, count: u32) -> InFlight {
        match self.in_flight {
            Some(in_flight) if in_flight.count == count => in_flight,
            _ => InFlight {
                producer_id: self.producer_id,
                epoch: self.epoch,
                base_sequence: self.next_sequence,
                count,
            },
        }
    }

    /// The record after `batch` was written. A batch of an older epoch leaves the sequences of
    /// the current one alone.
    fn settled(self, batch: InFlight) -> Self {
        let next_sequence = if batch.producer_id == self.producer_id && batch.epoch == self.epoch {
            BatchProducer::advance(batch.base_sequence, batch.count)
        } else {
            self.next_sequence
        };
        Self {
            next_sequence,
            in_flight: None,
            ..self
        }
    }
}

/// The exactly-once state of one running source.
#[derive(Debug)]
pub(crate) struct ExactlyOnce {
    key: String,
    target: ExactlyOnceTarget,
    /// Read from the state at the first batch of this run.
    record: Option<ProducerRecord>,
    /// The plugin's bytes of the last save. A save before a send keeps them.
    plugin: Option<Vec<u8>>,
    /// The batch that went out, until a save settles it.
    sent: Option<InFlight>,
}

impl ExactlyOnce {
    pub(crate) fn new(key: &str, target: ExactlyOnceTarget) -> Self {
        Self {
            key: key.to_owned(),
            target,
            record: None,
            plugin: None,
            sent: None,
        }
    }

    /// Sends `messages` as one batch with the producer section of the source. The state names the
    /// batch before it goes, so a batch that a crash or a lost reply leaves in doubt goes again
    /// with the same section.
    pub(crate) async fn send(
        &mut self,
        client: &IggyClient,
        storage: &StateStorage,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            return Ok(());
        }
        let count = u32::try_from(messages.len()).map_err(|_| IggyError::InvalidMessagesCount)?;
        let mut record = self.record(client, storage).await?;
        let mut batch = record.batch(count);
        if let Some(stored) = record.in_flight
            && stored != batch
        {
            warn!(
                "Exactly-once source {} stored a batch of {} messages, but its plugin produced {count}. The stored batch can land twice.",
                self.key, stored.count
            );
        }
        record.in_flight = Some(batch);
        self.store(storage, &record).await?;

        let partitioning = Partitioning::partition_id(self.target.partition_id);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let producer = BatchProducer::new(batch.producer_id, batch.epoch, batch.base_sequence)
                .ok_or(IggyError::InvalidCommand)?;
            match client
                .send_producer_messages(
                    &self.target.stream,
                    &self.target.topic,
                    &partitioning,
                    producer,
                    messages,
                )
                .await
            {
                Ok(_) => {
                    self.sent = Some(batch);
                    return Ok(());
                }
                // The same section makes a resend safe, whether or not the first one landed.
                Err(IggyError::OperationUncertain(op)) if attempts <= RESENDS => {
                    if let Err(error) = client.abandon_operation(op).await {
                        debug!(
                            "Exactly-once source {} could not release {op}: {error}",
                            self.key
                        );
                    }
                }
                Err(IggyError::LaneBlocked(op)) if attempts <= RESENDS => {
                    if let Err(error) = client
                        .resolve_operation(op, Instant::now() + BINARY_OPERATION_TIMEOUT)
                        .await
                    {
                        debug!(
                            "Exactly-once source {} could not resolve {op}: {error}",
                            self.key
                        );
                    }
                }
                // The partition forgot the producer, or holds other batches than the state
                // names. The next epoch starts at sequence 0, and a batch that the last run wrote
                // can then land twice.
                Err(
                    error @ (IggyError::ProducerUnknown | IggyError::ProducerSequenceOutOfOrder),
                ) if attempts <= RESENDS => {
                    warn!(
                        "Exactly-once source {} moves to a new epoch: {error}",
                        self.key
                    );
                    record = ProducerRecord {
                        in_flight: None,
                        ..self.next_epoch(client, record).await?
                    };
                    batch = record.batch(count);
                    record.in_flight = Some(batch);
                    self.store(storage, &record).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Saves the state after a batch: the plugin's new bytes, and the record past the batch that
    /// went out. `None` when there is nothing to save.
    pub(crate) async fn commit(
        &mut self,
        client: &IggyClient,
        storage: &StateStorage,
        plugin: Option<ConnectorState>,
    ) -> Option<Result<(), SdkError>> {
        let sent = self.sent.take();
        if sent.is_none() && plugin.is_none() {
            return None;
        }
        let record = match self.record(client, storage).await {
            Ok(record) => record,
            Err(error) => return Some(Err(SdkError::TransientState(error.to_string()))),
        };
        // A batch that this run did not send keeps its place in flight, so a replay finds it.
        let record = sent.map_or(record, |batch| record.settled(batch));
        let plugin = plugin.map(|state| state.0).or_else(|| self.plugin.clone());
        let saved = storage.save(encode(&record, plugin.as_deref())).await;
        if saved.is_ok() {
            self.record = Some(record);
            self.plugin = plugin;
        }
        Some(saved)
    }

    /// The record of this run, read from the state at the first batch. A new source gets a new
    /// producer id. A source that ran before moves to the next epoch, which fences an older copy
    /// of the runtime, and keeps the batch it had in flight.
    async fn record(
        &mut self,
        client: &IggyClient,
        storage: &StateStorage,
    ) -> Result<ProducerRecord, IggyError> {
        if let Some(record) = self.record {
            return Ok(record);
        }
        let stored = storage
            .load()
            .await
            .and_then(StoredState::split)
            .map_err(|error| {
                error!(
                    "Exactly-once source {} could not read its state: {error}",
                    self.key
                );
                IggyError::Error
            })?;
        self.plugin = stored.plugin.map(|state| state.0);
        let mut record = match stored.record {
            Some(record) => self.next_epoch(client, record).await?,
            None => ProducerRecord {
                producer_id: client.next_producer_id().await?,
                epoch: 0,
                partition_id: self.target.partition_id,
                next_sequence: 0,
                in_flight: None,
            },
        };
        if record.partition_id != self.target.partition_id {
            warn!(
                "Exactly-once source {} moved from partition {} to {}. Its batch in flight can land twice.",
                self.key, record.partition_id, self.target.partition_id
            );
            record = ProducerRecord {
                partition_id: self.target.partition_id,
                next_sequence: 0,
                in_flight: None,
                ..record
            };
        }
        info!(
            "Exactly-once source {} writes as producer {} at epoch {} to partition {}.",
            self.key, record.producer_id, record.epoch, record.partition_id
        );
        self.record = Some(record);
        Ok(record)
    }

    /// `record` at the next epoch, or with a new producer id after the last epoch.
    async fn next_epoch(
        &self,
        client: &IggyClient,
        record: ProducerRecord,
    ) -> Result<ProducerRecord, IggyError> {
        match record.next_epoch() {
            Some(record) => Ok(record),
            None => Ok(ProducerRecord {
                producer_id: client.next_producer_id().await?,
                epoch: 0,
                next_sequence: 0,
                ..record
            }),
        }
    }

    /// Saves `record` with the plugin's last bytes, before a batch goes out.
    async fn store(
        &mut self,
        storage: &StateStorage,
        record: &ProducerRecord,
    ) -> Result<(), IggyError> {
        storage
            .save(encode(record, self.plugin.as_deref()))
            .await
            .map_err(|error| {
                error!(
                    "Exactly-once source {} could not store its batch before the send: {error}",
                    self.key
                );
                IggyError::Error
            })?;
        self.record = Some(*record);
        Ok(())
    }
}

/// Reads the fixed fields of a stored record. The caller checked the length.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut field = [0; N];
        field.copy_from_slice(&self.bytes[self.at..self.at + N]);
        self.at += N;
        field
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.take())
    }

    fn flag(&mut self) -> Result<bool, SdkError> {
        match self.take::<1>() {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(SdkError::InvalidState),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(in_flight: Option<InFlight>) -> ProducerRecord {
        ProducerRecord {
            producer_id: 7 << 20,
            epoch: 2,
            partition_id: 1,
            next_sequence: 9,
            in_flight,
        }
    }

    fn in_flight(epoch: u16, base_sequence: u32, count: u32) -> InFlight {
        InFlight {
            producer_id: 7 << 20,
            epoch,
            base_sequence,
            count,
        }
    }

    #[test]
    fn given_a_record_and_plugin_bytes_when_round_tripped_should_split_back() {
        for plugin in [None, Some(&[][..]), Some(&b"\x91\x05"[..])] {
            for in_flight in [None, Some(in_flight(2, 9, 3))] {
                let stored = StoredState::split(Some(encode(&record(in_flight), plugin))).unwrap();

                assert_eq!(stored.record, Some(record(in_flight)));
                assert_eq!(
                    stored.plugin.map(|state| state.0),
                    plugin.map(<[u8]>::to_vec)
                );
            }
        }
    }

    #[test]
    fn given_plugin_bytes_without_the_marker_when_split_should_pass_them_through() {
        let stored = StoredState::split(Some(ConnectorState(b"\x91\x05".to_vec()))).unwrap();

        assert_eq!(stored.record, None);
        assert_eq!(
            stored.plugin.map(|state| state.0),
            Some(b"\x91\x05".to_vec())
        );
        assert!(StoredState::split(None).unwrap().plugin.is_none());
    }

    #[test]
    fn given_a_damaged_record_when_split_should_refuse_it() {
        let state = encode(&record(None), Some(b"plugin")).0;

        let short = ConnectorState(state[..HEADER_SIZE - 1].to_vec());
        assert!(matches!(
            StoredState::split(Some(short)),
            Err(SdkError::InvalidState)
        ));
        let mut bad_flag = state.clone();
        bad_flag[HEADER_SIZE - 1] = 2;
        assert!(matches!(
            StoredState::split(Some(ConnectorState(bad_flag))),
            Err(SdkError::InvalidState)
        ));
        let no_plugin = encode(&record(None), None).0;
        let mut trailing = no_plugin.clone();
        trailing.push(0);
        assert!(matches!(
            StoredState::split(Some(ConnectorState(trailing))),
            Err(SdkError::InvalidState)
        ));
    }

    #[test]
    fn given_a_stored_batch_when_the_count_matches_should_send_it_again() {
        let stored = in_flight(1, 4, 3);

        assert_eq!(record(Some(stored)).batch(3), stored);
        assert_eq!(record(Some(stored)).batch(5), in_flight(2, 9, 5));
        assert_eq!(record(None).batch(3), in_flight(2, 9, 3));
    }

    #[test]
    fn given_a_written_batch_when_settled_should_count_only_the_current_epoch() {
        assert_eq!(
            record(Some(in_flight(2, 9, 3))).settled(in_flight(2, 9, 3)),
            ProducerRecord {
                next_sequence: 12,
                ..record(None)
            }
        );
        assert_eq!(
            record(Some(in_flight(1, 4, 3))).settled(in_flight(1, 4, 3)),
            record(None),
            "a replay of the old epoch leaves the new one at its sequence"
        );
    }

    #[test]
    fn given_the_last_epoch_when_moving_on_should_need_a_new_id() {
        let next = record(None).next_epoch().unwrap();
        assert_eq!((next.epoch, next.next_sequence), (3, 0));

        let last = ProducerRecord {
            epoch: BatchProducer::EPOCH_MAX,
            ..record(None)
        };
        assert_eq!(last.next_epoch(), None);
    }
}
