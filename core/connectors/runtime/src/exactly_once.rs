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
//! The runtime keeps the producer in the source's state, around the plugin's own bytes. The record
//! names the epoch and the sequence of the next batch, and the save of the plugin's new bytes after
//! a batch moves the sequence past it. A batch without new plugin bytes is not saved. After a NACK
//! or a restart, the plugin produces the unsaved batches again from its saved state, and they go
//! with the same sections, so the partition answers them with their first offsets. A new record or
//! a new epoch is saved before its first batch goes. The first save after a batch of a restarted
//! source moves to the next epoch, which fences an older copy of the runtime.

use std::time::Instant;

use iggy::prelude::{
    BatchProducer, Client, Identifier, IggyClient, IggyError, IggyMessage, MessageClient,
};
use iggy_common::BINARY_OPERATION_TIMEOUT;
use iggy_connector_sdk::{ConnectorState, Error as SdkError};
use tracing::{debug, error, info, warn};

use crate::state::StateStorage;

/// Starts the state of an exactly-once source. No state that a plugin writes starts with these
/// bytes, and the last one is the layout version.
const MAGIC: [u8; 8] = *b"\0IGGYEO\x01";

/// Magic, the record, and the plugin flag, before the plugin's bytes.
const HEADER_SIZE: usize = MAGIC.len() + 8 + 2 + 4 + 4 + 1;

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
    pub epoch: u16,
    /// The partition that `next_sequence` counts for.
    pub partition_id: u32,
    /// The first sequence of the next batch. A batch that no save settled goes again with it.
    pub next_sequence: u32,
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
        let record = ProducerRecord {
            producer_id: reader.u64(),
            epoch: reader.u16(),
            partition_id: reader.u32(),
            next_sequence: reader.u32(),
        };
        if record.producer().is_err() {
            return Err(SdkError::InvalidState);
        }
        let has_plugin = reader.flag()?;
        let rest = &bytes[HEADER_SIZE..];
        if !has_plugin && !rest.is_empty() {
            return Err(SdkError::InvalidState);
        }
        Ok(Self {
            plugin: has_plugin.then(|| ConnectorState(rest.to_vec())),
            record: Some(record),
        })
    }

    /// A copy of the plugin's bytes, without the record, for the plugin's init.
    pub(crate) fn plugin_state(&self) -> Option<ConnectorState> {
        self.plugin
            .as_ref()
            .map(|state| ConnectorState(state.0.clone()))
    }
}

/// The state that an exactly-once source saves: the record, then the plugin's bytes.
pub(crate) fn encode(record: &ProducerRecord, plugin: Option<&[u8]>) -> ConnectorState {
    let mut bytes = Vec::with_capacity(HEADER_SIZE + plugin.map_or(0, <[u8]>::len));
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&record.producer_id.to_le_bytes());
    bytes.extend_from_slice(&record.epoch.to_le_bytes());
    bytes.extend_from_slice(&record.partition_id.to_le_bytes());
    bytes.extend_from_slice(&record.next_sequence.to_le_bytes());
    bytes.push(u8::from(plugin.is_some()));
    if let Some(plugin) = plugin {
        bytes.extend_from_slice(plugin);
    }
    ConnectorState(bytes)
}

impl ProducerRecord {
    /// The producer section of the next batch.
    fn producer(&self) -> Result<BatchProducer, IggyError> {
        BatchProducer::new(self.producer_id, self.epoch, self.next_sequence)
            .ok_or(IggyError::InvalidCommand)
    }

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

    /// The record after `producer` wrote a batch of `count` messages.
    fn settled(self, producer: BatchProducer, count: u32) -> Self {
        Self {
            next_sequence: producer.next(count).base_sequence(),
            ..self
        }
    }
}

/// The exactly-once state of one running source.
#[derive(Debug)]
pub(crate) struct ExactlyOnce {
    key: String,
    target: ExactlyOnceTarget,
    /// The record that the state held at init. The first batch starts from it.
    stored: Option<ProducerRecord>,
    /// The record of this run, set at the first batch.
    record: Option<ProducerRecord>,
    /// The plugin's bytes of the last save. A save of a new record keeps them.
    plugin: Option<Vec<u8>>,
    /// The batch that went out and its message count, until a save settles it.
    sent: Option<(BatchProducer, u32)>,
    /// An earlier run wrote with the record, so the save after the first batch of this run moves
    /// to the next epoch.
    fence: bool,
}

impl ExactlyOnce {
    /// Starts from `stored`, the state that init gave the plugin. A reload at the first batch
    /// would take the version of another runtime's save, so its conflict would go unseen.
    pub(crate) fn new(key: &str, target: ExactlyOnceTarget, stored: StoredState) -> Self {
        Self {
            key: key.to_owned(),
            target,
            stored: stored.record,
            record: None,
            plugin: stored.plugin.map(|state| state.0),
            sent: None,
            fence: false,
        }
    }

    /// Sends `messages` as one batch with the producer section of the source. A batch that a crash
    /// or a lost reply leaves in doubt goes again with the same section, because no save moved the
    /// record past it.
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
        let mut producer = record.producer()?;
        let mut attempts = 0;
        loop {
            attempts += 1;
            match client
                .send_producer_messages(
                    &self.target.stream,
                    &self.target.topic,
                    self.target.partition_id,
                    producer,
                    messages,
                )
                .await
            {
                Ok(_) => {
                    self.sent = Some((producer, count));
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
                    record = self.next_epoch(client, record).await?;
                    self.store(storage, &record).await?;
                    self.fence = false;
                    producer = record.producer()?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Saves the state after a batch: the plugin's new bytes, and the record past the batch that
    /// went out. `None` when the plugin gave no new bytes, so nothing is saved.
    pub(crate) async fn commit(
        &mut self,
        client: &IggyClient,
        storage: &StateStorage,
        plugin: Option<ConnectorState>,
    ) -> Option<Result<(), SdkError>> {
        let sent = self.sent.take();
        let Some(plugin) = plugin else {
            // A restart starts the plugin from its saved bytes, which come before the batch, so
            // the stored record keeps the batch's sequence. Only this run moves past it.
            if let Some((producer, count)) = sent
                && let Some(record) = self.record
            {
                self.record = Some(record.settled(producer, count));
            }
            return None;
        };
        let record = match self.record(client, storage).await {
            Ok(record) => record,
            Err(error) => return Some(Err(SdkError::TransientState(error.to_string()))),
        };
        let mut record = match sent {
            Some((producer, count)) => record.settled(producer, count),
            None => record,
        };
        // The batches of this run before this save went at the epoch of the last run, so a replay
        // found their first writes. Later batches take the next epoch, which fences an older copy.
        let fences = sent.is_some() && self.fence;
        if fences {
            record = match self.next_epoch(client, record).await {
                Ok(record) => record,
                Err(error) => return Some(Err(SdkError::TransientState(error.to_string()))),
            };
        }
        let saved = storage.save(encode(&record, Some(&plugin.0))).await;
        if saved.is_ok() {
            if fences {
                self.fence = false;
                info!(
                    "Exactly-once source {} writes as producer {} at epoch {} to partition {}.",
                    self.key, record.producer_id, record.epoch, record.partition_id
                );
            }
            self.record = Some(record);
            self.plugin = Some(plugin.0);
        }
        Some(saved)
    }

    /// The record of this run, from the state at init. A new source gets a new producer id. A
    /// source that ran before keeps its record, so the batch that no save settled goes again with
    /// the same section.
    async fn record(
        &mut self,
        client: &IggyClient,
        storage: &StateStorage,
    ) -> Result<ProducerRecord, IggyError> {
        if let Some(record) = self.record {
            return Ok(record);
        }
        let record = match self.stored {
            Some(record) if record.partition_id == self.target.partition_id => {
                self.fence = true;
                info!(
                    "Exactly-once source {} resumes as producer {} at epoch {} to partition {}.",
                    self.key, record.producer_id, record.epoch, record.partition_id
                );
                self.record = Some(record);
                return Ok(record);
            }
            Some(record) => {
                warn!(
                    "Exactly-once source {} moved from partition {} to {}. Its unsaved batch can land twice.",
                    self.key, record.partition_id, self.target.partition_id
                );
                ProducerRecord {
                    partition_id: self.target.partition_id,
                    ..self.next_epoch(client, record).await?
                }
            }
            None => ProducerRecord {
                producer_id: client.next_producer_id().await?,
                epoch: 0,
                partition_id: self.target.partition_id,
                next_sequence: 0,
            },
        };
        // A restart must find the producer of a batch that went out before any save settled it.
        self.store(storage, &record).await?;
        info!(
            "Exactly-once source {} writes as producer {} at epoch {} to partition {}.",
            self.key, record.producer_id, record.epoch, record.partition_id
        );
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

    /// Saves `record` with the plugin's last bytes, before a batch goes out with it.
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
                    "Exactly-once source {} could not store its producer before the send: {error}",
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
    use crate::state::FileStateProvider;

    fn record() -> ProducerRecord {
        ProducerRecord {
            producer_id: 7 << 20,
            epoch: 2,
            partition_id: 1,
            next_sequence: 9,
        }
    }

    #[test]
    fn given_a_record_and_plugin_bytes_when_round_tripped_should_split_back() {
        for plugin in [None, Some(&[][..]), Some(&b"\x91\x05"[..])] {
            let stored = StoredState::split(Some(encode(&record(), plugin))).unwrap();

            assert_eq!(stored.record, Some(record()));
            assert_eq!(
                stored.plugin.map(|state| state.0),
                plugin.map(<[u8]>::to_vec)
            );
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
        let state = encode(&record(), Some(b"plugin")).0;

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
        let over_max = ProducerRecord {
            epoch: BatchProducer::EPOCH_MAX + 1,
            ..record()
        };
        assert!(matches!(
            StoredState::split(Some(encode(&over_max, None))),
            Err(SdkError::InvalidState)
        ));
        let no_plugin = encode(&record(), None).0;
        let mut trailing = no_plugin.clone();
        trailing.push(0);
        assert!(matches!(
            StoredState::split(Some(ConnectorState(trailing))),
            Err(SdkError::InvalidState)
        ));
    }

    #[test]
    fn given_a_written_batch_when_settled_should_name_the_sequence_after_it() {
        let producer = record().producer().unwrap();

        assert_eq!(
            record().settled(producer, 3),
            ProducerRecord {
                next_sequence: 12,
                ..record()
            }
        );
    }

    #[test]
    fn given_the_last_epoch_when_moving_on_should_need_a_new_id() {
        let next = record().next_epoch().unwrap();
        assert_eq!((next.epoch, next.next_sequence), (3, 0));

        let last = ProducerRecord {
            epoch: BatchProducer::EPOCH_MAX,
            ..record()
        };
        assert_eq!(last.next_epoch(), None);
    }

    #[tokio::test]
    async fn given_a_batch_without_plugin_state_when_committed_should_keep_the_stored_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = StateStorage::File(FileStateProvider::new(
            dir.path().join("state").to_string_lossy().to_string(),
        ));
        storage
            .save(encode(&record(), Some(b"saved")))
            .await
            .unwrap();
        let init = StoredState::split(storage.load().await.unwrap()).unwrap();
        let target = ExactlyOnceTarget {
            stream: Identifier::named("stream").unwrap(),
            topic: Identifier::named("topic").unwrap(),
            partition_id: record().partition_id,
        };
        let mut exactly_once = ExactlyOnce::new("source", target, init);
        let client = IggyClient::default();
        let resumed = exactly_once.record(&client, &storage).await.unwrap();
        exactly_once.sent = Some((resumed.producer().unwrap(), 3));

        assert!(exactly_once.commit(&client, &storage, None).await.is_none());

        // A restart produces the batch again from the saved bytes, with the same sequence.
        let stored = StoredState::split(storage.load().await.unwrap()).unwrap();
        assert_eq!(stored.record, Some(record()));
        assert_eq!(stored.plugin.map(|state| state.0), Some(b"saved".to_vec()));
        let next = exactly_once.record.unwrap();
        assert_eq!(next.next_sequence, 12);

        exactly_once.sent = Some((next.producer().unwrap(), 2));
        let saved = exactly_once
            .commit(&client, &storage, Some(ConnectorState(b"later".to_vec())))
            .await;

        assert!(matches!(saved, Some(Ok(()))));
        let stored = StoredState::split(storage.load().await.unwrap()).unwrap();
        assert_eq!(stored.record, record().next_epoch());
        assert_eq!(stored.plugin.map(|state| state.0), Some(b"later".to_vec()));
    }

    #[tokio::test]
    async fn given_a_save_after_init_when_the_first_batch_goes_should_start_from_the_init_state() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage = StateStorage::File(FileStateProvider::new(
            dir.path().join("state").to_string_lossy().to_string(),
        ));
        // Another runtime saved after this one read the state at init.
        let other = ProducerRecord {
            producer_id: 9 << 20,
            ..record()
        };
        storage.save(encode(&other, Some(b"other"))).await.unwrap();
        let init = StoredState::split(Some(encode(&record(), Some(b"mine")))).unwrap();
        let target = ExactlyOnceTarget {
            stream: Identifier::named("stream").unwrap(),
            topic: Identifier::named("topic").unwrap(),
            partition_id: record().partition_id,
        };
        let mut exactly_once = ExactlyOnce::new("source", target, init);

        let resumed = exactly_once
            .record(&IggyClient::default(), &storage)
            .await
            .unwrap();

        assert_eq!(resumed, record());
        assert_eq!(exactly_once.plugin.as_deref(), Some(&b"mine"[..]));
    }
}
