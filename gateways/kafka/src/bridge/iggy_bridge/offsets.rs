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

//! Offset and high-watermark lookups.

use std::sync::Arc;

use iggy::prelude::{IggyMessage, IggyTimestamp, Partition, PollingStrategy};
use tokio::time::Instant;

use super::IggyBridge;
use super::fetch::{FetchSlot, PartitionProbe};
use crate::bridge::error::BridgeError;
use crate::records::record_timestamp;

/// Which record a `ListOffsets` lookup by time asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordLookup {
    /// The first record appended at or after this time, in Kafka milliseconds.
    AppendedAtOrAfter(i64),
    /// The last record, the latest Iggy appended. Its reported timestamp is the producer's when
    /// one was set, so on a `CreateTime` topic it need not be the largest one.
    Last,
}

/// A record a [`RecordLookup`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocatedRecord {
    pub offset: i64,
    /// As Fetch reports it: the producer timestamp when one was set, otherwise the append time.
    pub timestamp: i64,
    /// When Iggy appended it, in microseconds. What a lookup by time matches against.
    pub appended_at: u64,
}

impl LocatedRecord {
    /// Whether Iggy appended this record before `millis`, so a lookup by that time cannot match
    /// it.
    pub fn appended_before(&self, millis: i64) -> bool {
        self.appended_at < kafka_millis_to_micros(millis)
    }
}

impl From<&IggyMessage> for LocatedRecord {
    fn from(message: &IggyMessage) -> Self {
        let offset = i64::try_from(message.header.offset).unwrap_or(i64::MAX);
        let appended_at = message.header.timestamp;
        // A message the gateway cannot map still has a server time, and the offset is what the
        // client seeks to; Fetch reports the mapping failure when it reaches the record.
        let timestamp = record_timestamp(message)
            .unwrap_or_else(|_| i64::try_from(appended_at / 1000).unwrap_or(-1));
        Self {
            offset,
            timestamp,
            appended_at,
        }
    }
}

impl IggyBridge {
    /// The high watermark of each of `partitions`, in the same order, from one `probe` of the
    /// topic.
    ///
    /// `i64`, as `ListOffsets` sends it. A partition the topic lacks, or one Iggy loads, fails
    /// alone.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's rules.
    /// [`BridgeError::Iggy`] if the stream or topic is missing, or the call fails.
    /// [`BridgeError::Timeout`] past `REQUEST_TIMEOUT`.
    pub async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        Ok(self
            .partition_probes(kafka_topic, partitions)
            .await?
            .into_iter()
            .map(|(partition, probe)| {
                let watermark =
                    probe.map(|probe| i64::try_from(probe.high_watermark).unwrap_or(i64::MAX));
                (partition, watermark)
            })
            .collect())
    }

    /// [`Self::high_watermarks`], keeping the whole probe of each partition.
    ///
    /// # Errors
    ///
    /// See [`Self::high_watermarks`].
    pub(crate) async fn partition_probes(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<PartitionProbe, BridgeError>)>, BridgeError> {
        let topic = self.probe(kafka_topic).await?;
        Ok(partitions
            .iter()
            .map(|&partition| {
                let probe = match topic.get(partition) {
                    None => Err(BridgeError::PartitionOutOfRange {
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count: topic.partitions_count(),
                    }),
                    // A seek to that watermark lands short, so the consumer reads again.
                    Some(probe) if probe.is_loading() => Err(BridgeError::PartitionLoading {
                        topic: kafka_topic.to_string(),
                        partition,
                    }),
                    Some(probe) => Ok(probe),
                };
                (partition, probe)
            })
            .collect())
    }

    /// Finds the record `lookup` names in `partition`, with the client of `slot`, and hands the
    /// slot back. `Ok(None)` when the poll comes back empty, which Iggy also answers on a read
    /// fault or before the partition seeds its commit point, so it proves no miss alone. `None` if
    /// no answer comes by `deadline`.
    ///
    /// Iggy matches a time against the broker append time of each batch, so a record is found by
    /// when Iggy stored it, not by the producer timestamp a Kafka broker searches on a
    /// `CreateTime` topic. The timestamp reported back is the one Fetch shows.
    pub(crate) async fn locate_record(
        self: &Arc<Self>,
        slot: FetchSlot,
        kafka_topic: &str,
        partition: u32,
        lookup: RecordLookup,
        deadline: Instant,
    ) -> Option<(Result<Option<LocatedRecord>, BridgeError>, FetchSlot)> {
        let strategy = match lookup {
            RecordLookup::AppendedAtOrAfter(millis) => {
                PollingStrategy::timestamp(IggyTimestamp::from(kafka_millis_to_micros(millis)))
            }
            RecordLookup::Last => PollingStrategy::last(),
        };
        let (polled, slot) = self
            .poll_by(slot, kafka_topic, partition, strategy, 1, deadline)
            .await?;
        let located = polled.map(|polled| polled.messages.first().map(LocatedRecord::from));
        Some((located, slot))
    }

    /// [`Self::high_watermarks`] for one partition.
    ///
    /// # Errors
    ///
    /// See [`Self::high_watermarks`].
    ///
    /// # Panics
    ///
    /// Never in practice: [`Self::high_watermarks`] returns exactly one result per requested
    /// partition on `Ok`, and this always requests exactly one.
    pub async fn high_watermark(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> Result<i64, BridgeError> {
        let (_, watermark) = self
            .high_watermarks(kafka_topic, &[partition])
            .await?
            .into_iter()
            .next()
            .expect(
                "high_watermarks returns exactly one result per requested partition, \
                 and exactly one was requested",
            );
        watermark
    }
}

/// A time before the epoch matches from the first record, as it does on a Kafka broker, and one
/// past what Iggy's microseconds can hold matches nothing.
fn kafka_millis_to_micros(millis: i64) -> u64 {
    u64::try_from(millis).unwrap_or(0).saturating_mul(1000)
}

/// One past the last committed offset of `partition`, or 0 when it is empty.
///
/// Fetch and `ListOffsets` both use this, so a consumer that seeks to LATEST fetches in range.
///
/// - Empty needs both counters: retention lowers `messages_count`, never `current_offset`.
/// - Known gap: `(0, 0)` also comes back when the only message, at 0, expired, or when the server
///   has no stats for the partition yet.
/// - Not the offset of the next write: the server can reserve offsets past it.
pub(super) const fn high_watermark(partition: &Partition) -> u64 {
    if partition.messages_count == 0 && partition.current_offset == 0 {
        0
    } else {
        partition.current_offset.saturating_add(1)
    }
}
