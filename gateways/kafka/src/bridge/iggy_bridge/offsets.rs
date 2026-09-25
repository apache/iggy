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

use iggy::prelude::Partition;

use super::IggyBridge;
use crate::bridge::error::BridgeError;

impl IggyBridge {
    /// The high watermark of each of `partitions`, in the same order, from one [`Self::probe`].
    ///
    /// `i64`, as `ListOffsets` sends it. A partition the topic lacks fails alone.
    ///
    /// # Errors
    ///
    /// As [`Self::probe`].
    pub async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        let topic = self.probe(kafka_topic).await?;
        Ok(partitions
            .iter()
            .map(|&partition| {
                let watermark = topic
                    .get(partition)
                    .map(|probe| i64::try_from(probe.high_watermark).unwrap_or(i64::MAX))
                    .ok_or_else(|| BridgeError::PartitionOutOfRange {
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count: topic.partitions_count(),
                    });
                (partition, watermark)
            })
            .collect())
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
