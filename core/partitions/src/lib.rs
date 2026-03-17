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

#![allow(clippy::future_not_send)]

mod iggy_partition;
mod iggy_partitions;
mod journal;
mod log;
mod types;

use bytes::{Bytes, BytesMut};
use iggy_common::{
    INDEX_SIZE, IggyError, IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet,
    PooledBuffer, header::PrepareHeader, message::Message,
};
pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{
    AppendResult, PartitionOffsets, PartitionsConfig, PollingArgs, PollingConsumer,
    SendMessagesResult,
};

pub(crate) fn decode_send_messages_batch(body: Bytes) -> Option<IggyMessagesBatchMut> {
    // TODO: This very is bad, IGGY-114 Fixes this.
    let mut body = body
        .try_into_mut()
        .unwrap_or_else(|body| BytesMut::from(body.as_ref()));

    if body.len() < 4 {
        return None;
    }

    let count_bytes = body.split_to(4);
    let count = u32::from_le_bytes(count_bytes.as_ref().try_into().ok()?);
    let indexes_len = (count as usize).checked_mul(INDEX_SIZE)?;

    if body.len() < indexes_len {
        return None;
    }

    let indexes_bytes = body.split_to(indexes_len);
    let indexes = IggyIndexesMut::from_bytes(PooledBuffer::from(indexes_bytes), 0);
    let messages = PooledBuffer::from(body);

    Some(IggyMessagesBatchMut::from_indexes_and_messages(
        indexes, messages,
    ))
}

/// Partition-level data plane operations.
///
/// `send_messages` MUST only append to the partition journal (prepare phase),
/// without committing/persisting to disk.
pub trait Partition {
    fn append_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> impl Future<Output = Result<AppendResult, IggyError>>;

    fn poll_messages(
        &self,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> impl Future<Output = Result<IggyMessagesBatchSet, IggyError>> {
        let _ = (consumer, args);
        async { Err(IggyError::FeatureUnavailable) }
    }

    /// # Errors
    /// Returns `IggyError::FeatureUnavailable` by default.
    fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let _ = (consumer, offset);
        Err(IggyError::FeatureUnavailable)
    }

    fn get_consumer_offset(&self, consumer: PollingConsumer) -> Option<u64> {
        let _ = consumer;
        None
    }

    fn offsets(&self) -> PartitionOffsets {
        PartitionOffsets::default()
    }
}
