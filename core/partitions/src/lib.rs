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

mod iggy_index;
mod iggy_index_writer;
mod iggy_partition;
mod iggy_partitions;
mod journal;
mod log;
mod messages_writer;
mod offset_storage;
mod segment;
mod types;

use iggy_binary_protocol::{Message, PrepareHeader};
use iggy_common::IggyError;
pub use iggy_common::send_messages2::{IggyMessage2, IggyMessage2Header, IggyMessages2};
pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{
    AppendResult, Fragment, PartitionOffsets, PartitionsConfig, PollFragments, PollQueryResult,
    PollingArgs, PollingConsumer, SendMessagesResult,
};

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
    ) -> impl Future<Output = Result<PollQueryResult<4096>, IggyError>> {
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
