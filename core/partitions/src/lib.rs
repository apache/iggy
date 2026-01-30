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

mod iggy_partition;
mod iggy_partitions;
mod types;

use iggy_common::sharding::IggyNamespace;
pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{AppendResult, PartitionOffsets, PollingArgs, PollingConsumer, SendMessagesResult};

/// High-level partition operations for request handlers.
///
/// This trait abstracts over both single-node vs clustered modes.
///
/// # Implementation:
///
/// ## Single-node:
///
/// ```text
/// send_messages() ──► storage.append_prepared()
///                           │
///                           ▼
///                     storage.advance_commit()
///                           │
///                           ▼
///                     Return success
/// ```
///
/// ## Clustered
///
/// ```text
/// send_messages() ──► Create Prepare(messages)
///                           │
///                           ▼
///                     Broadcast to replicas
///                           │
///                           ▼
///                     Replicas: storage.append_prepared()
///                           │
///                           ▼
///                     Wait for quorum PrepareOk
///                           │
///                           ▼
///                     storage.advance_commit()
///                           │
///                           ▼
///                     Return success
/// ```
pub trait Partitions {
    /// The message batch type for write operations.
    type MessageBatch;

    /// The result type returned from poll operations.
    type PollResult;

    type Error;

    /// Send messages to a partition.
    ///
    /// Messages are appended atomically as a batch with sequentially assigned
    /// offsets. Returns only:
    /// - Single-node: after durable local write (fsync)
    /// - Clustered: after VSR quorum acknowledgment
    fn send_messages(
        &self,
        namespace: IggyNamespace,
        batch: Self::MessageBatch,
    ) -> impl Future<Output = Result<SendMessagesResult, Self::Error>>;

    /// Store consumer offset for progress tracking.
    ///
    /// Persists the consumer's position, enabling resumption after restarts.
    fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        namespace: IggyNamespace,
        offset: u64,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Poll messages from a partition.
    ///
    /// Returns only **committed** messages according to the polling strategy.
    /// Messages that are prepared but not yet committed are not visible.
    fn poll_messages(
        &self,
        consumer: PollingConsumer,
        namespace: IggyNamespace,
        args: PollingArgs,
    ) -> impl Future<Output = Result<Self::PollResult, Self::Error>>;

    /// Get stored consumer offset.
    ///
    /// Returns the last stored offset, or 0 if none exists.
    fn get_consumer_offset(
        &self,
        consumer: PollingConsumer,
        namespace: IggyNamespace,
    ) -> impl Future<Output = Result<u64, Self::Error>>;

    /// Get partition's current commit offset.
    ///
    /// Returns the highest committed offset (visible to consumers).
    /// In clustered mode, may lag slightly behind the primary.
    fn get_offset(
        &self,
        namespace: IggyNamespace,
    ) -> impl Future<Output = Result<u64, Self::Error>>;

    /// Initialize storage for a new partition.
    ///
    /// Sets up segments directory, initial segment, and offset tracking.
    fn create_partition(
        &self,
        namespace: IggyNamespace,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Delete partition and all its data.
    ///
    /// Removes messages, segments, indexes, and consumer offsets.
    fn delete_partition(
        &self,
        namespace: IggyNamespace,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Purge all messages, preserving partition structure.
    ///
    /// Resets offset to 0. Consumer offsets become invalid.
    fn purge_partition(
        &self,
        namespace: IggyNamespace,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}
