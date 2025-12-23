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

pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{PollMetadata, PollingArgs, PollingConsumer, SendMessagesResult};

/// The core abstraction for partition operations in clustering.
///
/// This trait defines the data-plane operations for partitions that
/// need to be coordinated across a cluster using viewstamped replication.
/// Implementations can vary between single-node and clustered deployments.
pub trait Partitions {
    // /// Message batch type for sending messages.
    // type MessageBatch;
    // /// Message batch set type for poll results.
    // type MessageBatchSet;

    // /// Poll messages from a partition.
    // fn poll_messages(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     consumer: PollingConsumer,
    //     args: PollingArgs,
    // ) -> impl Future<Output = Result<(PollMetadata, Self::MessageBatchSet), IggyError>> + Send;

    // /// Send/append messages to a partition.
    // fn send_messages(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     batch: Self::MessageBatch,
    // ) -> impl Future<Output = Result<SendMessagesResult, IggyError>> + Send;

    // /// Create a new partition.
    // fn create_partition(
    //     &self,
    //     namespace: &IggyNamespace,
    // ) -> impl Future<Output = Result<LocalIdx, IggyError>> + Send;

    // /// Delete partitions from the collection.
    // fn delete_partitions(
    //     &self,
    //     namespaces: &[IggyNamespace],
    // ) -> impl Future<Output = Result<Vec<LocalIdx>, IggyError>> + Send;

    // /// Get the stored offset for a consumer on a partition.
    // fn get_consumer_offset(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     consumer: PollingConsumer,
    // ) -> impl Future<Output = Result<Option<ConsumerOffsetInfo>, IggyError>> + Send;

    // /// Store/update the offset for a consumer on a partition.
    // fn store_consumer_offset(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     consumer: PollingConsumer,
    //     offset: u64,
    // ) -> impl Future<Output = Result<(), IggyError>> + Send;

    // /// Delete the stored offset for a consumer on a partition.
    // fn delete_consumer_offset(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     consumer: PollingConsumer,
    // ) -> impl Future<Output = Result<(), IggyError>> + Send;

    // /// Flush unsaved messages to disk.
    // fn flush_unsaved_buffer(
    //     &self,
    //     namespace: &IggyNamespace,
    //     local_idx: LocalIdx,
    //     fsync: bool,
    // ) -> impl Future<Output = Result<(), IggyError>> + Send;
}
