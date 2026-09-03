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
mod iggy_index_reader;
mod iggy_index_writer;
mod iggy_partition;
mod iggy_partitions;
mod journal;
mod log;
mod messages_writer;
pub mod offset_storage;
mod poll_plan;
mod segment;
pub mod state_transfer;
mod types;

use iggy_binary_protocol::PrepareHeader;
use iggy_common::IggyError;
pub use iggy_index::IggyIndex;
pub use iggy_index_reader::IggyIndexReader;
pub use iggy_index_writer::IggyIndexWriter;
pub use iggy_partition::{IggyPartition, PurgeError, SegmentRemoval};
pub use iggy_partitions::IggyPartitions;
pub use journal::{EVICTED_RING_BYTES_MAX, EVICTED_RING_CAPACITY};
pub use messages_writer::MessagesWriter;
pub use offset_storage::delete_persisted_offset;
pub use poll_plan::{AutoCommitApplied, PollPlan};
pub use segment::Segment;
use server_common::Message;
pub use server_common::send_messages::{IggyMessage, IggyMessageHeader, IggyMessages};
pub use types::{
    AppendResult, FatalCommit, Fragment, PartitionOffsets, PartitionPathLayout, PartitionsConfig,
    PollFragments, PollQueryResult, PollingArgs, PollingConsumer, REPAIR_RETRY_TICKS,
    RepairConclusion, RepairSession, SendMessagesResult,
};

/// A partition's message log, named so a caller can carry one across a rebuild.
///
/// Exists for the simulator, which has no segment files and so must hold the log
/// itself for a restarted replica to come back with its data (see
/// [`IggyPartition::adopt_retained_log`]). Names the only journal
/// `IggyPartition::log` is instantiated with rather than widening anything.
#[cfg(any(test, feature = "simulator"))]
pub type RetainedPartitionLog =
    log::SegmentedLog<journal::PartitionJournal<journal::PartitionJournalMemStorage>>;

/// Everything a partition hands its own next incarnation across a simulated
/// restart.
#[cfg(any(test, feature = "simulator"))]
pub struct RetainedPartitionState {
    pub log: RetainedPartitionLog,
    /// Offset counter the previous incarnation had proved durable.
    pub durable_offset: u64,
    /// Highest offset it had written, durable or not.
    pub write_offset: u64,
    /// Whether that incarnation ever stamped an offset, i.e. whether the two
    /// numbers above describe an offset space at all.
    pub offset_space_used: bool,
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
