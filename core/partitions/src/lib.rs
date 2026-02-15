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
mod journal;
mod log;
mod types;

use consensus::Consensus;
pub use iggy_partition::IggyPartition;
pub use iggy_partitions::IggyPartitions;
pub use types::{
    AppendResult, PartitionOffsets, PartitionsConfig, PollingArgs, PollingConsumer,
    SendMessagesResult,
};

// TODO: Figure out how this can be somehow merged with `Metadata` trait, in a sense, where the `Metadata` trait would be gone
// and something more general purpose is put in the place.

/// Consensus lifecycle for partition operations (mirrors `Metadata<C>`).
///
/// Handles the VSR replication flow for partition writes:
/// - `on_request`: Primary receives a client write, projects to Prepare, pipelines it
/// - `on_replicate`: Replica receives Prepare, appends to journal, sends PrepareOk
/// - `on_ack`: Primary receives PrepareOk, checks quorum, commits
pub trait Partitions<C>
where
    C: Consensus,
{
    fn on_request(&self, message: C::RequestMessage) -> impl Future<Output = ()>;
    fn on_replicate(&self, message: C::ReplicateMessage) -> impl Future<Output = ()>;
    fn on_ack(&self, message: C::AckMessage) -> impl Future<Output = ()>;
}
