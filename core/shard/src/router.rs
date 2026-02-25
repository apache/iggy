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

use crate::IggyShard;
use iggy_common::header::{
    ConsensusHeader, GenericHeader, Operation, PrepareHeader, PrepareOkHeader, RequestHeader,
};
use iggy_common::message::{Message, MessageBag};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::stm::StateMachine;

/// Routes incoming network messages to the appropriate [`IggyShard`].
///
/// Current routing strategy (single-shard):
///   - All messages are dispatched to shard 0.
///
/// Future routing strategy (multi-shard):
///   - Metadata operations -> the designated metadata shard.
///   - Partition operations -> the shard owning the namespace extracted from the
///     message header.
pub struct ShardRouter<B, J, S, M>
where
    B: MessageBus,
{
    shards: Vec<IggyShard<B, J, S, M>>,
}

impl<B, J, S, M> ShardRouter<B, J, S, M>
where
    B: MessageBus,
{
    pub fn new(shards: Vec<IggyShard<B, J, S, M>>) -> Self {
        Self { shards }
    }

    pub fn shards(&self) -> &[IggyShard<B, J, S, M>] {
        &self.shards
    }

    pub fn shards_mut(&mut self) -> &mut [IggyShard<B, J, S, M>] {
        &mut self.shards
    }

    /// Dispatch a raw network message: determine the target shard, then
    /// forward to its consensus planes.
    pub async fn dispatch(&self, message: Message<GenericHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<Input = Message<PrepareHeader>>,
    {
        match MessageBag::from(message) {
            MessageBag::Request(request) => self.on_request(request).await,
            MessageBag::Prepare(prepare) => self.on_replicate(prepare).await,
            MessageBag::PrepareOk(prepare_ok) => self.on_ack(prepare_ok).await,
        }
    }

    pub async fn on_request(&self, request: Message<RequestHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<Input = Message<PrepareHeader>>,
    {
        let header = request.header();
        let shard = self.resolve(header.operation(), header.namespace);
        shard.on_request(request).await;
    }

    pub async fn on_replicate(&self, prepare: Message<PrepareHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<Input = Message<PrepareHeader>>,
    {
        let header = prepare.header();
        let shard = self.resolve(header.operation(), header.namespace);
        shard.on_replicate(prepare).await;
    }

    pub async fn on_ack(&self, prepare_ok: Message<PrepareOkHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<Input = Message<PrepareHeader>>,
    {
        let header = prepare_ok.header();
        let shard = self.resolve(header.operation(), header.namespace);
        shard.on_ack(prepare_ok).await;
    }

    /// Resolve which shard should handle a message given its operation and namespace.
    ///
    /// Assumes at least one shard exists. Panics otherwise.
    fn resolve(&self, _operation: Operation, _namespace: u64) -> &IggyShard<B, J, S, M> {
        // TODO: Multi-shard routing.
        //
        // Metadata operations (CreateStream, CreateUser, …) should be routed to
        // the designated metadata shard.
        //
        // Partition operations (SendMessages, StoreConsumerOffset, DeleteSegments)
        // should use the namespace (packed stream_id + topic_id + partition_id) to
        // determine which shard owns that partition.
        //
        // For now, everything routes to shard 0.
        &self.shards[0]
    }
}
