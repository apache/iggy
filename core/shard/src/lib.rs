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

use consensus::{MuxPlane, NamespacedPipeline, Plane, PlaneIdentity, VsrConsensus};
use iggy_common::header::{GenericHeader, PrepareHeader, PrepareOkHeader, RequestHeader};
use iggy_common::message::{Message, MessageBag};
use iggy_common::sharding::IggyNamespace;
use iggy_common::variadic;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::IggyMetadata;
use metadata::stm::StateMachine;
use partitions::IggyPartitions;

pub type ShardPlane<B, J, S, M> = MuxPlane<
    variadic!(
        IggyMetadata<VsrConsensus<B>, J, S, M>,
        IggyPartitions<VsrConsensus<B, NamespacedPipeline>>
    ),
>;

pub struct IggyShard<B, J, S, M>
where
    B: MessageBus,
{
    pub id: u8,
    pub name: String,
    pub plane: ShardPlane<B, J, S, M>,
}

impl<B, J, S, M> IggyShard<B, J, S, M>
where
    B: MessageBus,
{
    /// Create a new shard from pre-built metadata and partition planes.
    pub fn new(
        id: u8,
        name: String,
        metadata: IggyMetadata<VsrConsensus<B>, J, S, M>,
        partitions: IggyPartitions<VsrConsensus<B, NamespacedPipeline>>,
    ) -> Self {
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        Self { id, name, plane }
    }

    /// Dispatch an incoming network message to the appropriate consensus plane.
    ///
    /// Routes requests, replication messages, and acks to either the metadata
    /// plane or the partitions plane based on `PlaneIdentity::is_applicable`.
    pub async fn on_message(&self, message: Message<GenericHeader>)
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
        let planes = self.plane.inner();
        if planes.0.is_applicable(&request) {
            planes.0.on_request(request).await;
        } else {
            planes.1.0.on_request(request).await;
        }
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
        let planes = self.plane.inner();
        if planes.0.is_applicable(&prepare) {
            planes.0.on_replicate(prepare).await;
        } else {
            planes.1.0.on_replicate(prepare).await;
        }
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
        let planes = self.plane.inner();
        if planes.0.is_applicable(&prepare_ok) {
            planes.0.on_ack(prepare_ok).await;
        } else {
            planes.1.0.on_ack(prepare_ok).await;
        }
    }

    /// Drain and dispatch loopback messages for each consensus plane.
    ///
    /// Each plane's loopback is dispatched directly to that plane's `on_ack`,
    /// avoiding a flat merge that would require re-routing through `on_message`.
    ///
    /// Invariant: planes do not produce loopback messages for each other.
    /// `on_ack` commits and applies but never calls `push_loopback`, so
    /// draining metadata before partitions is order-independent.
    pub async fn process_loopback(&self, buf: &mut Vec<Message<GenericHeader>>) -> usize
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
        debug_assert!(buf.is_empty(), "buf must be empty on entry");

        let mut total = 0;
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus {
            consensus.drain_loopback_into(buf);
            let count = buf.len();
            total += count;
            for msg in buf.drain(..) {
                let typed: Message<PrepareOkHeader> = msg
                    .try_into_typed()
                    .expect("loopback queue must only contain PrepareOk messages");
                planes.0.on_ack(typed).await;
            }
        }

        if let Some(consensus) = planes.1.0.consensus() {
            consensus.drain_loopback_into(buf);
            let count = buf.len();
            total += count;
            for msg in buf.drain(..) {
                let typed: Message<PrepareOkHeader> = msg
                    .try_into_typed()
                    .expect("loopback queue must only contain PrepareOk messages");
                planes.1.0.on_ack(typed).await;
            }
        }

        total
    }

    pub fn init_partition(&mut self, namespace: IggyNamespace)
    where
        B: MessageBus<
                Replica = u8,
                Data = iggy_common::message::Message<iggy_common::header::GenericHeader>,
                Client = u128,
            >,
    {
        let partitions = &mut self.plane.inner_mut().1.0;
        partitions.init_partition_in_memory(namespace);
        partitions.register_namespace_in_pipeline(namespace.inner());
    }
}
