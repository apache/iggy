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

mod router;
pub mod shards_table;

use consensus::{MuxPlane, NamespacedPipeline, PartitionsHandle, Plane, VsrConsensus};
use iggy_binary_protocol::{
    GenericHeader, Message, MessageBag, PrepareHeader, PrepareOkHeader, RequestHeader,
};
use iggy_common::sharding::IggyNamespace;
use iggy_common::variadic;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::IggyMetadata;
use metadata::stm::StateMachine;
use partitions::IggyPartitions;
use shards_table::ShardsTable;

pub type ShardPlane<B, J, S, M> = MuxPlane<
    variadic!(
        IggyMetadata<VsrConsensus<B>, J, S, M>,
        IggyPartitions<VsrConsensus<B, NamespacedPipeline>>
    ),
>;

/// Bounded mpsc channel sender (blocking send).
pub type Sender<T> = crossfire::MTx<crossfire::mpsc::Array<T>>;

/// Bounded mpsc channel receiver (async recv).
pub type Receiver<T> = crossfire::AsyncRx<crossfire::mpsc::Array<T>>;

/// Create a bounded mpsc channel with a blocking sender and async receiver.
#[must_use]
pub fn channel<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    crossfire::mpsc::bounded_blocking_async(capacity)
}

/// Envelope for inter-shard channel messages.
///
/// Wraps a consensus [`Message`] together with an optional one-shot response
/// channel.  Fire-and-forget dispatches leave `response_sender` as `None`;
/// request-response dispatches provide a sender that the message pump will
/// notify once the message has been processed.
///
/// The response type `R` is generic so that higher layers (e.g. HTTP handlers)
/// can carry a response enum while the consensus layer can default to `()`.
pub struct ShardFrame<R: Send + 'static = ()> {
    pub message: Message<GenericHeader>,
    pub response_sender: Option<Sender<R>>,
}

impl<R: Send + 'static> ShardFrame<R> {
    /// Create a fire-and-forget frame (no caller waiting for completion).
    #[must_use]
    pub const fn fire_and_forget(message: Message<GenericHeader>) -> Self {
        Self {
            message,
            response_sender: None,
        }
    }

    /// Create a request-response frame.  Returns the frame and a receiver
    /// that the caller can await for completion notification.
    pub fn with_response(message: Message<GenericHeader>) -> (Self, Receiver<R>) {
        let (tx, rx) = channel(1);
        (
            Self {
                message,
                response_sender: Some(tx),
            },
            rx,
        )
    }
}

pub struct IggyShard<B, J, S, M, T = (), R: Send + 'static = ()>
where
    B: MessageBus,
{
    pub id: u16,
    pub name: String,
    pub plane: ShardPlane<B, J, S, M>,

    /// Channel senders to every shard, indexed by shard id.
    /// Includes a sender to self so that local routing goes through the
    /// same channel path as remote routing.
    senders: Vec<Sender<ShardFrame<R>>>,

    /// Receiver end of this shard's inbox.  Peer shards (and self) send
    /// messages here via the corresponding sender.
    inbox: Receiver<ShardFrame<R>>,

    /// Partition namespace -> owning shard lookup.
    shards_table: T,
}

impl<B, J, S, M, T, R: Send + 'static> IggyShard<B, J, S, M, T, R>
where
    B: MessageBus,
    T: ShardsTable,
{
    /// Create a new shard with channel links and a shards table.
    ///
    /// * `senders` - one sender per shard in the cluster (indexed by shard id).
    /// * `inbox` - the receiver that this shard drains in its message pump.
    /// * `shards_table` - namespace -> shard routing table.
    #[must_use]
    pub const fn new(
        id: u16,
        name: String,
        metadata: IggyMetadata<VsrConsensus<B>, J, S, M>,
        partitions: IggyPartitions<VsrConsensus<B, NamespacedPipeline>>,
        senders: Vec<Sender<ShardFrame<R>>>,
        inbox: Receiver<ShardFrame<R>>,
        shards_table: T,
    ) -> Self {
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        Self {
            id,
            name,
            plane,
            senders,
            inbox,
            shards_table,
        }
    }

    #[must_use]
    pub const fn shards_table(&self) -> &T {
        &self.shards_table
    }
}

/// Local message processing — these methods handle messages that have been
/// routed to this shard via the message pump.
impl<B, J, S, M, T, R: Send + 'static> IggyShard<B, J, S, M, T, R>
where
    B: MessageBus,
{
    /// Dispatch an incoming network message to the appropriate consensus plane.
    ///
    /// Routes requests, replication messages, and acks to either the metadata
    /// plane or the partitions plane based on `PlaneIdentity::is_applicable`.
    #[allow(clippy::future_not_send)]
    pub async fn on_message(&self, message: Message<GenericHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        match MessageBag::try_from(message) {
            Ok(MessageBag::Request(request)) => self.on_request(request).await,
            Ok(MessageBag::Prepare(prepare)) => self.on_replicate(prepare).await,
            Ok(MessageBag::PrepareOk(prepare_ok)) => self.on_ack(prepare_ok).await,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
            }
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_request(&self, request: Message<RequestHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        self.plane.on_request(request).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_replicate(&self, prepare: Message<PrepareHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        self.plane.on_replicate(prepare).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&self, prepare_ok: Message<PrepareOkHeader>)
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        self.plane.on_ack(prepare_ok).await;
    }

    /// Drain and dispatch loopback messages for each consensus plane.
    ///
    /// Each plane's loopback is dispatched directly to that plane's `on_ack`,
    /// avoiding a flat merge that would require re-routing through `on_message`.
    ///
    /// Invariant: planes do not produce loopback messages for each other.
    /// `on_ack` commits and applies but never calls `push_loopback`, so
    /// draining metadata before partitions is order-independent.
    ///
    /// # Panics
    /// Panics if a loopback message is not a valid `PrepareOk` message.
    #[allow(clippy::future_not_send)]
    pub async fn process_loopback(&self, buf: &mut Vec<Message<GenericHeader>>) -> usize
    where
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
        J: JournalHandle,
        <J as JournalHandle>::Target: Journal<
                <J as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
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
                Data = iggy_binary_protocol::Message<iggy_binary_protocol::GenericHeader>,
                Client = u128,
            >,
    {
        let partitions = self.plane.partitions_mut();
        partitions.init_partition_in_memory(namespace);
        partitions.register_namespace_in_pipeline(namespace.inner());
    }
}
