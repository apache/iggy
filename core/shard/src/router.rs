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

use crate::shards_table::ShardsTable;
use crate::{IggyShard, Receiver, ShardFrame};
use futures::FutureExt;
use iggy_binary_protocol::{ConsensusError, GenericHeader, Message, MessageBag, PrepareHeader};
use iggy_common::sharding::IggyNamespace;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::stm::StateMachine;

/// Inter-shard dispatch logic.
///
/// All messages — whether destined for a local or remote shard — are routed
/// through the channel into the target shard's message pump.  This ensures
/// that every mutation on a shard is serialized through a single point (the
/// pump), preventing concurrent access from independent async tasks.
impl<B, J, S, M, T, R> IggyShard<B, J, S, M, T, R>
where
    B: MessageBus,
    T: ShardsTable,
    R: Send + 'static,
{
    /// Classify a raw network message and route it to
    /// the correct shard's message pump.
    ///
    /// Decomposes the generic message into its typed form (Request, Prepare,
    /// or `PrepareOk`) to access the operation and namespace, then resolves
    /// the target shard and enqueues the message via its channel sender.
    pub fn dispatch(&self, message: Message<GenericHeader>) {
        let bag = match MessageBag::try_from(message) {
            Ok(bag) => bag,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
                return;
            }
        };
        let (operation, namespace, generic) = match bag {
            MessageBag::Request(ref r) => {
                let h = r.header();
                (h.operation, h.namespace, r.as_generic().clone())
            }
            MessageBag::Prepare(ref p) => {
                let h = p.header();
                (h.operation, h.namespace, p.as_generic().clone())
            }
            MessageBag::PrepareOk(ref p) => {
                let h = p.header();
                (h.operation, h.namespace, p.as_generic().clone())
            }
        };
        let namespace = IggyNamespace::from_raw(namespace);
        let target = if operation.is_metadata() {
            0
        } else if operation.is_partition() {
            self.shards_table.shard_for(namespace).unwrap_or_else(|| {
                tracing::warn!(
                    shard = self.id,
                    stream = namespace.stream_id(),
                    topic = namespace.topic_id(),
                    partition = namespace.partition_id(),
                    "namespace not found in shards_table, falling back to shard 0"
                );
                0
            })
        } else {
            0
        };
        let _ = self.senders[target as usize].send(ShardFrame::fire_and_forget(generic));
    }

    /// Dispatch a message and return a receiver that resolves when the target
    /// shard has finished processing it.
    ///
    /// # Errors
    /// Returns `ConsensusError` if the message cannot be routed.
    pub fn dispatch_request(
        &self,
        message: Message<GenericHeader>,
    ) -> Result<Receiver<R>, ConsensusError> {
        let bag = MessageBag::try_from(message)?;
        let (operation, namespace, generic) = match bag {
            MessageBag::Request(ref r) => {
                let h = r.header();
                (h.operation, h.namespace, r.as_generic().clone())
            }
            MessageBag::Prepare(ref p) => {
                let h = p.header();
                (h.operation, h.namespace, p.as_generic().clone())
            }
            MessageBag::PrepareOk(ref p) => {
                let h = p.header();
                (h.operation, h.namespace, p.as_generic().clone())
            }
        };
        let namespace = IggyNamespace::from_raw(namespace);

        // Determine which shard should handle a message given its operation and
        // namespace.
        //
        // - Metadata operations always route to shard 0 (the control plane).
        // - Partition operations route to the shard that owns the namespace,
        //   looked up via the [`ShardsTable`].
        // - Unknown operations fall back to shard 0.
        let target = if operation.is_metadata() {
            0
        } else if operation.is_partition() {
            self.shards_table.shard_for(namespace).unwrap_or_else(|| {
                tracing::warn!(
                    shard = self.id,
                    stream = namespace.stream_id(),
                    topic = namespace.topic_id(),
                    partition = namespace.partition_id(),
                    "namespace not found in shards_table, falling back to shard 0"
                );
                0
            })
        } else {
            0
        };
        // Create a frame and send it to the target shard.
        let (frame, rx) = ShardFrame::<R>::with_response(generic);
        let _ = self.senders[target as usize].send(frame);
        Ok(rx)
    }

    /// Drain this shard's inbox and process each frame locally.
    #[allow(clippy::future_not_send)]
    pub async fn run_message_pump(&self, stop: Receiver<()>)
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
        loop {
            futures::select! {
                _ = stop.recv().fuse() => break,
                frame = self.inbox.recv().fuse() => {
                    match frame {
                        Ok(frame) => self.process_frame(frame).await,
                        Err(_) => break,
                    }
                }
            }
        }

        // Drain remaining frames so in-flight requests get a response.
        while let Ok(frame) = self.inbox.try_recv() {
            self.process_frame(frame).await;
        }
    }

    #[allow(clippy::future_not_send)]
    async fn process_frame(&self, frame: ShardFrame<R>)
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
        self.on_message(frame.message).await;
        // TODO: once on_message returns an R (e.g. ShardResponse), send it
        // back via frame.response_sender.  For now the sender is dropped and
        // the caller's receiver will observe a disconnect.
        drop(frame.response_sender);
    }
}
