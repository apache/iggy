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

use crate::shards_table::{ShardsTable, calculate_shard_from_consensus_ns};
use crate::{IggyShard, Receiver, ShardFrame};
use crossfire::TrySendError;
use futures::FutureExt;
use iggy_binary_protocol::{
    ConsensusError, ConsensusHeader, GenericHeader, Message, MessageBag, PrepareHeader,
};
use iggy_common::sharding::IggyNamespace;
use journal::{Journal, JournalHandle};
use message_bus::{ConnectionInstaller, MessageBus};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;

/// Inter-shard dispatch logic.
///
/// All messages — whether destined for a local or remote shard — are routed
/// through the channel into the target shard's message pump.  This ensures
/// that every mutation on a shard is serialized through a single point (the
/// pump), preventing concurrent access from independent async tasks.
impl<B, MJ, S, M, T, R> IggyShard<B, MJ, S, M, T, R>
where
    B: MessageBus + ConnectionInstaller,
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
            MessageBag::Request(r) => {
                let h = *r.header();
                (h.operation, h.namespace, r.into_generic())
            }
            MessageBag::Prepare(p) => {
                let h = *p.header();
                (h.operation, h.namespace, p.into_generic())
            }
            MessageBag::PrepareOk(p) => {
                let h = *p.header();
                (h.operation, h.namespace, p.into_generic())
            }
            MessageBag::StartViewChange(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::DoViewChange(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::StartView(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::Commit(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
        };
        let raw_namespace = namespace;
        let partition_namespace = IggyNamespace::from_raw(raw_namespace);
        let target = if operation.is_metadata() {
            0
        } else if operation.is_partition() {
            self.shards_table
                .shard_for(partition_namespace)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        shard = self.id,
                        stream = partition_namespace.stream_id(),
                        topic = partition_namespace.topic_id(),
                        partition = partition_namespace.partition_id(),
                        "namespace not found in shards_table, falling back to shard 0"
                    );
                    0
                })
        } else {
            // View-change / Commit messages carry an opaque u64 consensus
            // namespace (not an `IggyNamespace`). Hash it with the same
            // function the partition-plane lookup table uses so the
            // shard owning the consensus group is deterministically the
            // same across every node. Single-shard deployments trivially
            // collapse to 0.
            #[allow(clippy::cast_lossless)]
            let shard_count = u32::try_from(self.senders.len()).unwrap_or(u32::MAX);
            calculate_shard_from_consensus_ns(raw_namespace, shard_count)
        };
        // `senders[target]` is a `crossfire::MTx`, which in compio is
        // running on an io_uring reactor: a blocking `send` on a full
        // inbox would park the reactor thread and stall every other
        // connection on the shard. Drop instead - consensus recovers via
        // WAL retransmit or a view change.
        match self.senders[target as usize].try_send(ShardFrame::fire_and_forget(generic)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch: shard inbox full, message dropped"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch: shard inbox closed, message dropped"
                );
            }
        }
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
            MessageBag::Request(r) => {
                let h = *r.header();
                (h.operation, h.namespace, r.into_generic())
            }
            MessageBag::Prepare(p) => {
                let h = *p.header();
                (h.operation, h.namespace, p.into_generic())
            }
            MessageBag::PrepareOk(p) => {
                let h = *p.header();
                (h.operation, h.namespace, p.into_generic())
            }
            MessageBag::StartViewChange(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::DoViewChange(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::StartView(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
            MessageBag::Commit(m) => {
                let h = *m.header();
                (h.operation(), h.namespace, m.into_generic())
            }
        };
        let raw_namespace = namespace;
        let partition_namespace = IggyNamespace::from_raw(raw_namespace);

        // Determine which shard should handle a message given its operation and
        // namespace.
        //
        // - Metadata operations always route to shard 0 (the control plane).
        // - Partition operations route to the shard that owns the namespace,
        //   looked up via the [`ShardsTable`].
        // - Consensus control-plane (`StartViewChange`, `DoViewChange`,
        //   `StartView`, `Commit`) carries a raw `u64` consensus namespace;
        //   hash-route it the same way partitions are looked up so every
        //   node agrees on the owning shard.
        let target = if operation.is_metadata() {
            0
        } else if operation.is_partition() {
            self.shards_table
                .shard_for(partition_namespace)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        shard = self.id,
                        stream = partition_namespace.stream_id(),
                        topic = partition_namespace.topic_id(),
                        partition = partition_namespace.partition_id(),
                        "namespace not found in shards_table, falling back to shard 0"
                    );
                    0
                })
        } else {
            #[allow(clippy::cast_lossless)]
            let shard_count = u32::try_from(self.senders.len()).unwrap_or(u32::MAX);
            calculate_shard_from_consensus_ns(raw_namespace, shard_count)
        };
        // Create a frame and send it to the target shard. Same non-
        // blocking `try_send` reason as `dispatch` above: blocking on a
        // full inbox would park the io_uring reactor.
        //
        // On `Full` / `Disconnected` we drop the frame and let the
        // caller's `rx` observe the disconnect (the `response_sender`
        // inside the frame is dropped together with the frame). Callers
        // see an early error rather than hanging.
        let (frame, rx) = ShardFrame::<R>::with_response(generic);
        match self.senders[target as usize].try_send(frame) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch_request: shard inbox full, message dropped"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch_request: shard inbox closed, message dropped"
                );
            }
        }
        Ok(rx)
    }

    /// Drain this shard's inbox and process each frame locally.
    #[allow(clippy::future_not_send)]
    pub async fn run_message_pump(&self, stop: Receiver<()>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        let mut loopback_buf = Vec::new();
        loop {
            futures::select! {
                _ = stop.recv().fuse() => break,
                frame = self.inbox.recv().fuse() => {
                    match frame {
                        Ok(frame) => {
                            self.process_frame(frame).await;
                            self.process_loopback(&mut loopback_buf).await;
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // Drain remaining frames so in-flight requests get a response.
        while let Ok(frame) = self.inbox.try_recv() {
            self.process_frame(frame).await;
            self.process_loopback(&mut loopback_buf).await;
        }
    }

    #[allow(clippy::future_not_send)]
    async fn process_frame(&self, frame: ShardFrame<R>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        match frame.payload {
            crate::ShardFramePayload::Consensus(message) => {
                self.on_message(message).await;
            }
            crate::ShardFramePayload::ReplicaConnectionSetup { fd, replica_id } => {
                tracing::info!(
                    shard = self.id,
                    replica_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated replica fd"
                );
                self.bus
                    .install_replica_fd(fd, replica_id, self.on_replica_message.clone());
            }
            crate::ShardFramePayload::ClientConnectionSetup { fd, client_id } => {
                tracing::info!(
                    shard = self.id,
                    client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated client fd"
                );
                self.bus
                    .install_client_fd(fd, client_id, self.on_client_request.clone());
            }
            crate::ShardFramePayload::ReplicaMappingUpdate {
                replica_id,
                owning_shard,
            } => {
                self.bus.set_shard_mapping(replica_id, owning_shard);
            }
            crate::ShardFramePayload::ReplicaMappingClear { replica_id } => {
                self.bus.remove_shard_mapping(replica_id);
            }
            crate::ShardFramePayload::ForwardReplicaSend { replica_id, msg } => {
                // Fast path on the owning shard: re-enter `send_to_replica`
                // which finds the local `BusSender` in the replicas registry.
                if let Err(e) = self.bus.send_to_replica(replica_id, msg).await {
                    tracing::debug!(
                        shard = self.id,
                        replica_id,
                        error = ?e,
                        "forward-replica-send delivery failed"
                    );
                }
            }
            crate::ShardFramePayload::ForwardClientSend { client_id, msg } => {
                if let Err(e) = self.bus.send_to_client(client_id, msg).await {
                    tracing::debug!(
                        shard = self.id,
                        client_id,
                        error = ?e,
                        "forward-client-send delivery failed"
                    );
                }
            }
            crate::ShardFramePayload::ConnectionLost { replica_id } => {
                // Shard 0 is the sole responder; the next reconnect sweep
                // will re-dial the peer. Non-zero shards should not see this
                // variant.
                if self.id == 0 {
                    tracing::warn!(replica_id, "shard 0 observed replica connection loss");
                    // If a coordinator is attached, let it broadcast the
                    // clear AND drop its tracked mapping so the periodic
                    // refresh task stops re-broadcasting a dead replica's
                    // mapping. Tests that bypass the coordinator fall back
                    // to the direct broadcast.
                    if let Some(coord) = &self.coordinator {
                        coord.forget_mapping(replica_id);
                        coord.broadcast_mapping_clear(replica_id);
                    } else {
                        crate::broadcast_mapping_clear(&self.senders, replica_id);
                    }
                } else {
                    tracing::warn!(
                        shard = self.id,
                        replica_id,
                        "non-zero shard received ConnectionLost; dropping"
                    );
                }
            }
        }
        // TODO: once on_message returns an R (e.g. ShardResponse), send it
        // back via frame.response_sender.  For now the sender is dropped and
        // the caller's receiver will observe a disconnect.
        drop(frame.response_sender);
    }
}
