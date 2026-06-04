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

//! One-stop construction for an [`IggyShard`] plus, on shard 0 only, its
//! associated [`ShardZeroCoordinator`].
//!
//! The coordinator owns shard-0's round-robin state for replica and
//! client placement. Cluster-wide `replica_id -> owning_shard` routing
//! flows through the cluster-shared [`message_bus::ReplicaOwnerTable`],
//! stamped by the owning shard's installer and CAS-cleared on disconnect.
//!
//! The coordinator is constructed before the shard and passed to
//! [`IggyShard::new`] as a ctor argument, so an `IggyShard` is fully
//! wired the instant it becomes observable to any reader.

use crate::coordinator::{ShardZeroCoordinator, classify_try_send_err};
use crate::metrics::{ShardMetrics, frame_drop_variant};
use crate::{
    CoordinatorConfig, IggyShard, LifecycleFrame, ListClientsHandler, MetadataSubmitHandler,
    PartitionConsensusConfig, Receiver, ShardCtorError, ShardFrame, ShardIdentity, TaggedSender,
};
use consensus::VsrConsensus;
use journal::JournalHandle;
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use message_bus::{MessageBus, SendError};
use metadata::IggyMetadata;
use metadata::stm::StateMachine;
use partitions::IggyPartitions;
use std::rc::Rc;

use crate::shards_table::ShardsTable;

/// A freshly constructed [`IggyShard`].
pub struct BuiltShard<B, MJ, S, M, T>
where
    B: MessageBus,
{
    pub shard: IggyShard<B, MJ, S, M, T>,
}

/// Builder that pairs [`IggyShard`] construction with coordinator wiring
/// on shard 0. Non-zero shards skip the coordinator entirely; the
/// `coord_config` field is then ignored.
pub struct IggyShardBuilder<B, MJ, S, M, T>
where
    B: MessageBus,
{
    identity: ShardIdentity,
    bus: B,
    on_replica_message: MessageHandler,
    on_client_request: RequestHandler,
    on_metadata_submit: MetadataSubmitHandler,
    on_list_clients: ListClientsHandler,
    metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
    partitions: IggyPartitions<B>,
    senders: Vec<TaggedSender>,
    inbox: Receiver<ShardFrame>,
    shards_table: T,
    partition_consensus: PartitionConsensusConfig<B>,
    coord_config: CoordinatorConfig,
    metrics: ShardMetrics,
}

impl<B, MJ, S, M, T> IggyShardBuilder<B, MJ, S, M, T>
where
    B: MessageBus + Clone + 'static,
    T: ShardsTable,
    MJ: JournalHandle,
    S: Send + 'static,
    M: StateMachine,
{
    /// Create a builder carrying every input needed by both
    /// [`IggyShard::new`] and (for shard 0) `ShardZeroCoordinator::new`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ShardIdentity,
        bus: B,
        on_replica_message: MessageHandler,
        on_client_request: RequestHandler,
        on_metadata_submit: MetadataSubmitHandler,
        on_list_clients: ListClientsHandler,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        senders: Vec<TaggedSender>,
        inbox: Receiver<ShardFrame>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
        coord_config: CoordinatorConfig,
        metrics: ShardMetrics,
    ) -> Self {
        Self {
            identity,
            bus,
            on_replica_message,
            on_client_request,
            on_metadata_submit,
            on_list_clients,
            metadata,
            partitions,
            senders,
            inbox,
            shards_table,
            partition_consensus,
            coord_config,
            metrics,
        }
    }

    /// Consume the builder and produce a fully wired [`BuiltShard`]. On
    /// shard 0 this constructs the coordinator and passes it to the shard
    /// ctor. On any other shard the coordinator-specific fields are
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns [`ShardCtorError::SenderOrderingInvalid`] if `senders` is
    /// not in canonical order (`senders[i].shard_id() != i`) and
    /// [`ShardCtorError::ShardCountOverflow`] if `senders.len()` does not
    /// fit in `u16`. Both are bootstrap programming errors and the
    /// `u16` overflow check fires on every shard, not only shard 0.
    pub fn build(self) -> Result<BuiltShard<B, MJ, S, M, T>, ShardCtorError> {
        let is_shard_zero = self.identity.id == 0;

        // Fail fast before installing forward closures: a misordered
        // senders vec would silently misroute every cross-shard frame.
        crate::validate_sender_ordering(&self.senders)?;
        let total_shards =
            u16::try_from(self.senders.len()).map_err(|_| ShardCtorError::ShardCountOverflow {
                count: self.senders.len(),
            })?;

        // Install the cross-shard forward closures. The bus' slow path
        // (`send_to_replica` / `send_to_client`) calls these when the
        // destination connection lives on a different shard; the closure
        // wraps the message into a `LifecycleFrame::Forward{Replica,
        // Client}Send` and `try_send`s it to the owning shard's inbox.
        // The receiving shard's router unwraps the frame and re-enters
        // the bus' fast path locally. The `senders` Vec is canonically
        // ordered (`senders[i].shard_id() == i`).
        let senders_for_replica: Rc<Vec<TaggedSender>> = Rc::new(self.senders.clone());
        let senders_for_client = Rc::clone(&senders_for_replica);
        let metrics_for_replica = self.metrics.clone();
        let metrics_for_client = self.metrics.clone();
        self.bus
            .set_replica_forward_fn(Box::new(move |replica_id, owning_shard, msg| {
                senders_for_replica.get(usize::from(owning_shard)).map_or(
                    Err(SendError::ReplicaForwardFailed(replica_id)),
                    |sender| {
                        let frame = ShardFrame::lifecycle(LifecycleFrame::ForwardReplicaSend {
                            replica_id,
                            msg,
                        });
                        sender.try_send(frame).map_err(|err| {
                            metrics_for_replica.record_frame_drop(
                                frame_drop_variant::FORWARD_REPLICA_SEND,
                                classify_try_send_err(&err),
                            );
                            SendError::ReplicaForwardFailed(replica_id)
                        })
                    },
                )
            }));
        self.bus
            .set_client_forward_fn(Box::new(move |client_id, owning_shard, msg| {
                senders_for_client.get(usize::from(owning_shard)).map_or(
                    Err(SendError::ClientForwardFailed(client_id)),
                    |sender| {
                        let frame = ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
                            client_id,
                            msg,
                        });
                        sender.try_send(frame).map_err(|err| {
                            metrics_for_client.record_frame_drop(
                                frame_drop_variant::FORWARD_CLIENT_SEND,
                                classify_try_send_err(&err),
                            );
                            SendError::ClientForwardFailed(client_id)
                        })
                    },
                )
            }));

        // On shard 0: build the coordinator BEFORE the shard, so the
        // coordinator can be passed to `IggyShard::new` as a ctor arg.
        let coordinator = if is_shard_zero {
            let coord_senders: Vec<TaggedSender> = self.senders.clone();
            Some(Rc::new(ShardZeroCoordinator::new(
                Rc::new(coord_senders),
                total_shards,
                self.coord_config.clone(),
                self.metrics.clone(),
            )?))
        } else {
            None
        };

        let shard = IggyShard::new(
            self.identity,
            self.bus,
            self.on_replica_message,
            self.on_client_request,
            self.on_metadata_submit,
            self.on_list_clients,
            self.metadata,
            self.partitions,
            self.senders,
            self.inbox,
            self.shards_table,
            self.partition_consensus,
            coordinator,
            self.metrics,
        )?;

        Ok(BuiltShard { shard })
    }
}
