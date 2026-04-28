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
//! associated [`ShardZeroCoordinator`] and periodic mapping-refresh task.
//!
//! The coordinator owns the authoritative `replica_id -> owning_shard`
//! snapshot used by the router's `ConnectionLost` fast path and by the
//! refresh task that re-broadcasts the snapshot to recover shards whose
//! inbox was full when an earlier `ReplicaMappingUpdate` was sent.
//! Both the `set_coordinator` call and the refresh-task spawn must happen
//! together: wiring one without the other breaks the drop-recovery story
//! the coordinator's rustdoc promises.
//!
//! Bootstrap code constructs an [`IggyShardBuilder`] per shard and calls
//! [`IggyShardBuilder::build`]. On shard 0 the returned [`BuiltShard`]
//! carries the refresh task's [`JoinHandle`]; bootstrap is responsible for
//! tracking it on the bus' background-task set so graceful shutdown awaits
//! it. On non-zero shards the handle is `None` and the coordinator field
//! on the shard stays unset.

use crate::coordinator::ShardZeroCoordinator;
use crate::{
    CoordinatorConfig, IggyShard, PartitionConsensusConfig, Receiver, ShardFrame,
    ShardFramePayload, ShardIdentity, TaggedSender,
};
use compio::runtime::JoinHandle;
use consensus::VsrConsensus;
use journal::JournalHandle;
use message_bus::MessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::lifecycle::ShutdownToken;
use message_bus::replica_listener::MessageHandler;
use metadata::IggyMetadata;
use metadata::stm::StateMachine;
use partitions::IggyPartitions;
use std::rc::Rc;
use tracing::warn;

use crate::shards_table::ShardsTable;

/// A freshly constructed [`IggyShard`], paired with the refresh-task
/// [`JoinHandle`] when this is shard 0. Bootstrap tracks the handle on
/// the bus' background tasks so graceful shutdown awaits it.
pub struct BuiltShard<B, MJ, S, M, T, R = ()>
where
    B: MessageBus,
    R: Send + 'static,
{
    pub shard: IggyShard<B, MJ, S, M, T, R>,
    /// `Some` on shard 0, `None` otherwise. Pass to
    /// `IggyMessageBus::track_background` (or an equivalent tracker) so
    /// the task is awaited on shutdown.
    pub refresh_task: Option<JoinHandle<()>>,
}

/// Builder that pairs [`IggyShard`] construction with coordinator wiring
/// on shard 0. Non-zero shards skip the coordinator entirely; the
/// `coord_config` and `shutdown_token` fields are then ignored.
pub struct IggyShardBuilder<B, MJ, S, M, T, R = ()>
where
    B: MessageBus,
    R: Send + 'static,
{
    identity: ShardIdentity,
    bus: B,
    on_replica_message: MessageHandler,
    on_client_request: RequestHandler,
    metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
    partitions: IggyPartitions<B>,
    senders: Vec<TaggedSender<R>>,
    inbox: Receiver<ShardFrame<R>>,
    shards_table: T,
    partition_consensus: PartitionConsensusConfig<B>,
    coord_config: CoordinatorConfig,
    shutdown_token: ShutdownToken,
}

impl<B, MJ, S, M, T, R> IggyShardBuilder<B, MJ, S, M, T, R>
where
    B: MessageBus,
    R: Send + 'static,
    T: ShardsTable,
    MJ: JournalHandle,
    S: Send + 'static,
    M: StateMachine,
{
    /// Create a builder carrying every input needed by both
    /// [`IggyShard::new`] and (for shard 0) `ShardZeroCoordinator::new`
    /// plus `spawn_refresh_task`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ShardIdentity,
        bus: B,
        on_replica_message: MessageHandler,
        on_client_request: RequestHandler,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        senders: Vec<TaggedSender<R>>,
        inbox: Receiver<ShardFrame<R>>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
        coord_config: CoordinatorConfig,
        shutdown_token: ShutdownToken,
    ) -> Self {
        Self {
            identity,
            bus,
            on_replica_message,
            on_client_request,
            metadata,
            partitions,
            senders,
            inbox,
            shards_table,
            partition_consensus,
            coord_config,
            shutdown_token,
        }
    }

    /// Consume the builder and produce a fully wired [`BuiltShard`]. On
    /// shard 0 this constructs the coordinator, attaches it to the shard,
    /// and spawns the periodic refresh task. On any other shard the
    /// coordinator-specific fields are dropped and `refresh_task` is
    /// `None`.
    ///
    /// # Panics
    ///
    /// Panics if `senders` is not in canonical order
    /// (`senders[i].shard_id() != i`) or, on shard 0, if `senders.len()`
    /// does not fit in `u16`. Both are bootstrap programming errors.
    pub fn build(self) -> BuiltShard<B, MJ, S, M, T, R> {
        let is_shard_zero = self.identity.id == 0;

        let total_shards = u16::try_from(self.senders.len())
            .expect("cluster shard count must fit in u16 (bootstrap invariant)");

        // Wire the bus' connection-lost notifier to push a ConnectionLost
        // frame into shard 0's inbox. Every shard's bus needs this so that
        // a dead replica connection anywhere surfaces at the coordinator.
        // Shard 0's sender is cloned once here; the closure captures that
        // clone and runs on the owning shard's reader/writer tasks.
        let shard_zero_sender = self.senders[0].sender().clone();
        let connection_lost_fn: message_bus::ConnectionLostFn = Rc::new(move |replica_id: u8| {
            let frame = ShardFrame::<R> {
                payload: ShardFramePayload::ConnectionLost { replica_id },
                response_sender: None,
            };
            if let Err(e) = shard_zero_sender.try_send(frame) {
                // Inbox full: the coordinator's periodic refresh task
                // re-broadcasts the authoritative snapshot and the bus
                // retries on next dial, so a single drop is recoverable.
                // Warn so operators see repeated drops if they occur.
                warn!(
                    replica_id,
                    "shard-0 inbox rejected ConnectionLost frame: {e}"
                );
            }
        });
        self.bus.set_connection_lost_fn(connection_lost_fn);

        let coord_inputs = if is_shard_zero {
            let coord_senders: Vec<TaggedSender<R>> = self.senders.clone();
            Some((
                Rc::new(coord_senders),
                total_shards,
                self.coord_config,
                self.shutdown_token,
            ))
        } else {
            None
        };

        let mut shard = IggyShard::new(
            self.identity,
            self.bus,
            self.on_replica_message,
            self.on_client_request,
            self.metadata,
            self.partitions,
            self.senders,
            self.inbox,
            self.shards_table,
            self.partition_consensus,
        );

        let refresh_task = if let Some((senders_rc, total_shards, cfg, token)) = coord_inputs {
            let refresh_period = cfg.refresh_period;
            let coord = Rc::new(ShardZeroCoordinator::new(senders_rc, total_shards, cfg));
            shard.set_coordinator(Rc::clone(&coord));
            Some(coord.spawn_refresh_task(token, refresh_period))
        } else {
            None
        };

        BuiltShard {
            shard,
            refresh_task,
        }
    }
}
