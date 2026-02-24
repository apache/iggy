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

use consensus::{LocalPipeline, MuxPlane, NamespacedPipeline, Plane, PlaneIdentity, VsrConsensus};
use iggy_common::header::{GenericHeader, PrepareHeader};
use iggy_common::message::{Message, MessageBag};
use iggy_common::sharding::{IggyNamespace, ShardId};
use iggy_common::variadic;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::IggyMetadata;
use metadata::stm::StateMachine;
use partitions::{IggyPartitions, PartitionsConfig};

// variadic!(Metadata, Partitions) = (Metadata, (Partitions, ()))
type PlaneInner<B, J, S, M> = (
    IggyMetadata<VsrConsensus<B>, J, S, M>,
    (IggyPartitions<VsrConsensus<B, NamespacedPipeline>>, ()),
);

pub type ShardPlane<B, J, S, M> = MuxPlane<PlaneInner<B, J, S, M>>;

pub struct IggyShard<B, J, S, M>
where
    B: MessageBus,
{
    pub id: u8,
    pub name: String,
    pub replica_count: u8,
    pub plane: ShardPlane<B, J, S, M>,
}

impl<B, J, S, M> IggyShard<B, J, S, M>
where
    B: MessageBus + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: u8,
        name: String,
        cluster_id: u128,
        bus: B,
        replica_count: u8,
        journal: J,
        snapshot: S,
        mux_stm: M,
        partitions_config: PartitionsConfig,
    ) -> Self {
        // Metadata uses namespace=0 (not partition-scoped)
        let metadata_consensus = VsrConsensus::new(
            cluster_id,
            id,
            replica_count,
            0,
            bus.clone(),
            LocalPipeline::new(),
        );
        metadata_consensus.init();

        let metadata = IggyMetadata {
            consensus: Some(metadata_consensus),
            journal: Some(journal),
            snapshot: Some(snapshot),
            mux_stm,
        };

        let mut partitions = IggyPartitions::new(ShardId::new(id as u16), partitions_config);

        // TODO: namespace=0 collides with metadata consensus. Safe for now because the simulator
        // routes by Operation type, but a shared view change bus would produce namespace collisions.
        let partition_consensus = VsrConsensus::new(
            cluster_id,
            id,
            replica_count,
            0,
            bus,
            NamespacedPipeline::new(),
        );
        partition_consensus.init();
        partitions.set_consensus(partition_consensus);

        let plane = MuxPlane::new(variadic!(metadata, partitions));

        Self {
            id,
            name,
            replica_count,
            plane,
        }
    }

    /// Dispatch an incoming network message to the appropriate consensus plane.
    ///
    /// Routes requests, replication messages, and acks to either the metadata
    /// plane or the partitions plane based on `PlaneIdentity::is_applicable`.
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
        let planes = self.plane.inner();
        match MessageBag::from(message) {
            MessageBag::Request(request) => {
                if planes.0.is_applicable(&request) {
                    planes.0.on_request(request).await;
                } else {
                    planes.1.0.on_request(request).await;
                }
            }
            MessageBag::Prepare(prepare) => {
                if planes.0.is_applicable(&prepare) {
                    planes.0.on_replicate(prepare).await;
                } else {
                    planes.1.0.on_replicate(prepare).await;
                }
            }
            MessageBag::PrepareOk(prepare_ok) => {
                if planes.0.is_applicable(&prepare_ok) {
                    planes.0.on_ack(prepare_ok).await;
                } else {
                    planes.1.0.on_ack(prepare_ok).await;
                }
            }
        }
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
