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

use crate::bus::{MemBus, SharedMemBus};
use crate::deps::{MemStorage, SimJournal, SimMuxStateMachine, SimSnapshot};
use consensus::{LocalPipeline, NamespacedPipeline, VsrConsensus};
use iggy_common::IggyByteSize;
use iggy_common::sharding::ShardId;
use iggy_common::variadic;
use metadata::IggyMetadata;
use metadata::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use partitions::{IggyPartitions, PartitionsConfig};
use std::sync::Arc;

// TODO: Make configurable
const CLUSTER_ID: u128 = 1;

// For now there is only one shard per replica,
// we will add support for multiple shards per replica in the future.
pub type Replica =
    shard::IggyShard<SharedMemBus, SimJournal<MemStorage>, SimSnapshot, SimMuxStateMachine>;

pub fn new_replica(id: u8, name: String, bus: Arc<MemBus>, replica_count: u8) -> Replica {
    let users: Users = UsersInner::new().into();
    let streams: Streams = StreamsInner::new().into();
    let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();
    let mux = SimMuxStateMachine::new(variadic!(users, streams, consumer_groups));

    // Metadata uses namespace=0 (not partition-scoped)
    let metadata_consensus = VsrConsensus::new(
        CLUSTER_ID,
        id,
        replica_count,
        0,
        SharedMemBus(Arc::clone(&bus)),
        LocalPipeline::new(),
    );
    metadata_consensus.init();

    let metadata = IggyMetadata {
        consensus: Some(metadata_consensus),
        journal: Some(SimJournal::<MemStorage>::default()),
        snapshot: Some(SimSnapshot::default()),
        mux_stm: mux,
    };

    let partitions_config = PartitionsConfig {
        messages_required_to_save: 1000,
        size_of_messages_required_to_save: IggyByteSize::from(4 * 1024 * 1024),
        enforce_fsync: false, //Disable fsync for simulation
        segment_size: IggyByteSize::from(1024 * 1024 * 1024),
    };

    let mut partitions = IggyPartitions::new(ShardId::new(id as u16), partitions_config);

    // TODO: namespace=0 collides with metadata consensus. Safe for now because the simulator
    // routes by Operation type, but a shared view change bus would produce namespace collisions.
    let partition_consensus = VsrConsensus::new(
        CLUSTER_ID,
        id,
        replica_count,
        0,
        SharedMemBus(Arc::clone(&bus)),
        NamespacedPipeline::new(),
    );
    partition_consensus.init();
    partitions.set_consensus(partition_consensus);

    shard::IggyShard::new(id, name, metadata, partitions)
}
