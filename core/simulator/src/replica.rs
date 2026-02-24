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
use iggy_common::IggyByteSize;
use iggy_common::variadic;
use metadata::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use partitions::PartitionsConfig;
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

    let partitions_config = PartitionsConfig {
        messages_required_to_save: 1000,
        size_of_messages_required_to_save: IggyByteSize::from(4 * 1024 * 1024),
        enforce_fsync: false, //Disable fsync for simulation
        segment_size: IggyByteSize::from(1024 * 1024 * 1024),
    };

    shard::IggyShard::new(
        id,
        name,
        CLUSTER_ID,
        SharedMemBus(Arc::clone(&bus)),
        replica_count,
        SimJournal::<MemStorage>::default(),
        SimSnapshot::default(),
        mux,
        partitions_config,
    )
}
