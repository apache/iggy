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
use crate::deps::{
    MemStorage, ReplicaPartitions, SimJournal, SimMetadata, SimMuxStateMachine, SimSnapshot,
};
use consensus::VsrConsensus;
use metadata::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use metadata::{IggyMetadata, variadic};
use std::sync::Arc;

#[derive(Debug)]
pub struct Replica {
    pub id: u8,
    pub name: String,
    pub metadata: SimMetadata,
    pub partitions: ReplicaPartitions,
    pub bus: Arc<MemBus>,
}

impl Replica {
    pub fn new(id: u8, name: String, bus: Arc<MemBus>, replica_count: u8) -> Self {
        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();
        let mux = SimMuxStateMachine::new(variadic!(users, streams, consumer_groups));

        let cluster_id: u128 = 1; // TODO: Make configurable
        let consensus = VsrConsensus::new(
            cluster_id,
            id,
            replica_count,
            SharedMemBus(Arc::clone(&bus)),
        );
        consensus.init();

        Self {
            id,
            name,
            metadata: IggyMetadata {
                consensus: Some(consensus),
                journal: Some(SimJournal::<MemStorage>::default()),
                snapshot: Some(SimSnapshot::default()),
                mux_stm: mux,
            },
            partitions: ReplicaPartitions::default(),
            bus,
        }
    }
}
