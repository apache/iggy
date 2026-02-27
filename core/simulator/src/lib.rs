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

pub mod bus;
pub mod client;
pub mod deps;
pub mod network;
pub mod packet;
pub mod ready_queue;
pub mod replica;

use bus::MemBus;
use iggy_common::header::ReplyHeader;
use iggy_common::message::Message;
use message_bus::MessageBus;
use replica::{Replica, new_replica};
use std::sync::Arc;

pub struct Simulator {
    pub replicas: Vec<Replica>,
    pub message_bus: Arc<MemBus>,
}

impl Simulator {
    /// Initialize a partition with its own consensus group on all replicas.
    pub fn init_partition(&mut self, namespace: iggy_common::sharding::IggyNamespace) {
        for replica in &mut self.replicas {
            replica.init_partition(namespace);
        }
    }

    pub fn new(replica_count: usize, clients: impl Iterator<Item = u128>) -> Self {
        let mut message_bus = MemBus::new();
        for client in clients {
            message_bus.add_client(client, ());
        }

        for i in 0..replica_count as u8 {
            message_bus.add_replica(i);
        }

        let message_bus = Arc::new(message_bus);
        let replicas = (0..replica_count)
            .map(|i| {
                new_replica(
                    i as u8,
                    format!("replica-{}", i),
                    Arc::clone(&message_bus),
                    replica_count as u8,
                )
            })
            .collect();

        Self {
            replicas,
            message_bus,
        }
    }

    pub fn with_message_bus(replica_count: usize, mut message_bus: MemBus) -> Self {
        for i in 0..replica_count as u8 {
            message_bus.add_replica(i);
        }

        let message_bus = Arc::new(message_bus);
        let replicas = (0..replica_count)
            .map(|i| {
                new_replica(
                    i as u8,
                    format!("replica-{}", i),
                    Arc::clone(&message_bus),
                    replica_count as u8,
                )
            })
            .collect();

        Self {
            replicas,
            message_bus,
        }
    }
}

impl Simulator {
    pub async fn step(&self) -> Option<Message<ReplyHeader>> {
        if let Some(envelope) = self.message_bus.receive() {
            if let Some(_client_id) = envelope.to_client {
                let reply: Message<ReplyHeader> = envelope
                    .message
                    .try_into_typed()
                    .expect("invalid message, wrong command type for an client response");
                return Some(reply);
            }

            if let Some(replica_id) = envelope.to_replica
                && let Some(replica) = self.replicas.get(replica_id as usize)
            {
                self.dispatch_to_replica(replica, envelope.message).await;
            }
        }

        None
    }

    async fn dispatch_to_replica(
        &self,
        replica: &Replica,
        message: Message<iggy_common::header::GenericHeader>,
    ) {
        replica.on_message(message).await;
    }
}

// TODO(IGGY-66): Add acceptance test for per-partition consensus independence.
// Setup: 3-replica simulator, two partitions (ns_a, ns_b).
// 1. Fill ns_a's pipeline to PIPELINE_PREPARE_QUEUE_MAX without delivering acks.
// 2. Send a request to ns_b, step until ns_b reply arrives.
// 3. Assert ns_b committed while ns_a pipeline is still full.
// Requires namespace-aware stepping (filter bus by namespace) or two-phase delivery.
