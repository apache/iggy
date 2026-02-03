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
mod cache;

use crate::cache::connection::{
    ConnectionCache, Coordinator, LeastLoadedStrategy, ShardedConnections,
};
use iggy_common::{IggyError, SenderKind, TcpSender, header::GenericHeader, message::Message};
use std::{collections::HashMap, rc::Rc};

/// Message bus parameterized by allocation strategy and sharded state
pub trait MessageBus {
    type Client;
    type Replica;
    type Data;
    type Sender;

    fn add_client(&mut self, client: Self::Client, sender: Self::Sender) -> bool;
    fn remove_client(&mut self, client: Self::Client) -> bool;

    fn add_replica(&mut self, replica: Self::Replica) -> bool;
    fn remove_replica(&mut self, replica: Self::Replica) -> bool;

    // TODO: refactor consesus headers.
    fn send_to_client(
        &self,
        client_id: Self::Client,
        data: Self::Data,
    ) -> impl Future<Output = Result<(), IggyError>>;
    fn send_to_replica(
        &self,
        replica: Self::Replica,
        data: Self::Data,
    ) -> impl Future<Output = Result<(), IggyError>>;
}

// TODO: explore generics for Strategy
#[derive(Debug)]
pub struct IggyMessageBus {
    clients: HashMap<u128, SenderKind>,
    replicas: ShardedConnections<LeastLoadedStrategy, ConnectionCache>,
    shard_id: u16,
}

impl IggyMessageBus {
    pub fn new(total_shards: usize, shard_id: u16, seed: u64) -> Self {
        Self {
            clients: HashMap::new(),
            replicas: ShardedConnections {
                coordinator: Coordinator::new(LeastLoadedStrategy::new(total_shards, seed)),
                state: ConnectionCache {
                    shard_id,
                    ..Default::default()
                },
            },
            shard_id,
        }
    }

    pub fn get_replica_connection(&self, replica: u8) -> Option<Rc<TcpSender>> {
        let mapped_shard = self.replicas.state.get_mapped_shard(replica)?;
        if mapped_shard == self.shard_id {
            self.replicas.state.get_connection(replica)
        } else {
            None
        }
    }
}

impl MessageBus for IggyMessageBus {
    type Client = u128;
    type Replica = u8;
    type Data = Message<GenericHeader>;
    type Sender = SenderKind;

    fn add_client(&mut self, client: Self::Client, sender: Self::Sender) -> bool {
        if self.clients.contains_key(&client) {
            return false;
        }
        self.clients.insert(client, sender);
        true
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.clients.remove(&client).is_some()
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.allocate(replica)
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.deallocate(replica)
    }

    async fn send_to_client(
        &self,
        client_id: Self::Client,
        _message: Self::Data,
    ) -> Result<(), IggyError> {
        let _sender = self
            .clients
            .get(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id as u32))?;
        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: Self::Replica,
        _message: Self::Data,
    ) -> Result<(), IggyError> {
        // TODO: Handle lazily creating the connection.
        let _connection = self
            .get_replica_connection(replica)
            .ok_or(IggyError::ResourceNotFound(format!("Replica {}", replica)))?;
        Ok(())
    }
}
