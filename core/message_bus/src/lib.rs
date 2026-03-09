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
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

/// Maximum number of messages that can be buffered in the outbox before delivery.
/// Overflow is a logic error (pump loop not draining). Matches the assert pattern
/// used by `VsrConsensus::push_loopback` in `core/consensus/src/impls.rs`.
const OUTBOX_CAPACITY_MAX: usize = 128;

/// Routing target for a message drained from the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recipient<C, R> {
    Client(C),
    Replica(R),
}

/// An outbound message paired with its intended [`Recipient`].
pub type Outbound<C, R, D> = (Recipient<C, R>, D);

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

    // TODO: refactor consensus headers.
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

    /// Drain all buffered outbound messages into `buf`, leaving the internal queue empty.
    ///
    /// Each entry pairs the message with its intended [`Recipient`].
    /// The caller is responsible for dispatching them to their targets.
    fn drain(&self, _buf: &mut Vec<Outbound<Self::Client, Self::Replica, Self::Data>>) {}
}

// TODO: explore generics for Strategy
#[derive(Debug)]
pub struct IggyMessageBus {
    clients: HashMap<u128, SenderKind>,
    replicas: ShardedConnections<LeastLoadedStrategy, ConnectionCache>,
    shard_id: u16,
    outbox: RefCell<VecDeque<Outbound<u128, u8, Message<GenericHeader>>>>,
}

impl IggyMessageBus {
    #[must_use]
    pub fn new(total_shards: u16, shard_id: u16, seed: u64) -> Self {
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
            outbox: RefCell::new(VecDeque::with_capacity(OUTBOX_CAPACITY_MAX)),
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

#[allow(clippy::future_not_send)] // Single-threaded runtime (compio), Rc/RefCell by design
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
        message: Self::Data,
    ) -> Result<(), IggyError> {
        #[allow(clippy::cast_possible_truncation)] // IggyError::ClientNotFound takes u32
        let _sender = self
            .clients
            .get(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id as u32))?;
        let mut outbox = self.outbox.borrow_mut();
        assert!(
            outbox.len() < OUTBOX_CAPACITY_MAX,
            "outbox overflow: {} items",
            outbox.len()
        );
        outbox.push_back((Recipient::Client(client_id), message));
        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: Self::Replica,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        // TODO: Handle lazily creating the connection.
        let _connection = self
            .get_replica_connection(replica)
            .ok_or(IggyError::ResourceNotFound(format!("Replica {replica}")))?;
        let mut outbox = self.outbox.borrow_mut();
        assert!(
            outbox.len() < OUTBOX_CAPACITY_MAX,
            "outbox overflow: {} items",
            outbox.len()
        );
        outbox.push_back((Recipient::Replica(replica), message));
        Ok(())
    }

    fn drain(&self, buf: &mut Vec<Outbound<Self::Client, Self::Replica, Self::Data>>) {
        buf.extend(self.outbox.borrow_mut().drain(..));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message() -> Message<GenericHeader> {
        Message::<GenericHeader>::new(std::mem::size_of::<GenericHeader>())
    }

    #[test]
    fn drain_empty_yields_nothing() {
        let bus = IggyMessageBus::new(1, 0, 42);
        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn double_drain_second_empty() {
        let bus = IggyMessageBus::new(1, 0, 42);
        bus.outbox
            .borrow_mut()
            .push_back((Recipient::Client(0), make_message()));

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 1);

        buf.clear();
        bus.drain(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn drain_preserves_client_routing() {
        let bus = IggyMessageBus::new(1, 0, 42);
        bus.outbox
            .borrow_mut()
            .push_back((Recipient::Client(42), make_message()));

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].0, Recipient::Client(42));
    }

    #[test]
    fn drain_preserves_replica_routing() {
        let bus = IggyMessageBus::new(1, 0, 42);
        bus.outbox
            .borrow_mut()
            .push_back((Recipient::Replica(3), make_message()));

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].0, Recipient::Replica(3));
    }

    #[test]
    fn client_and_replica_preserves_fifo_order() {
        let bus = IggyMessageBus::new(1, 0, 42);
        {
            let mut outbox = bus.outbox.borrow_mut();
            outbox.push_back((Recipient::Client(10), make_message()));
            outbox.push_back((Recipient::Replica(1), make_message()));
        }

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].0, Recipient::Client(10));
        assert_eq!(buf[1].0, Recipient::Replica(1));
    }
}
