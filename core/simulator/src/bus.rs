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

use iggy_common::{IggyError, header::GenericHeader, message::Message};
use message_bus::{MessageBus, Outbound, Recipient};
use std::collections::{HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// Message envelope for tracking sender/recipient
#[derive(Debug, Clone)]
pub struct Envelope {
    pub from_replica: Option<u8>,
    pub to_replica: Option<u8>,
    pub to_client: Option<u128>,
    pub message: Message<GenericHeader>,
}

// TODO: Proper bus with an `Network` component which would simulate sending packets.
// Tigerbeetle handles this by having an list of "buses", and calling callbacks for clients when an response is send.
// This requires self-referntial structs (as message_bus has to store collection of other buses), which is overcomplilcated.
// I think the way we could handle that is by having an dedicated collection for client responses (clients_table).
#[derive(Debug, Default)]
pub struct MemBus {
    clients: Mutex<HashSet<u128>>,
    replicas: Mutex<HashSet<u8>>,
    pending_messages: Mutex<VecDeque<Envelope>>,
}

impl MemBus {
    #[must_use]
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashSet::new()),
            replicas: Mutex::new(HashSet::new()),
            pending_messages: Mutex::new(VecDeque::new()),
        }
    }

    /// Get the next pending message from the bus.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn receive(&self) -> Option<Envelope> {
        self.pending_messages.lock().unwrap().pop_front()
    }
}

impl MessageBus for MemBus {
    type Client = u128;
    type Replica = u8;
    type Data = Message<GenericHeader>;
    type Sender = ();

    fn add_client(&mut self, client: Self::Client, _sender: Self::Sender) -> bool {
        self.clients.lock().unwrap().insert(client)
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.clients.lock().unwrap().remove(&client)
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.lock().unwrap().insert(replica)
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.lock().unwrap().remove(&replica)
    }

    async fn send_to_client(
        &self,
        client_id: Self::Client,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        if !self.clients.lock().unwrap().contains(&client_id) {
            #[allow(clippy::cast_possible_truncation)]
            return Err(IggyError::ClientNotFound(client_id as u32));
        }

        self.pending_messages.lock().unwrap().push_back(Envelope {
            from_replica: None,
            to_replica: None,
            to_client: Some(client_id),
            message,
        });

        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: Self::Replica,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        if !self.replicas.lock().unwrap().contains(&replica) {
            return Err(IggyError::ResourceNotFound(format!("Replica {replica}")));
        }

        self.pending_messages.lock().unwrap().push_back(Envelope {
            from_replica: None,
            to_replica: Some(replica),
            to_client: None,
            message,
        });

        Ok(())
    }

    fn drain(&self, buf: &mut Vec<Outbound<Self::Client, Self::Replica, Self::Data>>) {
        buf.extend(
            self.pending_messages
                .lock()
                .unwrap()
                .drain(..)
                .map(|envelope| {
                    let recipient = if let Some(client) = envelope.to_client {
                        Recipient::Client(client)
                    } else if let Some(replica) = envelope.to_replica {
                        Recipient::Replica(replica)
                    } else {
                        unreachable!("envelope must have either to_client or to_replica set")
                    };
                    (recipient, envelope.message)
                }),
        );
    }
}

/// Newtype wrapper for shared [`MemBus`] that implements [`MessageBus`]
#[derive(Debug, Clone)]
pub struct SharedMemBus(pub Arc<MemBus>);

impl Deref for SharedMemBus {
    type Target = MemBus;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MessageBus for SharedMemBus {
    type Client = u128;
    type Replica = u8;
    type Data = Message<GenericHeader>;
    type Sender = ();

    fn add_client(&mut self, client: Self::Client, _sender: Self::Sender) -> bool {
        self.0.clients.lock().unwrap().insert(client)
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.0.clients.lock().unwrap().remove(&client)
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        self.0.replicas.lock().unwrap().insert(replica)
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.0.replicas.lock().unwrap().remove(&replica)
    }

    async fn send_to_client(
        &self,
        client_id: Self::Client,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        self.0.send_to_client(client_id, message).await
    }

    async fn send_to_replica(
        &self,
        replica: Self::Replica,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        self.0.send_to_replica(replica, message).await
    }

    fn drain(&self, buf: &mut Vec<Outbound<Self::Client, Self::Replica, Self::Data>>) {
        self.0.drain(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::header::GenericHeader;

    fn make_message() -> Message<GenericHeader> {
        Message::<GenericHeader>::new(std::mem::size_of::<GenericHeader>())
    }

    #[test]
    fn drain_empty_yields_nothing() {
        let bus = MemBus::new();
        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn send_to_replica_then_drain() {
        let mut bus = MemBus::new();
        bus.add_replica(0);

        futures::executor::block_on(bus.send_to_replica(0, make_message())).unwrap();
        futures::executor::block_on(bus.send_to_replica(0, make_message())).unwrap();

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].0, Recipient::Replica(0));
        assert_eq!(buf[1].0, Recipient::Replica(0));
    }

    #[test]
    fn send_to_client_then_drain() {
        let mut bus = MemBus::new();
        bus.add_client(42, ());

        futures::executor::block_on(bus.send_to_client(42, make_message())).unwrap();

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].0, Recipient::Client(42));
    }

    #[test]
    fn double_drain_second_empty() {
        let mut bus = MemBus::new();
        bus.add_replica(0);

        futures::executor::block_on(bus.send_to_replica(0, make_message())).unwrap();

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 1);

        buf.clear();
        bus.drain(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn send_to_unknown_replica_errors_and_drain_empty() {
        let bus = MemBus::new();
        let result = futures::executor::block_on(bus.send_to_replica(99, make_message()));
        assert!(result.is_err());

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn drain_preserves_routing() {
        let mut bus = MemBus::new();
        bus.add_replica(1);
        bus.add_client(42, ());

        futures::executor::block_on(bus.send_to_replica(1, make_message())).unwrap();
        futures::executor::block_on(bus.send_to_client(42, make_message())).unwrap();

        let mut buf = Vec::new();
        bus.drain(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].0, Recipient::Replica(1));
        assert_eq!(buf[1].0, Recipient::Client(42));
    }

    #[test]
    fn shared_mem_bus_drain_delegates() {
        let bus = Arc::new(MemBus::new());
        let mut shared = SharedMemBus(bus);
        shared.add_replica(0);

        futures::executor::block_on(shared.send_to_replica(0, make_message())).unwrap();

        let mut buf = Vec::new();
        shared.drain(&mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].0, Recipient::Replica(0));
    }
}
