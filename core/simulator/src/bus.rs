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
use message_bus::MessageBus;
use std::collections::{HashMap, VecDeque};
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
    clients: Mutex<HashMap<u128, ()>>,
    replicas: Mutex<HashMap<u8, ()>>,
    pending_messages: Mutex<VecDeque<Envelope>>,
}

impl MemBus {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            replicas: Mutex::new(HashMap::new()),
            pending_messages: Mutex::new(VecDeque::new()),
        }
    }

    /// Get the next pending message from the bus
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
        if self.clients.lock().unwrap().contains_key(&client) {
            return false;
        }
        self.clients.lock().unwrap().insert(client, ());
        true
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.clients.lock().unwrap().remove(&client).is_some()
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        if self.replicas.lock().unwrap().contains_key(&replica) {
            return false;
        }
        self.replicas.lock().unwrap().insert(replica, ());
        true
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.lock().unwrap().remove(&replica).is_some()
    }

    async fn send_to_client(
        &self,
        client_id: Self::Client,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        if !self.clients.lock().unwrap().contains_key(&client_id) {
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
        if !self.replicas.lock().unwrap().contains_key(&replica) {
            return Err(IggyError::ResourceNotFound(format!("Replica {}", replica)));
        }

        self.pending_messages.lock().unwrap().push_back(Envelope {
            from_replica: None,
            to_replica: Some(replica),
            to_client: None,
            message,
        });

        Ok(())
    }
}

/// Newtype wrapper for shared MemBus that implements MessageBus
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

    fn add_client(&mut self, client: Self::Client, sender: Self::Sender) -> bool {
        self.0
            .clients
            .lock()
            .unwrap()
            .insert(client, sender)
            .is_none()
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.0.clients.lock().unwrap().remove(&client).is_some()
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        self.0
            .replicas
            .lock()
            .unwrap()
            .insert(replica, ())
            .is_none()
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.0.replicas.lock().unwrap().remove(&replica).is_some()
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
}
