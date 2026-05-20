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

use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::{Frozen, Owned};
use iggy_binary_protocol::{GenericHeader, Message};
use message_bus::{MessageBus, SendError};
use std::collections::{HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// Convert an inbound Frozen (bus trait payload) into a `Message` for the
/// simulator's packet layer. The sim keeps `Message<GenericHeader>` in its
/// envelope because the network and tick loop reason about typed headers,
/// not raw byte buffers. One memcpy per send is acceptable under sim.
fn frozen_to_message(frozen: &Frozen<MESSAGE_ALIGN>) -> Message<GenericHeader> {
    Message::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(frozen.as_slice()))
        .expect("simulator bus must receive a valid generic message")
}

/// Message envelope for tracking sender/recipient
#[derive(Debug)]
pub enum EnvelopePayload {
    Replica(Message<GenericHeader>),
    Client(Message<GenericHeader>),
}

#[derive(Debug)]
pub struct Envelope {
    pub from_replica: Option<u8>,
    pub to_replica: Option<u8>,
    pub to_client: Option<u128>,
    pub payload: EnvelopePayload,
}

/// Per-replica outbox for staging outbound messages.
///
/// Consensus code calls `send_to_replica()` / `send_to_client()` which stage
/// messages here. The simulator's tick loop drains each replica's outbox and
/// feeds the messages into the [`Network`] for simulated delivery.
#[derive(Debug)]
pub struct SimOutbox {
    /// Replica id that owns this outbox. Populated as `from_replica` on every envelope.
    self_id: u8,
    clients: Mutex<HashSet<u128>>,
    replicas: Mutex<HashSet<u8>>,
    pending_messages: Mutex<VecDeque<Envelope>>,
}

impl SimOutbox {
    #[must_use]
    pub fn new(self_id: u8) -> Self {
        Self {
            self_id,
            clients: Mutex::new(HashSet::new()),
            replicas: Mutex::new(HashSet::new()),
            pending_messages: Mutex::new(VecDeque::new()),
        }
    }

    /// Drain all staged messages from this outbox.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn drain(&self) -> Vec<Envelope> {
        self.pending_messages.lock().unwrap().drain(..).collect()
    }

    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn add_client(&mut self, client: u128) -> bool {
        self.clients.lock().unwrap().insert(client)
    }

    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn add_replica(&mut self, replica: u8) -> bool {
        self.replicas.lock().unwrap().insert(replica)
    }

    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn remove_client(&mut self, client: u128) -> bool {
        self.clients.lock().unwrap().remove(&client)
    }

    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn remove_replica(&mut self, replica: u8) -> bool {
        self.replicas.lock().unwrap().remove(&replica)
    }
}

impl MessageBus for SimOutbox {
    async fn send_to_client(
        &self,
        client_id: u128,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if !self.clients.lock().unwrap().contains(&client_id) {
            return Err(SendError::ClientNotFound(client_id));
        }

        self.pending_messages.lock().unwrap().push_back(Envelope {
            from_replica: Some(self.self_id),
            to_replica: None,
            to_client: Some(client_id),
            payload: EnvelopePayload::Client(frozen_to_message(&data)),
        });

        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if !self.replicas.lock().unwrap().contains(&replica) {
            return Err(SendError::ReplicaNotConnected(replica));
        }

        self.pending_messages.lock().unwrap().push_back(Envelope {
            from_replica: Some(self.self_id),
            to_replica: Some(replica),
            to_client: None,
            payload: EnvelopePayload::Replica(frozen_to_message(&data)),
        });

        Ok(())
    }
}

/// Newtype wrapper for shared [`SimOutbox`] that implements [`MessageBus`]
#[derive(Debug, Clone)]
pub struct SharedSimOutbox(pub Arc<SimOutbox>);

impl Deref for SharedSimOutbox {
    type Target = SimOutbox;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MessageBus for SharedSimOutbox {
    async fn send_to_client(
        &self,
        client_id: u128,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.0.send_to_client(client_id, data).await
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.0.send_to_replica(replica, data).await
    }
}
