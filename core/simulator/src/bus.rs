use iggy_common::{header::GenericHeader, message::Message, IggyError};
use message_bus::MessageBus;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

/// Message envelope for tracking sender/recipient
#[derive(Debug, Clone)]
pub struct Envelope {
    pub from_replica: Option<u8>,
    pub to_replica: Option<u8>,
    pub to_client: Option<u128>,
    pub message: Message<GenericHeader>,
}

/// In-memory message bus implementing the MessageBus trait
#[derive(Debug, Default)]
pub struct MemBus {
    clients: RefCell<HashMap<u128, ()>>,
    replicas: RefCell<HashMap<u8, ()>>,
    pending_messages: RefCell<VecDeque<Envelope>>,
}

impl MemBus {
    pub fn new() -> Self {
        Self {
            clients: RefCell::new(HashMap::new()),
            replicas: RefCell::new(HashMap::new()),
            pending_messages: RefCell::new(VecDeque::new()),
        }
    }

    /// Get the next pending message from the bus
    pub fn receive(&self) -> Option<Envelope> {
        self.pending_messages.borrow_mut().pop_front()
    }

    /// Get all pending messages from the bus
    pub fn receive_all(&self) -> Vec<Envelope> {
        self.pending_messages.borrow_mut().drain(..).collect()
    }

    /// Check if there are pending messages
    pub fn has_pending(&self) -> bool {
        !self.pending_messages.borrow().is_empty()
    }

    /// Get the count of pending messages
    pub fn pending_count(&self) -> usize {
        self.pending_messages.borrow().len()
    }
}

impl MessageBus for MemBus {
    type Client = u128;
    type Replica = u8;
    type Data = Message<GenericHeader>;
    type Sender = ();

    fn add_client(&mut self, client: Self::Client, _sender: Self::Sender) -> bool {
        if self.clients.borrow().contains_key(&client) {
            return false;
        }
        self.clients.borrow_mut().insert(client, ());
        true
    }

    fn remove_client(&mut self, client: Self::Client) -> bool {
        self.clients.borrow_mut().remove(&client).is_some()
    }

    fn add_replica(&mut self, replica: Self::Replica) -> bool {
        if self.replicas.borrow().contains_key(&replica) {
            return false;
        }
        self.replicas.borrow_mut().insert(replica, ());
        true
    }

    fn remove_replica(&mut self, replica: Self::Replica) -> bool {
        self.replicas.borrow_mut().remove(&replica).is_some()
    }

    async fn send_to_client(
        &self,
        client_id: Self::Client,
        message: Self::Data,
    ) -> Result<(), IggyError> {
        if !self.clients.borrow().contains_key(&client_id) {
            return Err(IggyError::ClientNotFound(client_id as u32));
        }

        self.pending_messages.borrow_mut().push_back(Envelope {
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
        if !self.replicas.borrow().contains_key(&replica) {
            return Err(IggyError::ResourceNotFound(format!("Replica {}", replica)));
        }

        self.pending_messages.borrow_mut().push_back(Envelope {
            from_replica: None,
            to_replica: Some(replica),
            to_client: None,
            message,
        });

        Ok(())
    }
}
