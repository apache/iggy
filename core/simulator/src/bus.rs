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

use crate::deps::SimClock;
use crate::executor::{PendingSpawns, TimerHandle};
use clock::Clock;
#[cfg(test)]
use futures::channel::oneshot;
use iggy_binary_protocol::GenericHeader;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::conn_info::ClientConnMeta;
use message_bus::replica::listener::MessageHandler;
use message_bus::{
    BusMessage, ClientConnectionLostFn, ConnectionInstaller, ConnectionPermit, ConnectionRegistry,
    InstanceToken, MessageBus, ReplicaHandshakeDoneFn, SendError, SharedTlsServerConfig,
};
use server_common::{
    MESSAGE_ALIGN, Message,
    iobuf::{Frozen, Owned},
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::time::{Duration, Instant};

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
/// feeds the messages into the [`Network`](crate::network::Network) for simulated delivery.
pub struct SimOutbox {
    /// Replica id that owns this outbox. Populated as `from_replica` on every envelope.
    self_id: u8,
    shell: bool,
    /// One installation per connected client, so a reply captured for an
    /// earlier connection is refused after a reconnect, as on the real bus.
    clients: ConnectionRegistry<u128>,
    replicas: RefCell<HashSet<u8>>,
    pending_messages: RefCell<VecDeque<Envelope>>,
    /// Virtual clock backing [`MessageBus::sleep`], so the shard pump's
    /// consensus tick advances with the simulation, not the wall clock.
    timer: TimerHandle,
    /// Pending-spawn queue shared with the executor, backing
    /// [`MessageBus::spawn`]: tasks spawned off the pump (dispatch's poll
    /// IO, client-request drains, metadata submits) stage here, and the
    /// executor drains them into deterministic tasks tagged to this replica.
    spawns: Rc<PendingSpawns>,
    /// Per-connection metadata registered when a `SimClient` connects, so the
    /// dispatch layer's `client_meta` lookups resolve (production reads the same
    /// from `IggyMessageBus`). Empty until the shell wires clients.
    client_metas: RefCell<HashMap<u128, Rc<ClientConnMeta>>>,
    /// Client-disconnect callback the dispatch handler registers; the harness
    /// fires it via `notify_client_connection_lost` to drive the real
    /// session-removal + logout path when it models a client disconnect.
    client_lost_fn: RefCell<Option<ClientConnectionLostFn>>,
    #[cfg(test)]
    next_replica_send: RefCell<Option<oneshot::Receiver<()>>>,
}

impl std::fmt::Debug for SimOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `client_lost_fn` (an `Rc<dyn Fn>`) is not `Debug`, so this is hand
        // written and omits it.
        f.debug_struct("SimOutbox")
            .field("self_id", &self.self_id)
            .field("shell", &self.shell)
            .field("clients", &self.clients)
            .field("replicas", &self.replicas)
            .field("pending_messages", &self.pending_messages)
            .field("timer", &self.timer)
            .field("spawns", &self.spawns)
            .field("client_metas", &self.client_metas)
            .finish_non_exhaustive()
    }
}

impl SimOutbox {
    #[must_use]
    pub fn new(self_id: u8, timer: TimerHandle, spawns: Rc<PendingSpawns>, shell: bool) -> Self {
        Self {
            self_id,
            shell,
            clients: ConnectionRegistry::new(),
            replicas: RefCell::new(HashSet::new()),
            pending_messages: RefCell::new(VecDeque::new()),
            timer,
            spawns,
            client_metas: RefCell::new(HashMap::new()),
            client_lost_fn: RefCell::new(None),
            #[cfg(test)]
            next_replica_send: RefCell::new(None),
        }
    }

    /// Suspend the next replica send so tests can inspect owner reply ordering.
    #[cfg(test)]
    pub(crate) fn delay_next_replica_send(&self) -> oneshot::Sender<()> {
        let (resume, wait) = oneshot::channel();
        assert!(self.next_replica_send.borrow_mut().replace(wait).is_none());
        resume
    }

    /// Drain all staged messages from this outbox.
    pub fn drain(&self) -> Vec<Envelope> {
        self.pending_messages.borrow_mut().drain(..).collect()
    }

    pub fn add_client(&mut self, client: u128) -> bool {
        self.clients.insert_in_process(client).is_some()
    }

    pub fn add_replica(&mut self, replica: u8) -> bool {
        self.replicas.borrow_mut().insert(replica)
    }

    pub fn remove_client(&mut self, client: u128) -> bool {
        self.clients.remove(client)
    }

    pub fn remove_replica(&mut self, replica: u8) -> bool {
        self.replicas.borrow_mut().remove(&replica)
    }

    /// Register a connecting client's metadata so dispatch's `client_meta`
    /// resolves it (mirrors `IggyMessageBus` at connection install).
    pub fn insert_client_meta(&self, meta: ClientConnMeta) {
        self.client_metas
            .borrow_mut()
            .insert(meta.client_id, Rc::new(meta));
    }

    /// Drop a disconnected client's metadata.
    pub fn remove_client_meta(&self, client_id: u128) {
        self.client_metas.borrow_mut().remove(&client_id);
    }

    /// Per-connection metadata for `client_id`, or `None` if not connected.
    #[must_use]
    pub fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>> {
        self.client_metas.borrow().get(&client_id).cloned()
    }

    /// Register the client-connection-lost callback (the dispatch handler
    /// installs it during handler construction).
    pub fn set_client_connection_lost_fn(&self, f: ClientConnectionLostFn) {
        *self.client_lost_fn.borrow_mut() = Some(f);
    }

    /// Fire the registered connection-lost callback for `client_id`, a no-op if
    /// none is installed. The harness calls this to model a client disconnect,
    /// driving the real session-removal + logout dispatch path. The lock is
    /// released before the call so the callback may re-enter the bus.
    pub fn notify_client_connection_lost(&self, client_id: u128) {
        let handler = self.client_lost_fn.borrow().clone();
        if let Some(handler) = handler {
            handler(client_id);
        }
    }
}

// The bus is single-threaded and `!Send` (it holds the `Rc` spawn queue),
// so its async methods return `!Send` futures. That is correct for the sim
// and matches the `SimJournal` I/O impls in `deps.rs`.
#[allow(clippy::future_not_send)]
impl MessageBus for SimOutbox {
    fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

    async fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        if !self.clients.contains(client_id) {
            return Err(SendError::ClientNotFound(client_id));
        }

        self.pending_messages.borrow_mut().push_back(Envelope {
            from_replica: Some(self.self_id),
            to_replica: None,
            to_client: Some(client_id),
            payload: EnvelopePayload::Client(frozen_to_message(&data.into().into_contiguous())),
        });

        Ok(())
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if !self.replicas.borrow().contains(&replica) {
            return Err(SendError::ReplicaNotConnected(replica));
        }

        #[cfg(test)]
        {
            let wait = self.next_replica_send.borrow_mut().take();
            if let Some(wait) = wait {
                let _ = wait.await;
            }
        }

        self.pending_messages.borrow_mut().push_back(Envelope {
            from_replica: Some(self.self_id),
            to_replica: Some(replica),
            to_client: None,
            payload: EnvelopePayload::Replica(frozen_to_message(&data)),
        });

        Ok(())
    }

    fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        self.timer.sleep(duration)
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.spawns.push(self.self_id, Box::pin(future));
    }

    fn realtime_micros(&self) -> u64 {
        // Same virtual timeline `SimClock` injects into every consensus
        // group, so the bus clock a shard-side handler reads and the
        // prepare-timestamp clock agree to the microsecond.
        SimClock::new(self.timer.clone()).realtime().as_micros()
    }

    fn monotonic_now(&self) -> Instant {
        self.timer.monotonic_now()
    }
}

/// Newtype wrapper for shared [`SimOutbox`] that implements [`MessageBus`]
#[derive(Debug, Clone)]
pub struct SharedSimOutbox(pub Rc<SimOutbox>);

impl Deref for SharedSimOutbox {
    type Target = SimOutbox;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::future_not_send)]
impl MessageBus for SharedSimOutbox {
    fn track_background(&self, handle: message_bus::JoinHandle<()>) {
        self.0.track_background(handle);
    }

    async fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        if self.0.shell {
            return Err(SendError::ClientNotFound(client_id));
        }
        // Raw mode dispatches client packets with no installation to reply to.
        self.0.send_to_client(client_id, data).await
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        self.0.send_to_replica(replica, data).await
    }

    fn set_connection_lost_fn(&self, f: message_bus::ConnectionLostFn) {
        self.0.set_connection_lost_fn(f);
    }

    fn set_replica_forward_fn(&self, f: message_bus::ReplicaForwardFn) {
        self.0.set_replica_forward_fn(f);
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        self.0.sleep(duration)
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.0.spawn(future);
    }

    fn realtime_micros(&self) -> u64 {
        self.0.realtime_micros()
    }

    fn monotonic_now(&self) -> Instant {
        self.0.monotonic_now()
    }
}

/// The router's dispatch/pump impl block requires `ConnectionInstaller`
/// (`router.rs`, `B: MessageBus + ConnectionInstaller + Clone`). The
/// simulator delivers messages as in-memory frames, never as delegated
/// fds, so the install surface is unreachable: fd installs panic loudly
/// if a future change ever routes a connection-setup frame into the sim,
/// while the shard-0 bookkeeping acks are harmless no-ops.
impl ConnectionInstaller for SharedSimOutbox {
    fn install_replica_inbound_fd(
        &self,
        _fd: DupedFd,
        _on_message: MessageHandler,
        _on_done: ReplicaHandshakeDoneFn,
    ) {
        panic!("simulator has no fd transfer: replica inbound install is unreachable");
    }

    fn install_replica_outbound_fd(
        &self,
        _fd: DupedFd,
        _replica_id: u8,
        _on_message: MessageHandler,
        _on_done: ReplicaHandshakeDoneFn,
    ) {
        panic!("simulator has no fd transfer: replica outbound install is unreachable");
    }

    fn release_replica_handshake_slot(&self, _slot: u64) {}

    fn clear_replica_dial_pending(&self, _replica_id: u8) {}

    fn install_client_fd(
        &self,
        _fd: DupedFd,
        _meta: ClientConnMeta,
        _permit: ConnectionPermit,
        _on_request: RequestHandler,
    ) {
        panic!("simulator has no fd transfer: client install is unreachable");
    }

    fn install_client_ws_fd(
        &self,
        _fd: DupedFd,
        _meta: ClientConnMeta,
        _permit: ConnectionPermit,
        _on_request: RequestHandler,
    ) {
        panic!("simulator has no fd transfer: ws client install is unreachable");
    }

    fn install_client_tcp_tls_fd(
        &self,
        _fd: DupedFd,
        _meta: ClientConnMeta,
        _config: SharedTlsServerConfig,
        _permit: ConnectionPermit,
        _on_request: RequestHandler,
    ) {
        panic!("simulator has no fd transfer: tcp tls client install is unreachable");
    }

    fn install_client_wss_fd(
        &self,
        _fd: DupedFd,
        _meta: ClientConnMeta,
        _config: SharedTlsServerConfig,
        _permit: ConnectionPermit,
        _on_request: RequestHandler,
    ) {
        panic!("simulator has no fd transfer: wss client install is unreachable");
    }

    // Real, unlike the fd installs above: dispatch reads client_meta to seed
    // the session manager, and registers the disconnect callback the harness
    // fires. Both delegate to the in-memory registry on the inner outbox.
    fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>> {
        self.0.client_meta(client_id)
    }

    fn set_client_connection_lost_fn(&self, f: ClientConnectionLostFn) {
        self.0.set_client_connection_lost_fn(f);
    }

    fn client_instance(&self, client_id: u128) -> Option<InstanceToken> {
        self.0.clients.instance(client_id)
    }

    #[allow(clippy::future_not_send)]
    async fn send_to_client_instance(
        &self,
        client_id: u128,
        instance: Option<InstanceToken>,
        message: BusMessage,
    ) -> Result<(), SendError> {
        if instance.is_none() || self.0.clients.instance(client_id) != instance {
            return Err(SendError::ClientNotFound(client_id));
        }
        self.0.send_to_client(client_id, message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use message_bus::installer::conn_info::ClientTransportKind;
    use partitions::PreparedPrimaryTransfer;
    use std::cell::Cell;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_outbox() -> SimOutbox {
        SimOutbox::new(
            0,
            TimerHandle::new(),
            Rc::new(PendingSpawns::default()),
            false,
        )
    }

    #[test]
    fn given_shared_virtual_clock_when_time_advances_should_expire_grants_without_pump_ticks() {
        let timer = TimerHandle::new();
        let spawns = Rc::new(PendingSpawns::default());
        let bus = Rc::new(SharedSimOutbox(Rc::new(SimOutbox::new(
            0,
            timer.clone(),
            Rc::clone(&spawns),
            false,
        ))));
        let start = MessageBus::monotonic_now(&bus);
        let timeout = Duration::from_secs(1);
        let grant = PreparedPrimaryTransfer {
            created_revision: 1,
            expected_view: 0,
            target: 1,
            expires_at: start + timeout,
        };
        assert!(grant.is_active_at(MessageBus::monotonic_now(&bus)));
        assert_eq!(MessageBus::monotonic_now(&bus), start);

        assert_eq!(timer.advance(timeout / 2), [] as [u64; 0]);
        let restarted = SharedSimOutbox(Rc::new(SimOutbox::new(0, timer.clone(), spawns, false)));
        assert_eq!(restarted.monotonic_now(), start + timeout / 2);
        assert_eq!(MessageBus::monotonic_now(&bus), restarted.monotonic_now());
        assert!(grant.is_active_at(restarted.monotonic_now()));

        assert_eq!(timer.advance(timeout / 2), [] as [u64; 0]);
        assert_eq!(MessageBus::monotonic_now(&bus), start + timeout);
        assert!(!grant.is_active_at(MessageBus::monotonic_now(&bus)));
    }

    #[test]
    fn client_meta_round_trips_and_lost_fn_fires() {
        let bus = test_outbox();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
        bus.insert_client_meta(ClientConnMeta::new(42, addr, ClientTransportKind::Tcp));

        let meta = bus.client_meta(42).expect("registered client meta");
        assert_eq!(meta.client_id, 42);
        assert_eq!(meta.peer_addr, addr);
        assert!(bus.client_meta(99).is_none(), "unknown client has no meta");

        let lost = Rc::new(Cell::new(0u128));
        let lost_clone = Rc::clone(&lost);
        bus.set_client_connection_lost_fn(Rc::new(move |id| lost_clone.set(id)));
        bus.notify_client_connection_lost(42);
        assert_eq!(lost.get(), 42, "registered lost-fn must fire with the id");

        bus.remove_client_meta(42);
        assert!(bus.client_meta(42).is_none(), "removed client meta is gone");
    }

    /// A reply captured for an earlier connection never reaches the client that
    /// reconnected under its id, as on the real bus.
    #[test]
    fn given_reconnected_client_when_replying_to_earlier_installation_should_refuse() {
        const CLIENT: u128 = 7;
        let reply = || {
            let size = std::mem::size_of::<GenericHeader>();
            Message::<GenericHeader>::new(size)
                .transmute_header(|_, header: &mut GenericHeader| {
                    header.size = u32::try_from(size).expect("header size fits u32");
                })
                .into_frozen()
                .into()
        };
        let mut outbox = test_outbox();
        outbox.shell = true;
        assert!(outbox.add_client(CLIENT));
        let earlier = outbox.clients.instance(CLIENT);
        assert!(outbox.remove_client(CLIENT));
        assert!(outbox.add_client(CLIENT));
        let bus = SharedSimOutbox(Rc::new(outbox));

        let stale = bus
            .send_to_client_instance(CLIENT, earlier, reply())
            .now_or_never();
        assert!(matches!(
            stale,
            Some(Err(SendError::ClientNotFound(CLIENT)))
        ));
        assert!(
            bus.0.drain().is_empty(),
            "a stale reply must not reach the reconnected client"
        );

        let current = bus.client_instance(CLIENT);
        let delivered = bus
            .send_to_client_instance(CLIENT, current, reply())
            .now_or_never();
        assert!(matches!(delivered, Some(Ok(()))));
        assert_eq!(bus.0.drain().len(), 1);
    }
}
