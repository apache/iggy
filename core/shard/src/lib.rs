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

pub mod config;
pub mod coordinator;
mod router;
pub mod shards_table;

pub use config::CoordinatorConfig;

use consensus::{
    LocalPipeline, MetadataHandle, MuxPlane, PartitionsHandle, Pipeline, Plane, PlaneKind,
    Sequencer, VsrAction, VsrConsensus,
};
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{
    Command2, CommitHeader, DoViewChangeHeader, GenericHeader, Message, MessageBag, PrepareHeader,
    PrepareOkHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
};
use iggy_common::variadic;
use iggy_common::{PartitionStats, sharding::IggyNamespace};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::replica_listener::MessageHandler;
use metadata::IggyMetadata;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;
use partitions::{IggyPartition, IggyPartitions};
use shards_table::ShardsTable;
use std::rc::Rc;
use std::sync::Arc;

pub type ShardPlane<B, J, S, M> =
    MuxPlane<variadic!(IggyMetadata<VsrConsensus<B>, J, S, M>, IggyPartitions<B>)>;

pub struct ShardIdentity {
    pub id: u16,
    pub name: String,
}

impl ShardIdentity {
    #[must_use]
    pub const fn new(id: u16, name: String) -> Self {
        Self { id, name }
    }
}

pub struct PartitionConsensusConfig<B>
where
    B: MessageBus,
{
    pub cluster_id: u128,
    pub replica_count: u8,
    pub bus: B,
}

impl<B> PartitionConsensusConfig<B>
where
    B: MessageBus,
{
    #[must_use]
    pub const fn new(cluster_id: u128, replica_count: u8, bus: B) -> Self {
        Self {
            cluster_id,
            replica_count,
            bus,
        }
    }
}

/// Bounded mpsc channel sender (blocking send).
pub type Sender<T> = crossfire::MTx<crossfire::mpsc::Array<T>>;

/// Bounded mpsc channel receiver (async recv).
pub type Receiver<T> = crossfire::AsyncRx<crossfire::mpsc::Array<T>>;

/// Create a bounded mpsc channel with a blocking sender and async receiver.
#[must_use]
pub fn channel<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    crossfire::mpsc::bounded_blocking_async(capacity)
}

/// Create a bounded inter-shard channel whose sender is tagged with the
/// owning shard.
///
/// Bootstrap uses this to build the per-shard sender `Vec` such that
/// `vec[i]` necessarily reaches shard `i`.
#[must_use]
pub fn shard_channel<R: Send + 'static>(
    owner_shard: u16,
    capacity: usize,
) -> (TaggedSender<R>, Receiver<ShardFrame<R>>) {
    let (tx, rx) = channel::<ShardFrame<R>>(capacity);
    (TaggedSender::new(owner_shard, tx), rx)
}

/// A [`Sender`] annotated with the id of the shard whose paired receiver it
/// feeds.
///
/// Inter-shard routing indexes `senders[i]` with `i == target_shard`. The
/// plain `Sender` form has no way to verify that invariant at runtime, so a
/// permuted `Vec<Sender<_>>` would silently misroute every setup, mapping,
/// and forward frame. Construct senders through [`shard_channel`] (or
/// [`TaggedSender::new`]) at the channel-creation site; the coordinator and
/// [`IggyShard`] ctors then assert `senders[i].shard_id() == i`, turning the
/// ordering invariant from a doc comment into a ctor-checked type property.
pub struct TaggedSender<R: Send + 'static = ()> {
    shard_id: u16,
    inner: Sender<ShardFrame<R>>,
}

impl<R: Send + 'static> TaggedSender<R> {
    /// Wrap an already-constructed sender with the id of the shard whose
    /// paired receiver drains it. Prefer [`shard_channel`] unless an
    /// existing sender is being re-tagged (e.g., tests that build senders
    /// manually and know the ordering is correct).
    #[must_use]
    pub const fn new(shard_id: u16, inner: Sender<ShardFrame<R>>) -> Self {
        Self { shard_id, inner }
    }

    #[must_use]
    pub const fn shard_id(&self) -> u16 {
        self.shard_id
    }

    #[must_use]
    pub const fn sender(&self) -> &Sender<ShardFrame<R>> {
        &self.inner
    }
}

impl<R: Send + 'static> std::ops::Deref for TaggedSender<R> {
    type Target = Sender<ShardFrame<R>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Assert the canonical ordering `senders[i].shard_id() == i`. Called from
/// every ctor that accepts a pre-built `Vec<TaggedSender<_>>`.
///
/// # Panics
///
/// Panics if any sender in `senders` carries a `shard_id` that does not
/// match its index. Bootstrap programming error.
fn assert_sender_ordering<R: Send + 'static>(senders: &[TaggedSender<R>]) {
    for (idx, sender) in senders.iter().enumerate() {
        let expected = u16::try_from(idx).expect("shard count must fit in u16");
        assert_eq!(
            sender.shard_id(),
            expected,
            "senders[{idx}] carries shard_id {}; inter-shard vec must be in canonical order",
            sender.shard_id(),
        );
    }
}

/// Payload carried by a [`ShardFrame`].
pub enum ShardFramePayload {
    /// A consensus protocol message routed between shards.
    Consensus(Message<GenericHeader>),
    /// Shard 0 distributes an inbound replica TCP connection fd to the owning
    /// shard. The receiving shard wraps the fd in a `TcpStream` and spawns
    /// writer + reader tasks on its own compio runtime. The `fd` is an
    /// owning [`DupedFd`] so that a frame dropped unprocessed (shutdown,
    /// pump drain abort, router panic before `install_*_fd`) closes the
    /// dup instead of leaking it.
    ReplicaConnectionSetup { fd: DupedFd, replica_id: u8 },
    /// Shard 0 distributes an inbound SDK client TCP connection fd to the
    /// owning shard. The receiving shard wraps the fd and installs client
    /// reader / writer tasks locally. The owning shard is encoded in the top
    /// 16 bits of `client_id`.
    ClientConnectionSetup { fd: DupedFd, client_id: u128 },
    /// Shard 0 broadcasts the owner for a replica to every shard so each
    /// bus' `send_to_replica` slow path can route through the correct owner.
    ReplicaMappingUpdate { replica_id: u8, owning_shard: u16 },
    /// Shard 0 broadcasts that a replica mapping should be forgotten (e.g.
    /// after a connection loss and before the next allocate).
    ReplicaMappingClear { replica_id: u8 },
    /// A non-owning shard forwards a replica send to the owning shard's
    /// local bus; the owning shard then takes the fast path.
    ForwardReplicaSend {
        replica_id: u8,
        msg: Frozen<MESSAGE_ALIGN>,
    },
    /// A shard that doesn't hold the client's TCP connection forwards a
    /// client send to the owning shard (top 16 bits of `client_id`).
    ForwardClientSend {
        client_id: u128,
        msg: Frozen<MESSAGE_ALIGN>,
    },
    /// Owning shard notifies shard 0 that a replica connection died.
    /// Shard 0 clears the mapping cluster-wide and drives a reconnect.
    ConnectionLost { replica_id: u8 },
}

/// Envelope for inter-shard channel messages.
///
/// Wraps a [`ShardFramePayload`] together with an optional one-shot response
/// channel.  Fire-and-forget dispatches leave `response_sender` as `None`;
/// request-response dispatches provide a sender that the message pump will
/// notify once the message has been processed.
///
/// The response type `R` is generic so that higher layers (e.g. HTTP handlers)
/// can carry a response enum while the consensus layer can default to `()`.
pub struct ShardFrame<R: Send + 'static = ()> {
    pub payload: ShardFramePayload,
    pub response_sender: Option<Sender<R>>,
}

impl<R: Send + 'static> ShardFrame<R> {
    /// Create a fire-and-forget consensus message frame.
    #[must_use]
    pub const fn fire_and_forget(message: Message<GenericHeader>) -> Self {
        Self {
            payload: ShardFramePayload::Consensus(message),
            response_sender: None,
        }
    }

    /// Create a request-response consensus message frame. Returns the frame
    /// and a receiver that the caller can await for completion notification.
    #[must_use]
    pub fn with_response(message: Message<GenericHeader>) -> (Self, Receiver<R>) {
        let (tx, rx) = channel(1);
        (
            Self {
                payload: ShardFramePayload::Consensus(message),
                response_sender: Some(tx),
            },
            rx,
        )
    }

    /// Create a fire-and-forget lifecycle frame (connection setup, loss).
    #[must_use]
    pub const fn lifecycle(payload: ShardFramePayload) -> Self {
        Self {
            payload,
            response_sender: None,
        }
    }
}

/// Broadcast a `ReplicaMappingClear` to every shard (including sender-self).
///
/// Used by shard 0's `ConnectionLost` handler and by
/// [`coordinator::ShardZeroCoordinator::broadcast_mapping_clear`] so both
/// paths go through the same try-send-and-log logic. `try_send` failures
/// (inbox full or disconnected) are logged at debug: the next mapping
/// broadcast or reconnect sweep will reconcile.
pub(crate) fn broadcast_mapping_clear<R: Send + 'static>(
    senders: &[TaggedSender<R>],
    replica_id: u8,
) {
    for sender in senders {
        let clear = ShardFramePayload::ReplicaMappingClear { replica_id };
        if let Err(e) = sender.try_send(ShardFrame::lifecycle(clear)) {
            tracing::debug!(
                shard_id = sender.shard_id(),
                replica_id,
                "mapping clear try_send failed: {e:?}"
            );
        }
    }
}

pub struct IggyShard<B, MJ, S, M, T = (), R: Send + 'static = ()>
where
    B: MessageBus,
{
    pub id: u16,
    pub name: String,
    pub plane: ShardPlane<B, MJ, S, M>,

    /// Handle to the local bus. Retained alongside the bus owned by every
    /// consensus plane so the router can reach the `ConnectionInstaller` /
    /// mapping surface without going through consensus.
    pub bus: B,

    /// Callback attached to every delegated replica connection installed
    /// on this shard. The bus' reader task invokes this for each inbound
    /// consensus message; the callback is typically `|_, msg| shard.dispatch(msg)`.
    on_replica_message: MessageHandler,

    /// Callback attached to every delegated client connection installed on
    /// this shard. Invoked for each inbound `Request` frame.
    on_client_request: RequestHandler,

    /// Channel senders to every shard, indexed by shard id.
    /// Includes a sender to self so that local routing goes through the
    /// same channel path as remote routing.
    ///
    /// [`assert_sender_ordering`] is invoked in the ctor so `senders[i]`
    /// is guaranteed to feed the shard whose `id == i`. Call sites can
    /// therefore index by `target_shard` without re-checking.
    senders: Vec<TaggedSender<R>>,

    /// Receiver end of this shard's inbox.  Peer shards (and self) send
    /// messages here via the corresponding sender.
    inbox: Receiver<ShardFrame<R>>,

    /// Partition namespace -> owning shard lookup.
    shards_table: T,

    partition_consensus: PartitionConsensusConfig<B>,

    /// Shard 0 coordinator, set at bootstrap on shard 0 only. The router's
    /// `ConnectionLost` handler calls [`coordinator::ShardZeroCoordinator::forget_mapping`]
    /// so the periodic refresh task stops re-broadcasting the mapping of
    /// a replica that has disconnected. `None` on non-zero shards and in
    /// single-shard tests that bypass the coordinator.
    coordinator: Option<Rc<crate::coordinator::ShardZeroCoordinator<R>>>,
}

impl<B, MJ, S, M, T, R: Send + 'static> IggyShard<B, MJ, S, M, T, R>
where
    B: MessageBus,
    T: ShardsTable,
{
    /// Create a new shard with channel links and a shards table.
    ///
    /// * `bus` - shard-local bus handle (kept alongside the buses owned
    ///   by the consensus planes so the router can reach installer /
    ///   mapping operations directly).
    /// * `senders` - one [`TaggedSender`] per shard. The ctor asserts
    ///   `senders[i].shard_id() == i`; use [`shard_channel`] at
    ///   construction time so every sender carries the id of the shard
    ///   whose receiver drains it.
    /// * `inbox` - the receiver that this shard drains in its message pump.
    /// * `shards_table` - namespace -> shard routing table.
    ///
    /// # Panics
    ///
    /// Panics if `senders` is not in canonical order (any
    /// `senders[i].shard_id() != i`). That is a bootstrap programming
    /// error; the resulting permutation would silently misroute every
    /// inter-shard frame.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ShardIdentity,
        bus: B,
        on_replica_message: MessageHandler,
        on_client_request: RequestHandler,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        senders: Vec<TaggedSender<R>>,
        inbox: Receiver<ShardFrame<R>>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
    ) -> Self {
        assert_sender_ordering(&senders);
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        Self {
            id,
            name,
            plane,
            bus,
            on_replica_message,
            on_client_request,
            senders,
            inbox,
            shards_table,
            partition_consensus,
            coordinator: None,
        }
    }

    /// Attach a shard-0 coordinator. Bootstrap calls this on the shard 0
    /// `IggyShard` only; other shards keep `coordinator = None`. The
    /// router's `ConnectionLost` handler will call
    /// [`coordinator::ShardZeroCoordinator::forget_mapping`] via this
    /// reference so the periodic refresh stops re-broadcasting dead
    /// replica mappings.
    pub fn set_coordinator(&mut self, coord: Rc<crate::coordinator::ShardZeroCoordinator<R>>) {
        self.coordinator = Some(coord);
    }

    /// Create a shard without inter-shard channels or delegated connections.
    ///
    /// Useful for the simulator where inbound messages are delivered
    /// directly via [`on_message`](Self::on_message) instead of the TCP /
    /// fd-transfer path. Installs no-op connection handlers because the
    /// simulator never receives a `ReplicaConnectionSetup` frame.
    #[must_use]
    pub fn without_inbox(
        identity: ShardIdentity,
        bus: B,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
    ) -> Self {
        // TODO: previously we used unbounded channel with flume,
        // but this is not possible with crossfire without mangling types due to Flavor trait in crossfire.
        // This needs to be revisited in the future.
        let (_tx, inbox) = channel(1);
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        Self {
            id,
            name,
            bus,
            on_replica_message: std::rc::Rc::new(|_, _| {}),
            on_client_request: std::rc::Rc::new(|_, _| {}),
            plane,
            coordinator: None,
            senders: Vec::new(),
            inbox,
            shards_table,
            partition_consensus,
        }
    }

    #[must_use]
    pub const fn shards_table(&self) -> &T {
        &self.shards_table
    }
}

/// Local message processing — these methods handle messages that have been
/// routed to this shard via the message pump.
impl<B, MJ, S, M, T, R: Send + 'static> IggyShard<B, MJ, S, M, T, R>
where
    B: MessageBus,
{
    /// Dispatch an incoming network message to the appropriate consensus plane.
    ///
    /// Routes requests, replication messages, and acks to either the metadata
    /// plane or the partitions plane based on `PlaneIdentity::is_applicable`.
    #[allow(clippy::future_not_send)]
    pub async fn on_message(&self, message: Message<GenericHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        match MessageBag::try_from(message) {
            Ok(MessageBag::Request(request)) => self.on_request(request).await,
            Ok(MessageBag::Prepare(prepare)) => self.on_replicate(prepare).await,
            Ok(MessageBag::PrepareOk(prepare_ok)) => self.on_ack(prepare_ok).await,
            Ok(MessageBag::StartViewChange(msg)) => self.on_start_view_change(msg).await,
            Ok(MessageBag::DoViewChange(msg)) => self.on_do_view_change(msg).await,
            Ok(MessageBag::StartView(msg)) => self.on_start_view(msg).await,
            Ok(MessageBag::Commit(ref msg)) => self.on_commit(msg).await,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
            }
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_request(&self, request: Message<RequestHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_request(request).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_replicate(&self, prepare: Message<PrepareHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_replicate(prepare).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&self, prepare_ok: Message<PrepareOkHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_ack(prepare_ok).await;
    }

    /// Drain and dispatch loopback messages for each consensus plane.
    ///
    /// Each plane's loopback is dispatched directly to that plane's `on_ack`,
    /// avoiding a flat merge that would require re-routing through `on_message`.
    ///
    /// Invariant: planes do not produce loopback messages for each other.
    /// `on_ack` commits and applies but never calls `push_loopback`, so
    /// draining metadata before partitions is order-independent.
    ///
    /// # Panics
    /// Panics if a loopback message is not a valid `PrepareOk` message.
    #[allow(clippy::future_not_send)]
    pub async fn process_loopback(&self, buf: &mut Vec<Message<GenericHeader>>) -> usize
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        debug_assert!(buf.is_empty(), "buf must be empty on entry");

        let mut total = 0;
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus {
            consensus.drain_loopback_into(buf);
            let count = buf.len();
            total += count;
            for msg in buf.drain(..) {
                let typed: Message<PrepareOkHeader> = msg
                    .try_into_typed()
                    .expect("loopback queue must only contain PrepareOk messages");
                planes.0.on_ack(typed).await;
            }
        }

        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let partition = planes
                .1
                .0
                .get_by_ns(&namespace)
                .expect("partition namespace must resolve during loopback drain");
            partition.consensus().drain_loopback_into(buf);
        }
        let count = buf.len();
        total += count;
        for msg in buf.drain(..) {
            let typed: Message<PrepareOkHeader> = msg
                .try_into_typed()
                .expect("loopback queue must only contain PrepareOk messages");
            planes.1.0.on_ack(typed).await;
        }

        total
    }

    /// Initializes a partition and its dedicated consensus instance on this shard.
    ///
    /// # Panics
    /// Panics if the shard id does not fit in `u8`, which is currently required
    /// by the partition consensus replica id.
    pub fn init_partition(&mut self, namespace: IggyNamespace)
    where
        B: MessageBus + Clone,
    {
        let partitions = self.plane.partitions_mut();
        if partitions.contains(&namespace) {
            return;
        }

        let replica_id =
            u8::try_from(self.id).expect("shard id must fit in u8 for partition consensus");
        let consensus = VsrConsensus::new(
            self.partition_consensus.cluster_id,
            replica_id,
            self.partition_consensus.replica_count,
            namespace.inner(),
            self.partition_consensus.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();

        let stats = Arc::new(PartitionStats::default());
        let partition = IggyPartition::with_in_memory_storage(
            stats,
            consensus,
            partitions.config().segment_size,
            partitions.config().enforce_fsync,
        );
        partitions.insert(namespace, partition);
    }

    /// Handle incoming view-change/control message. Metadata use metadata
    /// consensus. Partitions loop all partitions, use partition consensus.
    #[allow(clippy::future_not_send)]
    async fn on_start_view_change(&self, msg: Message<StartViewChangeHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_start_view_change(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            return;
        }

        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_start_view_change(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping StartViewChange: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_do_view_change(&self, msg: Message<DoViewChangeHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StreamsFrontend
            + StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
            {
                planes.0.commit_journal().await;
            }
            return;
        }

        let config = planes.1.0.config().clone();
        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_do_view_change(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
            {
                partition.commit_journal(&config).await;
            }
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping DoViewChange: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_start_view(&self, msg: Message<StartViewHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_start_view(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            return;
        }

        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_start_view(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping StartView: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_commit(&self, msg: &Message<CommitHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StreamsFrontend
            + StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            if consensus.handle_commit(&header) {
                planes.0.commit_journal().await;
            }
            return;
        }

        let config = planes.1.0.config().clone();
        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            if consensus.handle_commit(&header) {
                partition.commit_journal(&config).await;
            }
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping Commit: namespace matches neither metadata nor partition consensus"
        );
    }

    /// Tick partition consensuses. Loop partitions. No partitions-plane journal.
    #[allow(clippy::future_not_send)]
    pub async fn tick_partitions(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();

        for namespace in namespaces {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                continue;
            };

            let consensus = partition.consensus();
            let current_op = consensus.sequencer().current_sequence();
            let current_commit = consensus.commit_min();
            let actions = consensus.tick(PlaneKind::Partitions, current_op, current_commit);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn tick_metadata(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let metadata = self.plane.metadata();
        let Some(ref consensus) = metadata.consensus else {
            return;
        };

        let current_op = consensus.sequencer().current_sequence();
        let current_commit = consensus.commit_min();
        let actions = consensus.tick(PlaneKind::Metadata, current_op, current_commit);

        dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &actions).await;
    }
}

/// Dispatch a list of `VsrAction`s by constructing the appropriate
/// protocol messages and sending them via the consensus message bus.
#[allow(
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn dispatch_vsr_actions<B, P, J>(
    consensus: &VsrConsensus<B, P>,
    journal: Option<&J>,
    actions: &[VsrAction],
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
    J: JournalHandle,
    <J as JournalHandle>::Target: Journal<
            <J as JournalHandle>::Storage,
            Entry = Message<PrepareHeader>,
            Header = PrepareHeader,
        >,
{
    use std::mem::size_of;

    let bus = consensus.message_bus();
    let self_id = consensus.replica();
    let cluster = consensus.cluster();
    let replica_count = consensus.replica_count();

    let send = |target: u8, msg: Frozen<MESSAGE_ALIGN>| async move {
        if let Err(e) = bus.send_to_replica(target, msg).await {
            tracing::debug!(replica = self_id, target, "bus send failed: {e}");
        }
    };

    let broadcast = async |frozen: Frozen<MESSAGE_ALIGN>| {
        // Freeze once at the primary; each target just bumps the atomic
        // refcount on the underlying ControlBlock.
        for target in 0..replica_count {
            if target != self_id {
                send(target, frozen.clone()).await;
            }
        }
    };

    for action in actions {
        match action {
            VsrAction::SendStartViewChange { view, namespace } => {
                let msg = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                    .transmute_header(|_, h: &mut StartViewChangeHeader| {
                        h.command = Command2::StartViewChange;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.namespace = *namespace;
                        h.size = size_of::<StartViewChangeHeader>() as u32;
                    });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendDoViewChange {
                view,
                target,
                log_view,
                op,
                commit,
                namespace,
            } => {
                let msg = Message::<DoViewChangeHeader>::new(size_of::<DoViewChangeHeader>())
                    .transmute_header(|_, h: &mut DoViewChangeHeader| {
                        h.command = Command2::DoViewChange;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.log_view = *log_view;
                        h.op = *op;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.size = size_of::<DoViewChangeHeader>() as u32;
                    });
                send(*target, msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendStartView {
                view,
                op,
                commit,
                namespace,
            } => {
                let msg = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
                    .transmute_header(|_, h: &mut StartViewHeader| {
                        h.command = Command2::StartView;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.op = *op;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.size = size_of::<StartViewHeader>() as u32;
                    });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendPrepareOk {
                view,
                from_op,
                to_op,
                target,
                namespace,
            } => {
                let Some(journal) = journal else {
                    continue;
                };
                for op in *from_op..=*to_op {
                    let Some(prepare_header) = journal.handle().header(op as usize) else {
                        continue;
                    };
                    let prepare_header = *prepare_header;
                    let msg = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                        .transmute_header(|_, h: &mut PrepareOkHeader| {
                            h.command = Command2::PrepareOk;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.op = op;
                            h.commit = consensus.commit_max();
                            h.timestamp = prepare_header.timestamp;
                            h.parent = prepare_header.parent;
                            h.prepare_checksum = prepare_header.checksum;
                            h.request = prepare_header.request;
                            h.operation = prepare_header.operation;
                            h.namespace = *namespace;
                            h.size = size_of::<PrepareOkHeader>() as u32;
                        });
                    send(*target, msg.into_generic().into_frozen()).await;
                }
            }
            VsrAction::RetransmitPrepares { targets } => {
                let Some(journal) = journal else {
                    continue;
                };
                for (header, replicas) in targets {
                    let Some(prepare) = journal.handle().entry(header).await else {
                        continue;
                    };
                    // Freeze the retransmit payload once; clone per target.
                    let frozen = prepare.into_generic().into_frozen();
                    for replica in replicas {
                        send(*replica, frozen.clone()).await;
                    }
                }
            }
            VsrAction::RebuildPipeline { from_op, to_op } => {
                let Some(journal) = journal else {
                    continue;
                };
                // Collect headers before borrowing the pipeline to avoid
                // holding borrow_mut() across journal reads.
                let mut gap_at = None;
                let entries: Vec<_> = (*from_op..=*to_op)
                    .map_while(|op| {
                        let Some(header) = journal.handle().header(op as usize) else {
                            gap_at = Some(op);
                            return None;
                        };
                        let mut entry = consensus::PipelineEntry::new(*header);
                        entry.add_ack(self_id);
                        Some(entry)
                    })
                    .collect();
                if let Some(missing_op) = gap_at {
                    // Journal repair is not yet implemented.Truncate the sequencer
                    // to the last op we could rebuild so the next client
                    // prepare chains correctly. Ops above the
                    // gap are lost until journal repair is added.
                    let rebuilt_up_to = missing_op.saturating_sub(1);
                    tracing::warn!(
                        replica = self_id,
                        missing_op,
                        range_start = from_op,
                        range_end = to_op,
                        rebuilt = entries.len(),
                        "RebuildPipeline: journal gap at op {missing_op}, \
                         truncating sequencer from {to_op} to {rebuilt_up_to} \
                         ({}/{} ops rebuilt)",
                        entries.len(),
                        to_op - from_op + 1,
                    );
                    consensus.sequencer().set_sequence(rebuilt_up_to);
                }
                let mut pipeline = consensus.pipeline().borrow_mut();
                for entry in entries {
                    pipeline.push(entry);
                }
            }
            // Handled by the caller (shard view change handlers) since it
            // requires access to the plane's commit_journal method.
            VsrAction::CommitJournal => {}
            VsrAction::SendCommit {
                view,
                commit,
                namespace,
                timestamp_monotonic,
            } => {
                let msg = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
                    |_, h: &mut CommitHeader| {
                        h.command = Command2::Commit;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.timestamp_monotonic = *timestamp_monotonic;
                        h.size = size_of::<CommitHeader>() as u32;
                    },
                );
                broadcast(msg.into_generic().into_frozen()).await;
            }
        }
    }
}

#[allow(
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn dispatch_partition_journal_actions<B, P>(
    consensus: &VsrConsensus<B, P>,
    partition: &IggyPartition<B>,
    actions: &[VsrAction],
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    use std::mem::size_of;

    let bus = consensus.message_bus();
    let self_id = consensus.replica();
    let cluster = consensus.cluster();
    let journal = &partition.log.journal().inner;

    let send = |target: u8, msg: Frozen<MESSAGE_ALIGN>| async move {
        if let Err(e) = bus.send_to_replica(target, msg).await {
            tracing::debug!(replica = self_id, target, "bus send failed: {e}");
        }
    };

    for action in actions {
        match action {
            VsrAction::SendPrepareOk {
                view,
                from_op,
                to_op,
                target,
                namespace,
            } => {
                for op in *from_op..=*to_op {
                    let Some(prepare_header) = journal.header_by_op(op) else {
                        continue;
                    };
                    let msg = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                        .transmute_header(|_, h: &mut PrepareOkHeader| {
                            h.command = Command2::PrepareOk;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.op = op;
                            h.commit = consensus.commit_max();
                            h.timestamp = prepare_header.timestamp;
                            h.parent = prepare_header.parent;
                            h.prepare_checksum = prepare_header.checksum;
                            h.request = prepare_header.request;
                            h.operation = prepare_header.operation;
                            h.namespace = *namespace;
                            h.size = size_of::<PrepareOkHeader>() as u32;
                        });
                    send(*target, msg.into_generic().into_frozen()).await;
                }
            }
            VsrAction::RetransmitPrepares { targets } => {
                // DURABILITY CAVEAT: the only `Storage` impl on
                // `PartitionJournal` right now is the in-memory
                // `PartitionJournalMemStorage`. After a process restart
                // the journal is empty and every `journal.entry` below
                // returns `None`, so retransmit silently drops the
                // request and peers stall until a view change. The bus
                // and consensus plumbing is correct; only the storage
                // needs to become durable before cluster workloads go to
                // production. Server boot emits a loud warning to the
                // operator (see `main.rs`).
                for (header, replicas) in targets {
                    let Some(prepare) = journal.entry(header).await else {
                        continue;
                    };
                    // The partition journal already stores the wire-format
                    // `Frozen<4096>` (PrepareHeader followed by payload),
                    // so `send_to_replica` can take it directly and `clone`
                    // is a refcount bump. Matches the metadata-plane path
                    // above and avoids both the per-target 4 KiB memcpy
                    // and the prior `.expect` that would panic the shard
                    // on a corrupted journal entry.
                    for replica in replicas {
                        send(*replica, prepare.clone()).await;
                    }
                }
            }
            VsrAction::RebuildPipeline { from_op, to_op } => {
                let mut gap_at = None;
                let entries: Vec<_> = (*from_op..=*to_op)
                    .map_while(|op| {
                        let Some(header) = journal.header_by_op(op) else {
                            gap_at = Some(op);
                            return None;
                        };
                        let mut entry = consensus::PipelineEntry::new(header);
                        entry.add_ack(self_id);
                        Some(entry)
                    })
                    .collect();
                if let Some(missing_op) = gap_at {
                    let rebuilt_up_to = missing_op.saturating_sub(1);
                    tracing::warn!(
                        replica = self_id,
                        missing_op,
                        range_start = from_op,
                        range_end = to_op,
                        rebuilt = entries.len(),
                        "RebuildPipeline: journal gap at op {missing_op}, \
                         truncating sequencer from {to_op} to {rebuilt_up_to} \
                         ({}/{} ops rebuilt)",
                        entries.len(),
                        to_op - from_op + 1,
                    );
                    consensus.sequencer().set_sequence(rebuilt_up_to);
                }
                let mut pipeline = consensus.pipeline().borrow_mut();
                for entry in entries {
                    pipeline.push(entry);
                }
            }
            _ => {}
        }
    }
}
