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

use crate::MuxStateMachine;
use crate::stm::authz::gated_apply;
use crate::stm::consumer_group::CompleteConsumerGroupRevocationRequest;
use crate::stm::snapshot::{FillSnapshot, MetadataSnapshot, Snapshot, SnapshotError};
use crate::stm::stream::{Streams, TruncatePartitionRequest};
use crate::stm::user::{DeletePersonalAccessTokenRequest, Users};
use crate::stm::{ConsensusGroupAllocator, StateMachine};
use consensus::{
    CLIENTS_TABLE_MAX, Canceled, ClientTable, CommitLogEvent, Consensus, EvictionContext, Pipeline,
    PipelineEntry, Plane, PlaneIdentity, PlaneKind, PreflightOutcome, Project, ReplicaLogContext,
    RequestLogEvent, Sequencer, SimEventKind, VsrConsensus, ack_preflight, ack_quorum_reached,
    apply_preflight_consensus_plane, build_eviction_message, build_reply_message,
    build_reply_message_with, build_result_rejection_reply, emit_sim_event,
    fence_old_prepare_by_commit, is_caught_up_primary,
    panic_if_hash_chain_would_break_in_same_view, peek_committable_head, pipeline_prepare_common,
    register_preflight,
    replicate_preflight, replicate_to_next_in_chain, request_preflight, send_eviction_to_client,
    send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::CreatePartitionsRequest as WireCreatePartitionsRequest;
use iggy_binary_protocol::requests::partitions::CreatePartitionsWithAssignmentsRequest as PersistedCreatePartitionsRequest;
use iggy_binary_protocol::requests::topics::CreateTopicRequest as WireCreateTopicRequest;
use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest as PersistedCreateTopicRequest;
use iggy_binary_protocol::requests::topics::UpdateTopicRequest as WireUpdateTopicRequest;
use iggy_binary_protocol::{
    Command2, ConsensusHeader, EvictionReason, GenericHeader, Operation, PrepareHeader,
    PrepareOkHeader, ReplyHeader, RequestHeader, WireDecode, WireEncode, WireName,
};
use iggy_common::IggyError;
use iggy_common::UserId;
use iggy_common::variadic;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use server_common::Message;
use std::cell::{Cell, RefCell};
use std::mem::size_of;
use std::path::Path;
use std::rc::Rc;
use tracing::{debug, error, warn};

fn freeze_client_reply(
    message: Message<GenericHeader>,
) -> server_common::iobuf::Frozen<{ server_common::MESSAGE_ALIGN }> {
    message.into_frozen()
}

pub trait StreamsFrontend {
    #[must_use]
    fn users(&self) -> &Users;
    #[must_use]
    fn streams(&self) -> &Streams;
}

impl StreamsFrontend for MuxStateMachine<variadic!(Users, Streams)> {
    fn users(&self) -> &Users {
        &self.inner().0
    }

    fn streams(&self) -> &Streams {
        &self.inner().1.0
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct IggySnapshot {
    snapshot: MetadataSnapshot,
}

#[allow(unused)]
impl IggySnapshot {
    #[must_use]
    pub const fn new(sequence_number: u64) -> Self {
        Self {
            snapshot: MetadataSnapshot::new(sequence_number),
        }
    }

    #[must_use]
    pub const fn snapshot(&self) -> &MetadataSnapshot {
        &self.snapshot
    }

    /// Persist the snapshot to disk.
    ///
    /// # Errors
    /// Returns `SnapshotError` if serialization or I/O fails.
    pub fn persist(&self, path: &Path) -> Result<(), SnapshotError> {
        use crate::stm::snapshot::PersistStage;
        use std::fs;
        use std::io::Write;

        let encoded = self.encode()?;

        let tmp_path = path.with_extension("bin.tmp");

        let mut file = fs::File::create(&tmp_path).map_err(|e| SnapshotError::Persist {
            stage: PersistStage::Write,
            source: e,
        })?;
        file.write_all(&encoded)
            .map_err(|e| SnapshotError::Persist {
                stage: PersistStage::Write,
                source: e,
            })?;
        file.sync_all().map_err(|e| SnapshotError::Persist {
            stage: PersistStage::Sync,
            source: e,
        })?;
        drop(file);

        fs::rename(&tmp_path, path).map_err(|e| SnapshotError::Persist {
            stage: PersistStage::Rename,
            source: e,
        })?;

        // Fsync the parent directory to ensure the rename is durable.
        if let Some(parent) = path.parent() {
            let dir = fs::File::open(parent).map_err(|e| SnapshotError::Persist {
                stage: PersistStage::DirSync,
                source: e,
            })?;
            dir.sync_all().map_err(|e| SnapshotError::Persist {
                stage: PersistStage::DirSync,
                source: e,
            })?;
        }

        Ok(())
    }

    /// Load a snapshot from disk.
    ///
    /// # Errors
    /// Returns `SnapshotError` if the file cannot be read or deserialization fails.
    pub fn load(path: &Path) -> Result<Self, SnapshotError> {
        let data = std::fs::read(path)?;

        // TODO: when checksum is added we need to check
        // if data.len() is atleast the size of checksum

        Self::decode(data.as_slice())
    }
}

impl Snapshot for IggySnapshot {
    type Error = SnapshotError;
    type SequenceNumber = u64;
    type Timestamp = u64;
    type Inner = MetadataSnapshot;

    fn create<T>(stm: &T, sequence_number: u64, created_at: u64) -> Result<Self, SnapshotError>
    where
        T: FillSnapshot<MetadataSnapshot>,
    {
        let mut snapshot = MetadataSnapshot::new(sequence_number);
        snapshot.created_at = created_at;

        stm.fill_snapshot(&mut snapshot)?;

        Ok(Self { snapshot })
    }

    fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        self.snapshot.encode()
    }

    fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let snapshot = MetadataSnapshot::decode(bytes)?;
        Ok(Self { snapshot })
    }

    fn sequence_number(&self) -> u64 {
        self.snapshot.sequence_number
    }

    fn created_at(&self) -> u64 {
        self.snapshot.created_at
    }
}

/// Coordinates snapshot creation, persistence, and WAL compaction.
///
/// Owns the data directory path and the snapshot creation function.
pub struct SnapshotCoordinator<M> {
    data_dir: std::path::PathBuf,
    create_snapshot: fn(&M, u64, u64) -> Result<IggySnapshot, SnapshotError>,
}

impl<M> SnapshotCoordinator<M> {
    /// Number of remaining journal slots at which a checkpoint is forced.
    // TODO: tune this margin size
    const CHECKPOINT_MARGIN: usize = 64;

    #[must_use]
    pub fn new(
        data_dir: std::path::PathBuf,
        create_snapshot: fn(&M, u64, u64) -> Result<IggySnapshot, SnapshotError>,
    ) -> Self {
        Self {
            data_dir,
            create_snapshot,
        }
    }

    /// Create a snapshot, persist it, and drain snapshotted entries from the
    /// journal to reclaim WAL space.
    ///
    /// # Errors
    /// Returns `SnapshotError` if snapshotting, persistence, or drain fails.
    #[allow(clippy::future_not_send)]
    pub async fn checkpoint<J>(
        &self,
        stm: &M,
        journal: &J,
        last_op: u64,
        created_at: u64,
    ) -> Result<(), SnapshotError>
    where
        J: JournalHandle,
    {
        let snapshot = (self.create_snapshot)(stm, last_op, created_at)?;
        let path = self.data_dir.join(super::METADATA_DIR).join("snapshot.bin");
        snapshot.persist(&path)?;

        let _ = journal
            .handle()
            .drain(0..=last_op)
            .await
            .map_err(SnapshotError::Io)?;

        Ok(())
    }

    /// Force a checkpoint if the journal is running low on capacity.
    ///
    /// Returns `Ok(true)` if a checkpoint was taken, `Ok(false)` if not needed.
    ///
    /// # Errors
    /// Returns `SnapshotError` if the checkpoint fails.
    #[allow(clippy::future_not_send)]
    pub async fn checkpoint_if_needed<J>(
        &self,
        stm: &M,
        journal: &J,
        commit_op: u64,
        created_at: u64,
    ) -> Result<bool, SnapshotError>
    where
        J: JournalHandle,
    {
        let needs_checkpoint = journal
            .handle()
            .remaining_capacity()
            .is_some_and(|c| c <= Self::CHECKPOINT_MARGIN);

        if needs_checkpoint {
            self.checkpoint(stm, journal, commit_op, created_at).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Failures shared by the in-process metadata submit helpers.
///
/// Returned by [`IggyMetadata::submit_register_in_process`],
/// [`IggyMetadata::submit_logout_in_process`],
/// [`IggyMetadata::submit_request_in_process`], and
/// [`IggyMetadata::submit_delete_personal_access_token_in_process`]. Every variant is
/// transient: the caller retries on a later attempt, and the login/register
/// handler wraps them in `LoginRegisterError::Transient` so the SDK
/// read-timeout replays.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataSubmitError {
    /// Not primary / not Normal.
    NotPrimary,
    /// Primary but `commit_min < commit_max` (committed prefix not yet
    /// drained). Dispatching now would race ops inherited from a prior view;
    /// for `Register` that trips `commit_register`'s session-eq assert.
    NotCaughtUp,
    /// Prepare queue full.
    PipelineFull,
    /// In-flight prepare from this client.
    InProgress,
    /// The pending prepare was canceled before commit (a view change reset
    /// the pipeline). The caller retries; the SDK read-timeout replay reaches
    /// the new primary.
    Canceled,
}

impl std::fmt::Display for MetadataSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrimary => f.write_str("not primary in normal status"),
            Self::NotCaughtUp => f.write_str("primary not yet caught up on commit_journal"),
            Self::PipelineFull => f.write_str("metadata prepare queue is full"),
            Self::InProgress => f.write_str("another in-flight prepare from this client"),
            Self::Canceled => f.write_str("view change canceled the pending prepare"),
        }
    }
}

impl std::error::Error for MetadataSubmitError {}

/// Log + surface `None` when a metadata callback runs on a peer shard
/// (whose `consensus` / `journal` slot is `None`). The `Plane` trait
/// callbacks are addressed to the shard 0 owner; if the routing layer
/// ever dispatches one to a peer the only honest answer is "drop and
/// alert" - panicking would crash the bus and mask the routing bug.
fn require_shard_zero<'a, T>(
    slot: Option<&'a T>,
    callback: &'static str,
    field: &'static str,
) -> Option<&'a T> {
    if slot.is_none() {
        error!(
            target: "iggy.metadata.diag",
            plane = "metadata",
            callback,
            field,
            "metadata callback fired on a peer shard (field is None); routing layer \
             must direct metadata traffic to shard 0 - dropping the message"
        );
    }
    slot
}

/// Late-bound callback invoked after every committed op on shard 0's metadata
/// commit path (via `gated_apply`, including a gated no-op).
///
/// Wired by server-ng bootstrap once the metadata bundle has broadcast;
/// receives the committed [`Operation`] so the recipient can filter (the
/// partition reconciliation loop only cares about partition-shaped
/// events). Wrapped in [`RefCell`] for late binding; the per-shard
/// single-thread invariant keeps access safe without [`Sync`].
pub type CommitNotifier = std::rc::Rc<dyn Fn(Operation)>;

pub struct IggyMetadata<C, J, S, M> {
    /// `Some` on shard 0, `None` on other shards. Server-ng bootstrap
    /// holds the invariant: only shard 0 owns the metadata consensus
    /// replica; every other shard reconstructs `mux_stm` from the
    /// `MetadataHandoff::Waiter` factory bundle broadcast by shard 0
    /// (no consensus replica, no journal access).
    pub consensus: Option<C>,
    /// `Some` on shard 0, `None` on other shards. Shard 0 owns the WAL
    /// writer (via `PrepareJournal::open`); non-owning shards never open
    /// the WAL at all. They receive a `MetadataHandoff::Waiter` factory
    /// bundle from shard 0 over the bootstrap broadcast channel and
    /// reconstruct `mux_stm` from the in-memory snapshot it carries (see
    /// `server-ng/src/bootstrap.rs` `await_metadata_bundle` /
    /// `broadcast_metadata_bundle`).
    pub journal: Option<J>,
    /// `Some` on shard 0, `None` on other shards.
    pub snapshot: Option<S>,
    /// State machine - lives on all shards
    pub mux_stm: M,
    pub allocator: ConsensusGroupAllocator,
    /// Snapshot coordinator - present when persistent checkpointing is configured.
    pub coordinator: Option<SnapshotCoordinator<M>>,
    /// Per-client session state (sessions, dedup, eviction). Metadata-only.
    pub client_table: RefCell<ClientTable>,
    /// Late-bound post-commit notifier. Fires once per committed normal op
    /// after `gated_apply` returns (including a gated `Unauthorized` no-op that
    /// never reaches [`crate::stm::StateMachine::update`]) in both
    /// [`Plane::on_ack`] and [`Self::commit_journal`]. `None` until
    /// [`Self::set_commit_notifier`] runs (server-ng bootstrap on shard
    /// 0 sets it; peer shards and tests leave it `None`).
    commit_notifier: RefCell<Option<CommitNotifier>>,
    /// Resolved byte value for `MaxTopicSize::ServerDefault` (`0` on the
    /// wire). Primary admission rewrites the sentinel to this value before
    /// replication so the committed state carries a concrete size and every
    /// replica resolves identically regardless of local config. Set from
    /// server config at bootstrap ([`Self::set_default_max_topic_size`]);
    /// defaults to unlimited, matching the shipped server config.
    default_max_topic_size: Cell<u64>,
    /// Resolved micros value for `IggyExpiry::ServerDefault` (`0` on the wire).
    /// Same admission-time sentinel resolution as [`Self::default_max_topic_size`];
    /// set from server config at bootstrap ([`Self::set_default_message_expiry`]).
    /// Defaults to never-expire, matching the shipped server config.
    default_message_expiry: Cell<u64>,
}

impl<C, J, S, M> IggyMetadata<C, J, S, M>
where
    M: StreamsFrontend + FillSnapshot<MetadataSnapshot>,
{
    /// Create a new `IggyMetadata` instance.
    ///
    /// The `FillSnapshot<MetadataSnapshot>` bound is captured here via a
    /// function pointer so that no downstream caller needs the bound.
    #[must_use]
    pub fn new(
        consensus: Option<C>,
        journal: Option<J>,
        snapshot: Option<S>,
        mux_stm: M,
        data_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let allocator =
            ConsensusGroupAllocator::new(mux_stm.streams().highest_partition_consensus_group_id());
        let coordinator = data_dir.map(|dir| SnapshotCoordinator::new(dir, IggySnapshot::create));
        Self {
            consensus,
            journal,
            snapshot,
            mux_stm,
            allocator,
            coordinator,
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
            commit_notifier: RefCell::new(None),
            default_max_topic_size: Cell::new(u64::MAX),
            default_message_expiry: Cell::new(u64::MAX),
        }
    }
}

impl<C, J, S, M> IggyMetadata<C, J, S, M> {
    /// Install (or replace) the post-commit notifier. Passing `None`
    /// removes any previous one. Server-ng bootstrap calls this on shard 0
    /// only; peer shards never commit metadata locally.
    pub fn set_commit_notifier(&self, notifier: Option<CommitNotifier>) {
        *self.commit_notifier.borrow_mut() = notifier;
    }

    /// Install the resolved byte value used for `MaxTopicSize::ServerDefault`.
    /// Server-ng bootstrap calls this with `system.topic.max_size` on every
    /// shard (responses read it too); only shard 0's copy feeds admission.
    pub fn set_default_max_topic_size(&self, max_topic_size_bytes: u64) {
        self.default_max_topic_size.set(max_topic_size_bytes);
    }

    /// Resolved byte value for `MaxTopicSize::ServerDefault`.
    #[must_use]
    pub const fn default_max_topic_size(&self) -> u64 {
        self.default_max_topic_size.get()
    }

    /// Install the resolved micros value used for `IggyExpiry::ServerDefault`.
    /// Server-ng bootstrap calls this with `system.topic.message_expiry` on every
    /// shard (responses read it too); only shard 0's copy feeds admission.
    pub fn set_default_message_expiry(&self, message_expiry_micros: u64) {
        self.default_message_expiry.set(message_expiry_micros);
    }

    /// Resolved micros value for `IggyExpiry::ServerDefault`.
    #[must_use]
    pub const fn default_message_expiry(&self) -> u64 {
        self.default_message_expiry.get()
    }

    /// Fire post-commit notifier. Clones the `Rc` out under a short
    /// borrow so a re-entrant `set_commit_notifier` from inside the
    /// closure cannot panic on `borrow_mut`.
    fn fire_commit_notifier(&self, operation: Operation) {
        let notifier = self.commit_notifier.borrow().as_ref().map(Rc::clone);
        if let Some(notifier) = notifier {
            notifier(operation);
        }
    }
}

#[allow(clippy::future_not_send)]
impl<B, J, S, M> Plane<VsrConsensus<B>> for IggyMetadata<VsrConsensus<B>, J, S, M>
where
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::Message<RequestHeader>) {
        let Some(consensus) =
            require_shard_zero(self.consensus.as_ref(), "on_request", "consensus")
        else {
            return;
        };
        let client_id = message.header().client;
        let session = message.header().session;
        let request = message.header().request;
        let operation = message.header().operation;

        // Preflight first: dedup, eviction sends, cached-reply replay all
        // must run regardless of pipeline pressure. Wire-path ingress has no
        // home-shard transport context, so resends fall back to the
        // consensus-plane (best-effort by VSR id).
        let dispatch = if operation == Operation::Register {
            register_preflight(consensus, &self.client_table, client_id).await
        } else {
            let outcome =
                request_preflight(consensus, &self.client_table, client_id, session, request);
            apply_preflight_consensus_plane(consensus, outcome, client_id).await
        };
        if !dispatch {
            return;
        }

        emit_sim_event(
            SimEventKind::ClientRequestReceived,
            &RequestLogEvent {
                replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Metadata),
                client_id: message.header().client,
                request_id: message.header().request,
                operation: message.header().operation,
            },
        );

        // Two-queue admission: prepare slot then project+replicate; prepare
        // full + request room then buffer; both full then drop+warn (SDK
        // retries via read-timeout).
        if consensus.pipeline().borrow().is_full() {
            let push_result = consensus
                .pipeline()
                .borrow_mut()
                .push_request(consensus::RequestEntry::new(message));
            if push_result.is_err() {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    client = client_id,
                    request = request,
                    "on_request: prepare and request queues both full, dropping"
                );
            }
            return;
        }

        let prepare = match self.prepare_request(message) {
            Ok(prepare) => prepare,
            Err(error) => {
                // Structurally-invalid request (not client-allowed, undecodable
                // body, or partition-id overflow). Evict instead of dropping: a
                // silent drop leaves the client unable to tell rejection from
                // loss, retrying forever.
                let reason = eviction_reason_for_invalid(operation);
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    error = %error,
                    ?reason,
                    "rejecting invalid metadata request with eviction"
                );
                send_eviction_to_client(consensus, client_id, reason).await;
                return;
            }
        };
        pipeline_prepare_common(consensus, PlaneKind::Metadata, prepare, |prepare| {
            self.on_replicate(prepare)
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let Some(consensus) =
            require_shard_zero(self.consensus.as_ref(), "on_replicate", "consensus")
        else {
            return;
        };
        let Some(journal) = require_shard_zero(self.journal.as_ref(), "on_replicate", "journal")
        else {
            return;
        };

        let header = *message.header();

        let current_op = match replicate_preflight(consensus, &header) {
            Ok(current_op) => current_op,
            Err(reason) => {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    view = consensus.view(),
                    op = header.op,
                    operation = ?header.operation,
                    reason = reason.as_str(),
                    "ignoring prepare during replicate preflight"
                );
                return;
            }
        };

        // Fenced by commit: the whole chain has already committed this op, so
        // nobody needs it again. Drop entirely. (Mirror of the partition
        // plane's split in `IggyPartition::on_replicate`.)
        #[allow(clippy::cast_possible_truncation)]
        if fence_old_prepare_by_commit(consensus, &header) {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                commit = consensus.commit_max(),
                operation = ?header.operation,
                "received old prepare (<= commit), skipping replication"
            );
            return;
        }

        // Durable here but not yet committed, and the primary is retransmitting
        // it: our original PrepareOk was lost (e.g. the primary's inbox
        // overflowed under a client burst). Re-forward the tail down the chain
        // so a downstream replica that missed it recovers, then re-ack ONLY
        // the retransmitted op. The primary's retransmit cycle walks every
        // un-acked op in the window (`retransmit_targets`), so a lost ack for
        // a lower op gets its own retransmit and its own re-ack; re-acking the
        // whole suffix here is O(window^2) PrepareOks per cycle across the
        // backups, which can overflow the primary's inbox -- the very failure
        // this path recovers from. Both downstream and primary are idempotent
        // on a duplicate (replica, op).
        #[allow(clippy::cast_possible_truncation)]
        if journal.handle().header(header.op as usize).is_some() {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                commit = consensus.commit_max(),
                operation = ?header.operation,
                "journal already holds prepare, re-forwarding + re-acking it"
            );
            self.replicate(&message).await;
            self.send_prepare_ok(&header).await;
            return;
        }

        // TODO add assertions for valid state here.

        // TODO handle gap in ops.

        // Verify hash chain integrity BEFORE checkpoint. `checkpoint_if_needed`
        // can drain WAL entries, making previous_header return None.
        if let Some(previous) = journal.handle().previous_header(&header) {
            panic_if_hash_chain_would_break_in_same_view(&previous, &header);
        }

        if !self.checkpoint_if_needed(consensus, journal).await {
            return;
        }

        // Backup: gap check (op == current_op + 1).
        // Primary: sequencer pre-advanced by push_prepare_entry (guards
        // sibling on_request races during journal.append await).
        // TODO: hard assert for backups once message repair lands.
        let is_backup = consensus.is_follower();
        if is_backup {
            if header.op != current_op + 1 {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    op = header.op,
                    expected = current_op + 1,
                    "on_replicate: dropping out-of-order prepare (gap)"
                );
                return;
            }
        } else {
            debug_assert_eq!(
                header.op, current_op,
                "primary: sequencer pre-advance broken"
            );
        }

        // Journal append first; sequencer + checksum after successful append
        // so a failed write doesn't leave state pointing at a phantom entry.
        //
        // Durability BEFORE chain-replicate / PrepareOk: forwarding an
        // un-persisted prepare advertises an op the WAL doesn't hold,
        // violates VSR tail-ahead-of-head, recoverable only via hash-chain
        // fence + view change (burns a view).
        //
        // TODO(hubcio): the primary path violates the invariant in the
        // comment above. `consensus::impls::push_prepare_entry` pre-advances
        // `sequencer.set_sequence(header.op)` and
        // `set_last_prepare_checksum(header.checksum)` BEFORE this append.
        // If the append below returns `Err`, sequencer + checksum stay
        // advanced while the WAL holds no matching entry: the next prepare
        // chains off a phantom op, cluster state diverges, and the
        // `MetadataHandoff::Waiter` factory bundle propagates the divergence
        // to peers. Fix: rollback `sequencer.set_sequence` +
        // `set_last_prepare_checksum` to their captured prior values on
        // append failure (preferred per CLAUDE.md "no panics in libraries"),
        // or abort the shard.
        if let Err(e) = journal.handle().append(message.clone()).await {
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                operation = ?header.operation,
                error = %e,
                "journal append failed"
            );
            return;
        }

        // Durable; chain-replicate. `replicate` borrows + freezes; we keep
        // message for the sequencer/checksum bookkeeping below.
        self.replicate(&message).await;

        self.observe_prepare_runtime_state(&message);
        // Backup only: advance sequencer + checksum post-append. Primary
        // already advanced in push_prepare_entry; re-setting here would
        // rewind a sibling prepare pipelined during the append await to a
        // stale op + parent, projecting a duplicate next.
        if is_backup {
            consensus.sequencer().set_sequence(header.op);
            consensus.set_last_prepare_checksum(header.checksum);
            consensus.observe_prepare_timestamp(header.timestamp);
        }

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(&header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal().await;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareOkHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let header = message.header();

        if let Err(reason) = ack_preflight(consensus) {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                reason = reason.as_str(),
                "ignoring ack during preflight"
            );
            return;
        }

        {
            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .entry_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
                debug!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    op = header.op,
                    prepare_checksum = header.prepare_checksum,
                    "ack target prepare not in pipeline"
                );
                return;
            }
        }

        let quorum = ack_quorum_reached(consensus, PlaneKind::Metadata, header);
        if quorum {
            let journal = self.journal.as_ref().unwrap();

            debug!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                "ack quorum received"
            );

            // Commit loop: peek -> await journal read -> revalidate head ->
            // sync {pop, apply, advance_commit_min}.
            //
            // The entry stays in the pipeline across the journal-read await,
            // so a driver of this function that is dropped there — a hyper
            // HTTP handler future canceled by peer disconnect, or a parked
            // in-process submitter — strands nothing: the next driver
            // (sibling submit, shard pump, repair tick) re-peeks the same
            // head and commits it. Popping BEFORE the await loses the entry
            // forever on cancellation (nothing re-applies a popped entry;
            // `repair_primary_self_acks` is re-ack-only), pinning
            // `commit_min` below `commit_max` and panicking the next commit
            // with "commit_min must advance sequentially".
            //
            // Concurrent drivers are serialized by the head revalidation:
            // only the driver that still finds its peeked header at the
            // pipeline head after the await owns that op's commit; everyone
            // else re-peeks and moves on to the next committable op.
            let mut commits = 0usize;
            let mut wire_replies: Vec<(CommitLogEvent, Message<ReplyHeader>)> = Vec::new();
            while let Some(prepare_header) = peek_committable_head(consensus) {
                // TODO(hubcio): should we replace this with graceful fallback (warn + return)?
                // When journal compaction is implemented compaction could race
                // with this lookup if it removes entries below the commit number.
                let prepare = journal
                    .handle()
                    .entry(&prepare_header)
                    .await
                    .unwrap_or_else(|| {
                        panic!(
                            "on_ack: committed prepare op={} checksum={} must be in journal",
                            prepare_header.op, prepare_header.checksum
                        )
                    });

                // Revalidate after the await: a sibling driver may have
                // committed this op (and more) while we were parked.
                let head_is_ours = consensus.pipeline().borrow().head().is_some_and(|head| {
                    head.header.op == prepare_header.op
                        && head.header.checksum == prepare_header.checksum
                });
                if !head_is_ours {
                    continue;
                }

                let mut entry = consensus
                    .pipeline()
                    .borrow_mut()
                    .pop()
                    .expect("on_ack: revalidated head exists");

                let pipeline_depth = consensus.pipeline().borrow().len();
                let event = CommitLogEvent {
                    replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Metadata),
                    op: prepare_header.op,
                    client_id: prepare_header.client,
                    request_id: prepare_header.request,
                    operation: prepare_header.operation,
                    pipeline_depth,
                };

                // Apply SM + mutate client_table BEFORE advancing commit_min.
                // `is_caught_up_primary` reads `commit_min == commit_max` as
                // proof the table is caught up. Table first, counter last:
                // panic mid-commit leaves the gate closed.
                //
                // Invariant: no .await or panic from the pop above through
                // `advance_commit_min` and the subscriber fire below.
                // Sync-only — this is what makes pop/apply/advance atomic on
                // the single-threaded shard and keeps the head revalidation
                // sound.
                let reply = if prepare_header.operation == Operation::Register {
                    // Register: commit_register creates session, no SM.
                    let reply = build_reply_message(&prepare_header, &bytes::Bytes::new());
                    let in_flight =
                        |c: u128| consensus.pipeline().borrow().has_message_from_client(c);
                    self.client_table.borrow_mut().commit_register(
                        prepare_header.client,
                        prepare_header.user_id,
                        reply.clone(),
                        in_flight,
                    );
                    reply
                } else if prepare_header.operation == Operation::Logout {
                    // Logout unregisters the VSR client session on every replica.
                    let reply = build_reply_message(&prepare_header, &bytes::Bytes::new());
                    self.client_table
                        .borrow_mut()
                        .remove_client(prepare_header.client);
                    // Drop the disconnected client from every consumer group it
                    // joined and rebalance. Deterministic side-effect of the
                    // Logout commit, applied identically on every replica.
                    self.mux_stm.streams().remove_consumer_group_member(
                        prepare_header.client,
                        iggy_common::IggyTimestamp::from(prepare_header.timestamp),
                    );
                    reply
                } else {
                    // Normal op: apply SM, commit_reply. `Err` is decode/corruption
                    // only; a business rejection commits as a deterministic no-op
                    // whose `code` rides the reply body, replayed on retry.
                    let apply = gated_apply(&self.mux_stm, prepare).unwrap_or_else(|err| {
                        panic!(
                            "on_ack: committed metadata op={} failed to apply: {err}",
                            prepare_header.op
                        );
                    });
                    // Post-commit notifier (e.g. partition reconciler
                    // wake-up). Filtering by operation is the
                    // recipient's responsibility.
                    self.fire_commit_notifier(prepare_header.operation);
                    let reply =
                        build_reply_message_with(&prepare_header, apply.reply_body_len(), |dst| {
                            apply.write_reply_body(dst);
                        });
                    // Cache only if session exists. Client evicted between
                    // prepare and commit: skip cache (`commit_reply` no-ops),
                    // wire reply still ships.
                    let session = self
                        .client_table
                        .borrow()
                        .get_session(prepare_header.client);
                    if let Some(session) = session {
                        self.client_table.borrow_mut().commit_reply(
                            prepare_header.client,
                            session,
                            reply.clone(),
                        );
                    } else {
                        tracing::trace!(
                            client = prepare_header.client,
                            op = prepare_header.op,
                            "on_ack: client evicted while being prepared; emitting reply but skipping cache"
                        );
                    }
                    reply
                };
                consensus.advance_commit_min(prepare_header.op);
                emit_sim_event(SimEventKind::OperationCommitted, &event);
                commits += 1;

                // Fire subscriber BEFORE wire send. Slot already updated
                // (slot-first ordering, see take_reply_sender). Dropped
                // receiver: ignored. Still inside the sync region, so an
                // in-process awaiter is woken atomically with its commit.
                let had_in_process_subscriber = entry.has_reply_sender();
                if let Some(sender) = entry.take_reply_sender() {
                    let _ = sender.send(reply.clone());
                }

                // Skip wire send when an in-process subscriber consumed the
                // reply: the caller (e.g. `complete_login_register`,
                // `handle_logout_request`) ships its own full-body reply on
                // the same socket. Sending both desyncs the SDK -- it reads
                // the first frame, fails to decode the typed body, and
                // leaves the second frame stuck in the socket buffer.
                if !had_in_process_subscriber {
                    wire_replies.push((event, reply));
                }
            }

            // Wire replies AFTER the commit loop: this region may await, and
            // a driver dropped here loses only reply frames — every commit
            // above is applied and its reply cached in the client_table, so
            // the SDK recovers it via request replay.
            for (event, reply) in wire_replies {
                let generic_reply = reply.into_generic();
                let reply_buffers = freeze_client_reply(generic_reply);
                emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

                if let Err(e) = consensus
                    .message_bus()
                    .send_to_client(event.client_id, reply_buffers)
                    .await
                {
                    error!(
                        client = event.client_id,
                        op = event.op,
                        request_id = event.request_id,
                        operation = ?event.operation,
                        %e,
                        "client reply forward failed, no retransmit path; client will time out",
                    );
                }
            }

            // Each commit frees one prepare slot, promote up to that many
            // buffered requests so the pipeline stays busy.
            self.drain_request_queue_into_prepares(commits).await;
        }
    }
}

impl<B, P, J, S, M> PlaneIdentity<VsrConsensus<B, P>> for IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    fn is_applicable<H>(&self, message: &<VsrConsensus<B, P> as Consensus>::Message<H>) -> bool
    where
        H: ConsensusHeader,
    {
        assert!(matches!(
            message.header().command(),
            Command2::Request | Command2::Prepare | Command2::PrepareOk
        ));
        let op = message.header().operation();
        op.is_metadata() || matches!(op, Operation::Register | Operation::Logout)
    }
}

impl<B, J, S, M> IggyMetadata<VsrConsensus<B>, J, S, M>
where
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    /// Submit `Register` from in-process, await commit. Wire reply still fires
    /// via `message_bus.send_to_client`; subscriber is additive.
    ///
    /// # Returns
    /// Session number (= commit op). Idempotent: existing session short-circuits.
    ///
    /// # Errors
    /// [`MetadataSubmitError`] (all transient): `NotPrimary`, `NotCaughtUp`,
    /// `PipelineFull`, `InProgress`, `Canceled`. `Canceled` dominates on view
    /// change; new primary inherits via `commit_journal`, SDK retries.
    ///
    /// # Panics
    /// On `client_id == 0` or shard without consensus.
    ///
    /// # Safety
    /// Catch-up gate load-bearing: dispatch with `commit_min < commit_max`
    /// produces two register entries and panics on replay.
    #[allow(clippy::future_not_send)]
    pub async fn submit_register_in_process(
        &self,
        client_id: u128,
        user_id: u32,
    ) -> Result<u64, MetadataSubmitError> {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_register_in_process: consensus only exists on shard 0");

        // Idempotent fast path: existing session skips pipeline + wire-reply.
        if let Some(session) = self.client_table.borrow().get_session(client_id) {
            return Ok(session);
        }

        // Status + catch-up gate (see doc). Split variants for telemetry:
        // NotPrimary (try peer) vs NotCaughtUp (retry). Caller policy same.
        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_syncing() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }

        // Mirror wire-path register_preflight: a racing second prepare fails
        // check_register on commit. Surface pre-synthesis.
        if consensus
            .pipeline()
            .borrow()
            .has_message_from_client(client_id)
        {
            return Err(MetadataSubmitError::InProgress);
        }

        // TODO(pipeline-backpressure): in-process has no request_queue yet;
        // terminal on full. Wire path buffers.
        if consensus.pipeline().borrow().is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }

        let request = build_register_request_message(consensus, client_id, user_id);
        // Wire path runs `RequestHeader::validate` at network boundary;
        // in-process skips it. debug_assert pins drift.
        debug_assert!(
            {
                use iggy_binary_protocol::ConsensusHeader;
                request.header().validate().is_ok()
            },
            "build_register_request_message produced a header that fails validate()"
        );
        // `prepare_request` only fails on `!is_client_allowed`; Register is
        // allowed, so unreachable. Panic loudly on regression instead of
        // smuggling through wire-eviction.
        let prepare = self
            .prepare_request(request)
            .expect("Operation::Register is client-allowed; prepare projection cannot fail");

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => {
                // View-change cancel. Re-check is correct-by-VSR: any
                // inherited Register applied via local commit_journal between
                // cancel and read produces a cluster-authoritative session
                // (`session = commit-op`, deterministic). Own surviving
                // Register would have routed through `AlreadyRegistered`
                // against the same entry, so no "this primary vs inherited
                // primary" split.
                self.client_table
                    .borrow()
                    .get_session(client_id)
                    .ok_or(MetadataSubmitError::Canceled)
            }
        }
    }

    /// Submit `Logout` from in-process, await commit.
    ///
    /// # Returns
    /// Commit op for the logout. If the client session is already absent, this
    /// is idempotent and returns the current metadata commit.
    ///
    /// # Errors
    /// Returns a consensus submission error when this node cannot accept the
    /// logout prepare, the metadata pipeline is saturated, or the pending
    /// request is canceled before commit.
    ///
    /// # Panics
    /// Panics when called with the reserved client id `0`, on a non-consensus
    /// metadata shard, or if the prepare gate flips between validation and
    /// local dispatch.
    #[allow(clippy::future_not_send)]
    pub async fn submit_logout_in_process(
        &self,
        client_id: u128,
        session: u64,
        request: u64,
    ) -> Result<u64, MetadataSubmitError> {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_logout_in_process: consensus only exists on shard 0");

        // Session guard: only propose a Logout when the slot still holds the
        // exact session this logout targets. A late disconnect-logout for a
        // reused client id (slot since rebound to a newer session) carries the
        // stale session and is dropped here, so it can never wipe the fresh
        // registration. A missing slot also fails the match and short-circuits.
        if self.client_table.borrow().get_session(client_id) != Some(session) {
            return Ok(consensus.commit_min());
        }

        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_syncing() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }

        if consensus
            .pipeline()
            .borrow()
            .has_message_from_client(client_id)
        {
            return Err(MetadataSubmitError::InProgress);
        }

        if consensus.pipeline().borrow().is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }

        let request = build_logout_request_message(consensus, client_id, session, request);
        debug_assert!(
            {
                use iggy_binary_protocol::ConsensusHeader;
                request.header().validate().is_ok()
            },
            "build_logout_request_message produced a header that fails validate()"
        );
        let prepare = self
            .prepare_request(request)
            .expect("Operation::Logout is client-allowed; prepare projection cannot fail");

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => {
                if self.client_table.borrow().get_session(client_id).is_none() {
                    Ok(consensus.commit_min())
                } else {
                    Err(MetadataSubmitError::Canceled)
                }
            }
        }
    }

    /// Submit a server-originated `CompleteConsumerGroupRevocation` through the
    /// metadata consensus group (shard 0). The partition reconciler calls this
    /// to complete a cooperative revocation once the source has drained the
    /// partition (or it timed out).
    ///
    /// Unlike a client op there is no session: a reserved internal client id
    /// (never coordinator-minted) carries it, `request_preflight` is skipped
    /// (server-originated), and the normal-op commit path skips reply-caching
    /// when the client has no session. The op is internal (not client-allowed),
    /// so it bypasses `prepare_request` and projects directly.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot accept the prepare,
    /// `InProgress` / `PipelineFull` on pipeline pressure (the reconciler
    /// retries next tick; completion is idempotent), `Canceled` if the pending
    /// prepare was canceled before commit.
    ///
    /// # Panics
    /// On a shard without consensus (only shard 0 owns the metadata consensus
    /// group); callers must route here only on shard 0.
    #[allow(clippy::future_not_send)]
    pub async fn submit_complete_revocation_in_process(
        &self,
        stream_id: u32,
        topic_id: u32,
        group_id: u64,
        source_client_id: u128,
        partition_id: u32,
    ) -> Result<u64, MetadataSubmitError> {
        const INTERNAL_REQUEST_ID: u64 = u64::MAX;
        // Reserved internal client id, distinct per (group, partition) target.
        // The high 64 bits are all-ones -- never coordinator-minted (those carry
        // a small home-shard number in the top bits) -- and the low 64 bits pack
        // (group_id, partition_id). Distinct ids matter: the pipeline dedups by
        // client id, so a shared id would cap internal completions at one
        // in-flight cluster-wide and drain a wide rebalance one per consensus
        // round-trip. Per-target ids let completions for different partitions
        // pipeline concurrently while still deduping a retry of the same target.
        let internal_client_id: u128 =
            (u128::from(u64::MAX) << 64) | (u128::from(group_id) << 32) | u128::from(partition_id);

        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_complete_revocation_in_process: consensus only exists on shard 0");

        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_syncing() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }
        if consensus
            .pipeline()
            .borrow()
            .has_message_from_client(internal_client_id)
        {
            return Err(MetadataSubmitError::InProgress);
        }
        if consensus.pipeline().borrow().is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }

        let request = CompleteConsumerGroupRevocationRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
            group_id,
            source_client_id,
            partition_id,
        };
        let body = request.to_bytes();
        let message = build_complete_revocation_request_message(
            consensus,
            internal_client_id,
            INTERNAL_REQUEST_ID,
            &body,
        );
        let prepare = message.project(consensus);

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// `true` when this node is the caught-up primary of the metadata
    /// consensus group. Gates leader-only maintenance (the PAT cleaner)
    /// off backups and lagging primaries.
    #[must_use]
    pub fn is_caught_up_primary(&self) -> bool {
        self.consensus.as_ref().is_some_and(is_caught_up_primary)
    }

    /// Submit a replicated `DeletePersonalAccessToken` originated by the
    /// server (the PAT cleaner), not a client.
    ///
    /// No client session exists, so this skips `request_preflight` (like
    /// the logout precedent) and uses the reserved internal `client` id
    /// `0`: never registered, so the commit path's `get_session(0)` is
    /// `None` and skips `commit_reply` (and its `assert!(client_id != 0)`),
    /// while the preflight and register asserts never run. Delete is
    /// idempotent, so the dropped dedup is harmless and a re-proposal on the
    /// next tick is a no-op.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot replicate,
    /// `PipelineFull` under pipeline pressure, `Canceled` if the prepare is
    /// dropped before commit.
    ///
    /// # Panics
    /// On a shard without consensus (shard 0 only), or if the prepare gate
    /// flips between validation and dispatch.
    #[allow(clippy::future_not_send)]
    pub async fn submit_delete_personal_access_token_in_process(
        &self,
        user_id: UserId,
        name: WireName,
    ) -> Result<u64, MetadataSubmitError> {
        let consensus = self.consensus.as_ref().expect(
            "submit_delete_personal_access_token_in_process: consensus only exists on shard 0",
        );

        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_syncing() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }

        if consensus.pipeline().borrow().is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }

        let body = DeletePersonalAccessTokenRequest {
            user_id,
            name,
            // Expiry-gated: a token recreated under the same name between the
            // cleaner's snapshot and this commit must not be purged. Apply
            // re-checks the stored token's expiry against the prepare timestamp.
            only_if_expired: true,
        }
        .to_bytes();
        // Build the prepare directly so the `client = 0` header skips the
        // client-header validation in `prepare_request` / `Project::project`
        // (the in-process path `build_prepare_message` documents).
        let header = RequestHeader {
            client: 0,
            namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            ..RequestHeader::default()
        };
        let prepare = build_prepare_message(
            consensus,
            &header,
            Operation::DeletePersonalAccessToken,
            &body,
        );

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// Submit a replicated client request from in-process and await the
    /// committed reply.
    ///
    /// A peer (home) shard relays a client's replicated request here (shard
    /// 0 owns the metadata consensus group) and awaits the full committed
    /// reply over the pipeline subscriber. The home shard then writes the
    /// reply to the originating socket -- it holds the connection and the
    /// `vsr -> transport` mapping that this side cannot reconstruct.
    ///
    /// Mirrors [`Self::submit_register_in_process`] but: (1) uses
    /// `request_preflight` (dedup / session check) instead of the register
    /// gate, (2) returns the committed reply as a `Message<GenericHeader>`
    /// (body = state machine output) rather than just the commit op. A
    /// `Duplicate`/eviction preflight outcome is returned here as the reply
    /// frame so the home shard resends it by transport id.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot accept the
    /// prepare, `InProgress` / `PipelineFull` on pipeline pressure,
    /// `Canceled` when preflight absorbed the request (dedup / eviction /
    /// gap) or the pending prepare was canceled before commit.
    ///
    /// # Panics
    /// On a shard without consensus (only shard 0 owns the metadata
    /// consensus group); callers must route here only on shard 0.
    #[allow(clippy::future_not_send)]
    pub async fn submit_request_in_process(
        &self,
        message: Message<RequestHeader>,
    ) -> Result<Message<GenericHeader>, MetadataSubmitError> {
        let request_header = *message.header();
        let client_id = request_header.client;
        let session = request_header.session;
        let request = request_header.request;

        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_request_in_process: consensus only exists on shard 0");

        // Not-primary / not-caught-up is transient: the same request replayed
        // once a primary is caught up commits fine. Reply with the explicit
        // transient frame (relayed to the socket by the home shard) so the
        // client replays immediately rather than waiting out its read-timeout.
        // `TransientNotAccepted` specifically: the request never entered the
        // pipeline here, so the client may re-issue it ANYWHERE -- including
        // under a fresh session after failing over to the current leader --
        // without double-apply risk. (`TransientNotCommitted` conversely means
        // the outcome is unknown and only a same-session replay is safe.)
        if !is_caught_up_primary(consensus) {
            return Ok(build_result_rejection_reply(
                &request_header,
                consensus.commit_max(),
                IggyError::TransientNotAccepted.as_code(),
            )
            .into_generic());
        }

        // Dedup / session / eviction. shard 0 cannot route by the VSR
        // consensus `client_id` (its top bits are random, not home-shard
        // routing), so a Replay/Evict/NotReady is returned to the home shard as
        // the reply -- `handle_client_request` writes it to the originating
        // socket by transport id, exactly like a fresh commit. Drop (client-bug
        // stale/gap) surfaces as Canceled so the home shard stays silent.
        match request_preflight(consensus, &self.client_table, client_id, session, request) {
            PreflightOutcome::Dispatch => {}
            PreflightOutcome::Replay(reply) => {
                return server_common::Message::<GenericHeader>::try_from(
                    server_common::iobuf::Owned::<{ server_common::MESSAGE_ALIGN }>::copy_from_slice(
                        reply.as_slice(),
                    ),
                )
                .map_err(|_| MetadataSubmitError::Canceled);
            }
            PreflightOutcome::Evict(reason) => {
                let ctx = EvictionContext::from_consensus(consensus);
                return Ok(build_eviction_message(ctx, client_id, reason).into_generic());
            }
            // In-flight prepare from this client: replaying the same request_id
            // is absorbed until the original commits, then served from cache.
            PreflightOutcome::NotReady => {
                return Ok(build_result_rejection_reply(
                    &request_header,
                    consensus.commit_max(),
                    IggyError::TransientNotCommitted.as_code(),
                )
                .into_generic());
            }
            PreflightOutcome::Drop => return Err(MetadataSubmitError::Canceled),
        }

        // Pipeline full: backpressure, not failure. The request was not
        // admitted, so `TransientNotAccepted` (re-issuable anywhere).
        if consensus.pipeline().borrow().is_full() {
            return Ok(build_result_rejection_reply(
                &request_header,
                consensus.commit_max(),
                IggyError::TransientNotAccepted.as_code(),
            )
            .into_generic());
        }

        // The acting-user RBAC stamp lives in the shared `prepare_request`
        // (op-guarded, fail-closed on an unknown session). `request_preflight`
        // above proved this client's session is live, so it resolves there.
        let Ok(prepare) = self.prepare_request(message) else {
            // Structurally-invalid request. Return an eviction frame (relayed to
            // the socket by the home shard) rather than `Canceled`, which leaves
            // the shard silent and the SDK retrying forever.
            let reason = eviction_reason_for_invalid(request_header.operation);
            let ctx = EvictionContext::from_consensus(consensus);
            return Ok(build_eviction_message(ctx, client_id, reason).into_generic());
        };

        // A view change canceled the pending prepare before commit. The op may
        // or may not have committed; replaying the same request_id is idempotent
        // (the new primary serves it from cache if committed, else re-dispatches),
        // so reply with the transient frame rather than staying silent.
        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.into_generic()),
            Err(Canceled) => Ok(build_result_rejection_reply(
                &request_header,
                consensus.commit_max(),
                IggyError::TransientNotCommitted.as_code(),
            )
            .into_generic()),
        }
    }

    /// Subscribe to a prepared metadata write, dispatch it into the pipeline,
    /// drain the self-loopback acks, and await the committed reply.
    ///
    /// Shared tail of every in-process submit path
    /// ([`Self::submit_register_in_process`],
    /// [`Self::submit_logout_in_process`], [`Self::submit_request_in_process`],
    /// and [`Self::submit_delete_personal_access_token_in_process`]). The
    /// caller owns its own preflight / dedup gate and builds `prepare`; this
    /// owns the dispatch mechanics. The view-change `Canceled` is returned
    /// verbatim so each caller can apply its own idempotent recheck.
    ///
    /// Subscribes before dispatch so the receiver is registered before any
    /// self-loopback ack fires (compio is single-threaded; explicit anyway).
    #[allow(clippy::future_not_send)]
    async fn dispatch_prepare_and_await(
        &self,
        consensus: &VsrConsensus<B>,
        prepare: Message<PrepareHeader>,
    ) -> Result<Message<ReplyHeader>, Canceled> {
        consensus.verify_pipeline();
        let receiver = consensus.pipeline_message_with_subscriber(PlaneKind::Metadata, &prepare);
        // Re-check gate post-subscribe: `pipeline_message_with_subscriber`
        // can drop the borrow. No commit-max advance flips the gate today;
        // pin against a future await between check and dispatch.
        debug_assert!(
            is_caught_up_primary(consensus),
            "dispatch_prepare_and_await: gate flipped between check and dispatch"
        );
        // `on_replicate` awaits: a sibling in-process submit may commit
        // (commit_min advances) or a view change may land (view advances) while
        // parked here. Both are handled downstream - a view change drops the
        // reply_sender so `receiver` resolves `Canceled`, and loopback acks are
        // op-routed - so no post-await view/commit invariant holds or is needed.
        self.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        for message in loopback {
            match message.header().command {
                Command2::PrepareOk => match message.try_into_typed::<PrepareOkHeader>() {
                    Ok(prepare_ok) => self.on_ack(prepare_ok).await,
                    Err(error) => warn!(
                        error = %error,
                        "dropping malformed PrepareOk from metadata loopback queue"
                    ),
                },
                command => warn!(
                    ?command,
                    "dropping unexpected message from metadata loopback queue"
                ),
            }
        }

        receiver.await
    }

    /// Repair the primary's own missing self-acks.
    ///
    /// The primary's `PrepareOk` for its own prepare is produced exactly once,
    /// as a loopback right after the WAL append (see `on_replicate`). If that
    /// one-shot is lost or suppressed (e.g. the `send_prepare_ok` persistence
    /// gate races the sequencer pre-advance under a client burst), no
    /// retransmit path regenerates it: `retransmit_targets` lists the primary
    /// itself among the missing replicas, but `RetransmitPrepares` to self is a
    /// no-op. The op then sits one vote short of quorum forever and pins the
    /// contiguous commit prefix, so `commit_min` never catches up to
    /// `commit_max` and the cluster wedges.
    ///
    /// This is a re-ack-only repair: for each pending op the primary holds
    /// DURABLY but has not self-acked, re-emit the self `PrepareOk` and drain
    /// it through `on_ack`. A pending op the primary does NOT yet hold durably
    /// is skipped - filling that hole needs full message repair, which is out
    /// of scope here. Driven each consensus tick;
    /// `on_ack` dedups a redundant self-ack via `has_ack`, so re-running is
    /// idempotent and stops once the op commits and leaves the pending range.
    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    pub async fn repair_primary_self_acks(&self) {
        let Some(consensus) = self.consensus.as_ref() else {
            return;
        };
        if !consensus.is_primary() || !consensus.is_normal() || consensus.is_syncing() {
            return;
        }
        let Some(journal) = self.journal.as_ref() else {
            return;
        };
        let self_replica = consensus.replica();

        // Snapshot durable, self-unacked pending ops, dropping the pipeline and
        // journal borrows before the `send_prepare_ok` awaits below.
        let mut headers: Vec<PrepareHeader> = Vec::new();
        {
            let pipeline = consensus.pipeline().borrow();
            let from = consensus.commit_max() + 1;
            let to = consensus.sequencer().current_sequence();
            for op in from..=to {
                let Some(entry) = pipeline.entry_by_op(op) else {
                    continue;
                };
                if entry.has_ack(self_replica) {
                    continue;
                }
                // Durable only: re-acking implies "I hold this op". A gap (op not
                // in the journal) must not be self-acked - that path needs repair.
                if let Some(header) = journal.handle().header(op as usize).map(|header| *header) {
                    headers.push(header);
                }
            }
        }
        if headers.is_empty() {
            return;
        }

        // Interleave push + drain per header instead of push-all-then-drain-once:
        // each `on_ack` below can promote a full window of buffered requests
        // (`drain_request_queue_into_prepares`), and every promoted prepare
        // self-acks through `send_or_loopback(self)` -> `push_loopback`. The
        // consensus-tick arm of the shard pump never drains the loopback
        // queue, so residuals would accumulate across ticks and trip the
        // `push_loopback` capacity assert (`PIPELINE_PREPARE_QUEUE_MAX`).
        // Draining to empty BEFORE each push bounds queue occupancy to one
        // promotion window; the trailing drain applies the acks this pass
        // produced (including promotion self-acks) instead of leaving them
        // for a tick that never comes.
        let mut loopback = Vec::new();
        for header in &headers {
            while self.apply_self_ack_loopback(&mut loopback).await {}
            self.send_prepare_ok(header).await;
        }
        while self.apply_self_ack_loopback(&mut loopback).await {}
    }

    /// Drain the consensus loopback queue once and feed every self-`PrepareOk`
    /// through [`Self::on_ack`], dropping anything else with a warning.
    /// Returns whether any message was processed, so callers can loop until
    /// the queue is empty (an `on_ack` can promote buffered requests whose
    /// self-acks land back on the queue).
    #[allow(clippy::future_not_send)]
    async fn apply_self_ack_loopback(&self, loopback: &mut Vec<Message<GenericHeader>>) -> bool {
        let consensus = self.consensus.as_ref().unwrap();
        consensus.drain_loopback_into(loopback);
        if loopback.is_empty() {
            return false;
        }
        for message in loopback.drain(..) {
            match message.header().command {
                Command2::PrepareOk => match message.try_into_typed::<PrepareOkHeader>() {
                    Ok(prepare_ok) => self.on_ack(prepare_ok).await,
                    Err(error) => warn!(
                        error = %error,
                        "dropping malformed PrepareOk from self-ack repair loopback"
                    ),
                },
                command => warn!(
                    ?command,
                    "dropping unexpected message from self-ack repair loopback"
                ),
            }
        }
        true
    }

    /// Promote up to `slots_freed` buffered requests into prepares after
    /// `on_ack` commits a prefix.
    ///
    /// # Safety
    /// Re-preflight per iteration: `commit_journal` may have advanced the
    /// client's request between push and drain (Stale / Duplicate /
    /// `AlreadyRegistered`). Skipping produces a duplicate prepare and panics.
    #[allow(clippy::future_not_send)]
    async fn drain_request_queue_into_prepares(&self, slots_freed: usize) {
        let consensus = self.consensus.as_ref().unwrap();
        for _ in 0..slots_freed {
            let req = consensus.pipeline().borrow_mut().pop_request();
            let Some(req) = req else { break };

            let client_id = req.message.header().client;
            let session = req.message.header().session;
            let request = req.message.header().request;
            let operation = req.message.header().operation;
            let dispatch = if operation == Operation::Register {
                register_preflight(consensus, &self.client_table, client_id).await
            } else {
                let outcome =
                    request_preflight(consensus, &self.client_table, client_id, session, request);
                apply_preflight_consensus_plane(consensus, outcome, client_id).await
            };
            if !dispatch {
                continue;
            }

            let prepare = match self.prepare_request(req.message) {
                Ok(prepare) => prepare,
                Err(error) => {
                    // Same invariant as `on_request`: a structurally-invalid
                    // request evicts, never a silent drop, or the SDK retries
                    // forever. Reachable here because requests are queued
                    // unvalidated (prepare queue full at arrival), projected now.
                    let reason = eviction_reason_for_invalid(operation);
                    warn!(
                        target: "iggy.metadata.diag",
                        plane = "metadata",
                        replica_id = consensus.replica(),
                        error = %error,
                        ?reason,
                        "drain_request_queue: rejecting invalid queued request with eviction"
                    );
                    send_eviction_to_client(consensus, client_id, reason).await;
                    continue;
                }
            };
            pipeline_prepare_common(consensus, PlaneKind::Metadata, prepare, |prepare| {
                self.on_replicate(prepare)
            })
            .await;
        }
    }
}

impl<B, P, J, S, M> IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    #[allow(clippy::future_not_send)]
    async fn checkpoint_if_needed(&self, consensus: &VsrConsensus<B, P>, journal: &J) -> bool {
        let Some(coordinator) = &self.coordinator else {
            return true;
        };

        // Use commit_min (locally executed), not commit_max. WAL entries
        // between commit_min+1 and commit_max haven't been applied to the
        // state machine yet, draining them would lose data on crash.
        let snap_op = consensus.commit_min();
        // Stamp created_at from the injected consensus clock (seed-derived
        // under the simulator), not the wall clock, so replayed snapshots are
        // byte-identical.
        let created_at = consensus.clock_realtime_micros();
        match coordinator
            .checkpoint_if_needed(&self.mux_stm, journal, snap_op, created_at)
            .await
        {
            Ok(true) => {
                debug!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op = snap_op,
                    "forced checkpoint completed"
                );
                true
            }
            Ok(false) => true,
            Err(e) => {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op = snap_op,
                    error = %e,
                    "forced checkpoint failed"
                );
                false
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn prepare_request(
        &self,
        mut message: Message<RequestHeader>,
    ) -> Result<Message<PrepareHeader>, iggy_common::IggyError> {
        let consensus = self.consensus.as_ref().unwrap();
        let operation = message.header().operation;
        let client_id = message.header().client;
        // `TruncatePartition` is server-originated (the owning shard resolves a
        // client `DeleteSegments` count to a concrete offset) but replicated AS
        // the client's own request, so the commit records the client's request
        // sequence in the `ClientTable`. It is internal -- no wire command code
        // maps to it, so a client cannot construct one directly -- hence the
        // `is_client_allowed` gate excludes it; admit it explicitly. The default
        // match arm below projects it through unchanged.
        if !operation.is_client_allowed() && operation != Operation::TruncatePartition {
            return Err(IggyError::InvalidCommand);
        }

        // Stamp the acting user id into the replicated header so the in-apply
        // RBAC gate (`crate::stm::authz`) resolves the same identity on every
        // replica, WAL replay included (no session table there). Every client-op
        // prepare funnels through here, primary-only by construction, so stamping
        // here -- not in the in-process client path alone -- also covers the
        // wire-plane ingresses (`on_request` and the request-queue drain). The
        // wire `user_id` is never trusted: it is overwritten for every gated
        // client op, and a client whose session is unknown is denied here
        // (fail-closed), never defaulted to root. `Register` / `Logout` are exempt
        // (see `resolve_acting_user_id`); server-originated internal ops
        // (`CompleteConsumerGroupRevocation`, the PAT-cleaner delete) build their
        // prepare directly, bypassing this path, and keep `user_id` 0 (gate skips).
        if let Some(acting_user_id) =
            resolve_acting_user_id(operation, client_id, &self.client_table)?
        {
            let request_header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
                &mut message.as_mut_slice()[..size_of::<RequestHeader>()],
            );
            request_header.user_id = acting_user_id;
        }

        // Must be read AFTER the stamp: the `CreateTopic` / `CreatePartitions`
        // arms hand this `header` copy to `build_prepare_message`, so it has to
        // carry the stamped acting user. Reading it before the stamp would ship
        // those prepares with the untrusted wire `user_id` (0 = root from
        // well-behaved SDKs, attacker-chosen otherwise), silently bypassing the
        // authz gate. The default arm projects the mutated buffer directly and
        // is order-independent.
        let header = *message.header();
        let body = &message.as_slice()[size_of::<RequestHeader>()..header.size as usize];

        match header.operation {
            Operation::CreateTopic => {
                let mut request = WireCreateTopicRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                // Resolve the `ServerDefault` sentinel (0) against server config
                // here, at primary admission, so the replicated payload carries a
                // concrete size and every replica commits the same value.
                if request.max_topic_size == 0 {
                    request.max_topic_size = self.default_max_topic_size.get();
                }
                if request.message_expiry == 0 {
                    request.message_expiry = self.default_message_expiry.get();
                }
                let partitions = self
                    .allocator
                    .allocate_many(request.partitions_count as usize)
                    .into_iter()
                    .enumerate()
                    .map(|(partition_id, consensus_group_id)| {
                        Ok(CreatedPartitionAssignment {
                            partition_id: u32::try_from(partition_id)
                                .map_err(|_| IggyError::InvalidCommand)?,
                            consensus_group_id,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let body = PersistedCreateTopicRequest {
                    request,
                    partitions,
                }
                .to_bytes();
                Ok(build_prepare_message(
                    consensus,
                    &header,
                    Operation::CreateTopicWithAssignments,
                    &body,
                ))
            }
            Operation::CreatePartitions => {
                let request = WireCreatePartitionsRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                // Parent-existence is validated at apply, returning
                // `CreatePartitionsResult::{Stream,Topic}NotFound`. A preflight
                // read here would decide against possibly-uncommitted state and
                // drop without a reply (TOCTOU + wedge). See the metadata
                // validation design doc.
                let partitions = self
                    .allocator
                    .allocate_many(request.partitions_count as usize)
                    .into_iter()
                    .enumerate()
                    .map(|(offset, consensus_group_id)| {
                        Ok(CreatedPartitionAssignment {
                            partition_id: u32::try_from(offset)
                                .map_err(|_| IggyError::InvalidCommand)?,
                            consensus_group_id,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let body = PersistedCreatePartitionsRequest {
                    request,
                    partitions,
                }
                .to_bytes();
                Ok(build_prepare_message(
                    consensus,
                    &header,
                    Operation::CreatePartitionsWithAssignments,
                    &body,
                ))
            }
            Operation::UpdateTopic => {
                let mut request = WireUpdateTopicRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                // Same `ServerDefault` resolution as `CreateTopic` above; rebuild
                // the prepare only if a sentinel actually needs stamping, else
                // project the untouched buffer zero-copy.
                let needs_rewrite = request.max_topic_size == 0 || request.message_expiry == 0;
                if request.max_topic_size == 0 {
                    request.max_topic_size = self.default_max_topic_size.get();
                }
                if request.message_expiry == 0 {
                    request.message_expiry = self.default_message_expiry.get();
                }
                if needs_rewrite {
                    let body = request.to_bytes();
                    return Ok(build_prepare_message(
                        consensus,
                        &header,
                        Operation::UpdateTopic,
                        &body,
                    ));
                }
                Ok(message.project(consensus))
            }
            _ => Ok(message.project(consensus)),
        }
    }

    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication pattern:
    /// - Primary sends to first backup
    /// - Each backup forwards to the next
    /// - Stops when we would forward back to primary
    ///
    /// Caller must have already appended `message` to the local journal
    /// before invoking this helper (VSR tail-ahead-of-head). Forwarding
    /// an un-persisted prepare would leave downstream WALs with an op
    /// this replica's journal does not hold.
    #[allow(clippy::future_not_send)]
    async fn replicate(&self, message: &Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = *message.header();

        // TODO: calculate the index;
        #[allow(clippy::cast_possible_truncation)]
        let idx = header.op as usize;
        assert_eq!(header.command, Command2::Prepare);
        assert!(
            journal.handle().header(idx).is_some(),
            "replicate: prepare must be durable in local journal before chain-forward"
        );
        if let Err(e) = replicate_to_next_in_chain(consensus, message).await {
            tracing::warn!(op = header.op, error = ?e, "chain replication failed");
        }
    }

    // TODO: Implement jump_to_newer_op
    // fn jump_to_newer_op(&self, header: &PrepareHeader) {}

    /// Apply ops `[commit_min+1 .. commit_max]` to state machine and
    /// `client_table`. Backup does NOT ship wire replies (primary's job).
    ///
    /// # Safety: ordering invariant
    ///
    /// `advance_commit_min(op)` and matching `client_table` mutation
    /// (`commit_register` / `commit_reply`) run back-to-back, no `.await`
    /// between. [`crate::metadata_helpers::is_caught_up_primary`] reads
    /// `commit_min == commit_max` as proof the table is caught up; an await
    /// here lets another task observe transient equality with stale table,
    /// dispatch a fresh Register on an already-registered client, and panic
    /// `commit_register`'s session-eq assert.
    ///
    /// Inner block sync today. Future async state-machine must either:
    /// 1. Apply SM + bump `commit_min` in one `RefCell` borrow, or
    /// 2. Buffer apply, bump `commit_min` post table-mutation, gate
    ///    `is_caught_up_primary` on a higher "applied frontier".
    ///
    /// `is_caught_up_primary_gate_states` pins clauses, NOT intra-loop window.
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&self) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        while consensus.commit_min() < consensus.commit_max() {
            let op = consensus.commit_min() + 1;

            let Some(header) = journal.handle().header(op as usize) else {
                // TODO: Implement message repair: request missing prepare from
                // primary or other replicas. Until then, the backup stalls here.
                break;
            };
            let header = *header;

            let Some(prepare) = journal.handle().entry(&header).await else {
                warn!("commit_journal: prepare body missing for op={op}, stopping");
                break;
            };

            // SM apply + client_table mutation BEFORE `advance_commit_min`
            // (see `on_ack` for matching invariant). No await between table
            // mutation and counter bump.
            if header.operation == Operation::Register {
                // Register: commit_register creates session, no SM.
                let reply = build_reply_message(&header, &bytes::Bytes::new());
                let in_flight = |c: u128| consensus.pipeline().borrow().has_message_from_client(c);
                self.client_table.borrow_mut().commit_register(
                    header.client,
                    header.user_id,
                    reply,
                    in_flight,
                );
            } else if header.operation == Operation::Logout {
                self.client_table.borrow_mut().remove_client(header.client);
                // Mirror the on_ack path: drop the disconnected client from
                // every consumer group it joined and rebalance.
                self.mux_stm.streams().remove_consumer_group_member(
                    header.client,
                    iggy_common::IggyTimestamp::from(header.timestamp),
                );
            } else {
                // Normal op: apply SM, commit_reply. `Err` is decode/corruption
                // only; a business rejection commits as a deterministic no-op
                // whose `code` rides the reply body, replayed on retry.
                let apply = gated_apply(&self.mux_stm, prepare).unwrap_or_else(|err| {
                    panic!("commit_journal: committed metadata op={op} failed to apply: {err}");
                });
                // Post-commit notifier (e.g. partition reconciler
                // wake-up). Same hook fires on backups so reconcilers
                // converge after replicated commits, not only quorum-acked
                // ones reached via `on_ack` on the primary.
                self.fire_commit_notifier(header.operation);
                let reply = build_reply_message_with(&header, apply.reply_body_len(), |dst| {
                    apply.write_reply_body(dst);
                });
                // Cache only if session still exists. WAL replay may carry a
                // reply for a later-evicted client; `commit_reply` no-ops.
                let session = self.client_table.borrow().get_session(header.client);
                if let Some(session) = session {
                    self.client_table
                        .borrow_mut()
                        .commit_reply(header.client, session, reply);
                } else {
                    tracing::trace!(
                        client = header.client,
                        op = op,
                        "commit_journal: client evicted while being prepared; skipping cache"
                    );
                }
            }
            consensus.advance_commit_min(op);
            debug!("commit_journal: committed op={op}");
        }
    }

    fn observe_prepare_runtime_state(&self, prepare: &Message<PrepareHeader>) {
        let header = prepare.header();
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..header.size as usize];

        match header.operation {
            Operation::CreateTopicWithAssignments => {
                let request = PersistedCreateTopicRequest::decode_from(body)
                    .expect("create topic with assignments prepare must decode");
                // A topic may be created with zero partitions and grown later,
                // so there may be no consensus group to observe yet.
                if let Some(highest_consensus_group_id) = request
                    .partitions
                    .iter()
                    .map(|partition| partition.consensus_group_id)
                    .max()
                {
                    self.allocator.observe(highest_consensus_group_id);
                }
            }
            Operation::CreatePartitionsWithAssignments => {
                let request = PersistedCreatePartitionsRequest::decode_from(body)
                    .expect("create partitions with assignments prepare must decode");
                if let Some(highest_consensus_group_id) = request
                    .partitions
                    .iter()
                    .map(|partition| partition.consensus_group_id)
                    .max()
                {
                    self.allocator.observe(highest_consensus_group_id);
                }
            }
            _ => {}
        }
    }

    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();
        let persisted = journal.handle().header(header.op as usize).is_some();
        send_prepare_ok_common(consensus, header, Some(persisted)).await;
    }
}

/// In-process Register `Message<RequestHeader>`. Mirrors
/// `SimClient::register`: `session=0`, `request=0` per
/// [`RequestHeader::validate`]; empty body.
///
/// `cluster` + `view` from `consensus` for self-consistency before
/// `Project::project` overwrites. `release = 0` matches wire today; both
/// paths should switch to `consensus.release()` once
/// `ClientReleaseTooLow/TooHigh` lands.
///
/// Buffer is `size_of::<RequestHeader>()`; `prepare_request` transmutes into
/// `PrepareHeader` (also 256 bytes), no realloc.
fn build_register_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    user_id: u32,
) -> Message<RequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RequestHeader>();
    let mut msg = Message::<RequestHeader>::new(header_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
        &mut msg.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid RequestHeader");
    *header = RequestHeader {
        command: Command2::Request,
        operation: Operation::Register,
        size: u32::try_from(header_size).expect("RequestHeader size fits u32"),
        cluster: consensus.cluster(),
        view: consensus.view(),
        release: 0,
        client: client_id,
        session: 0,
        request: 0,
        // Replicated on the prepare so every replica resolves session -> user.
        user_id,
        // Route through the metadata consensus group. The chain-forwarded
        // prepare is re-routed on each peer by namespace; a `0` here would
        // hash to a non-zero shard with no metadata consensus and be
        // silently dropped (see `shard::router::route_typed`).
        namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
        ..RequestHeader::default()
    };
    msg
}

fn build_logout_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    session: u64,
    request: u64,
) -> Message<RequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RequestHeader>();
    let mut msg = Message::<RequestHeader>::new(header_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
        &mut msg.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid RequestHeader");
    *header = RequestHeader {
        command: Command2::Request,
        operation: Operation::Logout,
        size: u32::try_from(header_size).expect("RequestHeader size fits u32"),
        cluster: consensus.cluster(),
        view: consensus.view(),
        release: 0,
        client: client_id,
        session,
        request,
        // Metadata consensus group (see `build_register_request_message`).
        namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
        ..RequestHeader::default()
    };
    msg
}

fn build_complete_revocation_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    request: u64,
    body: &[u8],
) -> Message<RequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RequestHeader>();
    let total = header_size + body.len();
    let mut msg = Message::<RequestHeader>::new(total);
    {
        let slice = msg.as_mut_slice();
        slice[header_size..total].copy_from_slice(body);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RequestHeader>(&mut slice[..header_size])
                .expect("zeroed bytes are a valid RequestHeader");
        *header = RequestHeader {
            command: Command2::Request,
            operation: Operation::CompleteConsumerGroupRevocation,
            size: u32::try_from(total).expect("request size fits u32"),
            cluster: consensus.cluster(),
            view: consensus.view(),
            release: 0,
            client: client_id,
            // `validate()` requires session/request > 0 for non-register ops;
            // there is no real session (the commit path skips reply-caching).
            session: 1,
            request,
            namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            ..RequestHeader::default()
        };
    }
    msg
}

/// Build a `TruncatePartition` request attributed to the originating client.
///
/// Replicated through the standard client-request path so the commit records
/// `(client, session, request)` in the `ClientTable`. The client numbers
/// `DeleteSegments` in the same monotonic request sequence as every other
/// metadata op, so attributing the truncate to an internal id (or skipping the
/// commit) leaves a hole that fails the next op's `request == committed + 1`
/// preflight.
///
/// `template` is the client's own `DeleteSegments` header: it supplies the wire
/// `cluster` / `view` / `release` and the client's `request` number.
/// `client_id` / `session` are the bound VSR identity.
///
/// # Panics
/// If the total request size exceeds `u32::MAX`; a `TruncatePartition` body is
/// a few fixed-width fields, so this cannot happen in practice.
#[must_use]
pub fn build_truncate_partition_client_message(
    template: &RequestHeader,
    client_id: u128,
    session: u64,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    up_to_offset: u64,
) -> Message<RequestHeader> {
    build_truncate_partition_client_message_with_identifiers(
        template,
        client_id,
        session,
        WireIdentifier::numeric(stream_id),
        WireIdentifier::numeric(topic_id),
        partition_id,
        up_to_offset,
    )
}

/// [`build_truncate_partition_client_message`] with the client's raw wire
/// identifiers (name or id) instead of resolved numeric ids.
///
/// Used when the target does not resolve on the handling node: the truncate
/// still commits, and the apply rejects it as a committed result, keeping the
/// client's request sequence contiguous while surfacing the typed error.
///
/// # Panics
/// If the total request size exceeds `u32::MAX`; a `TruncatePartition` body is
/// a few small fields, so this cannot happen in practice.
#[must_use]
pub fn build_truncate_partition_client_message_with_identifiers(
    template: &RequestHeader,
    client_id: u128,
    session: u64,
    stream_id: WireIdentifier,
    topic_id: WireIdentifier,
    partition_id: u32,
    up_to_offset: u64,
) -> Message<RequestHeader> {
    let body = TruncatePartitionRequest {
        stream_id,
        topic_id,
        partition_id,
        up_to_offset,
    }
    .to_bytes();
    let header_size = size_of::<RequestHeader>();
    let total = header_size + body.len();
    let mut msg = Message::<RequestHeader>::new(total);
    {
        let slice = msg.as_mut_slice();
        slice[header_size..total].copy_from_slice(&body);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RequestHeader>(&mut slice[..header_size])
                .expect("zeroed bytes are a valid RequestHeader");
        *header = RequestHeader {
            command: Command2::Request,
            operation: Operation::TruncatePartition,
            size: u32::try_from(total).expect("request size fits u32"),
            cluster: template.cluster,
            view: template.view,
            release: template.release,
            client: client_id,
            session,
            request: template.request,
            namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            ..RequestHeader::default()
        };
    }
    msg
}

fn build_prepare_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    request: &RequestHeader,
    operation: Operation,
    body: &[u8],
) -> Message<PrepareHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let op = consensus.sequencer().current_sequence() + 1;
    let size = size_of::<PrepareHeader>() + body.len();
    let mut prepare = Message::<PrepareHeader>::new(size);
    let prepare_bytes = prepare.as_mut_slice();
    prepare_bytes[size_of::<PrepareHeader>()..size].copy_from_slice(body);

    let header_bytes = &mut prepare_bytes[..size_of::<PrepareHeader>()];
    let new_header = bytemuck::checked::try_from_bytes_mut::<PrepareHeader>(header_bytes)
        .expect("prepare header bytes should be valid");
    // Match `Project::project` (core/consensus/src/impls.rs): the primary
    // stamps the injected clock once here (wall time in production, virtual
    // under the simulator) so every replica's `StateHandler::apply` reads the
    // same `created_at`. A `0` stamp would persist a 1970-01-01
    // `created_at` on every CreateStream/CreateTopic/CreatePartitions. The
    // in-process callers that bypass `Project::project` build their prepare
    // through this helper directly (the CreateTopic/CreatePartitions
    // assignment rewrites, the UpdateTopic default-size rewrite, and the
    // PAT-cleaner delete); the stamp is load-bearing for the creates and inert
    // for the UpdateTopic rewrite and the delete, whose applies ignore it.
    // Shared `next_monotonic_timestamp` keeps the in-process path on the same
    // monotonic-clock guard as the wire path.
    let timestamp = consensus.next_monotonic_timestamp();
    *new_header = PrepareHeader {
        cluster: consensus.cluster(),
        size: u32::try_from(size).expect("prepare message size exceeds u32"),
        view: consensus.view(),
        release: request.release,
        command: Command2::Prepare,
        replica: consensus.replica(),
        client: request.client,
        parent: consensus.last_prepare_checksum(),
        request_checksum: request.request_checksum,
        request: request.request,
        commit: consensus.commit_max(),
        op,
        timestamp,
        operation,
        namespace: request.namespace,
        // Carry the acting user id so the in-apply RBAC gate sees the same
        // identity on every replica. The default projection copies it (see
        // `Project::project`); this helper builds prepares for the ops it
        // rewrites (the CreateTopic/CreatePartitions assignment rewrites, the
        // UpdateTopic default-size rewrite, and the PAT-cleaner delete), which
        // would otherwise reset it to 0 via `..Default::default()`.
        user_id: request.user_id,
        ..Default::default()
    };

    prepare
}

/// Eviction reason for a request `prepare_request` rejected as structurally
/// invalid.
const fn eviction_reason_for_invalid(operation: Operation) -> EvictionReason {
    if operation.is_client_allowed() {
        EvictionReason::InvalidRequestBody
    } else {
        EvictionReason::InvalidRequestOperation
    }
}

/// Resolve the acting user id to stamp into a client op's replicated
/// `RequestHeader`, so the in-apply RBAC gate (`crate::stm::authz`) reads the
/// same identity on every replica (WAL replay has no session table).
///
/// - `Ok(Some(id))`: overwrite the header's `user_id` with the committed
///   session's user; the wire-supplied value is never trusted.
/// - `Ok(None)`: leave it untouched. `Register` carries the credential-verified
///   user from login (a mid-`Register` client has no session yet, so resolution
///   would miss and fail-close, denying every login) and `Logout` is not gated
///   -- both keep their builder value.
/// - `Err(Unauthenticated)`: fail-closed. A client op whose `client_id` has no
///   session cannot be attributed, so it is denied rather than defaulted to
///   root (`user_id` 0 is the gate's all-allow short-circuit). Every caller
///   preflights a live session first (`request_preflight` dispatches only on
///   `New`), so this guards a future ingress that reaches here without one.
fn resolve_acting_user_id(
    operation: Operation,
    client_id: u128,
    client_table: &RefCell<ClientTable>,
) -> Result<Option<u32>, IggyError> {
    if matches!(operation, Operation::Register | Operation::Logout) {
        return Ok(None);
    }
    client_table
        .borrow()
        .get_user_id(client_id)
        .ok_or(IggyError::Unauthenticated)
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::stream::Streams;
    use crate::stm::user::Users;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::requests::topics::CreateTopicRequest;
    use iggy_common::variadic;
    use journal::prepare_journal::PrepareJournal;
    use message_bus::{ClientForwardFn, ConnectionLostFn, JoinHandle, ReplicaForwardFn, SendError};
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Frozen;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn eviction_reason_splits_client_and_internal_ops() {
        // Client-allowed op with a bad body evicts InvalidRequestBody; an internal
        // / unknown op (here `CreateTopicWithAssignments`) evicts
        // InvalidRequestOperation.
        assert_eq!(
            eviction_reason_for_invalid(Operation::CreateStream),
            EvictionReason::InvalidRequestBody,
        );
        assert_eq!(
            eviction_reason_for_invalid(Operation::CreateTopicWithAssignments),
            EvictionReason::InvalidRequestOperation,
        );
    }

    type TestMux = MuxStateMachine<variadic!(Users, Streams)>;

    /// Build a peer-shard-style `IggyMetadata` with `consensus`,
    /// `journal`, and `snapshot` all `None`. Enough to test the
    /// commit-notifier slot without standing up VSR / WAL infrastructure:
    /// the test picks `()` for `C` / `J` / `S` since no notifier code path
    /// touches their methods.
    fn peer_metadata() -> IggyMetadata<(), (), (), TestMux> {
        IggyMetadata::new(None, None, None, TestMux::default(), None)
    }

    #[test]
    fn commit_notifier_fires_with_received_operation() {
        let md = peer_metadata();
        let captured: Rc<RefCell<Vec<Operation>>> = Rc::new(RefCell::new(Vec::new()));

        let observer = Rc::clone(&captured);
        md.set_commit_notifier(Some(Rc::new(move |op| {
            observer.borrow_mut().push(op);
        })));

        md.fire_commit_notifier(Operation::CreateTopicWithAssignments);
        md.fire_commit_notifier(Operation::DeletePartitions);
        md.fire_commit_notifier(Operation::DeleteStream);

        let seen = captured.borrow();
        assert_eq!(
            seen.as_slice(),
            &[
                Operation::CreateTopicWithAssignments,
                Operation::DeletePartitions,
                Operation::DeleteStream,
            ],
            "notifier must observe every fired operation in order"
        );
    }

    #[test]
    fn commit_notifier_is_no_op_when_unset() {
        // No notifier installed: firing must not panic, must not allocate.
        // Mirrors the production-side guarantee that peer shards (no
        // notifier) take the same commit path as shard 0 (with notifier).
        let md = peer_metadata();
        md.fire_commit_notifier(Operation::CreateStream);
    }

    #[test]
    fn commit_notifier_can_be_replaced_and_cleared() {
        let md = peer_metadata();
        let first_count: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let second_count: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));

        let first_observer = Rc::clone(&first_count);
        md.set_commit_notifier(Some(Rc::new(move |_op| {
            *first_observer.borrow_mut() += 1;
        })));
        md.fire_commit_notifier(Operation::CreateStream);
        assert_eq!(*first_count.borrow(), 1);

        // Replace: the first closure must no longer run.
        let second_observer = Rc::clone(&second_count);
        md.set_commit_notifier(Some(Rc::new(move |_op| {
            *second_observer.borrow_mut() += 1;
        })));
        md.fire_commit_notifier(Operation::DeleteStream);
        assert_eq!(*first_count.borrow(), 1, "old notifier must be detached");
        assert_eq!(*second_count.borrow(), 1, "new notifier must take over");

        // Clear: subsequent fires must be no-ops.
        md.set_commit_notifier(None);
        md.fire_commit_notifier(Operation::DeleteTopic);
        assert_eq!(
            *second_count.borrow(),
            1,
            "cleared notifier must stay quiet"
        );
    }

    /// Minimal committed `Register` reply for `ClientTable::commit_register`,
    /// which reads only `client` and `commit` (the assigned session).
    fn register_reply(client: u128, session: u64) -> Message<ReplyHeader> {
        let header_size = size_of::<ReplyHeader>();
        let mut reply = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut reply.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are a valid ReplyHeader");
        *header = ReplyHeader {
            client,
            request: 0,
            commit: session,
            command: Command2::Reply,
            operation: Operation::Register,
            ..Default::default()
        };
        reply
    }

    #[test]
    fn resolve_acting_user_id_skips_register_and_logout() {
        // Session-lifecycle ops keep the user id their request builder set
        // (Register's login identity, Logout's ungated value); the ClientTable
        // is never consulted, so an empty table still yields `Ok(None)`.
        let client_table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
        for operation in [Operation::Register, Operation::Logout] {
            assert!(
                matches!(
                    resolve_acting_user_id(operation, 1, &client_table),
                    Ok(None)
                ),
                "{operation:?} must not be stamped"
            );
        }
    }

    #[test]
    fn resolve_acting_user_id_stamps_from_client_table() {
        // A gated client op takes the acting user from the committed session,
        // independent of any wire-supplied header value.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
        table.commit_register(CLIENT, ACTING_USER, register_reply(CLIENT, SESSION), |_| {
            false
        });
        let client_table = RefCell::new(table);

        match resolve_acting_user_id(Operation::CreateStream, CLIENT, &client_table) {
            Ok(Some(user_id)) => assert_eq!(user_id, ACTING_USER),
            other => panic!("expected Ok(Some({ACTING_USER})), got {other:?}"),
        }
    }

    #[test]
    fn resolve_acting_user_id_fails_closed_for_unknown_session() {
        // A gated client op with no committed session is denied, never
        // defaulted to root (user id 0).
        let client_table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
        match resolve_acting_user_id(Operation::CreateStream, 999, &client_table) {
            Err(IggyError::Unauthenticated) => {}
            other => panic!("expected Err(Unauthenticated), got {other:?}"),
        }
    }

    /// No-op bus: `prepare_request` builds a prepare without ever sending, so
    /// every method is an unused stub.
    #[derive(Debug, Default)]
    struct NoopBus;

    impl MessageBus for NoopBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
    }

    /// Single-node metadata plane whose `prepare_request` is callable. `J` is
    /// `PrepareJournal` in name only (the value is `None`) to satisfy the
    /// impl-block bound; no journal or snapshot is constructed.
    fn metadata_plane() -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        IggyMetadata::new(Some(consensus), None, None, TestMux::default(), None)
    }

    fn create_topic_request(client: u128, wire_user_id: u32) -> Message<RequestHeader> {
        let body = CreateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            partitions_count: 1,
            compression_algorithm: 0,
            message_expiry: 0,
            max_topic_size: 0,
            replication_factor: 1,
            name: WireName::new("t").unwrap(),
        }
        .to_bytes();
        let header_size = size_of::<RequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(&body);
            let header =
                bytemuck::checked::from_bytes_mut::<RequestHeader>(&mut slice[..header_size]);
            *header = RequestHeader {
                command: Command2::Request,
                operation: Operation::CreateTopic,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request: 1,
                user_id: wire_user_id,
                namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
                ..Default::default()
            };
        }
        message
    }

    #[test]
    fn prepare_request_stamps_create_topic_from_client_table_not_wire() {
        // `CreateTopic` is the projection that reaches `build_prepare_message`
        // through the post-stamp `header` copy, so the built prepare must carry
        // the ClientTable identity, not the (bogus) wire value. This pins the
        // stamp-then-re-read ordering: a hoist would ship the untrusted wire
        // value.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        const WIRE_USER: u32 = 999;
        let plane = metadata_plane();
        plane.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
            |_| false,
        );

        let prepare = plane
            .prepare_request(create_topic_request(CLIENT, WIRE_USER))
            .expect("CreateTopic is client-allowed");
        assert_eq!(
            prepare.header().operation,
            Operation::CreateTopicWithAssignments,
            "CreateTopic projects to the enriched form"
        );
        assert_eq!(
            prepare.header().user_id,
            ACTING_USER,
            "prepare must carry the ClientTable identity, not the wire value"
        );
    }

    #[test]
    fn prepare_request_stamps_create_topic_message_expiry_default() {
        // A `CreateTopic` carrying the `ServerDefault` sentinel (0) must be
        // rewritten at primary admission to the configured default, so the
        // replicated prepare -- and thus every replica's commit -- holds a
        // concrete expiry. Mirrors the `max_topic_size` sentinel resolution.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        const CONFIGURED_EXPIRY_MICROS: u64 = 7_200_000_000;
        let plane = metadata_plane();
        plane.set_default_message_expiry(CONFIGURED_EXPIRY_MICROS);
        plane.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
            |_| false,
        );

        // `create_topic_request` builds the body with `message_expiry == 0`.
        let prepare = plane
            .prepare_request(create_topic_request(CLIENT, ACTING_USER))
            .expect("CreateTopic is client-allowed");
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
        let persisted = PersistedCreateTopicRequest::decode_from(body)
            .expect("create topic with assignments prepare must decode");
        assert_eq!(
            persisted.request.message_expiry, CONFIGURED_EXPIRY_MICROS,
            "ServerDefault expiry must be stamped to the configured default at admission"
        );
    }

    /// Bus whose `send_to_client` parks forever while `stall` is set,
    /// recording each parked send in `stall_hits`. Models a client whose
    /// connection writer stalled, so an `on_ack` driver suspends at a wire
    /// send — an await the test can then cancel the driver at.
    #[derive(Debug, Default)]
    struct StallBus {
        stall: std::cell::Cell<bool>,
        stall_hits: std::cell::Cell<u32>,
    }

    // Cell fields make the futures !Send; fine on the single-threaded shard.
    #[allow(clippy::future_not_send)]
    impl MessageBus for StallBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            if self.stall.get() {
                self.stall_hits.set(self.stall_hits.get() + 1);
                std::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
    }

    fn create_stream_request(client: u128, request: u64, name: &str) -> Message<RequestHeader> {
        let body = iggy_binary_protocol::requests::streams::CreateStreamRequest {
            name: WireName::new(name).unwrap(),
        }
        .to_bytes();
        let header_size = size_of::<RequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(&body);
            let header =
                bytemuck::checked::from_bytes_mut::<RequestHeader>(&mut slice[..header_size]);
            *header = RequestHeader {
                command: Command2::Request,
                operation: Operation::CreateStream,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request,
                user_id: 0,
                namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
                ..Default::default()
            };
        }
        message
    }

    /// Reproduces the `commit_min must advance sequentially` shard-0 crash.
    ///
    /// `on_ack` drains (pops) the committable prefix off the pipeline and only
    /// then applies it, with awaits in between (journal read, wire send). Any
    /// driver of `on_ack` that is dropped at one of those awaits — a hyper
    /// HTTP handler future canceled by peer disconnect (`http/state.rs`), or
    /// any parked in-process submitter — strands the popped-but-unapplied
    /// entries: nothing can re-apply them (`repair_primary_self_acks` is
    /// re-ack-only, above `commit_max`), `commit_min` is pinned below
    /// `commit_max` (every login rejected `NotCaughtUp`), and the next commit
    /// that quorums panics the shard.
    ///
    /// The test parks a driver mid-commit exactly there, cancels it, and then
    /// delivers the next ack. Correct behavior: the stranded op is still in
    /// the pipeline and the late ack commits it and everything after it, in
    /// order. Broken behavior: panic "expected 2, got 3".
    #[compio::test]
    async fn dropped_on_ack_driver_must_not_lose_popped_commits() {
        use std::future::Future;

        const CLIENT: u128 = 1;
        // The session is the Register op's commit number; it must be > 0 and
        // sort at-or-under the commits of the three ops below (1..=3).
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal = journal::prepare_journal::PrepareJournal::open(
            &dir.path().join("journal.wal"),
            0,
        )
        .await
        .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            StallBus::default(),
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(Some(consensus), Some(journal), None, TestMux::default(), None);
        let consensus = md.consensus.as_ref().unwrap();

        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
            |_| false,
        );

        // Three prepares through the real primary path: pipeline entry, WAL
        // append, self-ack onto the loopback queue.
        for (i, name) in ["s1", "s2", "s3"].iter().enumerate() {
            let prepare = md
                .prepare_request(create_stream_request(CLIENT, i as u64 + 1, name))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            md.on_replicate(prepare).await;
        }
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let mut acks = loopback
            .into_iter()
            .map(|message| {
                message
                    .try_into_typed::<PrepareOkHeader>()
                    .expect("loopback holds self PrepareOks")
            })
            .collect::<Vec<_>>();
        assert_eq!(acks.len(), 3, "one self-ack per replicated prepare");
        let ack3 = acks.pop().unwrap();
        let ack2 = acks.pop().unwrap();
        let ack1 = acks.pop().unwrap();

        // Ack op 2 first: quorum for op 2 alone, but the contiguous prefix
        // still starts at the un-acked op 1, so nothing commits yet.
        md.on_ack(ack2).await;
        assert_eq!(consensus.commit_max(), 0);
        assert_eq!(consensus.commit_min(), 0);

        // Ack op 1: the quorum walk covers ops 1..=2, so this single driver
        // commits both. Poll it by hand until it parks at a stalled wire
        // send mid-`on_ack`, then cancel it — the moral equivalent of hyper
        // dropping an HTTP handler future on peer disconnect.
        consensus.message_bus().stall.set(true);
        {
            let mut driver = Box::pin(md.on_ack(ack1));
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            let mut parked_at_send = false;
            for _ in 0..1_000 {
                assert!(
                    driver.as_mut().poll(&mut cx).is_pending(),
                    "driver must park at the stalled wire send, not complete"
                );
                if consensus.message_bus().stall_hits.get() > 0 {
                    parked_at_send = true;
                    break;
                }
                // Let the runtime process the journal-read completion the
                // driver is waiting on, then poll again.
                compio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            assert!(
                parked_at_send,
                "driver never reached a wire send (commit_min={})",
                consensus.commit_min()
            );
            // Cancel mid-`on_ack`, with at least op 1 applied and the reply
            // send in flight.
            drop(driver);
        }
        consensus.message_bus().stall.set(false);
        assert_eq!(consensus.commit_max(), 2, "quorum walk advanced commit_max");
        assert!(
            consensus.commit_min() >= 1,
            "driver applied op 1 before parking at the wire send"
        );

        // Whatever the canceled driver left behind must still be
        // committable: delivering the ack for op 3 has to commit every
        // remaining op, in order. The broken commit path lost op 2 with the
        // dropped driver (popped, never applied) and panics here with
        // "commit_min must advance sequentially: expected 2, got 3".
        md.on_ack(ack3).await;
        assert_eq!(
            consensus.commit_min(),
            3,
            "late ack must commit the stranded op 2 and then op 3"
        );
        assert_eq!(consensus.commit_max(), 3);
        assert!(
            is_caught_up_primary(consensus),
            "gate must reopen once the prefix is fully applied"
        );
    }
}
