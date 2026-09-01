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
use crate::stm::snapshot::{
    FillSnapshot, MetadataSnapshot, RestoreSnapshotInPlace, Snapshot, SnapshotError,
};
use crate::stm::stream::{Streams, TruncatePartitionRequest};
use crate::stm::user::{DeletePersonalAccessTokenRequest, Users};
use crate::stm::{ConsensusGroupAllocator, StateMachine};
use consensus::{
    CLIENTS_TABLE_MAX, Canceled, ClientTable, ClientTableSnapshot, CommitLogEvent, CommitReply,
    Consensus, EvictionContext, FatalReason, Pipeline, PipelineEntry, Plane, PlaneIdentity,
    PlaneKind, PreflightOutcome, PrepareRollback, Project, ReplicaLogContext, RequestLogEvent,
    Sequencer, SessionEnd, SimEventKind, VsrConsensus, ack_preflight, ack_quorum_reached,
    apply_preflight_consensus_plane, build_eviction_message, build_reply_message,
    build_reply_message_with, build_result_rejection_reply, emit_sim_event, fatal,
    fence_old_prepare_by_commit, is_caught_up_primary,
    panic_if_hash_chain_would_break_in_same_view, peek_committable_head, pipeline_prepare_common,
    register_preflight, replicate_preflight, replicate_to_next_in_chain, request_preflight,
    send_eviction_to_client, send_prepare_ok as send_prepare_ok_common, verify_prepare_integrity,
};
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::CreatePartitionsRequest as WireCreatePartitionsRequest;
use iggy_binary_protocol::requests::partitions::CreatePartitionsWithAssignmentsRequest as PersistedCreatePartitionsRequest;
use iggy_binary_protocol::requests::topics::CreateTopicRequest as WireCreateTopicRequest;
use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest as PersistedCreateTopicRequest;
use iggy_binary_protocol::{
    Command, ConsensusHeader, EvictionReason, GenericHeader, Operation, PrepareHeader,
    PrepareOkHeader, ProtocolVersion, ReplyHeader, RoutedRequestHeader, WireDecode, WireEncode,
    WireName,
};
use iggy_common::calculate_checksum;
use iggy_common::variadic;
use iggy_common::{
    IggyByteSize, IggyError, IggyExpiry, MaxTopicSize, TopicCreateOptions, TopicRuntimeDefaults,
    UserId, topic_option_keys, validate_topic_segment_size,
};
use journal::local_gate::LocalGate;
use journal::superblock::{
    PingPongSuperblock, SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS, SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS,
    SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT, SuperblockStore,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use server_common::Message;
use server_common::iobuf::{Frozen, Owned};
use std::cell::{Cell, RefCell};
use std::mem::size_of;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info, warn};

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

    /// Mutable view for tests that need to fold state into a checkpoint by hand,
    /// the way [`Self::persist_snapshot`] does on the live path.
    #[cfg(test)]
    pub(crate) const fn snapshot_mut(&mut self) -> &mut MetadataSnapshot {
        &mut self.snapshot
    }

    /// Persist the snapshot to disk.
    ///
    /// # Errors
    /// Returns `SnapshotError` if serialization or I/O fails.
    pub fn persist(&self, path: &Path) -> Result<(), SnapshotError> {
        Self::write_durably(path, &self.encode()?)
    }

    /// Write already-encoded snapshot bytes to `path` durably: temp, fsync,
    /// rename, parent-dir fsync, with the integrity trailer appended.
    ///
    /// Split from [`Self::persist`] so the checkpoint path can hash and write one
    /// buffer instead of encoding the whole snapshot twice under the durability
    /// lock (the client table is folded in, so a second encode is a full
    /// re-serialization).
    ///
    /// # Errors
    /// `SnapshotError::Persist` if any write, fsync, or rename fails.
    fn write_durably(path: &Path, encoded: &[u8]) -> Result<(), SnapshotError> {
        use crate::stm::snapshot::PersistStage;
        use std::fs;
        use std::io::Write;

        let tmp_path = path.with_extension("bin.tmp");

        let mut file = fs::File::create(&tmp_path).map_err(|e| SnapshotError::Persist {
            stage: PersistStage::Write,
            source: e,
        })?;
        file.write_all(encoded)
            .map_err(|e| SnapshotError::Persist {
                stage: PersistStage::Write,
                source: e,
            })?;
        // Self-verifying trailer. The superblock's checkpoint pairing cannot stand in:
        // phase 1 of a checkpoint renames the new snapshot over `snapshot.bin`, so a
        // crash before the pairing write is the NORMAL crash-inside-a-checkpoint
        // outcome, and it recovers through the `checkpoint_op < snapshot_op` arm, which
        // accepts the snapshot with nothing to check it against.
        file.write_all(&snapshot_trailer(encoded))
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

    /// Load a snapshot from disk, with the [`checkpoint_checksum`] of the exact bytes
    /// read.
    ///
    /// The checksum comes from the file's bytes, never from re-encoding what was
    /// decoded: the pairing must survive a schema change. Adding a trailing
    /// `#[serde(default)]` field is the repo's forward-compatible migration (see
    /// [`SNAPSHOT_FORMAT_VERSION`](crate::stm::snapshot::SNAPSHOT_FORMAT_VERSION) for
    /// the rules), and an older file re-encodes with one MORE msgpack array element
    /// after it, so a re-encode checksum would diverge on the first boot of the new
    /// build and refuse every checkpointed node with its WAL prefix already drained.
    ///
    /// # Errors
    /// `SnapshotError::ChecksumMismatch` if the file carries an integrity trailer that
    /// does not match its payload, `SnapshotError::UnsupportedFormatVersion` if it was
    /// written in a format version this build does not read, or `SnapshotError` if the
    /// file cannot be read or deserialized.
    pub fn load(path: &Path) -> Result<(Self, u128), SnapshotError> {
        let data = std::fs::read(path)?;
        let (payload, checksum) = split_trailer(&data, path)?;
        Ok((Self::decode(payload)?, checksum))
    }
}

/// Framing marker for the snapshot integrity trailer, "ISNP". Distinguishes a sealed
/// snapshot from one written before the trailer existed, so a MISSING trailer can be
/// accepted (unverified, loudly) while a PRESENT but mismatching one refuses boot. A
/// bare checksum could not tell those apart, and guessing wrong in either direction is
/// unacceptable: silently accepting corruption, or bricking a healthy node.
const SNAPSHOT_TRAILER_MAGIC: u32 = 0x4953_4E50;

/// `magic` + the payload's [`checkpoint_checksum`].
const SNAPSHOT_TRAILER_LEN: usize = size_of::<u32>() + size_of::<u128>();

/// The integrity trailer for an encoded snapshot.
fn snapshot_trailer(encoded: &[u8]) -> [u8; SNAPSHOT_TRAILER_LEN] {
    let mut trailer = [0u8; SNAPSHOT_TRAILER_LEN];
    trailer[..4].copy_from_slice(&SNAPSHOT_TRAILER_MAGIC.to_le_bytes());
    trailer[4..].copy_from_slice(&checkpoint_checksum(encoded).to_le_bytes());
    trailer
}

/// Split a snapshot file into `(payload, checkpoint_checksum)`, verifying the trailer
/// when one is present.
///
/// A file without the trailer is a snapshot written before sealing: its whole contents
/// are the payload and the checksum is computed over them, exactly as the pairing
/// recorded it, so an upgrade boots. It replays unverified, which is the same trade the
/// WAL makes for entries no producer sealed, so it is warned about rather than
/// silently accepted.
fn split_trailer<'a>(data: &'a [u8], path: &Path) -> Result<(&'a [u8], u128), SnapshotError> {
    let sealed = data.len() >= SNAPSHOT_TRAILER_LEN
        && data[data.len() - SNAPSHOT_TRAILER_LEN..][..4] == SNAPSHOT_TRAILER_MAGIC.to_le_bytes();
    if !sealed {
        tracing::warn!(
            path = %path.display(),
            "metadata snapshot carries no integrity trailer; restored unverified \
             (written before snapshot sealing)"
        );
        return Ok((data, checkpoint_checksum(data)));
    }

    let (payload, trailer) = data.split_at(data.len() - SNAPSHOT_TRAILER_LEN);
    let expected = u128::from_le_bytes(
        trailer[4..]
            .try_into()
            .expect("a sealed trailer holds 16 checksum bytes"),
    );
    let actual = checkpoint_checksum(payload);
    if actual != expected {
        return Err(SnapshotError::ChecksumMismatch { expected, actual });
    }
    Ok((payload, expected))
}

/// The superblock's `checkpoint_checksum` over a snapshot's on-disk bytes: the same
/// `XxHash3_64` the WAL and superblock use, widened to the `u128` the durable record
/// reserves.
///
/// Both sides of the pairing hash BYTES rather than a state: the checkpoint hashes
/// what it wrote (`SnapshotCoordinator::persist_snapshot`), recovery hashes what it
/// read ([`IggySnapshot::load`]). Nothing re-serializes a decoded snapshot, so the
/// cross-check does not depend on decode-then-encode being byte-identical across
/// builds.
#[must_use]
pub fn checkpoint_checksum(encoded: &[u8]) -> u128 {
    u128::from(calculate_checksum(encoded))
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
/// Owns the data directory path and the snapshot creation function. The
/// three-phase checkpoint (persist snapshot, record the pairing durably, drain the
/// WAL) is orchestrated one layer up in `IggyMetadata::checkpoint_if_needed`, so
/// the superblock write can sit between persist and drain; this type owns only the
/// snapshot I/O and the last-checkpoint bookkeeping.
pub struct SnapshotCoordinator<M> {
    data_dir: std::path::PathBuf,
    create_snapshot: fn(&M, u64, u64) -> Result<IggySnapshot, SnapshotError>,
    /// Remaining-journal-slots threshold at which a checkpoint is forced.
    /// Defaults to [`Self::CHECKPOINT_MARGIN`]; bootstrap raises it to at
    /// least the configured prepare-queue depth (see the static assert and
    /// [`Self::set_checkpoint_margin`]).
    checkpoint_margin: Cell<usize>,
    /// `(checkpoint_op, checkpoint_checksum)` of the last snapshot persisted or
    /// recovered at boot, `(0, 0)` when none. A view-change superblock write reads
    /// this so it records the current pairing instead of regressing it to zero.
    last_checkpoint: Cell<(u64, u128)>,
}

impl<M> SnapshotCoordinator<M> {
    /// Default number of remaining journal slots at which a checkpoint is
    /// forced. Must stay >= the prepare-queue depth: the ops already
    /// pipelined while a checkpoint runs skip it and append into this
    /// margin.
    ///
    /// Public so a caller sizing a journal can refuse a slot count at or below it:
    /// such a journal checkpoints on every commit rather than on occupancy.
    pub const CHECKPOINT_MARGIN: usize = 64;

    #[must_use]
    pub fn new(
        data_dir: std::path::PathBuf,
        create_snapshot: fn(&M, u64, u64) -> Result<IggySnapshot, SnapshotError>,
    ) -> Self {
        Self {
            data_dir,
            create_snapshot,
            checkpoint_margin: Cell::new(Self::CHECKPOINT_MARGIN),
            last_checkpoint: Cell::new((0, 0)),
        }
    }

    /// Raise (never lower) the forced-checkpoint margin. Bootstrap calls
    /// this with the configured prepare-queue depth so a deeper pipeline
    /// keeps its guarantee of journal room while a checkpoint runs; the
    /// default margin stays the floor.
    pub fn set_checkpoint_margin(&self, margin: usize) {
        self.checkpoint_margin
            .set(margin.max(Self::CHECKPOINT_MARGIN));
    }

    /// On-disk location of the persisted snapshot; also the artifact state
    /// transfer serves and installs.
    #[must_use]
    pub fn snapshot_path(&self) -> std::path::PathBuf {
        self.data_dir
            .join(super::METADATA_DIR)
            .join(super::SNAPSHOT_FILE_NAME)
    }

    /// The last persisted checkpoint's `(op, checksum)`, `(0, 0)` when none.
    const fn last_checkpoint(&self) -> (u64, u128) {
        self.last_checkpoint.get()
    }

    /// Seed the last-checkpoint pairing at boot from the recovered snapshot, so the
    /// first post-boot view-change superblock write records the real pairing rather
    /// than `(0, 0)`.
    fn seed_last_checkpoint(&self, checkpoint_op: u64, checkpoint_checksum: u128) {
        self.last_checkpoint
            .set((checkpoint_op, checkpoint_checksum));
    }

    /// Whether the journal is low enough on capacity to force a checkpoint. Gates
    /// on the configurable margin, which bootstrap raises to at least the
    /// prepare-queue depth; default [`Self::CHECKPOINT_MARGIN`].
    fn should_checkpoint<J: JournalHandle>(&self, journal: &J) -> bool {
        journal
            .handle()
            .remaining_capacity()
            .is_some_and(|c| c <= self.checkpoint_margin.get())
    }

    /// Create and durably persist a snapshot at `commit_op`, record the pairing, and
    /// return its checksum. Does NOT drain the WAL: the caller must durably record
    /// the pairing in the superblock first, so a crash between persist and drain
    /// recovers a consistent checkpoint with the WAL intact. Synchronous, since
    /// snapshot creation and `std::fs` persistence never await.
    fn persist_snapshot(
        &self,
        stm: &M,
        commit_op: u64,
        created_at: u64,
        client_table: Option<ClientTableSnapshot>,
    ) -> Result<u128, SnapshotError> {
        let mut snapshot = (self.create_snapshot)(stm, commit_op, created_at)?;
        // Fold in the client table, which `create_snapshot` does not see since it is
        // not a state machine. Recovery restores it as the reconstruction floor for
        // the drained WAL prefix.
        snapshot.snapshot.client_table = client_table;
        // Encode once: the checksum and the on-disk record share one buffer, so the
        // snapshot is not serialized twice while the checkpoint lock freezes this
        // core, and the pairing is provably over the bytes that reach the file.
        let encoded = snapshot.encode()?;
        let checksum = checkpoint_checksum(&encoded);
        let path = self.snapshot_path();
        IggySnapshot::write_durably(&path, &encoded)?;
        self.last_checkpoint.set((commit_op, checksum));
        Ok(checksum)
    }

    /// Drain the snapshotted prefix below `last_op` to reclaim WAL space. Runs
    /// only after the pairing is durable (see [`Self::persist_snapshot`]).
    ///
    /// `last_op` itself is retained, one entry the snapshot has already
    /// superseded. It is this replica's commit point, and a `DoViewChange`
    /// carries a header for every op from there up. Draining it inclusively
    /// leaves that entry blank, and blank at the commit point is the one slot
    /// the merge can neither adopt nor discard: a quorum of senders that all
    /// checkpointed at the same op deadlocks the view change
    /// (`dvc_merge::merge_dvc_quorum`). Reclaiming one more entry is not worth
    /// a group that cannot elect.
    #[allow(clippy::future_not_send)]
    async fn drain<J: JournalHandle>(
        &self,
        journal: &J,
        last_op: u64,
    ) -> Result<(), SnapshotError> {
        let Some(drain_to) = last_op.checked_sub(1) else {
            return Ok(());
        };
        journal
            .handle()
            .drain(0..=drain_to)
            .await
            .map_err(SnapshotError::Io)?;
        Ok(())
    }
}

// A checkpoint pauses journal reclamation, not admission: while one runs,
// up to a full prepare queue of already-pipelined ops still needs journal
// room (their `on_replicate` drivers skip the in-flight checkpoint and
// append). The margin must cover them or the journal wraps mid-checkpoint.
const _: () =
    assert!(SnapshotCoordinator::<()>::CHECKPOINT_MARGIN >= consensus::PIPELINE_PREPARE_QUEUE_MAX);

/// Failures shared by the in-process metadata submit helpers.
///
/// Returned by [`IggyMetadata::submit_register_in_process`],
/// [`IggyMetadata::submit_logout_in_process`],
/// [`IggyMetadata::submit_request_in_process`], and
/// [`IggyMetadata::submit_delete_personal_access_token_in_process`]. Every variant is
/// transient: the caller retries on a later attempt, and the login/register
/// A committed bind, as returned by
/// [`IggyMetadata::submit_register_in_process`].
///
/// `epoch` is the fence the client must echo in the wire `session` field
/// (the register's commit op). `watermark` is the entry's highest committed
/// request number: 0 for a fresh session, the inherited value on a resume.
/// Callers that kept their own counter can ignore it; the HTTP gateway --
/// whose counter lives in the process that restarted -- seeds its
/// per-session numbering at `watermark + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundSession {
    pub epoch: u64,
    pub watermark: u64,
}

/// handler wraps them in `LoginRegisterError::Transient` so the SDK
/// read-timeout replays.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataSubmitError {
    /// Not primary / not Normal.
    NotPrimary,
    /// Primary but `commit_min < commit_max` (committed prefix not yet
    /// drained). Dispatching now would race ops inherited from a prior view;
    /// for `Register` that double-commits a register and bumps the epoch
    /// past the first reply's, fencing a live client.
    NotCaughtUp,
    /// Prepare queue full.
    PipelineFull,
    /// In-flight prepare from this client.
    InProgress,
    /// The pending prepare was canceled before commit (a view change reset
    /// the pipeline). The caller retries; the SDK read-timeout replay reaches
    /// the new primary.
    Canceled,
    /// The node this view names primary is not reachable from this shard, so
    /// a forwarded session operation never left. Nothing was proposed.
    PrimaryUnreachable,
    /// A forwarded session operation left but no verdict came back within the
    /// forward timeout. The proposal's outcome is unknown.
    ForwardTimedOut,
    /// The presented `client_id` already has a table entry owned by a
    /// DIFFERENT user. TERMINAL, unlike every sibling: retrying cannot help,
    /// and admitting it would run the caller's replicated ops under the
    /// entry owner's authority (`resolve_acting_user_id` reads the table).
    ///
    /// Reachable two ways, both of which this refusal closes:
    /// - a caller authenticating with its own valid credentials while
    ///   presenting someone else's `client_id` (the login frame's `client`
    ///   field is caller-supplied);
    /// - after a restart, when WAL-replay recovery has rebuilt entries under
    ///   the previous boot's ids while the HTTP id minter restarts at 1, so
    ///   an honest login lands on a recovered entry owned by another user.
    ClientIdOwnedByAnotherUser,
}

impl MetadataSubmitError {
    /// Whether a retry (here, or against another replica) could succeed.
    /// Every variant is transient by contract except the ownership refusal.
    /// Deliberately a deny-list: a new variant is transient by default, so
    /// adding one cannot silently surface a terminal error to clients.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        !matches!(self, Self::ClientIdOwnedByAnotherUser)
    }
}

impl std::fmt::Display for MetadataSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrimary => f.write_str("not primary in normal status"),
            Self::NotCaughtUp => f.write_str("primary not yet caught up on commit_journal"),
            Self::PipelineFull => f.write_str("metadata prepare queue is full"),
            Self::InProgress => f.write_str("another in-flight prepare from this client"),
            Self::Canceled => f.write_str("view change canceled the pending prepare"),
            Self::PrimaryUnreachable => f.write_str("no route to the metadata primary"),
            Self::ForwardTimedOut => {
                f.write_str("the metadata primary did not answer the forwarded register")
            }
            Self::ClientIdOwnedByAnotherUser => {
                f.write_str("client id already registered to a different user")
            }
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

/// Apply one committed prepare to the state machine and client table.
///
/// The backup commit walk's per-op logic, shared so the simulator's WAL
/// reconstruction reaches identical state from the same log through one apply path.
/// Register creates or rebinds a session (no state-machine op); Logout drops the
/// session and rebalances consumer groups; every other op applies to the state
/// machine and caches the reply for at-most-once dedup. `fire_notifier` runs the
/// post-commit hook (a no-op during reconstruction, before it is wired). Does not
/// advance `commit_min`; the caller owns that counter.
///
/// `table_mutations_allowed` gates the CLIENT-TABLE half only; the state
/// machine always applies. After a state transfer the two artifacts sit at
/// different frontiers -- the snapshot at `S`, the transferred table at
/// `C >= S` -- so the tail replay over `(S, commit_max]` must run every state
/// machine effect (they are above the snapshot) while skipping table effects at
/// or below `C` (they are already in the transferred table). Pass `true`
/// wherever no transfer is in play.
///
/// # Panics
/// If a committed op fails to apply, which is a decode/corruption bug, since a
/// business rejection commits as a no-op rather than erroring: the committed log
/// must apply cleanly on every replica.
pub fn apply_committed_prepare<M>(
    mux_stm: &M,
    client_table: &RefCell<ClientTable>,
    table_mutations_allowed: bool,
    fire_notifier: impl Fn(Operation),
    prepare: Message<PrepareHeader>,
) where
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    let header = *prepare.header();
    if header.operation == Operation::Register {
        // Register: commit_register creates the session, no state-machine op.
        if table_mutations_allowed {
            let reply = build_reply_message(&header, &bytes::Bytes::new());
            client_table
                .borrow_mut()
                .commit_register(header.client, header.user_id, reply);
        }
        return;
    }
    if header.operation == Operation::Logout {
        if table_mutations_allowed {
            client_table.borrow_mut().remove_client(
                header.client,
                header.user_id,
                SessionEnd::from_logout_request(header.request),
            );
        }
        // Drop the disconnected client from every consumer group it joined and
        // rebalance, Logout's only state-machine effect.
        mux_stm.streams().remove_consumer_group_member(
            header.client,
            iggy_common::IggyTimestamp::from(header.timestamp),
        );
        return;
    }
    // Normal op: apply, build the reply. `Err` is decode/corruption only; a
    // business rejection commits as a deterministic no-op whose code rides
    // the reply body, replayed on retry.
    let apply = gated_apply(mux_stm, prepare).unwrap_or_else(|err| {
        panic!(
            "apply_committed_prepare: committed metadata op={} failed to apply: {err}",
            header.op
        );
    });
    fire_notifier(header.operation);
    let reply = build_reply_message_with(&header, apply.reply_body_len(), |dst| {
        apply.write_reply_body(dst);
    });
    // Best-effort cache; a WAL replay may carry a reply for a later-evicted
    // client, and replica-local eviction makes a stale-request replay
    // reachable. Both are skips, not faults.
    if table_mutations_allowed {
        let outcome = client_table
            .borrow_mut()
            .commit_reply(header.client, header.user_id, reply);
        log_commit_reply_outcome(outcome, header.client, header.op);
    }
}

/// Late-bound callback invoked after every committed op on shard 0's metadata
/// commit path (via `gated_apply`, including a gated no-op).
///
/// Wired by the server bootstrap once the metadata bundle has broadcast;
/// receives the committed [`Operation`] so the recipient can filter (the
/// partition reconciliation loop only cares about partition-shaped
/// events). Wrapped in [`RefCell`] for late binding; the per-shard
/// single-thread invariant keeps access safe without [`Sync`].
pub type CommitNotifier = std::rc::Rc<dyn Fn(Operation)>;

pub struct IggyMetadata<C, J, S, M, SB = PingPongSuperblock> {
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
    /// `server/src/bootstrap.rs` `await_metadata_bundle` /
    /// `broadcast_metadata_bundle`).
    pub journal: Option<J>,
    /// `Some` on shard 0, `None` on other shards.
    pub snapshot: Option<S>,
    /// Durable VSR-state record (`view`/`log_view`/`commit`). `Some` only on the
    /// shard owning metadata consensus (shard 0); `None` on peer shards. Generic
    /// (`PingPongSuperblock` in production, `SimSuperblock` in the simulator,
    /// recording doubles in tests) and behind `Rc` so the simulator harness can
    /// keep a clone outliving a replica across a restart.
    pub superblock: Option<Rc<SB>>,
    /// Serializes superblock writes on shard 0 so at most one is in flight.
    /// View-change persists ([`Self::persist_superblock_if_needed`]) and checkpoints
    /// ([`Self::checkpoint_if_needed`]) share the one ping-pong superblock, and
    /// in-process metadata submits each run on their own spawned task, so both can
    /// reach a write concurrently. `PingPongSuperblock::write` picks its slot before
    /// it awaits, so two overlapping writers would target the same slot and could
    /// tear it.
    ///
    /// Scoped to the write itself, NOT to a whole checkpoint. A pending view persist
    /// blocks every gated send behind it, including the ack path's
    /// `send_prepare_ok`, so it must not also wait out a checkpoint's snapshot
    /// encode, two `std::fs` fsyncs and an async WAL drain. Whoever writes builds
    /// its `VsrState` inside this section with no await in between, so the last
    /// writer carries the freshest view and the durable view cannot regress.
    ///
    /// Held across the write `.await`, so it uses the same single-threaded,
    /// cancel-safe [`LocalGate`] as `journal_gate`, a `Cell` flag with no atomics:
    /// this shard is never `Sync`, so a `tokio::sync::Mutex` would only add an
    /// atomic RMW per gated send for exclusion the `Cell` already provides.
    superblock_lock: LocalGate,
    /// Serializes whole checkpoints against each other: `persist_snapshot` renames
    /// over the single `snapshot.bin` and `drain` rewrites the WAL through a shared
    /// `wal.tmp`, so two concurrent checkpoints would race both. Distinct from
    /// [`Self::superblock_lock`], which a checkpoint takes only for its own pairing
    /// write. A checkpoint holds this one and then acquires that one; nothing takes
    /// them in the other order.
    checkpoint_lock: LocalGate,
    /// Consecutive failed superblock writes, and the clock reading after which the
    /// next attempt may run. A persistent `ENOSPC` / `EIO` would otherwise re-run a
    /// full `atomic_replace` (create, write, fsync, rename, dir fsync) on every 10 ms
    /// consensus tick, on the executor that also serves partition traffic. Reset on
    /// the first success. See [`Self::persist_superblock_if_needed`] for the terminal
    /// policy.
    superblock_write_failures: Cell<u64>,
    superblock_retry_after_micros: Cell<u64>,
    /// State machine - lives on all shards
    pub mux_stm: M,
    pub allocator: ConsensusGroupAllocator,
    /// Snapshot coordinator - present when persistent checkpointing is configured.
    pub coordinator: Option<SnapshotCoordinator<M>>,
    /// Serializes `on_replicate`'s journal-mutation section (forced
    /// checkpoint + WAL append) across concurrent drivers (the pump loop,
    /// detached per-client submit tasks, repair). Ungated they race
    /// `SnapshotCoordinator::checkpoint`: every driver crossing the
    /// `remaining_capacity <= CHECKPOINT_MARGIN` boundary runs a full
    /// checkpoint, and the concurrent `journal.drain()` calls collide on the
    /// WAL rewrite -- shared `wal.tmp`, ENOENT for every rename that loses,
    /// short reads after the winner's reopen. Appends racing a drain are just
    /// as unsound: the drain's live-set partition misses an append landing
    /// mid-rewrite and the rewrite silently discards it. See [`LocalGate`].
    journal_gate: LocalGate,
    /// Per-client session state (sessions, dedup, eviction). Metadata-only.
    pub client_table: RefCell<ClientTable>,
    /// Late-bound post-commit notifier. Fires once per committed normal op
    /// after `gated_apply` returns (including a gated `Unauthorized` no-op that
    /// never reaches [`crate::stm::StateMachine::update`]) in both
    /// [`Plane::on_ack`] and [`Self::commit_journal`]. `None` until
    /// [`Self::set_commit_notifier`] runs (the server bootstrap on shard
    /// 0 sets it; peer shards and tests leave it `None`).
    commit_notifier: RefCell<Option<CommitNotifier>>,
    /// Client-table mutations at or below this op are already reflected in a
    /// state-transferred table, so the tail-repair commit walk must skip
    /// them (re-running `commit_register` would double-bump epochs). `0`
    /// outside state transfer (no op is skipped). Monotone per install.
    client_table_frontier: Cell<u64>,
    /// Last built [`StateTransferOffer`], shared by every requester of the same
    /// snapshot generation. Rebuilding per request re-reads and re-decodes the
    /// whole snapshot on shard 0's pump, and hands each requester its own
    /// multi-MB copy.
    transfer_offer_cache: RefCell<Option<Rc<StateTransferOffer>>>,
    /// Highest metadata op whose apply has been PUBLISHED on this node, shared
    /// by every shard.
    ///
    /// `consensus.commit_min()` answers the same question but exists only on
    /// shard 0, so a read served by a peer shard has no way to tell whether the
    /// node caught up to an op its client already saw committed. One
    /// process-wide atomic does, for one `Acquire` load on the read fast path.
    ///
    /// Written `Release` right after each apply's `publish()`, read `Acquire`;
    /// observing `>= op` therefore happens-after that publish, so a following
    /// left-right `enter()` is guaranteed to see the op. `fetch_max` rather
    /// than `store` because three writers move it -- the commit loop, the
    /// recovery seed, and a state-transfer install -- and only monotonicity
    /// makes their order irrelevant.
    applied_frontier: Arc<AtomicU64>,
}

impl<C, J, S, M, SB> IggyMetadata<C, J, S, M, SB>
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
        superblock: Option<Rc<SB>>,
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
            superblock,
            superblock_lock: LocalGate::new(),
            checkpoint_lock: LocalGate::new(),
            superblock_write_failures: Cell::new(0),
            superblock_retry_after_micros: Cell::new(0),
            mux_stm,
            allocator,
            coordinator,
            journal_gate: LocalGate::new(),
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
            commit_notifier: RefCell::new(None),
            client_table_frontier: Cell::new(0),
            transfer_offer_cache: RefCell::new(None),
            applied_frontier: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<C, J, S, M, SB> IggyMetadata<C, J, S, M, SB> {
    /// Share one process-wide applied frontier with every other shard.
    ///
    /// Consumed at construction rather than swapped in later: a shard that
    /// served a read against its own private cell would gate on a number that
    /// never moves. Shard 0 mints the cell in bootstrap, before any shard is
    /// built, and hands each shard a clone.
    #[must_use]
    pub fn with_applied_frontier(mut self, applied_frontier: Arc<AtomicU64>) -> Self {
        self.applied_frontier = applied_frontier;
        self
    }

    /// Highest metadata op this NODE has applied and published, readable on
    /// every shard. Reads gate on it so a client cannot be served state older
    /// than a write it already saw acked.
    #[must_use]
    pub fn applied_frontier(&self) -> u64 {
        self.applied_frontier.load(Ordering::Acquire)
    }

    /// Publish `op` as applied. Monotone, so a lower value is a no-op.
    ///
    /// Must run AFTER the apply's `publish()` and, on the commit path, in the
    /// same await-free region as `advance_commit_min`: a reader that sees the
    /// frontier must be guaranteed to see the op's effects.
    pub fn advance_applied_frontier(&self, op: u64) {
        self.applied_frontier.fetch_max(op, Ordering::Release);
    }

    /// Slot capacity of the LIVE client table, i.e. the largest transferred
    /// table this replica can absorb.
    ///
    /// Read at decode instead of a separately plumbed `clients_table_max`: the
    /// live table sizes itself to `max(configured, highest recovered slot + 1)`,
    /// so a serving primary can legitimately hold more entries than this node's
    /// raw config value and decoding against that value would reject every
    /// round.
    #[must_use]
    pub fn client_table_capacity(&self) -> usize {
        self.client_table.borrow().capacity()
    }

    /// Drop the cached state-transfer offer, releasing its snapshot copy.
    ///
    /// Called by the shard's expiry sweep once no requester holds an offer:
    /// the cache exists to collapse repeat builds within one rejoin, not to
    /// pin a snapshot for the life of the process.
    pub fn clear_state_transfer_offer_cache(&self) {
        self.transfer_offer_cache.borrow_mut().take();
    }

    /// Install (or replace) the post-commit notifier. Passing `None`
    /// removes any previous one. Server-ng bootstrap calls this on shard 0
    /// only; peer shards never commit metadata locally.
    pub fn set_commit_notifier(&self, notifier: Option<CommitNotifier>) {
        *self.commit_notifier.borrow_mut() = notifier;
    }

    /// Seed the coordinator's last-checkpoint pairing at boot from the recovered
    /// snapshot, so the first post-boot view-change superblock write records the real
    /// `(checkpoint_op, checksum)` instead of `(0, 0)`. No-op without a coordinator
    /// (peer shards, the simulator). Server-ng bootstrap calls this on shard 0 after
    /// cross-checking the pairing.
    pub fn seed_checkpoint_ref(&self, checkpoint_op: u64, checkpoint_checksum: u128) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.seed_last_checkpoint(checkpoint_op, checkpoint_checksum);
        }
    }

    /// Install the client table rebuilt by WAL-replay recovery
    /// ([`crate::impls::recovery::recover`]). Boot-time only, on the owning
    /// shard, before it serves traffic - replacing a live table would drop
    /// committed session state. (State transfer replaces a LIVE table via
    /// [`IggyMetadata::install_state_transfer`], which also stamps the
    /// frontier.)
    ///
    /// Refuses (leaving the live table in place) when the current table
    /// already holds sessions, which means a client registered before recovery
    /// installed its table. Dropping those entries would leave each client
    /// holding an epoch the table no longer knows, so its next request reads
    /// as `NoSession`. A refusal is deliberately not a panic: this runs on the
    /// boot path, where taking the node down is a worse outcome than booting
    /// with the sessions it already has.
    ///
    /// # Returns
    /// `true` when the recovered table was installed.
    pub fn install_client_table(&self, client_table: ClientTable) -> bool {
        let mut current = self.client_table.borrow_mut();
        if current.count() > 0 {
            error!(
                live_sessions = current.count(),
                recovered_sessions = client_table.count(),
                "install_client_table: refusing to replace a table that already holds sessions; \
                 keeping the live one"
            );
            return false;
        }
        *current = client_table;
        true
    }

    /// Client-table mutations at or below the frontier are already in the
    /// state-transferred table; the commit walk skips them (re-running
    /// `commit_register` would double-bump epochs). STM effects still apply
    /// -- the frontier fences the TABLE only.
    const fn client_table_mutation_allowed(&self, op: u64) -> bool {
        op > self.client_table_frontier.get()
    }

    /// Raise the forced-checkpoint margin to cover a configured
    /// prepare-queue depth (`[metadata] prepare_queue_depth`). Clamped to
    /// the built-in floor by the coordinator; no-op on shards without a
    /// coordinator.
    pub fn set_checkpoint_margin(&self, margin: usize) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.set_checkpoint_margin(margin);
        }
    }

    /// Size the VSR client table to `[metadata] clients_table_max`
    /// (see [`ClientTable::set_capacity`]). Boot-only, before any client
    /// registers and before [`Self::install_client_table`]: the resize
    /// rebuilds the table, so a recovered one installed first would be lost.
    pub fn set_clients_table_max(&self, max_clients: usize) {
        self.client_table.borrow_mut().set_capacity(max_clients);
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

/// Stop the process after a WAL append failed and left a claim
/// [`VsrConsensus::rollback_pipelined_prepare`] could not prove was still its own.
///
/// The frontier is then an op ahead of the WAL with no local path back: the failed
/// prepare was never broadcast, so no peer can repair from it. Stopping IS the repair,
/// not an escalation: recovery re-derives the frontier from the WAL, which the failed
/// write never reached. Every alternative keeps serving on numbers this replica just
/// proved it cannot trust.
fn fatal_on_unreconcilable_frontier<B>(
    consensus: &VsrConsensus<B>,
    op: u64,
    error: &std::io::Error,
    rollback: PrepareRollback,
) -> !
where
    B: MessageBus,
{
    fatal(
        FatalReason::UnreconcilableLogFrontier,
        &format!(
            "metadata replica {replica} failed to append op {op} ({error}) and could not hand \
             the op back ({rollback:?}); the in-memory frontier is ahead of the WAL with no \
             local path back, so this node stops and recovers its frontier from the log",
            replica = consensus.replica(),
        ),
    );
}

#[allow(clippy::future_not_send)]
impl<B, J, S, M, SB> Plane<VsrConsensus<B>> for IggyMetadata<VsrConsensus<B>, J, S, M, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
    J: JournalHandle,
    J::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    async fn on_request(
        &self,
        message: <VsrConsensus<B> as Consensus>::Message<RoutedRequestHeader>,
    ) {
        let Some(consensus) =
            require_shard_zero(self.consensus.as_ref(), "on_request", "consensus")
        else {
            return;
        };
        let client_id = message.header().client;
        let session = message.header().session;
        let request = message.header().request;
        let request_checksum = message.header().request_checksum;
        let operation = message.header().operation;
        let user_id = message.header().user_id;

        // Preflight first: dedup, eviction sends, cached-reply replay all
        // must run regardless of pipeline pressure. Wire-path ingress has no
        // home-shard transport context, so resends fall back to the
        // consensus-plane (best-effort by VSR id).
        let dispatch = if operation == Operation::Register {
            register_preflight(consensus, &self.client_table, client_id, user_id)
        } else {
            let outcome = request_preflight(
                consensus,
                &self.client_table,
                client_id,
                session,
                request,
                request_checksum,
            );
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
        if consensus.pipeline_is_full() {
            let push_result = consensus.push_queued_request(consensus::RequestEntry::new(message));
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

        // Before anything trusts `checksum` as an identity token, and before the WAL
        // takes the bytes. Every live prepare travels this path: unverified, a frame
        // corrupted between primary and backup is journaled as-is and re-served to
        // peers, which the interior-corruption boot refusal turns into an unbootable
        // node on the next restart.
        if let Err(reason) = verify_prepare_integrity(&header, message.as_slice()) {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                "discarding prepare: {reason}"
            );
            return;
        }

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

        // Verify hash chain integrity BEFORE checkpoint. `checkpoint_if_needed`
        // can drain WAL entries, making previous_header return None.
        if let Some(previous) = journal.handle().previous_header(&header) {
            panic_if_hash_chain_would_break_in_same_view(&previous, &header);
        }

        // Serialize the journal-mutation section (forced checkpoint + append)
        // across concurrent `on_replicate` drivers. Ungated, every driver
        // crossing the checkpoint boundary ran its own checkpoint and the
        // concurrent `drain()`s raced the WAL rewrite (`snapshot I/O error:
        // No such file or directory`) — the single-node "metadata prepare
        // queue is full" wedge. Held through the append so a drain can never
        // rewrite the WAL out from under a racing append either.
        let journal_gate = self.journal_gate.acquire().await;

        // Best-effort WAL reclamation. A failed checkpoint must NOT drop the
        // prepare: `pipeline_message` already pushed the pipeline entry and
        // pre-advanced the sequencer, so bailing out here leaves a phantom op
        // that no repair path re-prepares — the commit frontier gaps behind
        // it permanently and the pipeline wedges full. `CHECKPOINT_MARGIN >=
        // PIPELINE_PREPARE_QUEUE_MAX` (static assert above) guarantees the
        // append below still has room after a failed or skipped attempt; a
        // journal that truly wraps is refused by append's slot-collision
        // guard, not here.
        self.checkpoint_if_needed(consensus, journal).await;

        // Backup: gap check against the JOURNAL head, not the sequencer.
        //
        // The two frontiers can disagree. The sequencer is pre-advanced on the
        // primary by `push_prepare_entry` and re-synced on a backup only after a
        // successful append, so a replica can carry a sequencer one ahead of what
        // its WAL holds. Gating admission on it then rejects the very prepare that
        // would heal the log: a backup with `last_op = 44` refused op 45 because
        // its sequencer said to expect 46. The primary retransmits that op for the
        // life of the process, every backup logs an out-of-order gap, it never
        // reaches a commit quorum, and its client is never answered.
        //
        // `max(last_op, snapshot_op)`, never `last_op` alone. A state transfer
        // installs a snapshot that IS ops `..=snapshot_op` applied and truncates the
        // WAL above that floor rather than refilling below it, so `last_op` reads the
        // receiver as needing an op the snapshot already contains and no peer will
        // send again. That drop
        // never heals: an offer built on a quiet cluster carries `commit_op ==
        // snapshot_seq`, so the install lands `commit_min == commit_max`, and
        // `maybe_request_metadata_repair`, the only path that refills the head,
        // arms on `commit_min < commit_max`. With the other backup down the primary
        // needs this replica's ack to commit anything, so the plane stops on a
        // cluster still inside its quorum.
        //
        // The journal is the only frontier that answers "what can be appended
        // next", which is what this check is for, and the hash-chain verification
        // below is stated against it too. A prepare at or below the head that this
        // replica already holds was re-acked and returned above. What reaches HERE
        // is the next op or a gap, and not every gap is fillable: metadata repair
        // covers only `commit_min + 1 ..= commit_max`, so an interior hole below
        // the head and a forward gap above `commit_max` both sit outside it.
        let is_backup = consensus.is_follower();
        if is_backup {
            let handle = journal.handle();
            let journal_head = handle.last_op().unwrap_or(0).max(handle.snapshot_op());
            if header.op != journal_head + 1 {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    op = header.op,
                    expected = journal_head + 1,
                    sequencer_op = current_op,
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
        // On the primary the pre-advance in `push_prepare_entry` already claimed
        // this op, so a failed append has to hand it back or the next prepare
        // chains off a phantom (see `rollback_pipelined_prepare`). A refused rollback
        // leaves the op claimed with nothing durable behind it and no protocol path
        // back, so the process stops rather than serving on it.
        if let Err(e) = journal.handle().append(message.clone()).await {
            let rollback = consensus.rollback_pipelined_prepare(&header);
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                operation = ?header.operation,
                error = %e,
                rollback = ?rollback,
                "journal append failed"
            );
            match rollback {
                // `Unwound`: the op went back and the waiting client wakes with
                // `Canceled`. `NotPreAdvanced`: a backup never claimed it, advancing
                // only after its own append succeeds. Neither leaves a disagreement.
                PrepareRollback::Unwound | PrepareRollback::NotPreAdvanced => {}
                // Every refusal means the same thing: the claim could not be proved
                // still this prepare's, so it cannot be safely reversed. Not split
                // further, since deciding per variant which disagreements are
                // survivable is the case analysis stopping exists to avoid.
                PrepareRollback::Superseded { .. }
                | PrepareRollback::Overtaken { .. }
                | PrepareRollback::TailMismatch => {
                    fatal_on_unreconcilable_frontier(consensus, header.op, &e, rollback);
                }
            }
            return;
        }

        // Journal mutation done; wire traffic below must not hold the gate.
        drop(journal_gate);

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
            if !consensus.pipeline_holds_entry(header.op, header.prepare_checksum) {
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
            debug!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                "ack quorum received"
            );

            self.commit_committable_prefix().await;
        }
    }
}

impl<B, P, J, S, M, SB> PlaneIdentity<VsrConsensus<B, P>>
    for IggyMetadata<VsrConsensus<B, P>, J, S, M, SB>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    fn is_applicable<H>(&self, message: &<VsrConsensus<B, P> as Consensus>::Message<H>) -> bool
    where
        H: ConsensusHeader,
    {
        assert!(matches!(
            message.header().command(),
            Command::Request | Command::Prepare | Command::PrepareOk
        ));
        message.header().operation().is_metadata_plane()
    }
}

/// One state-transfer serving payload.
///
/// The on-disk snapshot payload plus the live client table, both
/// frontier-stamped. Built by [`IggyMetadata::state_transfer_offer`] on the
/// serving primary; the shard serves chunks out of it and shares one instance
/// across every requester of the same snapshot generation.
pub struct StateTransferOffer {
    /// Serving primary's applied frontier when the offer was built; the
    /// receiver's tail repair targets past this.
    pub commit_op: u64,
    /// The offered snapshot's `sequence_number`, i.e. the generation this
    /// offer describes. Reused as the cache key: a later checkpoint rewrites
    /// `snapshot.bin` and invalidates every payload below.
    pub snapshot_seq: u64,
    /// Manifest entries paired with their bytes. One `Vec` of pairs rather
    /// than two index-aligned `Vec`s: the manifest is encoded in one file and
    /// the chunks served in another, so a desync would be invisible at both
    /// ends. Metadata plane: `[METADATA_SNAPSHOT (frontier = sequence_number),
    /// CLIENT_TABLE (frontier = commit_min at encode)]`.
    ///
    /// Payloads are refcounted so n simultaneous rejoiners share one copy
    /// rather than pinning n multi-MB snapshots on shard 0.
    pub artifacts: Vec<(consensus::StateArtifact, Rc<Vec<u8>>)>,
}

impl StateTransferOffer {
    /// Manifest entries for the descriptor body.
    #[must_use]
    pub fn manifest(&self) -> Vec<consensus::StateArtifact> {
        self.artifacts.iter().map(|(entry, _)| *entry).collect()
    }

    /// Bytes of the artifact at `index` in manifest order.
    #[must_use]
    pub fn payload(&self, index: usize) -> Option<&[u8]> {
        self.artifacts.get(index).map(|(_, bytes)| bytes.as_slice())
    }

    /// Number of artifacts on offer.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.artifacts.len()
    }

    /// Whether the offer carries no artifacts at all.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.artifacts.is_empty()
    }

    /// Total advertised bytes across every artifact.
    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.artifacts.iter().map(|(entry, _)| entry.len).sum()
    }
}

/// Why this replica cannot serve a state transfer right now.
///
/// Named rather than folded into `None` so the refusal the requester sees is
/// logged with its actual cause: "no snapshot persisted" and "snapshot.bin is
/// corrupt" call for opposite operator responses.
#[derive(Debug)]
pub enum StateTransferUnavailable {
    /// Not a caught-up primary, so a client-table read would not be
    /// authoritative.
    NotCaughtUpPrimary,
    /// This shard has no snapshot coordinator, so it never checkpoints.
    NoCoordinator,
    /// No snapshot has ever been persisted. The WAL still holds the full
    /// history, so the requester's journal repair covers its whole gap.
    NoSnapshot,
    /// `snapshot.bin` exists but could not be read, or failed its integrity
    /// trailer. Refusing is strictly better than shipping it: the receiver
    /// would re-seal the corruption under a fresh valid trailer.
    SnapshotUnreadable(SnapshotError),
}

impl std::fmt::Display for StateTransferUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCaughtUpPrimary => write!(f, "not a caught-up primary"),
            Self::NoCoordinator => write!(f, "no snapshot coordinator on this shard"),
            Self::NoSnapshot => write!(f, "no snapshot has been persisted yet"),
            Self::SnapshotUnreadable(source) => {
                write!(f, "persisted snapshot is unreadable: {source}")
            }
        }
    }
}

impl std::error::Error for StateTransferUnavailable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SnapshotUnreadable(source) => Some(source),
            _ => None,
        }
    }
}

/// What a completed [`IggyMetadata::install_state_transfer`] landed.
///
/// A degraded install is reported HERE rather than as an `Err`, because it is
/// a success: the snapshot, table, frontiers and commit point are all in
/// place by the time the pairing write is attempted. Returning it as an error
/// invites a caller to treat a completed install as a failure and redo it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallOutcome {
    /// The receiver's new applied frontier, `max(snapshot_seq,
    /// local_applied)`. These differ whenever a serving peer offered a
    /// snapshot BEHIND this replica and the local state machine was kept.
    pub applied_frontier: u64,
    /// Whether the transferred checkpoint's `(checkpoint_op, checksum)`
    /// pairing reached the durable superblock.
    ///
    /// `false` leaves the install fully usable: the coordinator already holds
    /// the new pairing, so the next superblock write (view change or
    /// checkpoint) records it. Until then a crash recovers the PREVIOUS
    /// checkpoint and this replica transfers again -- correct, just wasted
    /// work.
    pub pairing_durable: bool,
}

impl<B, J, S, M, SB> IggyMetadata<VsrConsensus<B>, J, S, M, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
    J: JournalHandle,
    J::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    /// Build a state-transfer offer for a restarted peer.
    ///
    /// The snapshot is served as the on-disk PAYLOAD, with its integrity
    /// trailer verified and stripped. Both halves matter. Verified, because a
    /// flipped bit inside the payload that still msgpack-decodes would
    /// otherwise be re-sealed on the receiver under a fresh valid trailer and
    /// a matching pairing: a fault the source node refuses to boot over would
    /// become undetectable on the second node. Stripped, because the receiver
    /// re-persists what it is sent through `write_durably`, which appends a
    /// trailer of its own -- shipping the sealed file grows `snapshot.bin` by
    /// one trailer per transfer generation and leaves it byte-shape-different
    /// from a locally checkpointed one.
    ///
    /// The snapshot may be stale, which costs nothing: the receiver
    /// journal-repairs `(snapshot_seq, commit_max]` afterwards through the
    /// existing repair machinery. The table is encoded live at this instant;
    /// both frontier stamps read `commit_min` inside one synchronous region,
    /// so they are mutually consistent.
    ///
    /// The result is cached and shared: a repeat request for the same snapshot
    /// generation reuses it instead of re-reading and re-decoding the file on
    /// shard 0's pump.
    ///
    /// # Errors
    /// [`StateTransferUnavailable`] naming why this replica cannot serve.
    pub fn state_transfer_offer(&self) -> Result<Rc<StateTransferOffer>, StateTransferUnavailable> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(StateTransferUnavailable::NoCoordinator)?;
        if !is_caught_up_primary(consensus) {
            return Err(StateTransferUnavailable::NotCaughtUpPrimary);
        }
        let coordinator = self
            .coordinator
            .as_ref()
            .ok_or(StateTransferUnavailable::NoCoordinator)?;
        let path = coordinator.snapshot_path();
        if !path.exists() {
            return Err(StateTransferUnavailable::NoSnapshot);
        }
        let sealed = std::fs::read(&path)
            .map_err(|source| StateTransferUnavailable::SnapshotUnreadable(source.into()))?;
        // Verifies the trailer and hands back the payload alone.
        let (payload, _) =
            split_trailer(&sealed, &path).map_err(StateTransferUnavailable::SnapshotUnreadable)?;
        // Still decoded rather than read off `last_checkpoint()`: `write_durably`
        // renames before the parent-dir fsync, so a DirSync failure leaves the new
        // file live with that cell stale, and the offer would then under-advertise
        // the frontier it is actually shipping.
        let snapshot_seq = IggySnapshot::decode(payload)
            .map_err(StateTransferUnavailable::SnapshotUnreadable)?
            .sequence_number();

        // Reuse the cached offer for this generation. Only the SNAPSHOT half is
        // expensive to rebuild, and the cached table is merely older, never
        // incoherent: its frontier is stamped at its own encode, and the receiver
        // replays everything above that frontier during tail repair.
        if let Some(cached) = self.transfer_offer_cache.borrow().as_ref()
            && cached.snapshot_seq == snapshot_seq
        {
            return Ok(Rc::clone(cached));
        }

        let commit_op = consensus.commit_min();
        let table = self.client_table.borrow().encode();
        let offer = Rc::new(StateTransferOffer {
            commit_op,
            snapshot_seq,
            artifacts: vec![
                (
                    consensus::StateArtifact::for_bytes(
                        consensus::artifact_kind::METADATA_SNAPSHOT,
                        snapshot_seq,
                        payload,
                    ),
                    Rc::new(payload.to_vec()),
                ),
                (
                    consensus::StateArtifact::for_bytes(
                        consensus::artifact_kind::CLIENT_TABLE,
                        commit_op,
                        &table,
                    ),
                    Rc::new(table),
                ),
            ],
        });
        *self.transfer_offer_cache.borrow_mut() = Some(Rc::clone(&offer));
        Ok(offer)
    }

    /// Install a fetched state transfer: persist + restore the snapshot,
    /// replace the client table, and jump the commit state to the snapshot
    /// floor so the tail repair takes over from there.
    ///
    /// Ordering: persist FIRST (a crash mid-install must reboot from the
    /// transferred state, not the pre-transfer one), then the in-place STM
    /// restore (readers observe it on their next read), then the table +
    /// frontier, then journal/commit bookkeeping.
    ///
    /// The partition plane is deliberately untouched: partitions load
    /// whatever their disks hold at boot and repair through their own
    /// consensus groups. Topology changes the snapshot carries below the
    /// receiver's old frontier (topics created/deleted while it was down)
    /// fire no commit notifier -- convergence rests on the partition
    /// reconciler's periodic full diff against the committed STM, which
    /// reads the restored state on its next tick.
    ///
    /// Returns an [`InstallOutcome`]: the new applied frontier, plus whether
    /// the transferred checkpoint's pairing reached the durable superblock.
    ///
    /// # Errors
    /// [`SnapshotError`] when the snapshot bytes do not decode, the persist
    /// fails, or the in-place restore is rejected. Every `Err` here means
    /// NOTHING was installed.
    ///
    /// # Panics
    /// If called on a shard without consensus (state transfer is a shard-0
    /// concern).
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn install_state_transfer(
        &self,
        snapshot_bytes: &[u8],
        client_table: ClientTable,
        table_frontier: u64,
        commit_op: u64,
    ) -> Result<InstallOutcome, SnapshotError>
    where
        M: RestoreSnapshotInPlace<MetadataSnapshot>,
    {
        let consensus = self
            .consensus
            .as_ref()
            .expect("install_state_transfer: consensus only exists on shard 0");

        // Refuses a format version this build does not read, ahead of every frontier
        // move below: the bytes come from a peer, so its build picked the shape.
        let snapshot = IggySnapshot::decode(snapshot_bytes)?;
        let snapshot_seq = snapshot.sequence_number();

        // The one place a snapshot crosses builds, so the only place the release
        // stamp answers a question the local logs cannot.
        tracing::info!(
            snapshot_seq,
            format_version = snapshot.snapshot().version,
            writer_release = %ProtocolVersion(snapshot.snapshot().writer_release),
            "decoded a transferred metadata snapshot"
        );

        // Manifest coherence. `commit_op` and `table_frontier` arrive from the
        // serving peer and are applied to THIS replica's frontiers, so a
        // malformed descriptor would move them somewhere the artifacts do not
        // justify. A peer cannot have committed less than its own snapshot
        // contains, nor have encoded a table below its commit point: both are
        // built from one caught-up-primary read in `state_transfer_offer`.
        // Refuse rather than install, which drops the caller back to journal
        // repair with the local state untouched.
        if commit_op < snapshot_seq || table_frontier > commit_op {
            tracing::error!(
                snapshot_seq,
                commit_op,
                table_frontier,
                "incoherent state transfer manifest; refusing to install"
            );
            return Err(SnapshotError::IncoherentManifest {
                snapshot_seq,
                commit_op,
                table_frontier,
            });
        }

        // Checkpoints are node-local, so a healthy serving primary can offer
        // a snapshot BEHIND this replica's own applied frontier (each node
        // snapshots at its own watermark; a backup checkpoints an op or two
        // below the primary it later replaces). Restoring such a snapshot
        // would rewind the STM below `commit_min` with no way back: the
        // commit walk never revisits ops it already counted as applied, so
        // the rewound-over effects would be lost until the next transfer.
        // Keep the local STM (it is a superset) and let tail repair cover
        // `(commit_min, commit_op]`. The client table still installs below:
        // it comes from the serving primary's LIVE state at `table_frontier
        // == commit_op`, which is never behind this replica.
        // Preliminary read, only to decide whether the gates are needed; the
        // binding decision is re-derived under them below.
        let snapshot_ahead = snapshot_seq > consensus.commit_min();

        // Serialize the whole install against a concurrent checkpoint, in the
        // checkpoint's own lock order (`checkpoint_lock` then
        // `superblock_lock`), so the two cannot deadlock against each other.
        //
        // Both do the same pair of steps -- rewrite `snapshot.bin`, record
        // `(checkpoint_op, checksum)` -- and `checkpoint_if_needed` holds
        // `checkpoint_lock` across BOTH while taking `superblock_lock` only
        // around the pairing write. `superblock_lock` alone therefore
        // serializes nothing against the checkpoint's file rewrite: interleave
        // them and the file comes from one while the durable pairing describes
        // the other, which is exactly the torn pairing the superblock exists to
        // detect (a crash inside that window refuses boot with
        // `CheckpointChecksumMismatch`). Checkpoints run on spawned tasks, so a
        // prepare that passed preflight before the transfer armed can drive one
        // during this install's superblock await -- a transferring replica
        // withholds acks, but "should not be committing" is not an invariant
        // this path can rest on.
        //
        // Deadlock-free: nothing between here and the superblock write awaits,
        // and `write_superblock` takes no lock of its own.
        let _install_gates = if snapshot_ahead {
            let checkpoint = self.checkpoint_lock.acquire().await;
            let superblock = if self.superblock.is_some() {
                Some(self.superblock_lock.acquire().await)
            } else {
                None
            };
            Some((checkpoint, superblock))
        } else {
            None
        };

        // The gate waits above suspend this task while commits -- and whole
        // checkpoints -- run, so the preliminary read is stale once the locks
        // are held. `commit_min` is monotonic, so the only possible flip is
        // ahead -> not-ahead, landing in the table-only arm below; deciding on
        // the stale value instead would overwrite a newer checkpoint's
        // snapshot.bin, regress its pairing, and rewind the STM below the
        // applied frontier -- then panic on `set_commit_floor`'s anti-rewind
        // assert with the damage already durable.
        let local_applied = consensus.commit_min();
        let snapshot_ahead = snapshot_seq > local_applied;

        if snapshot_ahead && let Some(journal) = &self.journal {
            // Discard the WAL suffix above the incoming floor BEFORE anything
            // installs: the commit walk matches entries by op number alone, so
            // a pre-crash suffix a view change has since reassigned would be
            // applied as committed once the floor jump below pulls the walk
            // past it. Ahead of the snapshot restore so a failed truncate
            // aborts a not-yet-started install (the transfer retries) instead
            // of stranding a restored state machine without its floor jump,
            // which would double-apply the snapshot's ops on the next walk.
            // Committed ops the range covered come back through the
            // post-install tail repair; uncommitted ones were decided away by
            // the view change that made this replica a transfer receiver.
            // Serialization against appends holds as in
            // `reconcile_metadata_view_divergence`: the pump is
            // single-threaded and `replicate_preflight` refuses prepares
            // while `is_transferring`.
            let removed = journal
                .handle()
                .truncate_from(snapshot_seq + 1)
                .await
                .map_err(SnapshotError::Io)?;
            if removed > 0 {
                // The DVC snapshot's `(op, commit)` tag does not move when
                // entries are removed under it; left stale it would advertise
                // headers this replica can no longer serve.
                consensus.invalidate_local_dvc_suffix();
                tracing::warn!(
                    snapshot_seq,
                    removed,
                    "state transfer dropped {removed} journal entries above the incoming floor"
                );
            }
        }

        if snapshot_ahead {
            if let Some(coordinator) = &self.coordinator {
                // The transferred snapshot REPLACES the one the superblock's
                // `(checkpoint_op, checksum)` pairing describes, so the pairing has
                // to move with it. Left stale it does not refuse boot -- the
                // `checkpoint_op < snapshot_op` arm of `verify_checkpoint_pairing`
                // reads it as a lagging local checkpoint and accepts -- which is
                // worse than a refusal: the recorded checksum belongs to a snapshot
                // that no longer exists, so a torn or corrupt transferred snapshot
                // stops being detectable until some later local checkpoint happens
                // to rewrite the pairing.
                //
                // Write the received bytes verbatim rather than re-encoding the
                // decoded snapshot: the checksum below is taken over exactly the
                // bytes that reach the file, so the pairing provably describes it.
                debug_assert!(
                    snapshot_seq >= coordinator.last_checkpoint().0,
                    "a transferred snapshot must not land below the recorded \
                     checkpoint op ({} < {}); recovery would refuse boot with \
                     CheckpointAheadOfSnapshot",
                    snapshot_seq,
                    coordinator.last_checkpoint().0
                );
                let checksum = checkpoint_checksum(snapshot_bytes);
                IggySnapshot::write_durably(&coordinator.snapshot_path(), snapshot_bytes)?;
                coordinator.seed_last_checkpoint(snapshot_seq, checksum);
                tracing::info!(
                    checkpoint_op = snapshot_seq,
                    "state transfer recorded its checkpoint pairing"
                );
            } else {
                tracing::warn!(
                    snapshot_seq,
                    "installing state transfer without a snapshot coordinator; \
                     the transferred state will not survive a further restart"
                );
            }

            self.mux_stm
                .restore_snapshot_in_place(snapshot.snapshot())?;
        } else {
            tracing::info!(
                snapshot_seq,
                local_applied,
                "transferred snapshot at or below the local applied frontier; \
                 keeping the local state machine and installing the table only"
            );
        }

        *self.client_table.borrow_mut() = client_table;
        self.client_table_frontier.set(table_frontier);

        if snapshot_ahead {
            // Entries at or below the installed floor are superseded by the
            // snapshot; without this the journal's wrap-eviction assert trips
            // on pre-transfer residents the next time slots recycle.
            if let Some(journal) = &self.journal {
                let handle = journal.handle();
                if snapshot_seq > handle.snapshot_op() {
                    handle.set_snapshot_op(snapshot_seq);
                }
            }

            // The snapshot IS ops `..=snapshot_seq` applied: jump the applied
            // frontier (this is the op-jump the tail repair resumes from) and
            // let the announced commit point pull the walk target forward.
            // The walk matches WAL entries by op number alone, so this floor
            // is only safe because the truncate at the top of this install
            // already discarded every journal entry above it.
            consensus.set_commit_floor(snapshot_seq);
            if snapshot_seq > consensus.sequencer().current_sequence() {
                consensus.sequencer().set_sequence(snapshot_seq);
            }
            self.advance_applied_frontier(snapshot_seq);
        }
        // Before the superblock write, so the durable record carries the frontier
        // this transfer just established rather than the pre-transfer one.
        consensus.advance_commit_max(commit_op);

        // Make the transferred checkpoint durable, mirroring the ordering a local
        // checkpoint uses (persist snapshot -> record the pairing -> only then treat
        // it as the recovery floor). A crash before this lands recovers the previous
        // checkpoint with the WAL intact and the transfer simply retries; a crash
        // after it recovers the transferred state. Failing here withholds nothing
        // already written -- the snapshot on disk subsumes the recorded pairing, which
        // `verify_checkpoint_pairing` accepts -- so it is reported as a DEGRADED
        // install rather than a failed one.
        let mut pairing_durable = true;
        if snapshot_ahead && let Some(superblock) = self.superblock.as_ref() {
            // Already under `_install_gates`, acquired above; re-acquiring here
            // would deadlock on the same non-reentrant gate.
            pairing_durable = self.write_superblock(consensus, superblock.as_ref()).await;
            if !pairing_durable {
                tracing::error!(
                    snapshot_seq,
                    commit_op,
                    "state transfer installed but the superblock write failed; the \
                     transferred checkpoint is not durable yet"
                );
            }
        }

        Ok(InstallOutcome {
            applied_frontier: snapshot_seq.max(local_applied),
            pairing_durable,
        })
    }

    /// Submit `Register` from in-process, await commit. Wire reply still fires
    /// via `message_bus.send_to_client`; subscriber is additive.
    ///
    /// Every bind proposes -- there is deliberately no fast path returning an
    /// existing entry's state. A bind is a fencing event: only a committed
    /// Register moves the entry's epoch (to the register's commit op), and
    /// that bump is what fences the previous holder of this session
    /// (`RequestStatus::Fenced`). Short-circuiting a rebind would leave two
    /// live holders sharing one fence, the zombie scenario the epoch exists
    /// to kill. Rebinding onto an existing entry preserves its watermark and
    /// reply ring, which is how session resume works.
    ///
    /// # Returns
    /// [`BoundSession`]: the fence epoch the client must stamp into `session`,
    /// plus the entry's current watermark so a caller that lost its position
    /// (the HTTP gateway after a restart) can resume numbering above it.
    ///
    /// # Errors
    /// [`MetadataSubmitError`]. All transient except
    /// `ClientIdOwnedByAnotherUser`, which is terminal: `NotPrimary`,
    /// `PipelineFull`, `InProgress`, `Canceled`. Never `NotCaughtUp`: a
    /// not-caught-up primary parks the register in the request queue instead
    /// of bouncing it. `Canceled` dominates on view change; the new primary
    /// inherits via `commit_journal` and the SDK retries.
    ///
    /// # Panics
    /// On `client_id == 0` or shard without consensus.
    ///
    /// # Safety
    /// Catch-up gate load-bearing: a Register dispatched with
    /// `commit_min < commit_max` can double-commit against an inherited one,
    /// fencing the live client's fresh reply for no reason.
    #[allow(clippy::future_not_send)]
    pub async fn submit_register_in_process(
        &self,
        client_id: u128,
        user_id: u32,
    ) -> Result<BoundSession, MetadataSubmitError> {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_register_in_process: consensus only exists on shard 0");

        // Wrong node: waiting or queueing cannot fix that, the client must
        // re-route to the primary.
        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return Err(MetadataSubmitError::NotPrimary);
        }

        // OWNERSHIP GATE: the login frame's `client` field is caller-supplied,
        // and `resolve_acting_user_id` resolves authority for every replicated
        // op from this entry, so rebinding someone else's entry would run the
        // caller's ops under that user (and `commit_register` would clobber
        // its `user_id`). Refuse unless the authenticated user owns it.
        // Terminal (see `ClientIdOwnedByAnotherUser`). An owned entry falls
        // through: the rebind must commit so the epoch actually moves.
        //
        // Only a CAUGHT-UP primary may issue it, like both sibling readers of
        // this table (`request_preflight` and `register_preflight`, which gate
        // the same way): the refusal is terminal, so a lagging or diverged
        // replica answering it would deny a legitimate login off state it has
        // not finished applying, and the client would never learn to redirect.
        // Not caught up therefore SKIPS the check rather than refusing -- the
        // register goes on to park in the request queue below, and
        // `register_preflight` re-applies this gate when the commit path
        // promotes it, by which point the table is authoritative.
        if is_caught_up_primary(consensus) {
            let table = self.client_table.borrow();
            if let Some(owner) = table.get_user_id(client_id)
                && owner != user_id
            {
                warn!(
                    target: "iggy.metadata.diag",
                    client_id,
                    authenticated_user = user_id,
                    entry_owner = owner,
                    "refusing register: client id is registered to a different user"
                );
                return Err(MetadataSubmitError::ClientIdOwnedByAnotherUser);
            }
        }

        // Mirror wire-path register_preflight: a racing second prepare would
        // commit a second register and bump the epoch past the first reply's.
        // Surface pre-synthesis. Scans both the prepare queue and the request
        // queue, so a register absorbed below dedups its own replays.
        if consensus.pipeline_has_message_from_client(client_id) {
            return Err(MetadataSubmitError::InProgress);
        }

        let request = build_register_request_message(consensus, client_id, user_id);
        // Wire path runs `RoutedRequestHeader::validate` at network boundary;
        // in-process skips it. debug_assert pins drift.
        debug_assert!(
            {
                use iggy_binary_protocol::ConsensusHeader;
                request.header().validate().is_ok()
            },
            "build_register_request_message produced a header that fails validate()"
        );

        // Fence floor, snapshotted BEFORE dispatch. This register's op is
        // assigned above the journal tail, so it is strictly greater than
        // `commit_max` is now -- which is what lets the cancel path below tell
        // OUR fence from an older entry's that happened to survive.
        let epoch_floor = consensus.commit_max();

        // Not caught up (admitting a register while a committed op is still
        // unapplied risks a double-register fence bump) or prepare queue full:
        // absorb into the request queue instead of bouncing with a transient
        // error. The queued entry carries this caller's reply subscriber; the
        // commit path promotes it (`drain_request_queue_into_prepares`, which
        // re-runs `register_preflight` and so applies the ownership gate) as
        // soon as the in-flight batch drains, and the await below resolves
        // exactly like the direct dispatch would.
        if !is_caught_up_primary(consensus) || consensus.pipeline_is_full() {
            let (entry, receiver) = consensus::RequestEntry::with_subscriber(request);
            if consensus.push_queued_request(entry).is_err() {
                // Both queues full: honest terminal backpressure.
                return Err(MetadataSubmitError::PipelineFull);
            }
            return match receiver.await {
                // The reply's `commit` IS the fence `commit_register` just
                // stored (`build_reply_message` stamps it from the prepare's
                // op), so take it from there rather than re-reading the table.
                Ok(reply) => {
                    self.bound_session(client_id, Some(reply.header().commit), epoch_floor)
                }
                // Entry dropped before commit: view-change reset, or a
                // promotion-time preflight rejection.
                Err(Canceled) => self.bound_session(client_id, None, epoch_floor),
            };
        }
        // `prepare_request` only fails on `!is_client_allowed`; Register is
        // allowed, so unreachable. Panic loudly on regression instead of
        // smuggling through wire-eviction.
        let prepare = self
            .prepare_request(request)
            .expect("Operation::Register is client-allowed; prepare projection cannot fail");

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => self.bound_session(client_id, Some(reply.header().commit), epoch_floor),
            Err(Canceled) => self.bound_session(client_id, None, epoch_floor),
        }
    }

    /// Assemble the bind result in one table borrow.
    ///
    /// `committed_epoch` is `Some` when this call's own Register committed, in
    /// which case the fence comes from the reply that carries it. `None` is the
    /// view-change cancel path, where the fence has to be read back -- and is
    /// only ours if it sits above `epoch_floor`. An entry at or below the floor
    /// predates this register, so returning its epoch would hand the caller a
    /// fence that never moved, and nothing downstream would notice: a stale
    /// epoch satisfies `check_request`'s equality test, so there is no `Fenced`
    /// and no `EpochAhead` to surface it. `Canceled` instead, and the retry
    /// gets a real bind.
    ///
    /// `Canceled` also covers an absent entry (evicted between commit and
    /// read).
    fn bound_session(
        &self,
        client_id: u128,
        committed_epoch: Option<u64>,
        epoch_floor: u64,
    ) -> Result<BoundSession, MetadataSubmitError> {
        let table = self.client_table.borrow();
        let epoch = committed_epoch.or_else(|| {
            table
                .get_epoch(client_id)
                .filter(|&epoch| epoch > epoch_floor)
        });
        epoch
            .zip(table.get_watermark(client_id))
            .map(|(epoch, watermark)| BoundSession { epoch, watermark })
            .ok_or(MetadataSubmitError::Canceled)
    }

    /// Turn a non-`Dispatch` [`PreflightOutcome`] into the answer the home
    /// shard writes to the originating socket, or `None` to dispatch.
    ///
    /// Shard 0 cannot route by the VSR consensus `client_id` (its top bits are
    /// random, not home-shard routing bits), so the frame is returned to the
    /// home shard rather than sent from here;
    /// `handle_client_request` writes it by transport id, exactly like a fresh
    /// commit.
    fn answer_preflight(
        consensus: &VsrConsensus<B>,
        request_header: &RoutedRequestHeader,
        outcome: PreflightOutcome,
    ) -> Option<Result<Message<GenericHeader>, MetadataSubmitError>> {
        let client_id = request_header.client;
        match outcome {
            PreflightOutcome::Dispatch => None,
            PreflightOutcome::Replay(reply) => {
                if let Some(refusal) = unreplayable_secret_refusal(
                    request_header,
                    &reply,
                    consensus.commit_max(),
                    client_id,
                ) {
                    return Some(Ok(refusal));
                }
                let owned =
                    Owned::<{ server_common::MESSAGE_ALIGN }>::copy_from_slice(reply.as_slice());
                Some(
                    Message::<GenericHeader>::try_from(owned)
                        .map_err(|_| MetadataSubmitError::Canceled),
                )
            }
            PreflightOutcome::Evict(reason) => {
                let ctx = EvictionContext::from_consensus(consensus);
                Some(Ok(
                    build_eviction_message(ctx, client_id, reason).into_generic()
                ))
            }
            // In-flight prepare from this client: replaying the same request_id
            // is absorbed until the original commits, then served from cache.
            PreflightOutcome::NotReady => Some(Ok(build_result_rejection_reply(
                request_header,
                consensus.commit_max(),
                IggyError::TransientNotCommitted.as_code(),
            )
            .into_generic())),
            // Terminal refusal with a correlated reply so the SDK surfaces the
            // typed error instead of blocking until its read timeout.
            PreflightOutcome::Reject(code) => Some(Ok(build_result_rejection_reply(
                request_header,
                consensus.commit_max(),
                code,
            )
            .into_generic())),
            // Client-bug shapes (future epoch, id reused for a different
            // operation): replaying cannot help, so the home shard stays silent.
            PreflightOutcome::Drop => Some(Err(MetadataSubmitError::Canceled)),
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

        // Epoch guard: only propose a Logout when the slot still holds the
        // exact epoch this logout targets. A late disconnect-logout for a
        // reused client id (slot since rebound to a newer epoch) carries the
        // stale epoch and is dropped here, so it can never wipe the fresh
        // registration. A missing slot also fails the match and short-circuits.
        if self.client_table.borrow().get_epoch(client_id) != Some(session) {
            return Ok(consensus.commit_min());
        }

        // No catch-up gate here: a logout admitted mid-commit-window is
        // safe — the wire path has always dispatched non-register ops
        // without one, and the per-client dedup below covers the only
        // logout-vs-logout race. It simply pipelines behind the in-flight
        // batch and commits with it, so a one-shot client's session
        // teardown is latency, never an error.
        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return Err(MetadataSubmitError::NotPrimary);
        }

        if consensus.pipeline_has_message_from_client(client_id) {
            return Err(MetadataSubmitError::InProgress);
        }

        let request = build_logout_request_message(consensus, client_id, session, request);
        debug_assert!(
            {
                use iggy_binary_protocol::ConsensusHeader;
                request.header().validate().is_ok()
            },
            "build_logout_request_message produced a header that fails validate()"
        );

        // Prepare queue full: absorb into the request queue with this
        // caller's reply subscriber, promoted as
        // commits free slots.
        if consensus.pipeline_is_full() {
            let (entry, receiver) = consensus::RequestEntry::with_subscriber(request);
            if consensus.push_queued_request(entry).is_err() {
                return Err(MetadataSubmitError::PipelineFull);
            }
            return match receiver.await {
                Ok(reply) => Ok(reply.header().commit),
                Err(Canceled) => {
                    if self.client_table.borrow().get_epoch(client_id).is_none() {
                        Ok(consensus.commit_min())
                    } else {
                        Err(MetadataSubmitError::Canceled)
                    }
                }
            };
        }
        let prepare = self
            .prepare_request(request)
            .expect("Operation::Logout is client-allowed; prepare projection cannot fail");

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => {
                if self.client_table.borrow().get_epoch(client_id).is_none() {
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

        // Deliberately bounce-based (no request-queue absorption, unlike the
        // client submit paths above): the caller is the partition
        // reconciler's completion loop, which retries on its own tick, and
        // parking internal completions would tie up request slots that
        // client submits compete for.
        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }
        if consensus.pipeline_has_message_from_client(internal_client_id) {
            return Err(MetadataSubmitError::InProgress);
        }
        if consensus.pipeline_is_full() {
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
    /// `0`: never registered, so the commit path's `get_epoch(0)` is
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

        // Deliberately bounce-based (no request-queue absorption): the
        // caller is the background PAT cleaner, which simply retries the
        // deletion on its next sweep.
        if !is_caught_up_primary(consensus) {
            return Err(
                if consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring() {
                    MetadataSubmitError::NotCaughtUp
                } else {
                    MetadataSubmitError::NotPrimary
                },
            );
        }

        if consensus.pipeline_is_full() {
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
        let header = RoutedRequestHeader {
            client: 0,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
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
        message: Message<RoutedRequestHeader>,
    ) -> Result<Message<GenericHeader>, MetadataSubmitError> {
        let request_header = *message.header();
        let client_id = request_header.client;
        let session = request_header.session;
        let request = request_header.request;
        let request_checksum = request_header.request_checksum;

        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_request_in_process: consensus only exists on shard 0");

        // Not-primary is transient: the same request replayed against the
        // current primary commits fine. Reply with the explicit transient
        // frame (relayed to the socket by the home shard) so the client
        // replays immediately rather than waiting out its read-timeout.
        // `TransientNotAccepted` specifically: the request never entered the
        // pipeline here, so the client may re-issue it ANYWHERE -- including
        // under a fresh session after failing over to the current leader --
        // without double-apply risk. (`TransientNotCommitted` conversely means
        // the outcome is unknown and only a same-session replay is safe.)
        //
        // No catch-up gate: a non-register op admitted mid-commit-window
        // simply pipelines behind the in-flight batch (the wire path has
        // always done this); the register-specific invariant is guarded in
        // `submit_register_in_process` / `register_preflight`.
        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return Ok(build_result_rejection_reply(
                &request_header,
                consensus.commit_max(),
                IggyError::TransientNotAccepted.as_code(),
            )
            .into_generic());
        }

        // Dedup / epoch fence / eviction. shard 0 cannot route by the VSR
        // consensus `client_id` (its top bits are random, not home-shard
        // routing), so a Replay/Evict/NotReady is returned to the home shard as
        // the reply -- `handle_client_request` writes it to the originating
        // socket by transport id, exactly like a fresh commit. Drop (client-bug
        // already-applied / future-epoch) surfaces as Canceled so the home
        // shard stays silent.
        let outcome = request_preflight(
            consensus,
            &self.client_table,
            client_id,
            session,
            request,
            request_checksum,
        );
        if let Some(answer) = Self::answer_preflight(consensus, &request_header, outcome) {
            return answer;
        }

        // Prepare queue full: backpressure, not failure. Absorb into the
        // request queue with this caller's reply subscriber; the commit
        // path promotes it as slots free up and the await below resolves
        // with the committed reply. Only a full request queue is terminal
        // (`TransientNotAccepted`, re-issuable anywhere: the request never
        // entered a queue).
        if consensus.pipeline_is_full() {
            let (entry, receiver) = consensus::RequestEntry::with_subscriber(message);
            if consensus.push_queued_request(entry).is_err() {
                return Ok(build_result_rejection_reply(
                    &request_header,
                    consensus.commit_max(),
                    IggyError::TransientNotAccepted.as_code(),
                )
                .into_generic());
            }
            return match receiver.await {
                Ok(reply) => Ok(reply.into_generic()),
                // Queued entry dropped (view-change reset) or promoted then
                // canceled: outcome unknown, same-session replay only.
                Err(Canceled) => Ok(build_result_rejection_reply(
                    &request_header,
                    consensus.commit_max(),
                    IggyError::TransientNotCommitted.as_code(),
                )
                .into_generic()),
            };
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
        // Register is the one op whose admission requires the catch-up gate
        // (double-register epoch bump); its submit path checks the gate and
        // the check-to-dispatch section is synchronous. Non-register ops
        // dispatch mid-window by design (they pipeline behind the in-flight
        // batch, like the wire path always has).
        debug_assert!(
            prepare.header().operation != Operation::Register || is_caught_up_primary(consensus),
            "dispatch_prepare_and_await: register dispatched with the catch-up gate closed"
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
                Command::PrepareOk => match message.try_into_typed::<PrepareOkHeader>() {
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
        if !consensus.is_primary() || !consensus.is_normal() || consensus.is_transferring() {
            return;
        }
        let Some(journal) = self.journal.as_ref() else {
            return;
        };
        let self_replica = consensus.replica();

        // Snapshot durable, self-unacked pending ops, dropping the pipeline and
        // journal borrows before the `send_prepare_ok` awaits below.
        let mut headers: Vec<PrepareHeader> = Vec::new();
        consensus.with_pipeline(|pipeline| {
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
        });
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
                Command::PrepareOk => match message.try_into_typed::<PrepareOkHeader>() {
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

    /// Commit the committable prefix, ship the resulting wire replies, and
    /// promote queued requests into the freed prepare slots.
    ///
    /// Runs at the tail of every quorum-advancing `on_ack` and from the
    /// shard tick via [`Self::resume_stranded_commits`]. Safe under
    /// concurrent drivers: ownership of each op is arbitrated by the head
    /// revalidation inside the loop.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::future_not_send)]
    async fn commit_committable_prefix(&self) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

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
        let mut wire_replies: Vec<(CommitLogEvent, Message<ReplyHeader>)> = Vec::new();
        while let Some(prepare_header) = peek_committable_head(consensus) {
            // A committed prepare missing from the journal is divergence; a
            // warn-and-return here would strand `commit_min` behind
            // `commit_max` forever (nothing re-applies a skipped op), so
            // panicking is the answer. Journal compaction, if ever added,
            // must not remove entries at or above the commit floor.
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
            let head_is_ours = consensus.pipeline_head_header().is_some_and(|head| {
                head.op == prepare_header.op && head.checksum == prepare_header.checksum
            });
            if !head_is_ours {
                continue;
            }

            let mut entry = consensus
                .pop_committed_prepare()
                .expect("on_ack: revalidated head exists");

            let pipeline_depth = consensus.pipeline_len();
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
                if self.client_table_mutation_allowed(prepare_header.op) {
                    self.client_table.borrow_mut().commit_register(
                        prepare_header.client,
                        prepare_header.user_id,
                        reply.clone(),
                    );
                }
                reply
            } else if prepare_header.operation == Operation::Logout {
                // Logout unregisters the VSR client session on every replica.
                let reply = build_reply_message(&prepare_header, &bytes::Bytes::new());
                if self.client_table_mutation_allowed(prepare_header.op) {
                    self.client_table.borrow_mut().remove_client(
                        prepare_header.client,
                        prepare_header.user_id,
                        SessionEnd::from_logout_request(prepare_header.request),
                    );
                }
                // Drop the disconnected client from every consumer group it
                // joined and rebalance. Deterministic side-effect of the
                // Logout commit, applied identically on every replica. Runs
                // regardless of the table frontier: the STM was restored at
                // the snapshot floor, so ops above it still owe their STM
                // effects even where the table already reflects them.
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
                // Best-effort cache; the wire reply ships either way. Ops at
                // or below the state-transfer frontier are already reflected
                // in the transferred table and are skipped.
                if self.client_table_mutation_allowed(prepare_header.op) {
                    let outcome = self.client_table.borrow_mut().commit_reply(
                        prepare_header.client,
                        prepare_header.user_id,
                        reply.clone(),
                    );
                    log_commit_reply_outcome(outcome, prepare_header.client, prepare_header.op);
                }
                reply
            };
            consensus.advance_commit_min(prepare_header.op);
            // Paired with the counter bump, and before the reply leaves: a
            // client that holds this reply may re-home onto any shard and read,
            // and the read gate admits it only once the frontier covers the op.
            self.advance_applied_frontier(prepare_header.op);
            emit_sim_event(SimEventKind::OperationCommitted, &event);

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

        // Commits freed prepare slots and reopened the catch-up gate;
        // promote buffered requests so the pipeline stays busy and
        // absorbed submits (queued while this batch was mid-commit)
        // dispatch immediately.
        self.drain_request_queue_into_prepares().await;
    }

    /// Timer-driven backstop (shard pump tick) for commit work stranded by
    /// a canceled `on_ack` driver.
    ///
    /// The commit loop and the promotion of queued requests run at the tail
    /// of the quorum-advancing `on_ack` — inside whichever future delivered
    /// that ack, and that future can be dropped at any of its awaits
    /// (journal read, wire-reply send). The pipeline is then left with
    /// committed-but-unapplied entries (`commit_min < commit_max`) and/or
    /// still-queued requests, and on an idle server nothing re-drives them:
    /// `ack_quorum_reached` opens the commit path only when `commit_max`
    /// advances, which duplicate and repair acks never do. This tick entry
    /// re-runs the same commit path; a still-parked sibling driver loses
    /// the head revalidation and exits clean.
    ///
    /// Ordering note: register promotion requires the catch-up gate open
    /// (`register_preflight` drops the entry otherwise), and the gate can
    /// only be closed here while stranded commits exist — which the commit
    /// loop applies, reopening the gate, before promotion runs.
    #[allow(clippy::future_not_send)]
    pub async fn resume_stranded_commits(&self) {
        let Some(consensus) = self.consensus.as_ref() else {
            return;
        };
        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return;
        }
        let stranded_commits = consensus.commit_min() < consensus.commit_max();
        let promotable_requests = consensus
            .with_pipeline(|pipeline| !pipeline.request_queue_is_empty() && !pipeline.is_full());
        if !stranded_commits && !promotable_requests {
            return;
        }
        self.commit_committable_prefix().await;
    }

    /// Promote buffered requests into free prepare slots after a commit
    /// batch drains.
    ///
    /// # Safety
    /// Re-preflight per iteration: `commit_journal` may have advanced the
    /// client's watermark between push and drain (`Duplicate` /
    /// `AlreadyApplied` / `AlreadyRegistered`). Skipping produces a duplicate
    /// prepare and panics.
    #[allow(clippy::future_not_send)]
    async fn drain_request_queue_into_prepares(&self) {
        let consensus = self.consensus.as_ref().unwrap();
        // Promote while prepare slots exist. Requests are queued for two
        // reasons — prepare queue full at arrival, or (in-process register)
        // the catch-up gate was closed — so promotion is bounded by slots,
        // not by how many commits just freed: a whole burst absorbed during
        // one commit window drains the moment the window closes. Promoted
        // prepares are un-quorum'd, so they never re-close the gate here.
        loop {
            if consensus.pipeline_is_full() {
                break;
            }
            let req = consensus.pop_queued_request();
            let Some(mut req) = req else { break };

            let client_id = req.message.header().client;
            let session = req.message.header().session;
            let request = req.message.header().request;
            let request_checksum = req.message.header().request_checksum;
            let operation = req.message.header().operation;
            let user_id = req.message.header().user_id;
            // If preflight or projection rejects below, dropping `req` (and
            // the sender taken from it) wakes an in-process awaiter with
            // `Canceled`; its submit path re-checks the client table.
            let reply_sender = req.take_reply_sender();
            let dispatch = if operation == Operation::Register {
                register_preflight(consensus, &self.client_table, client_id, user_id)
            } else {
                let outcome = request_preflight(
                    consensus,
                    &self.client_table,
                    client_id,
                    session,
                    request,
                    request_checksum,
                );
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
            // Mirror `pipeline_prepare_common`, threading the queued
            // subscriber into the pipeline entry so the awaiter that parked
            // at enqueue time resolves on this prepare's commit.
            assert!(!consensus.is_follower(), "promotion: primary only");
            assert!(consensus.is_normal(), "promotion: status must be normal");
            assert!(
                !consensus.is_transferring(),
                "promotion: must not be transferring state"
            );
            consensus.verify_pipeline();
            match reply_sender {
                Some(sender) => {
                    consensus.pipeline_message_with_sender(PlaneKind::Metadata, &prepare, sender);
                }
                None => consensus.pipeline_message(PlaneKind::Metadata, &prepare),
            }
            self.on_replicate(prepare).await;
        }
    }
}

impl<B, P, J, S, M, SB> IggyMetadata<VsrConsensus<B, P>, J, S, M, SB>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
    SB: SuperblockStore,
{
    /// Persist the current VSR state to the superblock when the view changed since
    /// the last write. The split-brain gate: callers MUST invoke this before
    /// dispatching any view-scoped VSR message, so a replica that acted in a view can
    /// never recover an older one after a crash.
    ///
    /// It fences the SEND, not the ACT. By the time a caller reaches here the handler
    /// has already moved `view`, `log_view`, `status`, the sequencer and the pipeline,
    /// and `commit_journal` runs outside the gate, so a failed persist still applies
    /// committed ops locally. That is the VSR fence and it is sufficient: local state a
    /// crash forgets is state no peer ever saw, whereas an externalized view must be
    /// recoverable. Do not read this as "nothing changed until the write lands".
    ///
    /// `true` when the send may proceed, either because the state is now durable or
    /// because there was nothing to persist (peer shard, partition plane, or an
    /// unchanged view). `false` only when a write was attempted and failed, and the
    /// caller must withhold the send. The in-memory view stays ahead of the durable
    /// one, which a crash safely rolls back, and the next tick retries.
    ///
    /// Kept on a `B`/`P`-only impl, with no journal/snapshot/state-machine bounds, so
    /// every VSR dispatch site can gate on it regardless of its own bounds.
    #[allow(clippy::future_not_send)]
    pub async fn persist_superblock_if_needed(&self, consensus: &VsrConsensus<B, P>) -> bool {
        let Some(superblock) = self.superblock.as_ref() else {
            return true;
        };
        // Lock-free fast path: the steady state is an unchanged view with nothing to
        // write, and skipping the lock keeps every gated send off it, notably
        // `send_prepare_ok`, which runs this per metadata prepare. Safe because
        // `view`/`log_view` advance only on this single-threaded executor and no
        // `.await` sits between the `Cell` read and the return, so the value cannot
        // change under us; a concurrent advance is caught by the re-check below.
        if !consensus.needs_superblock_persist() {
            return true;
        }
        // A write that keeps failing must not re-run a full `atomic_replace` on every
        // 10 ms tick. Back off first, while still reporting `false` so the send stays
        // withheld: fail-closed is the point of this gate, and the backoff only bounds
        // what the retry costs.
        if consensus.clock_realtime_micros() < self.superblock_retry_after_micros.get() {
            return false;
        }
        // Serialize superblock writes on this shard: view-change persists here and
        // checkpoints share the one ping-pong superblock, whose `write` picks its slot
        // before it awaits, so two overlapping writers would target the same slot and
        // could tear it while both report success. Re-check needs-persist AFTER
        // acquiring the lock so check and write are atomic and a redundant caller
        // coalesces, finding the state already made durable by the writer it queued
        // behind.
        let _superblock = self.superblock_lock.acquire().await;
        if !consensus.needs_superblock_persist() {
            return true;
        }
        self.write_superblock(consensus, superblock.as_ref()).await
    }

    /// Write the current VSR state, paired with the last durable checkpoint, under
    /// [`Self::superblock_lock`].
    ///
    /// The caller must hold that lock. The state is captured HERE rather than passed
    /// in: with writes serialized and no await between the capture and the write, the
    /// last writer carries the freshest view, so the durable view cannot regress even
    /// when a checkpoint and a view change interleave. See `mark_superblock_durable`
    /// for why the written values, not a re-read, mark durability.
    ///
    /// # Terminal policy
    /// There is none beyond staying fenced: a replica that cannot record the view it
    /// is in must not act in it, so it withholds every view-scoped send, goes quiet,
    /// and its peers elect around it. Failures are counted and the retry interval backs
    /// off to [`SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS`], with the error logged on the
    /// first failure and then at each backoff step rather than per tick.
    ///
    /// TODO(fail-stop): a replica wedged here is dead weight an operator has to notice
    /// from logs. Fail-stopping the process is the answer, and
    /// [`fatal`] is now that primitive; wire it where the shard owns shutdown.
    #[allow(clippy::future_not_send)]
    async fn write_superblock(&self, consensus: &VsrConsensus<B, P>, superblock: &SB) -> bool {
        // Carry the last durable pairing forward so a view-change write never
        // regresses the `(checkpoint_op, checksum)` a checkpoint recorded. `(0, 0)`
        // with no checkpoint taken, or no coordinator (peer shards, the simulator).
        let (checkpoint_op, checkpoint_checksum) = self
            .coordinator
            .as_ref()
            .map_or((0, 0), SnapshotCoordinator::last_checkpoint);
        let state = consensus.vsr_state(checkpoint_op, checkpoint_checksum);
        match superblock.write(&state.to_bytes()).await {
            Ok(()) => {
                consensus.mark_superblock_durable(state.view, state.log_view);
                self.superblock_write_failures.set(0);
                self.superblock_retry_after_micros.set(0);
                true
            }
            Err(error) => {
                let failures = self.superblock_write_failures.get() + 1;
                self.superblock_write_failures.set(failures);
                let backoff = SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS
                    .saturating_mul(1 << failures.min(SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT))
                    .min(SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS);
                self.superblock_retry_after_micros
                    .set(consensus.clock_realtime_micros() + backoff);
                // Rate-limited to the backoff steps: the tick would otherwise emit this
                // every 10 ms for as long as the disk stays broken.
                if failures.is_power_of_two() {
                    tracing::error!(
                        target: "iggy.metadata.diag",
                        plane = "metadata",
                        replica_id = consensus.replica(),
                        view = state.view,
                        log_view = state.log_view,
                        superblock_write_failures = failures,
                        retry_in_micros = backoff,
                        %error,
                        "superblock persist failed; withholding every view-scoped send \
                         until it succeeds, so this replica stays quorum-invisible"
                    );
                }
                false
            }
        }
    }

    /// Consecutive failed superblock writes, `0` when the last one succeeded. Read by
    /// diagnostics: a non-zero value means this replica is fenced out of view changes.
    #[must_use]
    pub const fn superblock_write_failures(&self) -> u64 {
        self.superblock_write_failures.get()
    }
}

impl<B, P, J, S, M, SB> IggyMetadata<VsrConsensus<B, P>, J, S, M, SB>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
    SB: SuperblockStore,
    J: JournalHandle,
    J::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    /// Run a forced checkpoint when the journal is low on capacity.
    ///
    /// Diagnostic-only outcome: the caller holds the `journal_gate`, so this
    /// is single-flight by construction, and a failure is deliberately not
    /// surfaced as control flow — the prepare being replicated must proceed
    /// to its append regardless (see the phantom-op comment at the call
    /// site). The next prepare over the boundary simply retries.
    #[allow(clippy::future_not_send)]
    async fn checkpoint_if_needed(&self, consensus: &VsrConsensus<B, P>, journal: &J) {
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        // Serialize whole checkpoints against each other. In-process metadata submits
        // each run on their own spawned task (`bus.spawn` in the server's metadata submit
        // handler), so at the checkpoint margin two can enter here concurrently; without
        // this lock they would run concurrent `persist_snapshot`s over the single
        // `snapshot.bin` and concurrently `drain` the WAL, which rewrites through a
        // shared `wal.tmp`. Acquire BEFORE `should_checkpoint` so check and sequence are
        // atomic: the second caller re-checks under the lock, finds the margin restored
        // by the first's drain, and coalesces away the redundant work.
        //
        // The superblock write below takes `superblock_lock` for itself, so a
        // concurrent view persist (and with it every gated send, including the ack
        // path) waits only for that write and not for this whole sequence.
        let _checkpoint = self.checkpoint_lock.acquire().await;
        if !coordinator.should_checkpoint(journal) {
            return;
        }

        // Use commit_min (locally executed), not commit_max. WAL entries
        // between commit_min+1 and commit_max haven't been applied to the
        // state machine yet, draining them would lose data on crash.
        let snap_op = consensus.commit_min();
        // Stamp created_at from the injected consensus clock (seed-derived
        // under the simulator), not the wall clock, so replayed snapshots are
        // byte-identical.
        let created_at = consensus.clock_realtime_micros();

        // Durability ordering, must not be reordered: persist the snapshot, durably
        // record the (checkpoint_op, checksum, commit_max) pairing in the superblock,
        // THEN drain the snapshotted prefix from the WAL. A crash before the
        // superblock write recovers the prior checkpoint with the WAL intact; a crash
        // after it recovers the new one. Draining before the superblock points at the
        // new snapshot could strand committed ops on a crash. Each fallible step
        // withholds the rest and returns early, leaving the WAL undrained, so
        // `should_checkpoint` stays true and the next tick retries from the top at the
        // then-current commit_min. The prepare being replicated appends regardless
        // (see the phantom-op comment at the call site).
        let client_table = self.client_table.borrow().to_snapshot();
        let checksum = match coordinator.persist_snapshot(
            &self.mux_stm,
            snap_op,
            created_at,
            Some(client_table),
        ) {
            Ok(checksum) => checksum,
            Err(e) => {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op = snap_op,
                    error = %e,
                    "checkpoint snapshot persist failed"
                );
                return;
            }
        };

        if let Some(superblock) = self.superblock.as_ref() {
            // `persist_snapshot` already recorded the new pairing on the coordinator, so
            // `write_superblock` picks it up from there. A view persist that interleaves
            // between those two steps writes the same new pairing, which only makes it
            // durable sooner.
            debug_assert_eq!(
                self.coordinator
                    .as_ref()
                    .map(SnapshotCoordinator::last_checkpoint),
                Some((snap_op, checksum)),
                "the checkpoint's pairing must be what the superblock write records"
            );
            let _superblock = self.superblock_lock.acquire().await;
            if !self.write_superblock(consensus, superblock.as_ref()).await {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op = snap_op,
                    "checkpoint superblock write failed; withholding WAL drain"
                );
                return;
            }
        }

        if let Err(e) = coordinator.drain(journal, snap_op).await {
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                checkpoint_op = snap_op,
                error = %e,
                "checkpoint WAL drain failed"
            );
            return;
        }

        // Info, not debug: checkpoints are rare and change what a restart can
        // recover locally, and spec tests pin checkpoint placement by grepping
        // this line off stdout at the default `info` level
        // (`metadata_checkpoint_restart`, `metadata_state_transfer`).
        info!(
            target: "iggy.metadata.diag",
            plane = "metadata",
            replica_id = consensus.replica(),
            checkpoint_op = snap_op,
            "forced checkpoint completed"
        );
    }

    #[allow(clippy::too_many_lines)]
    fn prepare_request(
        &self,
        mut message: Message<RoutedRequestHeader>,
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
        // A Logout is not gated, so it gets no acting user above, yet its apply
        // removes a dedup fence keyed by user. Resolve the owner of the session
        // it ends here, on the primary, from the live entry or its fence, so
        // every replica drops the same fence; an unmatched session stamps 0
        // and the apply leaves the fences alone.
        if operation == Operation::Logout {
            let request_header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
                &mut message.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
            )
            .expect("a routed request header was validated on receipt");
            request_header.user_id = self
                .client_table
                .borrow()
                .user_id_for_session(client_id, request_header.session)
                .unwrap_or(0);
        }
        if let Some(acting_user_id) =
            resolve_acting_user_id(operation, client_id, &self.client_table)?
        {
            let request_header = bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
                &mut message.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
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
        let body = &message.as_slice()[size_of::<RoutedRequestHeader>()..header.size as usize];

        match header.operation {
            Operation::CreateTopic => {
                let mut request = WireCreateTopicRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                // Resolve every absent catalog key against server config here,
                // at primary admission, so the replicated payload carries
                // concrete values and every replica commits the same state
                // regardless of local config. Resolved defaults ride a separate
                // derived block, preserving per-key provenance for `GetTopic`.
                let explicit = TopicCreateOptions::parse(&request.options)?;
                // Re-encode the explicit block from the parse rather than
                // forwarding the client's bytes. Parsing normalizes a zero
                // sentinel to "absent", so the resolved value goes in the
                // derived block -- but apply merges with explicit winning, so a
                // forwarded literal `0` would land back on top as the stored
                // effective value. `GetTopic` would report 0, and a restart
                // would re-parse that 0 to absent and fall back to whatever the
                // node default is by then, not the value resolved at creation.
                // Re-encoding also canonicalizes kinds (a `"128MiB"` string
                // becomes `Uint64`), so the stored map reads back uniformly.
                request.options = explicit.to_wire()?;
                let resolved_segment_size = explicit
                    .segment_size
                    .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
                let resolved_max_topic_size = explicit
                    .max_topic_size
                    .unwrap_or_else(|| MaxTopicSize::from(iggy_common::DEFAULT_MAX_TOPIC_SIZE));
                // Backstop for the transport-side typed checks: an explicit
                // segment size outside its bounds, or a topic cap below one
                // segment, must never enter the WAL.
                if let Some(segment_size) = explicit.segment_size {
                    validate_topic_segment_size(
                        segment_size.as_bytes_u64(),
                        iggy_common::MAX_TOPIC_SEGMENT_SIZE,
                    )?;
                }
                if resolved_max_topic_size.as_bytes_u64() < resolved_segment_size.as_bytes_u64() {
                    return Err(IggyError::InvalidOptionValue(
                        topic_option_keys::MAX_TOPIC_SIZE.to_string(),
                    ));
                }
                let derived_options = explicit.derived_block(
                    explicit.compression_algorithm.unwrap_or_default(),
                    explicit
                        .message_expiry
                        .unwrap_or_else(|| IggyExpiry::from(iggy_common::DEFAULT_MESSAGE_EXPIRY)),
                    resolved_max_topic_size,
                    TopicRuntimeDefaults {
                        segment_size: resolved_segment_size,
                        enforce_fsync: explicit
                            .enforce_fsync
                            .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC),
                        messages_required_to_save: explicit
                            .messages_required_to_save
                            .unwrap_or(iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE),
                        size_of_messages_required_to_save: explicit
                            .size_of_messages_required_to_save
                            .unwrap_or_else(|| {
                                IggyByteSize::from(
                                    iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
                                )
                            }),
                        preallocate_segments: explicit
                            .preallocate_segments
                            .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS),
                    },
                )?;
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
                    derived_options,
                    partitions,
                    // Minted once here, on the admitting primary, and sealed by
                    // `checksum_body`: the header's `view` is restamped on a
                    // post-view-change retransmit, so a body-carried copy is the
                    // only per-op view every replica commits identically.
                    created_view: consensus.view(),
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
                    // Same body-carried view as `PersistedCreateTopicRequest`.
                    created_view: consensus.view(),
                }
                .to_bytes();
                Ok(build_prepare_message(
                    consensus,
                    &header,
                    Operation::CreatePartitionsWithAssignments,
                    &body,
                ))
            }
            // `UpdateTopic` deliberately takes the default arm: unlike create,
            // an update stores `ServerDefault` sentinels verbatim (legacy
            // parity), so a later get echoes `ServerDefault` instead of the
            // node default frozen at update time.
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

        #[allow(clippy::cast_possible_truncation)]
        let op = header.op as usize;
        assert_eq!(header.command, Command::Prepare);
        assert!(
            journal.handle().header(op).is_some(),
            "replicate: prepare must be durable in local journal before chain-forward"
        );
        if let Err(e) = replicate_to_next_in_chain(consensus, message).await {
            tracing::warn!(op = header.op, error = ?e, "chain replication failed");
        }
    }

    /// Apply ops `[commit_min+1 .. commit_max]` to state machine and
    /// `client_table`. Backup does NOT ship wire replies (primary's job).
    ///
    /// # Safety: ordering invariant
    ///
    /// `advance_commit_min(op)` and matching `client_table` mutation
    /// (`commit_register` / `commit_reply`) run back-to-back, no `.await`
    /// between. [`consensus::is_caught_up_primary`] reads
    /// `commit_min == commit_max` as proof the table is caught up; an await
    /// here lets another task observe transient equality with stale table,
    /// dispatch a fresh Register on an already-registered client, and bump
    /// the epoch past the reply the live client holds.
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
                // Gap-stop: the walk halts at the first missing prepare and
                // resumes once it is refilled. Live drops refill via the
                // primary's prepare retransmit; a replica behind at recovery
                // or after StartView adoption arms a `MetadataRepairSession`
                // (shard) that re-requests the missing window.
                break;
            };
            let header = *header;

            let Some(prepare) = journal.handle().entry(&header).await else {
                warn!("commit_journal: prepare body missing for op={op}, stopping");
                break;
            };

            // SM apply + client_table mutation BEFORE `advance_commit_min`
            // (see `on_ack` for matching invariant). No await between table
            // mutation and counter bump. The post-commit notifier (e.g. partition
            // reconciler wake-up) fires on backups too, so reconcilers converge
            // after replicated commits, not only quorum-acked ones reached via
            // `on_ack` on the primary.
            //
            // Table mutations are skipped at or below the state-transfer
            // frontier: those ops are already reflected in the transferred
            // table, while their state-machine effects still have to replay
            // (the snapshot sits at a lower op).
            apply_committed_prepare(
                &self.mux_stm,
                &self.client_table,
                self.client_table_mutation_allowed(header.op),
                |operation| self.fire_commit_notifier(operation),
                prepare,
            );
            consensus.advance_commit_min(op);
            self.advance_applied_frontier(op);
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
        // Durable-before-send: a PrepareOk implies this replica's (view, log_view), so
        // it must not leave until they are durable, or a crash could recover an older
        // view than the one this ack helped commit in, losing a committed op. Mirrors
        // the view-change dispatch gate; withhold on persist failure and let the
        // primary's prepare retransmit re-drive the ack once the next tick persists.
        if !self.persist_superblock_if_needed(consensus).await {
            return;
        }
        // Containment, not occupancy: after a refused append (slot collision,
        // view-change race) the slot can hold a DIFFERENT prepare at this op,
        // and acking it would vouch durability for bytes this replica never
        // journaled. Checksum equality withholds the ack; the primary's
        // retransmit re-drives it once the right prepare lands.
        let journal = self.journal.as_ref().unwrap();
        let persisted = journal
            .handle()
            .header(header.op as usize)
            .is_some_and(|stored| stored.checksum == header.checksum);
        send_prepare_ok_common(consensus, header, persisted).await;
    }
}

/// In-process Register `Message<RoutedRequestHeader>`. Mirrors
/// `SimClient::register`: `session=0`, `request=0` per
/// [`RoutedRequestHeader::validate`]; empty body.
///
/// `cluster` + `view` from `consensus` for self-consistency before
/// `Project::project` overwrites. `release = 0` matches wire today; both
/// paths should switch to `consensus.release()` once
/// `ClientReleaseTooLow/TooHigh` lands.
///
/// Buffer is `size_of::<RoutedRequestHeader>()`; `prepare_request` transmutes into
/// `PrepareHeader` (also 256 bytes), no realloc.
fn build_register_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    user_id: u32,
) -> Message<RoutedRequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RoutedRequestHeader>();
    let mut msg = Message::<RoutedRequestHeader>::new(header_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
        &mut msg.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid RoutedRequestHeader");
    *header = RoutedRequestHeader {
        command: Command::Request,
        operation: Operation::Register,
        size: u32::try_from(header_size).expect("RoutedRequestHeader size fits u32"),
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
        group: server_common::sharding::METADATA_GROUP,
        ..RoutedRequestHeader::default()
    };
    msg
}

fn build_logout_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    session: u64,
    request: u64,
) -> Message<RoutedRequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RoutedRequestHeader>();
    let mut msg = Message::<RoutedRequestHeader>::new(header_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
        &mut msg.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid RoutedRequestHeader");
    *header = RoutedRequestHeader {
        command: Command::Request,
        operation: Operation::Logout,
        size: u32::try_from(header_size).expect("RoutedRequestHeader size fits u32"),
        cluster: consensus.cluster(),
        view: consensus.view(),
        release: 0,
        client: client_id,
        session,
        request,
        // Metadata consensus group (see `build_register_request_message`).
        group: server_common::sharding::METADATA_GROUP,
        ..RoutedRequestHeader::default()
    };
    msg
}

fn build_complete_revocation_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    request: u64,
    body: &[u8],
) -> Message<RoutedRequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut msg = Message::<RoutedRequestHeader>::new(total);
    {
        let slice = msg.as_mut_slice();
        slice[header_size..total].copy_from_slice(body);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size])
                .expect("zeroed bytes are a valid RoutedRequestHeader");
        *header = RoutedRequestHeader {
            command: Command::Request,
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
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
    }
    msg
}

/// Build a `TruncatePartition` request attributed to the originating client.
///
/// Replicated through the standard client-request path so the commit records
/// `(client, session, request)` in the `ClientTable` and advances that
/// session's watermark. Attributing the truncate to an internal id (or
/// skipping the commit) would leave this request id unrecorded, so the
/// client's own retry of it would re-execute instead of deduping.
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
    template: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    up_to_offset: u64,
) -> Message<RoutedRequestHeader> {
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
    template: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    stream_id: WireIdentifier,
    topic_id: WireIdentifier,
    partition_id: u32,
    up_to_offset: u64,
) -> Message<RoutedRequestHeader> {
    let body = TruncatePartitionRequest {
        stream_id,
        topic_id,
        partition_id,
        up_to_offset,
    }
    .to_bytes();
    let header_size = size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut msg = Message::<RoutedRequestHeader>::new(total);
    {
        let slice = msg.as_mut_slice();
        slice[header_size..total].copy_from_slice(&body);
        let header =
            bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size])
                .expect("zeroed bytes are a valid RoutedRequestHeader");
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::TruncatePartition,
            size: u32::try_from(total).expect("request size fits u32"),
            cluster: template.cluster,
            view: template.view,
            release: template.release,
            client: client_id,
            session,
            request: template.request,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
    }
    msg
}

fn build_prepare_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    request: &RoutedRequestHeader,
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
    // assignment rewrites and the PAT-cleaner delete); the stamp is
    // load-bearing for the creates and inert for the delete, whose apply
    // ignores it.
    // Shared `next_monotonic_timestamp` keeps the in-process path on the same
    // monotonic-clock guard as the wire path.
    let timestamp = consensus.next_monotonic_timestamp();
    *new_header = PrepareHeader {
        cluster: consensus.cluster(),
        size: u32::try_from(size).expect("prepare message size exceeds u32"),
        view: consensus.view(),
        release: request.release,
        command: Command::Prepare,
        replica: consensus.replica(),
        client: request.client,
        parent: consensus.last_prepare_checksum(),
        request_checksum: request.request_checksum,
        request: request.request,
        commit: consensus.commit_max(),
        op,
        timestamp,
        operation,
        // The group's namespace, never the request's: clients send 0, and a
        // journaled 0 mis-routes the entry when repair replays it verbatim.
        group: consensus.group(),
        // Carry the acting user id so the in-apply RBAC gate sees the same
        // identity on every replica. The default projection copies it (see
        // `Project::project`); this helper builds prepares for the ops it
        // rewrites (the CreateTopic/CreatePartitions assignment rewrites and
        // the PAT-cleaner delete), which would otherwise reset it to 0 via
        // `..Default::default()`.
        user_id: request.user_id,
        // Seal the body integrity field over the rewritten body, exactly as
        // `Project::project` does for wire-projected prepares. This helper builds
        // a NEW body, so the wire header's stamp does not describe it; leaving it
        // zero makes the journal scan read every rewritten entry as corrupt and
        // refuse boot on the next restart.
        checksum_body: u128::from(iggy_common::calculate_checksum(body)),
        ..Default::default()
    };

    // Last, because the identity checksum covers every other field. Same contract as
    // the wire path in `Project::project`; skipping it would leave the rewritten
    // prepares (CreateTopic/CreatePartitions assignments, the UpdateTopic default-size
    // rewrite, the PAT-cleaner delete) as the only ops the merge cannot tell apart
    // from a competing prepare.
    consensus::seal_prepare_checksum(prepare)
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
/// `RoutedRequestHeader`, so the in-apply RBAC gate (`crate::stm::authz`) reads the
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

/// Surface a non-`Cached` [`CommitReply`]. Both non-cached outcomes are
/// expected under replica-local eviction, so they are diagnostics, never
/// faults: the wire reply already shipped and only this entry's dedup is
/// degraded.
fn log_commit_reply_outcome(outcome: CommitReply, client_id: u128, op: u64) {
    match outcome {
        CommitReply::Cached => {}
        CommitReply::NoEntry => tracing::trace!(
            target: "iggy.metadata.diag",
            client_id,
            op,
            "commit_reply: client evicted while being prepared; reply shipped, cache skipped"
        ),
        CommitReply::SkippedRegression { stored, received } => warn!(
            target: "iggy.metadata.diag",
            client_id,
            op,
            stored,
            received,
            "commit_reply: committed op is older than the cached entry \
             (replica-local eviction replayed out of order); cache skipped"
        ),
        CommitReply::AdvancedFence => tracing::trace!(
            target: "iggy.metadata.diag",
            client_id,
            op,
            "commit_reply: client evicted while being prepared; reply shipped, fence advanced"
        ),
    }
}

/// Refusal reply for a replayed request whose committed answer carried a
/// secret the cache cannot reproduce; `None` when the cached reply is safe to
/// replay verbatim.
///
/// Only `CreatePersonalAccessToken` qualifies today. Its raw secret is
/// deliberately never replicated (minting inside the apply would re-roll
/// `ring::rand` per replica and diverge the token index; see
/// `CreatePersonalAccessTokenRequest::apply`), so the committed reply is
/// `ApplyReply::ok(Bytes::new())` and the cache holds no token. The secret
/// existed only on the wire of the original reply, spliced in by
/// `build_raw_pat_reply`, and is unrecoverable once that reply is lost.
/// Meanwhile the ingress rewrite has already minted a FRESH secret for the
/// replayed frame whose hash never reached consensus, so serving the cached
/// success would splice that orphan onto it and hand the caller a credential
/// that authenticates against nothing.
///
/// `PersonalAccessTokenAlreadyExists` is the honest code: the original create
/// committed, so the name IS taken, and the remedy it implies (delete by name,
/// then recreate) is exactly right.
///
/// A cached REJECTION replays untouched: it carries no secret, so serving it
/// is both safe and useful.
fn unreplayable_secret_refusal(
    request_header: &RoutedRequestHeader,
    cached: &Frozen<{ server_common::MESSAGE_ALIGN }>,
    commit: u64,
    client_id: u128,
) -> Option<Message<GenericHeader>> {
    if request_header.operation != Operation::CreatePersonalAccessToken {
        return None;
    }
    let cached_body = cached
        .as_slice()
        .get(size_of::<ReplyHeader>()..)
        .unwrap_or_default();
    if iggy_binary_protocol::result_code(cached_body) != Some(0) {
        return None;
    }
    warn!(
        target: "iggy.metadata.diag",
        client_id,
        request = request_header.request,
        "refusing replayed CreatePersonalAccessToken: the committed secret is \
         unrecoverable and a re-minted one would not match the stored hash"
    );
    Some(
        build_result_rejection_reply(
            request_header,
            commit,
            IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code(),
        )
        .into_generic(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::stream::Streams;
    use crate::stm::user::Users;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::WireOptions;
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

    #[test]
    fn populated_snapshot_reencode_and_checksum_are_stable() {
        // Two replicas holding identical state must serialize it identically, which
        // holds only while every serialized collection keeps a deterministic order.
        // Fold a multi-slot client table (the envelope's own Vec) and assert the raw
        // re-encode survives the round-trip. The streams sub-tree's map-derived Vecs are
        // covered by
        // `crate::stm::stream::tests::populated_streams_snapshot_reencode_is_byte_stable`.
        let mut snapshot = IggySnapshot::new(7);
        snapshot.snapshot.client_table = Some(consensus::ClientTableSnapshot {
            fences: vec![],
            slots: vec![
                (
                    0,
                    consensus::ClientEntrySnapshot {
                        client_id: 1,
                        epoch: 10,
                        user_id: 1,
                        watermark: 3,
                        watermark_checksum: 0xabc,
                        reply: vec![1, 2, 3],
                    },
                ),
                (
                    2,
                    consensus::ClientEntrySnapshot {
                        client_id: 2,
                        epoch: 20,
                        user_id: 2,
                        watermark: 0,
                        watermark_checksum: 0,
                        reply: vec![4, 5],
                    },
                ),
            ],
        });

        let encoded = snapshot.encode().unwrap();
        let decoded = IggySnapshot::decode(&encoded).unwrap();
        assert_eq!(
            encoded,
            decoded.encode().unwrap(),
            "a populated snapshot must re-encode byte-identically after a decode; an \
             unordered collection in the serialized form would make two replicas with \
             identical state serialize differently"
        );
        assert_ne!(
            checkpoint_checksum(&encoded),
            checkpoint_checksum(&IggySnapshot::new(7).encode().unwrap()),
            "the checksum must track content, else it could not detect a torn snapshot"
        );
    }

    #[test]
    fn client_table_reply_bytes_encode_as_a_msgpack_blob() {
        // A `Vec<u8>` serialized through serde's sequence path spends 2 bytes on every
        // byte >= 0x80, so a checkpoint's reply payload runs up to roughly double.
        // Reply bytes are wire messages, mostly high bytes, so pin the `bin` encoding:
        // the format freezes at release and this is much cheaper to fix now.
        const REPLY_LEN: usize = 512;
        let mut snapshot = IggySnapshot::new(1);
        snapshot.snapshot.client_table = Some(consensus::ClientTableSnapshot {
            fences: vec![],
            slots: vec![(
                0,
                consensus::ClientEntrySnapshot {
                    client_id: 1,
                    epoch: 1,
                    user_id: 1,
                    watermark: 0,
                    watermark_checksum: 0,
                    reply: vec![0xFF; REPLY_LEN],
                },
            )],
        });

        let encoded = snapshot.encode().unwrap();
        let baseline = IggySnapshot::new(1).encode().unwrap().len();
        let reply_cost = encoded.len() - baseline;
        assert!(
            reply_cost < REPLY_LEN * 2,
            "a {REPLY_LEN}-byte reply of high bytes cost {reply_cost} bytes, so it is \
             still encoding as an integer array rather than a msgpack blob"
        );

        // And it must decode back to the same bytes.
        let decoded = IggySnapshot::decode(&encoded).unwrap();
        let table = decoded.snapshot().client_table.as_ref().unwrap();
        assert_eq!(table.slots[0].1.reply, vec![0xFF; REPLY_LEN]);
    }

    type TestMux = MuxStateMachine<variadic!(Users, Streams)>;

    /// Build a peer-shard-style `IggyMetadata` with `consensus`,
    /// `journal`, and `snapshot` all `None`. Enough to test the
    /// commit-notifier slot without standing up VSR / WAL infrastructure:
    /// the test picks `()` for `C` / `J` / `S` since no notifier code path
    /// touches their methods.
    fn peer_metadata() -> IggyMetadata<(), (), (), TestMux> {
        IggyMetadata::new(None, None, None, None, TestMux::default(), None)
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
            command: Command::Reply,
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

    // The login frame's `client` field is caller-supplied and
    // `resolve_acting_user_id` resolves authority from the entry it names, so
    // the register ownership gate must refuse an entry owned by another user
    // rather than resume the caller onto it. Two shapes reach this: a caller
    // presenting someone else's id with its own valid credentials, and an
    // honest login landing on a recovered entry after a restart (the HTTP id
    // minter restarts at 1 while WAL replay rebuilds the previous boot's
    // entries).
    #[compio::test]
    async fn register_gate_refuses_an_entry_owned_by_another_user() {
        const CLIENT: u128 = 1;
        const OWNER: u32 = 7;
        const IMPOSTOR: u32 = 9;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        md.client_table
            .borrow_mut()
            .commit_register(CLIENT, OWNER, register_reply(CLIENT, 1));

        assert_eq!(
            md.submit_register_in_process(CLIENT, IMPOSTOR).await,
            Err(MetadataSubmitError::ClientIdOwnedByAnotherUser),
            "a different user must not be resumed onto this entry"
        );
        assert!(
            !MetadataSubmitError::ClientIdOwnedByAnotherUser.is_transient(),
            "the refusal is terminal; retrying anywhere cannot help"
        );
        assert_eq!(
            md.client_table.borrow().get_user_id(CLIENT),
            Some(OWNER),
            "the refused attempt must not rewrite the entry's owner"
        );

        // The owner itself passes the gate. Its rebind now goes through
        // consensus (a bind is a fencing event), which this NoopBus harness
        // never commits -- so passing the gate is observable as Pending,
        // while a refusal resolves immediately.
        let mut rebind = std::pin::pin!(md.submit_register_in_process(CLIENT, OWNER));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            rebind.as_mut().poll(&mut cx).is_pending(),
            "the owner's rebind must pass the gate and dispatch"
        );
    }

    #[test]
    fn resolve_acting_user_id_stamps_from_client_table() {
        // A gated client op takes the acting user from the committed session,
        // independent of any wire-supplied header value.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
        table.commit_register(CLIENT, ACTING_USER, register_reply(CLIENT, SESSION));
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
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None)
    }

    fn create_topic_request(client: u128, wire_user_id: u32) -> Message<RoutedRequestHeader> {
        let body = CreateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            partitions_count: 1,
            name: WireName::new("t").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(&body);
            let header =
                bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::CreateTopic,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request: 1,
                user_id: wire_user_id,
                group: server_common::sharding::METADATA_GROUP,
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
        // A `CreateTopic` without an explicit `message_expiry` option must be
        // resolved at primary admission to the build default, riding the
        // derived block, so the replicated prepare -- and thus every
        // replica's commit -- holds a concrete expiry.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let plane = metadata_plane();
        plane.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        // `create_topic_request` builds the body with no options at all.
        let prepare = plane
            .prepare_request(create_topic_request(CLIENT, ACTING_USER))
            .expect("CreateTopic is client-allowed");
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
        let persisted = PersistedCreateTopicRequest::decode_from(body)
            .expect("create topic with assignments prepare must decode");
        assert!(
            persisted.request.options.is_empty(),
            "a client that sent no options gets an empty explicit block"
        );
        let derived = iggy_common::TopicCreateOptions::parse(&persisted.derived_options)
            .expect("derived block parses against the catalog");
        assert_eq!(
            derived.message_expiry,
            Some(iggy_common::IggyExpiry::from(
                iggy_common::DEFAULT_MESSAGE_EXPIRY
            )),
            "ServerDefault expiry must be resolved into the derived block at admission"
        );
        assert_eq!(
            persisted.partitions.len(),
            1,
            "absent partitions_count defaults to one partition"
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

    /// A replayed `CreatePersonalAccessToken` must be refused, not served from
    /// the dedup cache: the committed secret is unrecoverable (never
    /// replicated) and the rewrite has already minted a fresh one whose hash
    /// never reached consensus, so replaying would hand back a credential that
    /// authenticates against nothing. A cached REJECTION still replays -- it
    /// carries no secret.
    #[compio::test]
    async fn replayed_pat_create_is_refused_but_other_replays_pass_through() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        md.client_table
            .borrow_mut()
            .commit_register(CLIENT, USER, register_reply(CLIENT, 1));

        // Cache a committed SUCCESS for request 1 under the PAT operation,
        // exactly as the commit path would (empty apply body + result section).
        let pat_success = committed_reply(CLIENT, 1, Operation::CreatePersonalAccessToken, 0);
        md.client_table
            .borrow_mut()
            .commit_reply(CLIENT, USER, pat_success);

        // Replaying request 1 as a PAT create must NOT return the cached
        // success; it must refuse with the name-taken code.
        let reply = md
            .submit_request_in_process(pat_create_request(CLIENT, 1))
            .await
            .expect("refusal is a reply, not a submit error");
        let body = &reply.as_slice()[size_of::<ReplyHeader>()..];
        assert_eq!(
            iggy_binary_protocol::result_code(body),
            Some(IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code()),
            "a replayed PAT create must be refused, never answered from cache"
        );

        // A cached REJECTION for the same operation carries no secret, so it
        // replays untouched.
        let rejected_code = IggyError::InvalidPersonalAccessTokenExpiry.as_code();
        let pat_rejection = committed_reply(
            CLIENT,
            2,
            Operation::CreatePersonalAccessToken,
            rejected_code,
        );
        md.client_table
            .borrow_mut()
            .commit_reply(CLIENT, USER, pat_rejection);
        let reply = md
            .submit_request_in_process(pat_create_request(CLIENT, 2))
            .await
            .expect("cached rejection replays");
        let body = &reply.as_slice()[size_of::<ReplyHeader>()..];
        assert_eq!(
            iggy_binary_protocol::result_code(body),
            Some(rejected_code),
            "a cached PAT rejection is safe to replay verbatim"
        );
    }

    /// Committed-reply fixture shaped like the commit path's output: a result
    /// section carrying `code`, no payload.
    fn committed_reply(
        client: u128,
        request: u64,
        operation: Operation,
        code: u32,
    ) -> Message<ReplyHeader> {
        let mut body = bytes::BytesMut::new();
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&code.to_le_bytes());
        let header_size = size_of::<ReplyHeader>();
        let total = header_size + body.len();
        let mut reply = Message::<ReplyHeader>::new(total);
        {
            let slice = reply.as_mut_slice();
            slice[header_size..total].copy_from_slice(&body);
            let header =
                bytemuck::checked::from_bytes_mut::<ReplyHeader>(&mut slice[..header_size]);
            *header = ReplyHeader {
                client,
                request,
                commit: request,
                size: u32::try_from(total).unwrap(),
                command: Command::Reply,
                operation,
                ..Default::default()
            };
        }
        reply
    }

    fn pat_create_request(client: u128, request: u64) -> Message<RoutedRequestHeader> {
        let header_size = size_of::<RoutedRequestHeader>();
        let mut message = Message::<RoutedRequestHeader>::new(header_size);
        let header = bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
            &mut message.as_mut_slice()[..header_size],
        );
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::CreatePersonalAccessToken,
            size: u32::try_from(header_size).unwrap(),
            client,
            session: 1,
            request,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
        message
    }

    fn create_stream_request(
        client: u128,
        request: u64,
        name: &str,
    ) -> Message<RoutedRequestHeader> {
        let body = iggy_binary_protocol::requests::streams::CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(&body);
            let header =
                bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::CreateStream,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request,
                user_id: 0,
                group: server_common::sharding::METADATA_GROUP,
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
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            StallBus::default(),
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();

        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
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

    /// A backup admits the prepare its JOURNAL needs next, even when its
    /// sequencer has run ahead of the journal.
    ///
    /// The two frontiers legitimately disagree: `on_start_view` sets the sequencer
    /// to the view's announced head, deliberately ahead of what this replica
    /// holds, because the bodies arrive afterwards by retransmit or repair. Gating
    /// admission on the sequencer therefore rejected exactly the prepare that
    /// would heal the log: a backup with journal head 44 refusing op 45 because
    /// its adopted head said to expect 46. The primary retransmits that op
    /// forever, every backup logs an out-of-order gap, it never reaches a commit
    /// quorum, and its client is never answered. Systematic for any rejoining
    /// replica, so the deterministic simulator wedged on every metadata workload
    /// under crash/restart injection until this was gated on the journal.
    #[compio::test]
    async fn backup_admits_the_prepare_its_journal_needs_despite_a_leading_sequencer() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        /// Stands in for a head adopted from a `StartView` whose bodies have not
        /// arrived, so it sits well above the empty journal.
        const ADOPTED_HEAD: u64 = 5;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        // Replica 1 of 3 at view 0, so `primary_index(0) == 0` makes this a backup
        // and `on_replicate` takes the gap-check branch.
        let consensus = VsrConsensus::new(
            1,
            1,
            3,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                Some(dir.path().to_path_buf()),
            );
        let consensus = md.consensus.as_ref().unwrap();
        assert!(
            consensus.is_follower(),
            "replica 1 of 3 at view 0 must be a backup for this to exercise the gap check"
        );
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        // Minted while the sequencer is still at 0, so it carries op 1: exactly
        // what the empty journal needs next.
        let prepare = md
            .prepare_request(create_stream_request(CLIENT, 1, "s1"))
            .expect("CreateStream is client-allowed");
        assert_eq!(prepare.header().op, 1, "the first prepare must be op 1");

        // Now run the sequencer ahead, as adopting a started view does.
        consensus.sequencer().set_sequence(ADOPTED_HEAD);
        let journal = md.journal.as_ref().unwrap();
        assert_eq!(
            journal.last_op(),
            None,
            "the journal must still be empty, else the divergence under test is absent"
        );

        md.on_replicate(prepare).await;

        assert!(
            journal.header(1).is_some(),
            "backup dropped the prepare its journal needed next because its sequencer \
             was ahead; the primary's retransmit of this op can never be accepted, so \
             the op never commits and its client never gets a reply"
        );
    }

    /// A state-transfer receiver admits the first live prepare above the floor it
    /// installed, instead of waiting for an op the snapshot already contains.
    ///
    /// `install_state_transfer` moves the snapshot floor, the commit floor, the
    /// sequencer and `commit_max`, and leaves the WAL head where it was: the
    /// snapshot IS every op below the floor, so there is nothing left to append
    /// for them. A gap check reading `last_op` alone therefore has the receiver
    /// ask for `last_op + 1`, an op inside the snapshot that no peer will send
    /// again, and it drops every live prepare forever.
    ///
    /// Nothing recovers it. An offer built on a quiet cluster carries `commit_op
    /// == snapshot_seq`, so the install lands `commit_min == commit_max` and
    /// `maybe_request_metadata_repair`, gated on `commit_min < commit_max`, never
    /// arms; repair is the only path that could refill the head. With the
    /// other backup down the primary needs this replica's ack to commit at all,
    /// so a 3-node cluster still inside its quorum stops serving metadata.
    #[compio::test]
    async fn state_transfer_receiver_admits_the_first_prepare_above_the_installed_floor() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        /// The `snapshot_seq` of a transferred offer, far above anything this
        /// replica's own WAL holds.
        const INSTALLED_FLOOR: u64 = 400;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        // Replica 1 of 3 at view 0, so `primary_index(0) == 0` makes this a backup
        // and `on_replicate` takes the gap-check branch.
        let consensus = VsrConsensus::new(
            1,
            1,
            3,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                Some(dir.path().to_path_buf()),
            );
        let consensus = md.consensus.as_ref().unwrap();
        assert!(
            consensus.is_follower(),
            "replica 1 of 3 at view 0 must be a backup for this to exercise the gap check"
        );
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        // Give the WAL a head far below the floor about to be installed, which is
        // what a replica that fell behind its peers' retention actually carries.
        let first = md
            .prepare_request(create_stream_request(CLIENT, 1, "s1"))
            .expect("CreateStream is client-allowed");
        md.on_replicate(first).await;
        let journal = md.journal.as_ref().unwrap();
        assert_eq!(
            journal.last_op(),
            Some(1),
            "the WAL head must sit below the installed floor, else the divergence \
             under test is absent"
        );

        // Exactly the frontiers `install_state_transfer` leaves for a quiet-cluster
        // offer, where the manifest's `commit_op` equals its `snapshot_seq`.
        journal.set_snapshot_op(INSTALLED_FLOOR);
        consensus.set_commit_floor(INSTALLED_FLOOR);
        consensus.sequencer().set_sequence(INSTALLED_FLOOR);
        consensus.advance_commit_max(INSTALLED_FLOOR);
        assert_eq!(
            consensus.commit_min(),
            consensus.commit_max(),
            "the wedge needs an install with no repair left to arm; diverged \
             frontiers heal through `maybe_request_metadata_repair`"
        );

        let next = md
            .prepare_request(create_stream_request(CLIENT, 2, "s2"))
            .expect("CreateStream is client-allowed");
        assert_eq!(
            next.header().op,
            INSTALLED_FLOOR + 1,
            "the primary numbers the next op off the installed floor"
        );

        md.on_replicate(next).await;

        assert!(
            journal
                .header(usize::try_from(INSTALLED_FLOOR + 1).unwrap())
                .is_some(),
            "state-transfer receiver dropped the first prepare above its installed \
             floor; the ops the snapshot already holds are never re-sent, and with \
             `commit_min == commit_max` no repair arms, so this op never commits"
        );
    }

    /// A checkpoint reclaims the WAL prefix the snapshot supersedes, but must
    /// stop one op short of the checkpoint op itself.
    ///
    /// That op is the replica's commit point, and its `DoViewChange` suffix is
    /// floored there. The merge scans the commit point and may not discard it,
    /// so a sender with no header to put there is deferring to a peer; when
    /// every sender has checkpointed at the same op the view change deadlocks
    /// (`dvc_merge::merge_dvc_quorum`). Checkpoints fire on local journal
    /// occupancy, which is symmetric across replicas seeing the same ops, so
    /// "every sender" is the ordinary case, not a coincidence.
    #[compio::test]
    async fn checkpoint_drain_retains_the_commit_point_header() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const OPS: u64 = 5;
        const CHECKPOINT_OP: u64 = 3;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                Some(dir.path().to_path_buf()),
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        for op in 1..=OPS {
            let prepare = md
                .prepare_request(create_stream_request(CLIENT, op, &format!("s{op}")))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            md.on_replicate(prepare).await;
        }

        let journal = md.journal.as_ref().unwrap();
        md.coordinator
            .as_ref()
            .expect("data_dir present arms the coordinator")
            .drain(journal, CHECKPOINT_OP)
            .await
            .expect("drain the snapshotted prefix");

        let header_at = |op: u64| journal.header(usize::try_from(op).expect("test ops fit usize"));
        for op in 1..CHECKPOINT_OP {
            assert!(
                header_at(op).is_none(),
                "op {op} is below the checkpoint and must be reclaimed"
            );
        }
        assert!(
            header_at(CHECKPOINT_OP).is_some(),
            "the checkpoint op is the commit point and must stay describable in a DVC"
        );
        for op in CHECKPOINT_OP + 1..=OPS {
            assert!(header_at(op).is_some(), "op {op} was never snapshotted");
        }
    }

    /// Reproduces the single-node "metadata prepare queue is full" wedge
    ///
    /// `checkpoint_if_needed` runs inside `on_replicate`, once per submit.
    /// Under a concurrent login/create burst, several `on_replicate` futures
    /// cross the forced-checkpoint boundary (journal `remaining_capacity <=
    /// CHECKPOINT_MARGIN`, i.e. op `SLOT_COUNT - MARGIN = 960`) together, and
    /// every one of them runs a full checkpoint concurrently. The concurrent
    /// `journal.drain()` calls race on the one fixed `wal.tmp`: the losers
    /// surface `snapshot I/O error: No such file or directory` (or a short
    /// read after the winner's reopen). Fatally, `on_replicate` then dropped
    /// the loser's prepare — AFTER `pipeline_message` had pushed the pipeline
    /// entry and pre-advanced the sequencer — so the op was never journaled,
    /// never acked, never committed. The commit frontier gaps permanently:
    /// logins first bounce `NotCaughtUp`, in-flight clients wedge
    /// (`InProgress` on logout), and once the pipeline fills every submit is
    /// rejected `PipelineFull` forever.
    ///
    /// Correct behavior: checkpoints are single-flight, a failed or skipped
    /// checkpoint never discards a pipelined prepare, all racers' ops are
    /// journaled and commit, and the caught-up gate reopens.
    #[compio::test]
    async fn concurrent_checkpoint_boundary_must_not_drop_prepares() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        /// One op below the forced-checkpoint trigger: the journal holds
        /// 1024 slots and forces a checkpoint when 64 or fewer remain.
        const FILL: u64 = 960;

        let dir = tempfile::tempdir().unwrap();
        // Bootstrap creates the metadata dir; the coordinator's snapshot
        // persist expects it to exist.
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        // `data_dir` present => SnapshotCoordinator armed, checkpoints live.
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                Some(dir.path().to_path_buf()),
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        // Fill to one op under the boundary through the real primary path,
        // acking each op so `commit_min` tracks `last_op` and the pipeline
        // stays shallow — the steady state the production server was in.
        let mut loopback = Vec::new();
        for i in 1..=FILL {
            let prepare = md
                .prepare_request(create_stream_request(CLIENT, i, &format!("s{i}")))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            md.on_replicate(prepare).await;
            loopback.clear();
            consensus.drain_loopback_into(&mut loopback);
            let ack = loopback
                .pop()
                .expect("one self-ack per prepare")
                .try_into_typed::<PrepareOkHeader>()
                .expect("loopback holds self PrepareOks");
            md.on_ack(ack).await;
        }
        assert_eq!(consensus.commit_min(), FILL);
        assert_eq!(
            md.journal.as_ref().unwrap().remaining_capacity(),
            Some(64),
            "fill must stop exactly at the forced-checkpoint boundary"
        );

        // Three submits race across the boundary — the concurrent
        // login/create burst from the incident. Every racer sees
        // `remaining_capacity <= CHECKPOINT_MARGIN` before any drain
        // completes.
        let md_ref = &md;
        let race = |request: u64, name: String| async move {
            let prepare = md_ref
                .prepare_request(create_stream_request(CLIENT, request, &name))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            md_ref.on_replicate(prepare).await;
        };
        futures::join!(
            race(FILL + 1, format!("s{}", FILL + 1)),
            race(FILL + 2, format!("s{}", FILL + 2)),
            race(FILL + 3, format!("s{}", FILL + 3)),
        );

        // Every racer's prepare must be durably journaled: a dropped one is
        // unrepairable (nothing re-prepares it) and gaps the frontier.
        let journal = md.journal.as_ref().unwrap();
        for op in FILL + 1..=FILL + 3 {
            assert!(
                journal
                    .header(usize::try_from(op).expect("test ops fit in usize"))
                    .is_some(),
                "op {op} vanished from the WAL: a failed checkpoint dropped a pipelined prepare"
            );
        }
        // The checkpoint itself must have happened — once: WAL reclaimed,
        // snapshot on disk.
        assert!(
            journal.remaining_capacity().unwrap() > 900,
            "checkpoint must have drained the snapshotted prefix, got {:?}",
            journal.remaining_capacity()
        );
        assert!(
            dir.path()
                .join(crate::impls::METADATA_DIR)
                .join("snapshot.bin")
                .exists(),
            "checkpoint must persist the snapshot"
        );

        // The self-acks commit all three racers; any gap here is the
        // production wedge (commit frontier pinned, PipelineFull forever).
        loopback.clear();
        consensus.drain_loopback_into(&mut loopback);
        assert_eq!(loopback.len(), 3, "one self-ack per racer");
        for message in loopback {
            let ack = message
                .try_into_typed::<PrepareOkHeader>()
                .expect("loopback holds self PrepareOks");
            md.on_ack(ack).await;
        }
        assert_eq!(
            consensus.commit_min(),
            FILL + 3,
            "commit frontier must cross the checkpoint boundary"
        );
        assert!(
            is_caught_up_primary(consensus),
            "caught-up gate must reopen after the boundary"
        );
    }

    /// The exact window behind the historical "logout/unregister failed
    /// ... primary not yet caught up on `commit_journal`".
    /// ANOTHER client's op sits between quorum-ack (`commit_max` advanced
    /// inside `on_ack`) and apply (`commit_min` behind, driver parked at
    /// the journal read).
    ///
    /// New contract (queue absorption): a logout landing in
    /// that window is NOT bounced with `NotCaughtUp` — non-register ops
    /// carry no catch-up gate. It pipelines behind the in-flight batch,
    /// and its submit's inline loopback pump commits both ops in order.
    /// The parked sibling driver then resumes onto an already-drained
    /// pipeline and exits via head-revalidation, exercising the
    /// concurrent-driver safety of the commit loop.
    #[compio::test]
    async fn logout_in_mid_commit_window_commits_instead_of_rejecting() {
        use std::future::Future;

        /// The client logging out; its session is already committed.
        const CLIENT_A: u128 = 1;
        /// The client whose in-flight commit closes the gate.
        const CLIENT_B: u128 = 2;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        for client in [CLIENT_A, CLIENT_B] {
            md.client_table.borrow_mut().commit_register(
                client,
                ACTING_USER,
                register_reply(client, SESSION),
            );
        }

        // B's op: prepared, journaled, self-acked onto the loopback queue.
        let prepare = md
            .prepare_request(create_stream_request(CLIENT_B, 1, "s1"))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");

        // Open the window: the first poll of `on_ack` reaches quorum and
        // advances commit_max synchronously, then parks at the journal
        // read — commit_min has not moved. This is the exact server state
        // every production NotCaughtUp line was emitted from.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut driver = Box::pin(md.on_ack(ack));
        assert!(
            driver.as_mut().poll(&mut cx).is_pending(),
            "driver must park at the journal read inside the commit"
        );
        assert_eq!(consensus.commit_max(), 1, "quorum advanced commit_max");
        assert_eq!(consensus.commit_min(), 0, "apply has not landed yet");
        assert!(
            !is_caught_up_primary(consensus),
            "gate must be closed mid-commit"
        );

        // A's logout lands in the window. No gate for non-register ops: it
        // pipelines behind B's committing op, and its inline loopback pump
        // drives BOTH commits (B's op 1 via head-revalidated takeover, then
        // its own op 2) before resolving.
        let outcome = md.submit_logout_in_process(CLIENT_A, SESSION, 2).await;
        assert_eq!(
            outcome,
            Ok(2),
            "mid-window logout must commit and reply, never bounce NotCaughtUp"
        );
        assert_eq!(consensus.commit_min(), 2, "both ops committed in order");
        assert!(
            is_caught_up_primary(consensus),
            "gate reopens once the batch drains"
        );

        // The parked sibling driver resumes onto a drained pipeline: the
        // head it peeked is gone, revalidation sends it out without
        // touching commit state. Drive it to completion to prove it.
        let mut resumed = false;
        for _ in 0..1_000 {
            if driver.as_mut().poll(&mut cx).is_ready() {
                resumed = true;
                break;
            }
            // Let the runtime deliver the journal-read completion.
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(resumed, "parked driver must exit via head-revalidation");
        assert_eq!(
            consensus.commit_min(),
            2,
            "resumed driver commits nothing new"
        );
        assert_eq!(
            md.client_table.borrow().get_epoch(CLIENT_A),
            None,
            "session removed by the committed logout"
        );
    }

    /// Register is the one op that still honors the catch-up gate (its
    /// admission races a committed-but-unapplied register; a double commit
    /// bumps the epoch past the first reply's and fences a live client).
    /// New contract: a register arriving in the mid-commit window is
    /// ABSORBED into the pipeline's request queue with its reply subscriber
    /// attached, promoted by the commit path once the batch drains, and the
    /// caller's await resolves with the committed epoch — instead of the
    /// historical `NotCaughtUp` bounce that one-shot CLI clients surfaced as
    /// "Disconnected" login failures.
    #[compio::test]
    async fn register_in_mid_commit_window_is_queued_then_committed() {
        use std::future::Future;

        /// The client whose in-flight commit closes the gate.
        const CLIENT_B: u128 = 2;
        /// The client registering mid-window.
        const CLIENT_C: u128 = 3;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT_B,
            ACTING_USER,
            register_reply(CLIENT_B, SESSION),
        );

        // B's op journaled + self-acked; park its commit mid-window.
        let prepare = md
            .prepare_request(create_stream_request(CLIENT_B, 1, "s1"))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut driver = Box::pin(md.on_ack(ack));
        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert_eq!(consensus.commit_max(), 1);
        assert_eq!(consensus.commit_min(), 0);

        // C's register lands in the window: absorbed, not bounced.
        let mut register = Box::pin(md.submit_register_in_process(CLIENT_C, ACTING_USER));
        assert!(
            register.as_mut().poll(&mut cx).is_pending(),
            "mid-window register must park in the request queue, not error"
        );
        assert_eq!(
            consensus.request_queue_len(),
            1,
            "register buffered in the request queue"
        );

        // The committing driver drains its batch, then promotes the queued
        // register into a prepare (its self-ack lands on the loopback).
        let mut resumed = false;
        for _ in 0..1_000 {
            if driver.as_mut().poll(&mut cx).is_ready() {
                resumed = true;
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(resumed, "B's commit must complete and promote the register");
        assert_eq!(consensus.commit_min(), 1, "B's op committed");
        assert_eq!(
            consensus.request_queue_len(),
            0,
            "promotion emptied the request queue"
        );

        // Commit the promoted register (production: the shard pump or any
        // sibling submit drains this ack) and the parked caller resolves.
        loopback.clear();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("promoted register must self-ack")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        md.on_ack(ack).await;

        let mut outcome = None;
        for _ in 0..1_000 {
            if let std::task::Poll::Ready(result) = register.as_mut().poll(&mut cx) {
                outcome = Some(result);
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(
            outcome.expect("absorbed register must resolve"),
            Ok(BoundSession {
                epoch: 2,
                watermark: 0
            }),
            "queued register commits with the next batch; the bind fences at its commit op"
        );
        assert_eq!(
            md.client_table.borrow().get_epoch(CLIENT_C),
            Some(2),
            "entry created by the promoted register"
        );
    }

    /// The commit loop and the promotion of queued requests run at the tail
    /// of `on_ack`, inside whichever future delivered the quorum ack. Drop
    /// that future mid-commit and — on an idle server — nothing re-drives
    /// the work: duplicate/repair acks do not re-open the commit path
    /// (quorum already recorded, `commit_max` does not advance), so the
    /// committed-but-unapplied op pins the catch-up gate closed and an
    /// absorbed register parks in the request queue indefinitely.
    ///
    /// `resume_stranded_commits` (wired into the shard pump tick) is the
    /// backstop: it re-enters the commit path, applies the stranded prefix,
    /// and promotes the queued register, whose awaiter then resolves.
    /// A state-transfer receiver's WAL can hold a pre-crash suffix above the
    /// incoming floor. The commit walk matches entries by op number alone, so
    /// installing without discarding that suffix would later apply its stale
    /// bodies as committed. The install must truncate the WAL above
    /// `snapshot_seq` and keep everything at or below it, which tail repair
    /// resumes from.
    #[compio::test]
    async fn state_transfer_install_truncates_the_wal_above_the_incoming_floor() {
        const CLIENT: u128 = 9;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const SNAPSHOT_SEQ: u64 = 2;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        // Journal ops 1..=3 directly (no acks, so `commit_min` stays 0 and the
        // transfer is "ahead"): 1 and 2 sit at or below the incoming floor, 3 is
        // the relic suffix a view change has since reassigned.
        for (request_id, name) in [(1, "s1"), (2, "s2"), (3, "s3")] {
            let projected = md
                .prepare_request(create_stream_request(CLIENT, request_id, name))
                .expect("CreateStream is client-allowed");
            consensus.pipeline_message(PlaneKind::Metadata, &projected);
            md.journal
                .as_ref()
                .unwrap()
                .handle()
                .append(projected)
                .await
                .expect("seeding the WAL succeeds");
        }
        let journal_handle = md.journal.as_ref().unwrap().handle();
        assert_eq!(journal_handle.last_op(), Some(3));

        // A donor mux fills the snapshot the way a serving primary would, so
        // the restore path sees populated sections rather than a bare envelope.
        let snapshot_bytes =
            <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_SEQ, 1)
                .expect("donor snapshot builds")
                .encode()
                .expect("donor snapshot encodes");
        md.install_state_transfer(
            &snapshot_bytes,
            ClientTable::new(CLIENTS_TABLE_MAX),
            0,
            SNAPSHOT_SEQ,
        )
        .await
        .expect("install succeeds");

        assert_eq!(
            journal_handle.last_op(),
            Some(SNAPSHOT_SEQ),
            "the relic above the incoming floor must be gone"
        );
        assert!(
            journal_handle.header(3).is_none(),
            "op 3 was above the floor; the commit walk must never see it again"
        );
        assert!(
            journal_handle.header(1).is_some() && journal_handle.header(2).is_some(),
            "ops at or below the floor stay for the walk and tail repair"
        );
        assert_eq!(
            md.applied_frontier(),
            SNAPSHOT_SEQ,
            "the snapshot IS ops up to its sequence applied, so the read gate has \
             to admit reads at the floor the install jumped to"
        );
    }

    #[compio::test]
    async fn tick_backstop_must_resume_stranded_commits_and_promotions() {
        use std::future::Future;

        /// The client whose in-flight commit is stranded by the dropped driver.
        const CLIENT_B: u128 = 2;
        /// The client whose register parks in the request queue.
        const CLIENT_C: u128 = 3;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT_B,
            ACTING_USER,
            register_reply(CLIENT_B, SESSION),
        );

        // B's op journaled + self-acked; park its commit driver mid-window
        // at the journal read.
        let prepare = md
            .prepare_request(create_stream_request(CLIENT_B, 1, "s1"))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut driver = Box::pin(md.on_ack(ack));
        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert_eq!(consensus.commit_max(), 1);
        assert_eq!(consensus.commit_min(), 0);

        // C's register lands in the window: absorbed into the request queue.
        let mut register = Box::pin(md.submit_register_in_process(CLIENT_C, ACTING_USER));
        assert!(register.as_mut().poll(&mut cx).is_pending());
        assert_eq!(consensus.request_queue_len(), 1);

        // The committing driver dies at its await — the hyper-disconnect
        // analogue. Commit and promotion are now stranded: op 1 is quorum'd
        // (commit_max = 1) but unapplied (commit_min = 0), and no further
        // ack will arrive to re-drive either.
        drop(driver);
        assert_eq!(consensus.commit_max(), 1);
        assert_eq!(consensus.commit_min(), 0);
        assert_eq!(consensus.request_queue_len(), 1);
        assert!(
            register.as_mut().poll(&mut cx).is_pending(),
            "queued register must still be parked with no driver alive"
        );

        // The pump tick backstop re-drives: commits op 1 (reopening the
        // catch-up gate) and promotes the queued register into a prepare
        // (its self-ack lands on the loopback).
        md.resume_stranded_commits().await;
        assert_eq!(consensus.commit_min(), 1, "stranded op 1 applied");
        assert_eq!(consensus.request_queue_len(), 0, "queued register promoted");

        // Commit the promoted register (production: pump loopback drain)
        // and the parked caller resolves with its session.
        loopback.clear();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("promoted register must self-ack")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        md.on_ack(ack).await;

        let mut outcome = None;
        for _ in 0..1_000 {
            if let std::task::Poll::Ready(result) = register.as_mut().poll(&mut cx) {
                outcome = Some(result);
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(
            outcome.expect("promoted register must resolve"),
            Ok(BoundSession {
                epoch: 2,
                watermark: 0
            })
        );
        assert_eq!(md.client_table.borrow().get_epoch(CLIENT_C), Some(2));
        assert!(is_caught_up_primary(consensus));
    }

    #[compio::test]
    async fn failed_journal_append_hands_the_op_back_instead_of_leaving_a_phantom() {
        // The primary claims its op before the append (`push_prepare_entry`), so a
        // failed append used to leave the sequencer one ahead of the WAL forever:
        // the next request projected over the hole, and no repair path refilled it.
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;

        let dir = tempfile::tempdir().unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::default(),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table.borrow_mut().commit_register(
            CLIENT,
            ACTING_USER,
            register_reply(CLIENT, SESSION),
        );

        let projected = md
            .prepare_request(create_stream_request(CLIENT, 1, "s1"))
            .expect("CreateStream is client-allowed");
        let op = projected.header().op;
        let parent = projected.header().parent;
        let sequence_before = consensus.sequencer().current_sequence();

        // Forces the append to fail deterministically, before any disk write: the
        // buffer carries eight bytes of slack past the header's `size`, which
        // `PrepareJournal::append` refuses rather than write slack that would
        // mis-frame the recovery scan. Any append failure reaches the same arm.
        let size = projected.header().size as usize;
        let mut padded = Message::<PrepareHeader>::new(size + 8);
        padded.as_mut_slice()[..size].copy_from_slice(projected.as_slice());

        consensus.pipeline_message(PlaneKind::Metadata, &padded);
        assert_eq!(consensus.sequencer().current_sequence(), op);

        md.on_replicate(padded).await;

        assert_eq!(
            consensus.sequencer().current_sequence(),
            sequence_before,
            "the claimed op must be handed back so the next request reuses it"
        );
        assert_eq!(consensus.last_prepare_checksum(), parent);
        assert!(
            consensus.pipeline_is_empty(),
            "the undurable prepare must not stay live in the pipeline"
        );
        #[allow(clippy::cast_possible_truncation)]
        let journaled = md.journal.as_ref().unwrap().handle().header(op as usize);
        assert!(
            journaled.is_none(),
            "the append failed, so the WAL must hold nothing at that op"
        );
    }
}
