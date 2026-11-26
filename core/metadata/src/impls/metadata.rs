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
use crate::applied_frontier::AppliedFrontier;
use crate::change_log::{
    AppliedChange, ChangeLogCapacities, ChangeLogSnapshot, CurrentState, MetadataChangeLog,
    change_actor, change_page, current_page, session_closed, session_registered, session_revoked,
};
use crate::impls::placement::{PlacementCounts, PlacementState};
use crate::permissioner::GrantsByCreation;
use crate::stm::authz::{
    PartitionsCreate, admits_partitions_create, gated_apply, reject_stale_session,
};
use crate::stm::consumer_group::CompleteConsumerGroupRevocationRequest;
use crate::stm::result::RevokedToken;
use crate::stm::shadow::ShadowTopicRequest;
use crate::stm::snapshot::{
    CaptureEncoder, FillSnapshot, MetadataCapture, MetadataSnapshot, PersistStage,
    RestoreSnapshotInPlace, Snapshot, SnapshotError, peek_sequence_number,
};
use crate::stm::stream::{Streams, StreamsInner, TruncatePartitionRequest};
use crate::stm::user::{CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest, Users};
use crate::stm::{ApplyContext, ConsensusGroupAllocator, StateMachine};
use bytes::Bytes;
use compio::BufResult;
use compio::io::AsyncWriteAtExt;
use consensus::{
    CLIENTS_TABLE_MAX, Canceled, ClientTable, ClientTableSnapshot, CommitLogEvent, CommitReply,
    Consensus, ContradictionSource, EXPIRED_SESSION_REQUEST_ID, EvictionContext, FatalReason,
    Pipeline, PipelineEntry, Plane, PlaneIdentity, PlaneKind, PreflightOutcome, PrepareRollback,
    Project, RESERVED_CLIENT_ID, ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind,
    VsrConsensus, ack_preflight, ack_quorum_reached, apply_preflight_consensus_plane,
    build_eviction_message, build_reply_message, build_reply_message_with,
    build_result_rejection_reply, emit_sim_event, fatal, fence_old_prepare_by_commit,
    is_caught_up_primary, is_caught_up_primary_except_barrier, is_settled_primary,
    panic_if_hash_chain_would_break_in_same_view, peek_committable_head, pipeline_prepare_common,
    register_preflight, replicate_preflight, replicate_prepare, request_preflight,
    send_eviction_to_client, send_prepare_ok as send_prepare_ok_common, verify_prepare_integrity,
};
use futures::channel::oneshot;
use futures::lock::Mutex;
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::consensus::CHECKSUM_UNSEALED;
use iggy_binary_protocol::primitives::metadata_changes::WireUserKey;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::CreatePartitionsRequest as WireCreatePartitionsRequest;
use iggy_binary_protocol::requests::partitions::CreatePartitionsWithAssignmentsRequest as PersistedCreatePartitionsRequest;
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest as WireCreateStreamRequest, RewrapStreamKeysRequest,
    UpdateStreamResolvedRequest,
};
use iggy_binary_protocol::requests::system::{
    FinalizeSessionRequest, ResourceTarget, RetirementDebt, SessionIdentity, UpdateResourceRequest,
};
use iggy_binary_protocol::requests::topics::CreateTopicRequest as WireCreateTopicRequest;
use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest as PersistedCreateTopicRequest;
use iggy_binary_protocol::requests::topics::UpdateTopicResolvedRequest;
use iggy_binary_protocol::requests::users::login_register::{CredentialProof, RegisterRequest};
use iggy_binary_protocol::requests::users::{CreateUserRequest, UpdateUserResolvedRequest};
use iggy_binary_protocol::responses::system::GetMetadataChangesResponse;
use iggy_binary_protocol::{
    Command, ConsensusHeader, EvictionReason, GenericHeader, Operation, PrepareHeader,
    PrepareOkHeader, ProtocolVersion, ReplyHeader, RoutedRequestHeader, WireDecode, WireEncode,
    WireName,
};
use iggy_common::calculate_checksum;
use iggy_common::variadic;
use iggy_common::wire_conversions::{resource_options_from_wire, resource_options_to_wire};
use iggy_common::{
    HeaderKey, HeaderValue, IggyError, IggyTimestamp, MetadataChangeStart, MetadataChangesQuery,
    OptionAdmissionContext, OptionKey, OptionOperation, OptionValue, OptionVisibility,
    OptionsProvenance, OptionsScope, Permissions, ResourceOptions, ServerKeyContext,
    TopicCreateOptions, TopicResolvedOptions, UserId, UserStatus, admit, admit_geo_topic,
    admit_tiered_storage, critical_keys_marker, derived_defaults, drop_options_fixed_by_type,
    stream_wrap_set, topic_option_keys, validate_geo_topic, validate_local_retention,
    validate_segment_max_age, validate_topic_segment_size,
};
use journal::local_gate::{LocalGate, LocalGateAcquire, OwnedLocalGateGuard};
use journal::superblock::{
    PingPongSuperblock, SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS, SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS,
    SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT, SuperblockStore,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use server_common::Message;
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::iobuf::{Frozen, Owned};
use server_common::sharding::IggyNamespace;
use server_common::yield_to_reactor;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
#[cfg(any(test, feature = "simulator"))]
use std::future::poll_fn;
use std::hash::Hasher;
use std::io;
use std::mem::size_of;
#[cfg(feature = "simulator")]
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::rc::{Rc, Weak};
use std::sync::Arc;
#[cfg(any(test, feature = "simulator"))]
use std::task::Poll;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use twox_hash::XxHash3_64;

// The writer retains this gate after caller cancellation, until the rename is durable.
static SNAPSHOT_IO: Mutex<()> = Mutex::new(());

/// Snapshot bytes encoded and written between two yields to the runtime. Encoding
/// one chunk holds the core for a few milliseconds, so the chunk size bounds each
/// stall, not the catalog size.
const SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;

/// A checkpoint job starts once `slot_count / CHECKPOINT_JOB_TRIGGER_DIVISOR`
/// ops committed above the checkpoint the superblock pairs: half the WAL, so the
/// job ends long before appends reach the forced-checkpoint margin.
const CHECKPOINT_JOB_TRIGGER_DIVISOR: u64 = 2;

/// After a failed job, the next one waits for `slot_count /
/// CHECKPOINT_JOB_RETRY_DIVISOR` more commits, so a failing disk does not start
/// a full snapshot write on every commit.
const CHECKPOINT_JOB_RETRY_DIVISOR: u64 = 16;

/// Starts a checkpoint job off the commit path, see
/// [`IggyMetadata::set_checkpoint_spawner`].
pub type CheckpointSpawner = Rc<dyn Fn(CheckpointJob)>;

/// A captured checkpoint that [`IggyMetadata::run_checkpoint_job`] makes durable
/// and trims the WAL to.
///
/// It holds `checkpoint_lock` until its snapshot and superblock writes end. A
/// job dropped before it runs keeps that lock closed, so a spawner drops one only
/// once the metadata it came from is gone.
pub struct CheckpointJob {
    lease: OwnedLocalGateGuard,
    capture: MetadataCapture,
    capture_time: Duration,
    /// Whether a requester held an offer of the snapshot that this job
    /// replaces. The job then leaves the WAL in place, see
    /// [`IggyMetadata::run_checkpoint_job`].
    keeps_wal: bool,
}

/// A WAL trim that waits for the journal gate or holds it, from
/// [`IggyMetadata::announce_checkpoint_trim`]. The trim stops being pending
/// when this drops, also when the job future is dropped at an await.
pub struct PendingTrim<'a>(&'a Cell<u32>);

impl Drop for PendingTrim<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// Who took a checkpoint and how long each step took, for its completion log.
#[derive(Clone, Copy, Debug, Default)]
struct CheckpointTimings {
    origin: CheckpointOrigin,
    capture: Duration,
    write: Duration,
    superblock: Duration,
}

/// Who captured and wrote a checkpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CheckpointOrigin {
    Job,
    Inline,
    /// The checkpoint has no stored timings, for example one that was paired
    /// before a restart or by a later superblock write.
    #[default]
    Unknown,
}

impl std::fmt::Display for CheckpointOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Job => "job",
            Self::Inline => "inline",
            Self::Unknown => "unknown",
        })
    }
}

/// Which path trimmed the WAL to a checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointTrimmer {
    Job,
    Margin,
}

impl std::fmt::Display for CheckpointTrimmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Job => "job",
            Self::Margin => "margin",
        })
    }
}

fn freeze_client_reply(
    message: Message<GenericHeader>,
) -> server_common::iobuf::Frozen<{ server_common::MESSAGE_ALIGN }> {
    message.into_frozen()
}

/// Last locally observed normal view of one committed partition incarnation.
/// Absence of a newer report preserves this view; it is not a health signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPlacementView {
    pub namespace: IggyNamespace,
    pub created_revision: u64,
    pub view: u32,
}

/// One suggested transfer, fenced by the observed view and committed incarnation.
/// Execution must separately confirm source and target partition readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPrimaryTransfer {
    pub namespace: IggyNamespace,
    pub created_revision: u64,
    pub view: u32,
    pub target: u8,
}

pub trait StreamsFrontend {
    #[must_use]
    fn users(&self) -> &Users;
    #[must_use]
    fn streams(&self) -> &Streams;

    /// Read stream or topic rows with the permissions of the same installed checkpoint.
    ///
    /// # Errors
    /// Returns a transient refusal between the two publications of a checkpoint.
    fn read_scoped_page<T>(
        &self,
        user_id: u32,
        project: impl FnOnce(
            &StreamsInner,
            Option<&Permissions>,
            Option<&GrantsByCreation>,
        ) -> Result<T, IggyError>,
    ) -> Result<T, IggyError>
    where
        Self: Sized,
    {
        // A normal delete prunes grants before a second stream publication can reuse the id.
        self.streams().read(|streams| {
            let (permissions, ordered) = self.users().read(|users| {
                if streams.installed_snapshot_op != users.installed_snapshot_op {
                    return Err(IggyError::TransientNotAccepted);
                }
                users
                    .items
                    .get(user_id as usize)
                    .filter(|user| user.status == UserStatus::Active)
                    .map(|user| {
                        (
                            user.permissions.clone(),
                            users.grants_by_creation.get(&user.id).cloned(),
                        )
                    })
                    .ok_or(IggyError::Unauthenticated)
            })?;
            project(streams, permissions.as_deref(), ordered.as_deref())
        })
    }
}

impl StreamsFrontend for MuxStateMachine<variadic!(Users, Streams)> {
    fn users(&self) -> &Users {
        &self.inner().0
    }

    fn streams(&self) -> &Streams {
        &self.inner().1.0
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointBoundary {
    TableCaptured,
    TemporaryCreated,
    Written,
    FileSynced,
    Renamed,
    DirectorySynced,
    SuperblockWritten,
    WalDrained,
}

#[cfg(test)]
thread_local! {
    static STOP_CHECKPOINT_AFTER: Cell<Option<CheckpointBoundary>> = const { Cell::new(None) };
    static COMPLETED_CHECKPOINTS: Cell<u64> = const { Cell::new(0) };
    static LAST_COMPLETED_CHECKPOINT: Cell<Option<(CheckpointOrigin, CheckpointTrimmer)>> =
        const { Cell::new(None) };
}

#[cfg(test)]
fn checkpoint_boundary(boundary: CheckpointBoundary) -> Result<(), SnapshotError> {
    if STOP_CHECKPOINT_AFTER.get() == Some(boundary) {
        STOP_CHECKPOINT_AFTER.set(None);
        return Err(SnapshotError::Io(io::Error::new(
            io::ErrorKind::Interrupted,
            "checkpoint crash boundary",
        )));
    }
    Ok(())
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
    /// the way [`SnapshotCoordinator::capture`] does on the live path.
    #[cfg(test)]
    pub(crate) const fn snapshot_mut(&mut self) -> &mut MetadataSnapshot {
        &mut self.snapshot
    }

    /// Persist the snapshot to disk.
    ///
    /// # Errors
    /// Returns `SnapshotError` if serialization or I/O fails.
    pub async fn persist(&self, path: &Path) -> Result<(), SnapshotError> {
        Self::write_durably(path, self.encode()?).await?;
        Ok(())
    }

    /// Write `payload` to `path` durably with [`write_snapshot_file`] in a task of
    /// its own, and return the payload's [`checkpoint_checksum`].
    ///
    /// The task holds [`SNAPSHOT_IO`] until the rename is durable, also after the
    /// caller stops waiting, so the next writer cannot rename under it.
    ///
    /// # Errors
    /// `SnapshotError::Persist` if any write, fsync, or rename fails, or
    /// `SnapshotError::Io` if the task ends without a result.
    pub(super) async fn write_durably(
        path: &Path,
        payload: impl SnapshotPayload + 'static,
    ) -> Result<u128, SnapshotError> {
        let permit = SNAPSHOT_IO.lock().await;
        let path = path.to_path_buf();
        let (sender, receiver) = oneshot::channel();
        compio::runtime::spawn(async move {
            let _permit = permit;
            let result =
                write_snapshot_file(&path, payload, SnapshotIo::Compio, SNAPSHOT_CHUNK_BYTES)
                    .await;
            if let Err(Err(error)) = sender.send(result) {
                warn!(path = %path.display(), %error, "snapshot write failed after its caller stopped");
            }
        })
        .detach();
        receiver
            .await
            .map_err(|_| SnapshotError::Io(io::Error::other("snapshot writer stopped")))?
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
    /// `SnapshotError::InvalidTrailer` if integrity framing is absent or damaged,
    /// `SnapshotError::ChecksumMismatch` if the trailer does not match its payload,
    /// `SnapshotError::UnsupportedFormatVersion` if it was
    /// written in a format version this build does not read, or `SnapshotError` if the
    /// file cannot be read or deserialized.
    pub fn load(path: &Path) -> Result<(Self, u128), SnapshotError> {
        let data = std::fs::read(path)?;
        let (payload, checksum) = split_trailer(&data)?;
        Ok((Self::decode(payload)?, checksum))
    }

    /// The sequence number of the snapshot at `path`. The trailer is verified as
    /// [`Self::load`] does, but the state sections are neither decoded nor
    /// validated, see [`peek_sequence_number`].
    ///
    /// # Errors
    /// As [`Self::load`], from the trailer and the leading fields only.
    pub fn load_sequence_number(path: &Path) -> Result<u64, SnapshotError> {
        let data = std::fs::read(path)?;
        let (payload, _) = split_trailer(&data)?;
        peek_sequence_number(payload)
    }
}

/// Bytes a snapshot file holds ahead of its integrity trailer, produced a chunk at
/// a time.
pub(super) trait SnapshotPayload {
    /// Move the next bytes into the empty `chunk`, cutting where the payload can
    /// be cut once `chunk` holds at least `chunk_bytes`. Returns whether bytes
    /// remain.
    fn fill(&mut self, chunk: &mut Vec<u8>, chunk_bytes: usize) -> Result<bool, SnapshotError>;
}

impl SnapshotPayload for Vec<u8> {
    fn fill(&mut self, chunk: &mut Vec<u8>, _chunk_bytes: usize) -> Result<bool, SnapshotError> {
        std::mem::swap(self, chunk);
        Ok(false)
    }
}

impl SnapshotPayload for CaptureEncoder {
    fn fill(&mut self, chunk: &mut Vec<u8>, chunk_bytes: usize) -> Result<bool, SnapshotError> {
        while self.encode_next(chunk)? {
            if chunk.len() >= chunk_bytes {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn persist_failure(stage: PersistStage) -> impl FnOnce(io::Error) -> SnapshotError {
    move |source| SnapshotError::Persist { stage, source }
}

/// Write `payload` and its integrity trailer to `path` durably: a temporary file,
/// fsync, rename, then a parent directory fsync. Returns the payload's
/// [`checkpoint_checksum`], hashed as the bytes go to the file, so the pairing is
/// over exactly what the file holds.
///
/// The payload is encoded, hashed and written in chunks of about `chunk_bytes`,
/// with a yield after each chunk, so the other tasks of this core run in
/// between.
#[allow(clippy::future_not_send)]
async fn write_snapshot_file(
    path: &Path,
    mut payload: impl SnapshotPayload,
    io: SnapshotIo,
    chunk_bytes: usize,
) -> Result<u128, SnapshotError> {
    let tmp_path = path.with_extension("bin.tmp");
    let mut file = io
        .create(&tmp_path)
        .await
        .note_descriptor_exhaustion(|| format!("creating {}", tmp_path.display()))
        .map_err(persist_failure(PersistStage::Write))?;
    #[cfg(test)]
    checkpoint_boundary(CheckpointBoundary::TemporaryCreated)?;
    let mut hasher = XxHash3_64::new();
    let mut chunk = Vec::new();
    let mut offset = 0;
    loop {
        let more = payload.fill(&mut chunk, chunk_bytes)?;
        hasher.write(&chunk);
        chunk = file
            .write_all_at(chunk, offset)
            .await
            .map_err(persist_failure(PersistStage::Write))?;
        offset += chunk.len() as u64;
        chunk.clear();
        io.yield_between_chunks().await;
        if !more {
            break;
        }
    }
    let checksum = u128::from(hasher.finish());
    // A separate write: appending to a payload handed over whole would copy it.
    chunk.extend_from_slice(&snapshot_trailer(checksum));
    file.write_all_at(chunk, offset)
        .await
        .map_err(persist_failure(PersistStage::Write))?;
    #[cfg(test)]
    checkpoint_boundary(CheckpointBoundary::Written)?;
    file.sync_all()
        .await
        .map_err(persist_failure(PersistStage::Sync))?;
    #[cfg(test)]
    checkpoint_boundary(CheckpointBoundary::FileSynced)?;
    drop(file);

    io.rename(&tmp_path, path)
        .await
        .map_err(persist_failure(PersistStage::Rename))?;
    #[cfg(test)]
    checkpoint_boundary(CheckpointBoundary::Renamed)?;

    // Fsync the parent directory to ensure the rename is durable.
    if let Some(parent) = path.parent() {
        let dir = io
            .open(parent)
            .await
            .note_descriptor_exhaustion(|| format!("opening directory {}", parent.display()))
            .map_err(persist_failure(PersistStage::DirSync))?;
        dir.sync_all()
            .await
            .map_err(persist_failure(PersistStage::DirSync))?;
    }
    #[cfg(test)]
    checkpoint_boundary(CheckpointBoundary::DirectorySynced)?;
    Ok(checksum)
}

/// Pending once, after waking its own task, so any executor polls it again. On
/// compio that is not a turn of the reactor, see [`SnapshotIo::yield_between_chunks`].
#[cfg(any(test, feature = "simulator"))]
async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        context.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

#[derive(Clone, Copy)]
enum SnapshotIo {
    Compio,
    #[cfg(feature = "simulator")]
    Synchronous,
}

enum SnapshotFile {
    Compio(compio::fs::File),
    #[cfg(feature = "simulator")]
    Synchronous(std::fs::File),
}

#[allow(clippy::future_not_send)]
impl SnapshotIo {
    async fn create(self, path: &Path) -> io::Result<SnapshotFile> {
        match self {
            Self::Compio => compio::fs::File::create(path)
                .await
                .map(SnapshotFile::Compio),
            #[cfg(feature = "simulator")]
            Self::Synchronous => std::fs::File::create(path).map(SnapshotFile::Synchronous),
        }
    }

    async fn open(self, path: &Path) -> io::Result<SnapshotFile> {
        match self {
            Self::Compio => compio::fs::File::open(path).await.map(SnapshotFile::Compio),
            #[cfg(feature = "simulator")]
            Self::Synchronous => std::fs::File::open(path).map(SnapshotFile::Synchronous),
        }
    }

    async fn read(self, path: &Path) -> io::Result<Vec<u8>> {
        match self {
            Self::Compio => compio::fs::read(path).await,
            #[cfg(feature = "simulator")]
            Self::Synchronous => std::fs::read(path),
        }
    }

    async fn rename(self, source: &Path, target: &Path) -> io::Result<()> {
        match self {
            Self::Compio => compio::fs::rename(source, target).await,
            #[cfg(feature = "simulator")]
            Self::Synchronous => std::fs::rename(source, target),
        }
    }

    /// Hand the core back between chunks. On compio a task that wakes itself runs
    /// again in the same executor pass, before the reactor reaps any I/O, so the
    /// pump would wait for many chunks. The simulator has no compio timer.
    async fn yield_between_chunks(self) {
        match self {
            Self::Compio => yield_to_reactor().await,
            #[cfg(feature = "simulator")]
            Self::Synchronous => yield_now().await,
        }
    }
}

#[allow(clippy::future_not_send)]
impl SnapshotFile {
    /// Write all of `buf` at `offset` and hand `buf` back for reuse.
    async fn write_all_at(&mut self, buf: Vec<u8>, offset: u64) -> io::Result<Vec<u8>> {
        match self {
            Self::Compio(file) => {
                let BufResult(result, buf) = file.write_all_at(buf, offset).await;
                result.map(|()| buf)
            }
            #[cfg(feature = "simulator")]
            Self::Synchronous(file) => file.write_all_at(&buf, offset).map(|()| buf),
        }
    }

    async fn sync_all(&self) -> io::Result<()> {
        match self {
            Self::Compio(file) => file.sync_all().await,
            #[cfg(feature = "simulator")]
            Self::Synchronous(file) => file.sync_all(),
        }
    }
}

/// Committed ops one [`IggyMetadata::commit_journal`] call applies before
/// returning to the pump.
///
/// The twin of `partitions::COMMIT_WALK_OPS_MAX`, and needed for the same
/// reason: the walk reads a WAL body and applies it per op with no await the
/// pump can interleave, and the resident `(commit_min, commit_max]` run is the
/// whole backlog after a repair or a rejoin, not the pipeline depth.
///
/// Every caller is re-driven, so a truncated walk resumes rather than losing
/// anything: `tick_metadata`'s walk backstop covers a follower and
/// `resume_stranded_commits` covers the primary, both level-triggered on
/// `commit_min < commit_max` every tick.
const COMMIT_WALK_OPS_MAX: usize = 64;

/// Why one [`IggyMetadata::commit_journal`] call stopped walking.
///
/// Only [`Self::BodyMissing`] names an op whose header the journal holds but
/// cannot serve. A [`Self::PipelineHead`] op is committed from the journal by
/// the pipeline driver (`commit_committable_prefix`, via `on_ack` or
/// `resume_stranded_commits`), so its prepare must stay resident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitWalkStop {
    /// `commit_min` reached `commit_max`.
    CaughtUp,
    /// The view's log is not yet proven, so the walk did not start.
    ViewLogUnproven,
    /// The per-call walk cap was reached; the next call resumes.
    WalkCap,
    /// The next op is the pipeline head; the pipeline driver commits it.
    PipelineHead,
    /// No header is resident at `op`.
    HeaderMissing { op: u64 },
    /// The header at `op` is resident but the WAL cannot produce its body.
    BodyMissing { op: u64 },
}

/// Integrity framing is mandatory because the superblock can lag snapshot publication.
const SNAPSHOT_TRAILER_MAGIC: u32 = 0x4953_4E50;

const SNAPSHOT_TRAILER_MAGIC_LEN: usize = size_of::<u32>();

/// `magic` + the payload's [`checkpoint_checksum`].
const SNAPSHOT_TRAILER_LEN: usize = SNAPSHOT_TRAILER_MAGIC_LEN + size_of::<u128>();

/// The integrity trailer for a payload with this [`checkpoint_checksum`].
fn snapshot_trailer(checksum: u128) -> [u8; SNAPSHOT_TRAILER_LEN] {
    let mut trailer = [0u8; SNAPSHOT_TRAILER_LEN];
    trailer[..SNAPSHOT_TRAILER_MAGIC_LEN].copy_from_slice(&SNAPSHOT_TRAILER_MAGIC.to_le_bytes());
    trailer[SNAPSHOT_TRAILER_MAGIC_LEN..].copy_from_slice(&checksum.to_le_bytes());
    trailer
}

/// Verify the mandatory trailer before exposing the snapshot payload.
fn split_trailer(data: &[u8]) -> Result<(&[u8], u128), SnapshotError> {
    if data.len() < SNAPSHOT_TRAILER_LEN {
        return Err(SnapshotError::Truncated {
            size: data.len() as u64,
        });
    }

    let (payload, trailer) = data.split_at(data.len() - SNAPSHOT_TRAILER_LEN);
    if trailer[..SNAPSHOT_TRAILER_MAGIC_LEN] != SNAPSHOT_TRAILER_MAGIC.to_le_bytes() {
        return Err(SnapshotError::InvalidTrailer);
    }
    let expected = u128::from_le_bytes(
        trailer[SNAPSHOT_TRAILER_MAGIC_LEN..]
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
/// what it wrote (`SnapshotCoordinator::persist_capture`), recovery hashes what it
/// read ([`IggySnapshot::load`]). Nothing re-serializes a decoded snapshot, so the
/// cross-check does not depend on decode-then-encode being byte-identical across
/// builds.
#[must_use]
pub fn checkpoint_checksum(encoded: &[u8]) -> u128 {
    u128::from(calculate_checksum(encoded))
}

/// The highest resident WAL entry at or above the snapshot floor and below
/// `bound`, or the snapshot floor with no checksum when no such entry remains.
#[must_use]
pub fn metadata_wal_head<J>(journal: &J, bound: u64) -> (u64, u128)
where
    J: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    let floor = journal.snapshot_op();
    let top = journal
        .last_op()
        .unwrap_or(floor)
        .min(bound.saturating_sub(1));
    (floor..=top)
        .rev()
        .find_map(|op| {
            usize::try_from(op)
                .ok()
                .and_then(|slot| journal.header(slot))
                .map(|header| (op, header.checksum))
        })
        .unwrap_or((floor, 0))
}

/// Refuses state transfer frontiers that no caught-up primary offers.
///
/// `commit_op` and `table_frontier` come from the serving peer and would move
/// the frontiers of this replica. A peer cannot have committed less than its
/// own snapshot contains, nor encode a table outside the range from the
/// snapshot to its commit point: `state_transfer_offer` builds all three from
/// one caught-up-primary read.
///
/// # Errors
/// [`SnapshotError::IncoherentManifest`].
pub const fn verify_state_transfer_manifest(
    snapshot_seq: u64,
    commit_op: u64,
    table_frontier: u64,
) -> Result<(), SnapshotError> {
    if commit_op < snapshot_seq || table_frontier < snapshot_seq || table_frontier > commit_op {
        return Err(SnapshotError::IncoherentManifest {
            snapshot_seq,
            commit_op,
            table_frontier,
        });
    }
    Ok(())
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
/// WAL) is orchestrated one layer up in `IggyMetadata`, by a checkpoint job or
/// inline at the margin (`checkpoint_if_needed`), so the superblock write can sit
/// between persist and drain; this type owns only the snapshot I/O and the
/// checkpoint bookkeeping.
pub struct SnapshotCoordinator<M> {
    data_dir: std::path::PathBuf,
    capture: fn(&M, u64, u64) -> Result<MetadataCapture, SnapshotError>,
    io: SnapshotIo,
    /// Remaining-journal-slots threshold at which a checkpoint is forced.
    /// Defaults to [`Self::CHECKPOINT_MARGIN`]; bootstrap raises it to at
    /// least the configured prepare-queue depth (see the static assert and
    /// [`Self::set_checkpoint_margin`]).
    checkpoint_margin: Cell<usize>,
    /// `(checkpoint_op, checkpoint_checksum)` of the last snapshot persisted or
    /// recovered at boot, `(0, 0)` when none. A view-change superblock write reads
    /// this so it records the current pairing instead of regressing it to zero.
    last_checkpoint: Cell<(u64, u128)>,
    // The snapshot pairing is published before superblock persistence and WAL drain.
    // An incomplete attempt must remain retryable at the same applied frontier.
    checkpoint_pending: Cell<bool>,
    /// Op of the checkpoint whose pairing the superblock durably holds, `0` when
    /// none. [`Self::last_checkpoint`] moves before the superblock write, so only
    /// this one bounds a WAL trim: a failed superblock write then trims nothing.
    /// Boot seeds it from the recovered snapshot, which the superblock may lag,
    /// see [`IggyMetadata::seed_checkpoint_ref`].
    superblock_checkpoint_op: Cell<u64>,
    /// Failed checkpoint jobs. A margin waiter that sees this move while it waits
    /// appends into the margin instead of retrying at once.
    failed_jobs: Cell<u64>,
    /// No job starts while `commit_min` is below this op, see
    /// [`CHECKPOINT_JOB_RETRY_DIVISOR`].
    job_retry_op: Cell<u64>,
    /// Timings of the durable checkpoint at this op, taken by the side that trims
    /// the WAL to it and logs its completion.
    untrimmed_timings: Cell<Option<(u64, CheckpointTimings)>>,
}

impl<M> SnapshotCoordinator<M> {
    /// Default number of remaining journal slots at which a checkpoint is
    /// forced. Must stay >= the prepare-queue depth: after a failed
    /// checkpoint the ops already pipelined append into this margin without
    /// one.
    ///
    /// Public so a caller sizing a journal can refuse a slot count at or below it:
    /// such a journal checkpoints on every commit rather than on occupancy.
    pub const CHECKPOINT_MARGIN: usize = 64;

    #[must_use]
    pub fn new(
        data_dir: std::path::PathBuf,
        capture: fn(&M, u64, u64) -> Result<MetadataCapture, SnapshotError>,
    ) -> Self {
        Self {
            data_dir,
            capture,
            io: SnapshotIo::Compio,
            checkpoint_margin: Cell::new(Self::CHECKPOINT_MARGIN),
            last_checkpoint: Cell::new((0, 0)),
            checkpoint_pending: Cell::new(false),
            superblock_checkpoint_op: Cell::new(0),
            failed_jobs: Cell::new(0),
            job_retry_op: Cell::new(0),
            untrimmed_timings: Cell::new(None),
        }
    }

    /// Do snapshot I/O with blocking calls, so host completions cannot change the
    /// deterministic simulator's schedule: the writer suspends only at its own
    /// yields. Select during replica construction.
    #[cfg(feature = "simulator")]
    pub const fn use_synchronous_io(&mut self) {
        self.io = SnapshotIo::Synchronous;
    }

    /// Raise (never lower) the forced-checkpoint margin. Bootstrap calls
    /// this with the configured prepare-queue depth so a deeper pipeline
    /// keeps its guarantee of journal room after a failed checkpoint; the
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

    /// Whether the superblock write of the last persisted checkpoint is still
    /// owed: its snapshot is durable, and no write that succeeded pairs it.
    fn checkpoint_unpaired(&self) -> bool {
        let checkpoint = self.last_checkpoint();
        checkpoint != (0, 0) && self.superblock_checkpoint_op.get() != checkpoint.0
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

    /// Capture the state at `commit_op` for [`Self::persist_capture`]. Synchronous,
    /// so every part belongs to the same frontier.
    fn capture(
        &self,
        stm: &M,
        commit_op: u64,
        created_at: u64,
        client_table: Option<ClientTableSnapshot>,
        change_log: ChangeLogSnapshot,
    ) -> Result<MetadataCapture, SnapshotError> {
        let mut capture = (self.capture)(stm, commit_op, created_at)?;
        // Fold in the client table and the change log, which the capture does not
        // see since neither is a state machine. Recovery restores the table as the
        // reconstruction floor for the drained WAL prefix.
        capture.client_table = client_table;
        capture.change_log = change_log;
        Ok(capture)
    }

    /// Durably persist `capture`, record its pairing, and return its checksum. Does
    /// NOT drain the WAL: the caller must durably record the pairing in the
    /// superblock first, so a crash between persist and drain recovers a consistent
    /// checkpoint with the WAL intact. The capture is encoded and written in chunks
    /// with a yield after each, so a large catalog does not stall this core.
    #[allow(clippy::future_not_send)]
    async fn persist_capture(&self, capture: MetadataCapture) -> Result<u128, SnapshotError> {
        capture.validate_credentials()?;
        let commit_op = capture.sequence_number;
        let checksum = self.write_snapshot(capture.into_encoder()).await?;
        self.last_checkpoint.set((commit_op, checksum));
        Ok(checksum)
    }

    /// Back off job starts after a job failed with `commit_min` committed.
    /// Counted from the failure, not from the capture: the commits during a
    /// slow write would otherwise use the backoff up.
    fn record_failed_job(&self, commit_min: u64, slot_count: u64) {
        self.failed_jobs.set(self.failed_jobs.get() + 1);
        self.job_retry_op
            .set(commit_min.saturating_add(slot_count / CHECKPOINT_JOB_RETRY_DIVISOR));
    }

    /// Write `payload` to [`Self::snapshot_path`] durably and return its
    /// [`checkpoint_checksum`], taken over the bytes as they reach the file.
    #[allow(clippy::future_not_send)]
    async fn write_snapshot(
        &self,
        payload: impl SnapshotPayload + 'static,
    ) -> Result<u128, SnapshotError> {
        let path = self.snapshot_path();
        match self.io {
            SnapshotIo::Compio => IggySnapshot::write_durably(&path, payload).await,
            #[cfg(feature = "simulator")]
            SnapshotIo::Synchronous => {
                write_snapshot_file(&path, payload, self.io, SNAPSHOT_CHUNK_BYTES).await
            }
        }
    }

    /// Drain the snapshotted prefix below `last_op` to reclaim WAL space. Runs
    /// only after the pairing is durable (see [`Self::persist_capture`]).
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

// A checkpoint job runs beside appends. At the margin an append waits for the
// job's durable part and trims, but a failed job leaves nothing to trim, and
// that append and the ones already pipelined behind it go into the margin
// without a checkpoint. The margin must cover a full prepare queue of them.
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
    /// DIFFERENT user. Terminal: retrying cannot help,
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
    /// The verified account is no longer active or the client ID is reserved.
    Unauthenticated,
    /// The request is at or below the watermark without a retained receipt.
    RequestTooOld,
    /// The request identity already belongs to another operation.
    OperationMismatch,
}

impl MetadataSubmitError {
    /// Whether a retry (here, or against another replica) could succeed.
    /// Ownership and request-identity refusals are terminal.
    /// Deliberately a deny-list: a new variant is transient by default, so
    /// adding one cannot silently surface a terminal error to clients.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        !matches!(
            self,
            Self::ClientIdOwnedByAnotherUser
                | Self::Unauthenticated
                | Self::RequestTooOld
                | Self::OperationMismatch
        )
    }
}

impl std::fmt::Display for MetadataSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrimary => f.write_str("not primary in normal status"),
            Self::NotCaughtUp => f.write_str("primary not yet caught up on commit_journal"),
            Self::PipelineFull => f.write_str("metadata prepare queue is full"),
            Self::InProgress => f.write_str("another in-flight prepare from this client"),
            Self::RequestTooOld => {
                f.write_str("request is at or below the watermark without a retained receipt")
            }
            Self::OperationMismatch => f.write_str("request identity belongs to another operation"),
            Self::Canceled => f.write_str("view change canceled the pending prepare"),
            Self::PrimaryUnreachable => f.write_str("no route to the metadata primary"),
            Self::ForwardTimedOut => {
                f.write_str("the metadata primary did not answer the forwarded register")
            }
            Self::ClientIdOwnedByAnotherUser => {
                f.write_str("client id already registered to a different user")
            }
            Self::Unauthenticated => f.write_str("verified account is no longer active"),
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

/// What one committed prepare produced: the reply to cache and send, and the
/// public changes the change log records for it, in canonical order.
#[derive(Debug)]
pub struct CommittedApply {
    pub reply: Message<ReplyHeader>,
    pub changes: Vec<AppliedChange>,
}

/// Apply one committed prepare to the state machine and client table.
///
/// The backup commit walk's per-op logic, shared so the simulator's WAL
/// reconstruction reaches identical state from the same log through one apply path.
/// Register creates an identity or resolves its matching retry. Logout ends the
/// exact session and rebalances its memberships; `FinalizeSession` removes it once
/// every partition group retired it or records that group's retirement debt. Every
/// other op applies to the state
/// machine and caches the reply for at-most-once dedup. `fire_notifier` runs the
/// post-commit hook (a no-op during reconstruction, before it is wired). Returns
/// the committed reply without advancing `commit_min`; the caller owns that counter.
///
/// `table_mutations_allowed` gates table updates only. Passing `false` requires
/// the table to retain the session identities needed by state-machine effects.
/// Live state transfer instead passes its separate replay table with `true`:
/// the newer protection table may already have finalized those identities.
///
/// # Panics
/// If a committed op fails to apply, which is a decode/corruption bug, since a
/// business rejection commits as a no-op rather than erroring: the committed log
/// must apply cleanly on every replica.
#[allow(clippy::too_many_lines)]
pub fn apply_committed_prepare<M>(
    mux_stm: &M,
    client_table: &RefCell<ClientTable>,
    table_mutations_allowed: bool,
    fire_notifier: impl Fn(Operation),
    prepare: Message<PrepareHeader>,
) -> CommittedApply
where
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = crate::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >,
{
    let header = *prepare.header();
    if table_mutations_allowed
        && let Err(error) = client_table
            .borrow_mut()
            .commit_capacity(header.retry_capacity as usize)
    {
        fatal(
            FatalReason::UnreconcilableLogFrontier,
            &format!(
                "invalid committed metadata retry capacity at op={}: {error}",
                header.op
            ),
        );
    }
    if header.operation == Operation::Register {
        let registration = RegisterRequest::decode_from(prepare.body()).unwrap_or_else(|_| {
            fatal(
                FatalReason::UnreconcilableLogFrontier,
                "invalid committed Register identity",
            )
        });
        if !mux_stm.users().read(|users| {
            users.accepts_credential_at(
                header.user_id,
                &registration.credential,
                iggy_common::IggyTimestamp::from(header.timestamp),
            )
        }) {
            let denied = crate::stm::result::ApplyReply::err(IggyError::Unauthenticated.as_code());
            return CommittedApply {
                reply: build_reply_message_with(&header, denied.reply_body_len(), |dst| {
                    denied.write_reply_body(dst);
                }),
                changes: Vec::new(),
            };
        }
        // A retry resolves to the existing entry and keeps its epoch, so only
        // an inserted client is a new session.
        let inserted = table_mutations_allowed && !client_table.borrow().contains(header.client);
        if table_mutations_allowed {
            let reply = build_reply_message(&header, &bytes::Bytes::new());
            client_table
                .borrow_mut()
                .commit_registration(header.client, header.user_id, &registration, reply)
                .unwrap_or_else(|error| {
                    fatal(
                        FatalReason::UnreconcilableLogFrontier,
                        &format!("cannot retain committed session: {error}"),
                    )
                });
        }
        let epoch = client_table
            .borrow()
            .get_epoch(header.client)
            .unwrap_or(header.op);
        // The session record comes first, then the handovers its memberships
        // took from the session change.
        let mut changes: Vec<AppliedChange> = inserted
            .then(|| session_registered(&header, registration.credential.user_created_at, epoch))
            .into_iter()
            .collect();
        changes.extend(mux_stm.streams().refresh_consumer_group_session(
            header.client,
            epoch,
            header.op,
            iggy_common::IggyTimestamp::from(header.timestamp),
            change_actor(&ApplyContext::from(&header)),
        ));
        return CommittedApply {
            reply: build_reply_message(&header, &bytes::Bytes::new()),
            changes,
        };
    }
    if header.operation == Operation::Logout {
        let ended = if table_mutations_allowed {
            client_table
                .borrow_mut()
                .commit_logout(
                    header.client,
                    header.user_id,
                    header.session,
                    build_reply_message(&header, &bytes::Bytes::new()),
                )
                .unwrap_or_else(|error| {
                    fatal(
                        FatalReason::UnreconcilableLogFrontier,
                        &format!("cannot retain Logout receipt: {error}"),
                    )
                })
        } else {
            mux_stm.streams().consumer_group_session(header.client) == Some(header.session)
        };
        let mut changes = Vec::new();
        if ended {
            // The session record comes first, then the memberships it ended.
            let user = mux_stm.users().read(|users| {
                users
                    .items
                    .get(header.user_id as usize)
                    .map(WireUserKey::from)
            });
            let closed = user.map(|user| session_closed(&header, user));
            let actor = closed.as_ref().and_then(|closed| closed.actor_user_id);
            changes.extend(closed);
            changes.extend(mux_stm.streams().remove_consumer_group_member(
                header.client,
                iggy_common::IggyTimestamp::from(header.timestamp),
                header.op,
                actor,
            ));
        }
        return CommittedApply {
            reply: build_reply_message(&header, &bytes::Bytes::new()),
            changes,
        };
    }
    if header.operation == Operation::FinalizeSession {
        let request = FinalizeSessionRequest::decode_from(prepare.body()).unwrap_or_else(|_| {
            fatal(
                FatalReason::UnreconcilableLogFrontier,
                "invalid session finalization",
            )
        });
        if table_mutations_allowed {
            client_table
                .borrow_mut()
                .finalize_sessions_through(request.retired_through);
        }
        mux_stm
            .streams()
            .record_retirement_debt(&request.debt, header.op);
        return CommittedApply {
            reply: build_reply_message(&header, &bytes::Bytes::new()),
            changes: Vec::new(),
        };
    }
    // Normal op: apply, build the reply. `Err` is decode/corruption only; a
    // business rejection commits as a deterministic no-op whose code rides
    // the reply body, replayed on retry.
    let prepared_at = IggyTimestamp::from(header.timestamp);
    let mut apply = reject_stale_session(&header, &client_table.borrow(), |user_id, token_id| {
        mux_stm
            .users()
            .read(|users| users.holds_live_token(user_id, token_id, prepared_at))
    })
    .map_or_else(|| gated_apply(mux_stm, prepare), Ok)
    .unwrap_or_else(|err| {
        panic!(
            "apply_committed_prepare: committed metadata op={} failed to apply: {err}",
            header.op
        );
    });
    if let Some(user) = apply.revoked_user {
        let ended = apply_user_revocation(
            mux_stm,
            client_table,
            table_mutations_allowed,
            Revocation::of_user(user, apply.kept_session_client),
            &header,
        );
        apply.changes.extend(ended);
    }
    if let Some(token) = apply.revoked_token {
        let ended = apply_user_revocation(
            mux_stm,
            client_table,
            table_mutations_allowed,
            Revocation::Token(token),
            &header,
        );
        apply.changes.extend(ended);
    }
    fire_notifier(header.operation);
    let reply = build_reply_message_with(&header, apply.reply_body_len(), |dst| {
        apply.write_reply_body(dst);
    });
    if table_mutations_allowed
        && header.client != RESERVED_CLIENT_ID
        && !message_bus::is_auto_commit_client(header.client)
        && !matches!(
            header.operation,
            Operation::CompleteConsumerGroupRevocation | Operation::CompleteLifecycle
        )
    {
        let outcome =
            client_table
                .borrow_mut()
                .commit_reply(header.client, header.user_id, reply.clone());
        log_commit_reply_outcome(outcome, header.client, header.op);
    }
    CommittedApply {
        reply,
        changes: apply.changes,
    }
}

/// The sessions a committed op ends: every session of a user, every session
/// but the one that changed the user's password, or the sessions a user opened
/// with one personal access token.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Revocation {
    User(WireUserKey),
    OtherSessions {
        user: WireUserKey,
        kept_client: u128,
    },
    Token(RevokedToken),
}

impl Revocation {
    /// Only a binary self-rotation preserves its submitting session.
    pub(crate) fn of_user(user: WireUserKey, kept_client: Option<u128>) -> Self {
        kept_client.map_or(Self::User(user), |kept_client| Self::OtherSessions {
            user,
            kept_client,
        })
    }

    pub(crate) const fn user(self) -> WireUserKey {
        match self {
            Self::User(user) | Self::OtherSessions { user, .. } => user,
            Self::Token(token) => token.user,
        }
    }

    /// Ends the sessions in `table` and returns them.
    pub(crate) fn end_sessions(self, table: &mut ClientTable, op: u64) -> Vec<SessionIdentity> {
        match self {
            Self::User(user) => table.end_user_sessions(user.user_id, op),
            Self::OtherSessions { user, kept_client } => {
                table.end_user_sessions_except(user.user_id, kept_client, op)
            }
            Self::Token(token) => table.end_token_sessions(token.user.user_id, token.token_id, op),
        }
    }

    fn covers(self, table: &ClientTable, client_id: u128) -> bool {
        table.get_user_id(client_id) == Some(self.user().user_id)
            && match self {
                Self::User(_) => true,
                Self::OtherSessions { kept_client, .. } => client_id != kept_client,
                Self::Token(token) => table.token_id(client_id) == token.token_id,
            }
    }
}

/// Ends the revoked sessions and returns their close records, then the records
/// of the memberships that ended with them.
fn apply_user_revocation<M: StreamsFrontend>(
    mux_stm: &M,
    client_table: &RefCell<ClientTable>,
    table_mutations_allowed: bool,
    revocation: Revocation,
    header: &PrepareHeader,
) -> Vec<AppliedChange> {
    let user = revocation.user();
    let mut sessions: Vec<SessionIdentity> = if table_mutations_allowed {
        revocation.end_sessions(&mut client_table.borrow_mut(), header.op)
    } else {
        // A transferred table may already contain this end marker while the
        // streams snapshot still needs the matching membership release.
        let table = client_table.borrow();
        table
            .ended_sessions()
            .filter(|identity| {
                identity.metadata_watermark == header.op
                    && revocation.covers(&table, identity.client_id)
            })
            .collect()
    };
    // Slot order may differ after a restore, so every apply path orders by client.
    sessions.sort_by_key(|identity| identity.client_id);
    let actor = change_actor(&ApplyContext::from(header));
    let client_ids: Vec<u128> = sessions.iter().map(|identity| identity.client_id).collect();
    let mut changes: Vec<AppliedChange> = sessions
        .iter()
        .map(|identity| session_revoked(user, identity.client_id, identity.session, actor))
        .collect();
    changes.extend(mux_stm.streams().remove_consumer_group_members(
        &client_ids,
        iggy_common::IggyTimestamp::from(header.timestamp),
        header.op,
        actor,
    ));
    changes
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
    /// `Some` on shard 0, `None` on other shards. Server bootstrap
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
    /// `server/src/boot/handoff.rs` `await_metadata_bundle` /
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
    /// encode, filesystem syncs and the WAL drain. Whoever writes builds
    /// its `VsrState` inside this section with no await in between, so the last
    /// writer carries the freshest view and the durable view cannot regress.
    ///
    /// Held across the write `.await`, so it uses the same single-threaded,
    /// cancel-safe [`LocalGate`] as `journal_gate`, which has no atomics: this
    /// shard is never `Sync`, so a `tokio::sync::Mutex` would only add an
    /// atomic RMW per gated send for exclusion that `Cell` state already provides.
    superblock_lock: LocalGate,
    /// Serializes whole checkpoints against each other: `persist_capture` renames
    /// over the single `snapshot.bin` and `drain` rewrites the WAL through a shared
    /// `wal.tmp`, so two concurrent checkpoints would race both. Distinct from
    /// [`Self::superblock_lock`], which a checkpoint takes only for its own pairing
    /// write. A checkpoint holds this one and then acquires that one; nothing takes
    /// them in the other order.
    ///
    /// Shared so a checkpoint job can hold it as an owned lease from its capture to
    /// the end of its superblock write. Every WAL trim also runs under it.
    checkpoint_lock: Rc<LocalGate>,
    /// Consecutive failed superblock writes, and the clock reading after which the
    /// next attempt may run. A persistent `ENOSPC` / `EIO` would otherwise re-run a
    /// full `atomic_replace` (create, write, fsync, rename, dir fsync) on every 10 ms
    /// consensus tick, on the executor that also serves partition traffic. Reset on
    /// the first success. See [`Self::persist_superblock_if_needed`] for the terminal
    /// policy.
    superblock_write_failures: Cell<u64>,
    superblock_retry_after_micros: Cell<u64>,
    /// State machine - lives on all shards.
    ///
    /// Shared so shard 0's bootstrap can keep a clone alive past every
    /// fallible step that owns this struct: the peer shards read through
    /// handles minted off this writer, and dropping it makes their
    /// `LeftRight::read` panic. See `server/src/boot::shard_main`.
    pub mux_stm: Rc<M>,
    pub allocator: ConsensusGroupAllocator,
    placement: RefCell<PlacementState>,
    /// Snapshot coordinator - present when persistent checkpointing is configured.
    pub coordinator: Option<SnapshotCoordinator<M>>,
    /// Serializes `on_replicate`'s journal-mutation section (forced
    /// checkpoint + WAL append) across concurrent drivers (the pump loop
    /// and the detached per-client submit tasks), with the shard's WAL
    /// rewrites and repair appends, which take it through
    /// [`Self::hold_journal_writes`], with a state transfer install, and with the
    /// WAL trim of a checkpoint job. Lock order: `journal_gate`,
    /// `checkpoint_lock`, `superblock_lock`. A job holds `checkpoint_lock` only
    /// before it takes this gate, never while it waits for it. Waiters
    /// take it in arrival order, so a primary appends in op order. Ungated
    /// they race `SnapshotCoordinator::checkpoint`: every driver crossing the
    /// `remaining_capacity <= CHECKPOINT_MARGIN` boundary runs a full
    /// checkpoint, and the concurrent `journal.drain()` calls collide on the
    /// WAL rewrite -- shared `wal.tmp`, ENOENT for every rename that loses,
    /// short reads after the winner's reopen. Appends racing a drain are just
    /// as unsound: the drain's live-set partition misses an append landing
    /// mid-rewrite and the rewrite silently discards it. See [`LocalGate`].
    journal_gate: LocalGate,
    /// Checkpoint jobs that wait for `journal_gate` or hold it for their WAL
    /// trim. The gate serves its waiters in arrival order, so an append that
    /// arrives behind such a job waits for the whole rewrite. The shard 0 pump
    /// must not wait that long: see [`Self::checkpoint_trim_pending`].
    pending_trims: Cell<u32>,
    /// Per-client session state (sessions, dedup, eviction). Metadata-only.
    pub client_table: RefCell<ClientTable>,
    /// Late-bound post-commit notifier. Fires once per committed normal op
    /// after `gated_apply` returns (including a gated `Unauthorized` no-op that
    /// never reaches [`crate::stm::StateMachine::update`]) in both
    /// [`Plane::on_ack`] and [`Self::commit_journal`]. `None` until
    /// [`Self::set_commit_notifier`] runs (the server bootstrap on shard
    /// 0 sets it; peer shards and tests leave it `None`).
    commit_notifier: RefCell<Option<CommitNotifier>>,
    /// Late-bound starter of checkpoint jobs, `None` until
    /// [`Self::set_checkpoint_spawner`] runs. Without it every checkpoint runs
    /// inline at the forced-checkpoint margin.
    checkpoint_spawner: RefCell<Option<CheckpointSpawner>>,
    /// Client-table mutations at or below this op are already reflected in a
    /// state-transferred table, so the tail-repair commit walk must skip
    /// them to preserve newer receipts and incarnations. `0`
    /// outside state transfer (no op is skipped). Monotone per install.
    client_table_frontier: Cell<u64>,
    /// Session history at the state machine's replay frontier. The transferred
    /// protection table can have already forgotten sessions whose membership
    /// effects still need replaying. Dropped once replay reaches that table.
    replay_client_table: RefCell<Option<RefCell<ClientTable>>>,
    /// Last built [`StateTransferOffer`], shared by every requester of the same
    /// snapshot generation. Rebuilding per request re-reads the whole snapshot
    /// on shard 0's pump, and hands each requester its own multi-MB copy. A
    /// checkpoint clears it at its capture and each writer of `snapshot.bin`
    /// after its rename, so it never serves a replaced file to a later requester.
    transfer_offer_cache: RefCell<Option<Rc<StateTransferOffer>>>,
    /// Every offer built, to tell which snapshots a requester can still pull:
    /// the shard that serves an offer holds it until it expires. See
    /// [`Self::offer_needs_wal_above`].
    served_offers: RefCell<Vec<Weak<StateTransferOffer>>>,
    /// Moves on every clear of `transfer_offer_cache`. An offer whose read of
    /// `snapshot.bin` spans a move may hold a file that is replaced or about to
    /// be, so it refuses rather than reach the cache or a peer.
    snapshot_generation: Cell<u64>,
    /// See [`Self::refuse_state_transfer_offers`].
    #[cfg(feature = "simulator")]
    state_transfer_offers_refused: Cell<bool>,
    /// Prepares the backup gap check destroyed since `tick_metadata` last
    /// drained the count into `metadata_prepare_gap_drops_total`. What it does
    /// and does not prove is `IggyPartition::prepare_gap_drops`, verbatim; what
    /// differs is the frontier the check runs against, the journal head rather
    /// than the sequencer, so this also counts the ops that fall outside what
    /// metadata repair can refill (an interior hole below the head, a forward
    /// gap above `commit_max`).
    ///
    /// `Cell` because every method on this type takes `&self`.
    prepare_gap_drops: Cell<u64>,
    /// `[metadata] partitions_max`, zero for no cap. See
    /// [`Self::admit_partitions`].
    partitions_max: Cell<u32>,
    /// `[personal_access_token] max_tokens_per_user`, zero for no cap. See
    /// [`Self::admit_personal_access_token`].
    max_tokens_per_user: Cell<u32>,
    /// `[cluster] default_replication_factor`, `None` for every node. See
    /// [`Self::default_replication_factor`].
    default_replication_factor: Cell<Option<u8>>,
    /// The metadata view in which shard 0 last found every member able to
    /// tier topics. See [`Self::observe_tiered_storage_readiness`].
    tiered_storage_ready_view: Cell<Option<u32>>,
    /// Highest metadata op whose apply has been PUBLISHED on this node, plus
    /// the reads parked on it. Shared by every shard; see
    /// [`AppliedFrontier`] for the ordering and the wake contract.
    applied_frontier: Arc<AppliedFrontier>,
    /// This node's server keys. The primary refuses to prepare a
    /// `CreateStream` whose wrap set none of them opens.
    stream_key_context: Arc<ServerKeyContext>,
    /// The public results of applied operations. The write side lives on
    /// shard 0 with the state machines, every other shard reads it.
    change_log: Rc<MetadataChangeLog>,
}

impl<B, J, S, M, SB> IggyMetadata<VsrConsensus<B>, J, S, M, SB>
where
    B: MessageBus,
    M: StreamsFrontend,
{
    /// Resume the applied frontier where recovery left the state machine.
    ///
    /// Recovery replays the committed WAL prefix before any listener binds, so
    /// without this the frontier reads zero on a rebooted node and every read
    /// whose caller holds a pre-restart commit parks until its deadline. A
    /// no-op on a peer shard, which owns no consensus and shares shard 0's
    /// cell.
    pub fn seed_applied_frontier_from_consensus(&self) {
        if let Some(consensus) = self.consensus.as_ref() {
            self.applied_frontier
                .record_recovery_revision(self.mux_stm.streams().read(|inner| inner.revision));
            self.advance_applied_frontier(consensus.commit_min());
        }
    }
}

impl<C, J, S, M, SB> IggyMetadata<C, J, S, M, SB>
where
    M: StreamsFrontend + FillSnapshot<MetadataCapture>,
{
    /// Create a new `IggyMetadata` instance.
    ///
    /// The `FillSnapshot<MetadataCapture>` bound is captured here via a
    /// function pointer so that no downstream caller needs the bound.
    #[must_use]
    pub fn new(
        consensus: Option<C>,
        journal: Option<J>,
        snapshot: Option<S>,
        superblock: Option<Rc<SB>>,
        mux_stm: impl Into<Rc<M>>,
        data_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let mux_stm = mux_stm.into();
        let allocator =
            ConsensusGroupAllocator::new(mux_stm.streams().highest_partition_consensus_group_id());
        let coordinator =
            data_dir.map(|dir| SnapshotCoordinator::new(dir, MetadataCapture::create));
        Self {
            consensus,
            journal,
            snapshot,
            superblock,
            superblock_lock: LocalGate::new(),
            checkpoint_lock: Rc::new(LocalGate::new()),
            superblock_write_failures: Cell::new(0),
            superblock_retry_after_micros: Cell::new(0),
            mux_stm,
            allocator,
            placement: RefCell::default(),
            coordinator,
            journal_gate: LocalGate::new(),
            pending_trims: Cell::new(0),
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
            commit_notifier: RefCell::new(None),
            checkpoint_spawner: RefCell::new(None),
            client_table_frontier: Cell::new(0),
            replay_client_table: RefCell::new(None),
            transfer_offer_cache: RefCell::new(None),
            served_offers: RefCell::new(Vec::new()),
            snapshot_generation: Cell::new(0),
            #[cfg(feature = "simulator")]
            state_transfer_offers_refused: Cell::new(false),
            prepare_gap_drops: Cell::new(0),
            partitions_max: Cell::new(0),
            max_tokens_per_user: Cell::new(0),
            default_replication_factor: Cell::new(None),
            tiered_storage_ready_view: Cell::new(None),
            applied_frontier: Arc::default(),
            stream_key_context: Arc::default(),
            change_log: Rc::new(MetadataChangeLog::new(ChangeLogCapacities::default())),
        }
    }
}

impl<C, J, S, M, SB> IggyMetadata<C, J, S, M, SB>
where
    M: StreamsFrontend,
{
    /// One change page of this node's log. Every shard answers it from its
    /// read view, on an image coherent with the applied frontier.
    ///
    /// # Errors
    /// `InvalidCommand` for a request the rules refuse or a current state
    /// start, `TransientNotAccepted` when no coherent image was captured.
    pub fn metadata_changes(
        &self,
        query: &MetadataChangesQuery,
    ) -> Result<GetMetadataChangesResponse, IggyError> {
        let groups = query.validate()?;
        self.change_log
            .read_coherent(&self.applied_frontier, |log, head_op| {
                change_page(log, head_op, query, groups)
            })
            .ok_or(IggyError::TransientNotAccepted)?
    }

    /// One current state page, assembled in one synchronous pass over the
    /// applied state. Only the metadata shard holds the client table and
    /// writes the state, so only it answers.
    ///
    /// # Errors
    /// `InvalidCommand` for a request the rules refuse or a change start,
    /// `InvalidConfiguration` on any other shard, `TransientNotAccepted` once
    /// the log's write side is gone.
    pub fn metadata_current_state(
        &self,
        query: &MetadataChangesQuery,
    ) -> Result<GetMetadataChangesResponse, IggyError> {
        let groups = query.validate()?;
        let MetadataChangeStart::Current(token) = &query.start else {
            return Err(IggyError::InvalidCommand);
        };
        if self.consensus.is_none() {
            return Err(IggyError::InvalidConfiguration);
        }
        let head_op = self.applied_frontier.get();
        let client_table = self.client_table.borrow();
        // A transferred protection table can run ahead of the applied state.
        let sessions = (self.client_table_frontier.get() <= head_op).then_some(&*client_table);
        self.mux_stm.streams().read(|streams| {
            self.mux_stm.users().read(|users| {
                self.change_log
                    .read(|log| {
                        current_page(
                            log,
                            head_op,
                            groups,
                            token.as_ref().map(AsRef::as_ref),
                            query.limit,
                            &CurrentState {
                                streams,
                                users,
                                sessions,
                            },
                        )
                    })
                    .ok_or(IggyError::TransientNotAccepted)?
            })
        })
    }
}

impl<C, J, S, M, SB> IggyMetadata<C, J, S, M, SB> {
    /// Take and clear the gap-drop count (`prepare_gap_drops`).
    #[must_use = "dropping the count loses the only record those prepares existed"]
    pub const fn take_prepare_gap_drops(&self) -> u64 {
        self.prepare_gap_drops.replace(0)
    }

    /// Share one process-wide applied frontier with every other shard.
    ///
    /// Consumed at construction rather than swapped in later: a shard that
    /// served a read against its own private cell would gate on a number that
    /// never moves. Shard 0 mints the cell in bootstrap, before any shard is
    /// built, and hands each shard a clone.
    #[must_use]
    pub fn with_applied_frontier(mut self, applied_frontier: Arc<AppliedFrontier>) -> Self {
        self.applied_frontier = applied_frontier;
        self
    }

    /// Install the change log: the one recovery rebuilt on shard 0, a read
    /// view of it on every other shard.
    #[must_use]
    pub fn with_change_log(mut self, change_log: impl Into<Rc<MetadataChangeLog>>) -> Self {
        self.change_log = change_log.into();
        self
    }

    #[must_use]
    pub const fn change_log(&self) -> &Rc<MetadataChangeLog> {
        &self.change_log
    }

    /// Admit `CreateStream` wrap sets against this node's server keys, the
    /// ones its partitions open stream keys with. Without them every wrapped
    /// create is refused.
    #[must_use]
    pub fn with_stream_key_context(mut self, stream_key_context: Arc<ServerKeyContext>) -> Self {
        self.stream_key_context = stream_key_context;
        self
    }

    /// The node-wide applied frontier, readable on every shard. Reads gate on
    /// it so a client cannot be served state older than a write it already saw
    /// acked, and park on its wait when it is behind.
    #[must_use]
    pub const fn applied_frontier(&self) -> &Arc<AppliedFrontier> {
        &self.applied_frontier
    }

    /// Publish `op` as applied and wake the reads waiting at or below it.
    /// Monotone, so a lower value is a no-op.
    ///
    /// Must run AFTER the apply's `publish()` and, on the commit path, in the
    /// same await-free region as `advance_commit_min`: a reader that sees the
    /// frontier must be guaranteed to see the op's effects.
    pub fn advance_applied_frontier(&self, op: u64) {
        self.applied_frontier.advance(op);
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

    /// Drop the cached state-transfer offer, releasing its snapshot copy, and
    /// start a new snapshot generation.
    ///
    /// Called at each checkpoint capture, after each replacement of
    /// `snapshot.bin`, and by the shard's expiry sweep once no requester holds
    /// an offer: the cache exists to collapse repeat builds within one rejoin,
    /// not to pin a snapshot for the life of the process. The sweep and the
    /// offer build both run on the pump, so a sweep never lands inside an
    /// offer's read.
    pub fn clear_state_transfer_offer_cache(&self) {
        self.transfer_offer_cache.borrow_mut().take();
        self.snapshot_generation
            .set(self.snapshot_generation.get().wrapping_add(1));
    }

    /// Whether an offer that is still held, by a shard that serves it or by
    /// the cache, needs the WAL above `floor`, the last op that the journal
    /// trimmed. A requester installs the snapshot of its offer and then
    /// repairs from the op after it.
    fn offer_needs_wal_above(&self, floor: u64) -> bool {
        self.served_offers
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .any(|offer| offer.snapshot_seq >= floor)
    }

    /// Whether `checkpoint_lock` is held: by a checkpoint job from its capture to
    /// its superblock write and again during its WAL trim, by a checkpoint at the
    /// forced-checkpoint margin, or by an install.
    #[must_use]
    pub fn checkpoint_running(&self) -> bool {
        self.checkpoint_lock.try_acquire().is_none()
    }

    /// Install (or replace) the post-commit notifier. Passing `None`
    /// removes any previous one. Server bootstrap calls this on shard 0
    /// only; peer shards never commit metadata locally.
    pub fn set_commit_notifier(&self, notifier: Option<CommitNotifier>) {
        *self.commit_notifier.borrow_mut() = notifier;
    }

    /// Install (or replace) the starter of checkpoint jobs. Passing `None` removes
    /// it. The spawner must run each job with [`Self::run_checkpoint_job`] on this
    /// shard. It is called from the commit path, so it must not run the job inline.
    pub fn set_checkpoint_spawner(&self, spawner: Option<CheckpointSpawner>) {
        *self.checkpoint_spawner.borrow_mut() = spawner;
    }

    /// Seed the coordinator's last-checkpoint pairing at boot from the recovered
    /// snapshot, so the first post-boot view-change superblock write records the real
    /// `(checkpoint_op, checksum)` instead of `(0, 0)`. No-op without a coordinator
    /// (peer shards, the simulator). Server bootstrap calls this on shard 0 after
    /// cross-checking the pairing.
    pub fn seed_checkpoint_ref(&self, checkpoint_op: u64, checkpoint_checksum: u128) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.seed_last_checkpoint(checkpoint_op, checkpoint_checksum);
            // The snapshot op, even when the superblock pairing lags it: boot
            // accepts that lag and opens the journal with this op as its floor.
            // So the seed itself trims nothing, and it counts the commits to
            // the first job from the state that boot recovered.
            coordinator.superblock_checkpoint_op.set(checkpoint_op);
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
        if client_table.capacity_committed() && current.capacity() != client_table.capacity() {
            warn!(
                configured_capacity = current.capacity(),
                committed_capacity = client_table.capacity(),
                "metadata retry capacity is fixed by committed state; ignoring local configuration"
            );
        }
        *current = client_table;
        true
    }

    /// Client-table mutations at or below the frontier are already in the
    /// state-transferred protection table. Replay uses a separate table at the
    /// state machine's frontier until it catches up.
    const fn client_table_mutation_allowed(&self, op: u64) -> bool {
        op > self.client_table_frontier.get()
    }

    fn apply_prepare(&self, prepare: Message<PrepareHeader>) -> Message<ReplyHeader>
    where
        M: StreamsFrontend
            + StateMachine<
                Input = Message<PrepareHeader>,
                Output = crate::stm::result::ApplyReply,
                Error = IggyError,
            >,
    {
        let header = *prepare.header();
        let op = header.op;
        let committed = if self.client_table_mutation_allowed(op) {
            apply_committed_prepare(
                &*self.mux_stm,
                &self.client_table,
                true,
                |operation| self.fire_commit_notifier(operation),
                prepare,
            )
        } else {
            let history = self.replay_client_table.borrow();
            let table = history
                .as_ref()
                .expect("transferred history must retain its replay table");
            let committed = apply_committed_prepare(
                &*self.mux_stm,
                table,
                true,
                |operation| self.fire_commit_notifier(operation),
                prepare,
            );
            drop(history);
            if op == self.client_table_frontier.get() {
                self.replay_client_table.borrow_mut().take();
            }
            committed
        };
        // Published before the caller advances the applied frontier and
        // `commit_min`, so no reader sees a record above them.
        if let Err(error) = self.change_log.append(&header, committed.changes) {
            panic!("apply_prepare: change log refused committed metadata op={op}: {error}");
        }
        committed.reply
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

    /// Cap the partitions of all streams and topics at `[metadata]
    /// partitions_max`, zero for no cap. Only the primary checks it, with the
    /// value of its own node, when it admits a `CreateTopic` or
    /// `CreatePartitions`, so every node needs the same value.
    pub fn set_partitions_max(&self, partitions_max: u32) {
        self.partitions_max.set(partitions_max);
    }

    /// Give a topic created without a replication factor `factor` nodes, or
    /// every node for `None`, from `[cluster] default_replication_factor`. Only
    /// the primary reads it, when it admits a `CreateTopic`, and the topic
    /// stores the factor it resolved, so every node needs the same value.
    pub fn set_default_replication_factor(&self, factor: Option<u8>) {
        self.default_replication_factor.set(factor);
    }

    /// The failure domain of each node, indexed by node id, from `[cluster]
    /// nodes`. Placement spreads a partition's members over as many as it can.
    /// Only the primary reads it, when it admits a create, so every node needs
    /// the same list.
    pub fn set_node_racks(&self, racks: &[Option<String>]) {
        let mut names: Vec<&str> = Vec::new();
        let domains = racks
            .iter()
            .map(|rack| {
                rack.as_deref().map(|rack| {
                    let index = names
                        .iter()
                        .position(|known| *known == rack)
                        .unwrap_or_else(|| {
                            names.push(rack);
                            names.len() - 1
                        });
                    u32::try_from(index).unwrap_or(u32::MAX)
                })
            })
            .collect();
        self.placement.borrow_mut().set_racks(domains);
    }

    /// The replication factor a topic created without one gets in a cluster
    /// of `nodes` nodes: the configured number lowered to `nodes`, or every
    /// node.
    #[must_use]
    pub fn default_replication_factor(&self, nodes: u8) -> u8 {
        self.default_replication_factor
            .get()
            .map_or(nodes, |factor| factor.min(nodes))
    }

    /// Cap the personal access tokens of each user at `[personal_access_token]
    /// max_tokens_per_user`, zero for no cap. Only the primary checks it, with
    /// the value of its own node, when it admits a `CreatePersonalAccessToken`,
    /// so every node needs the same value.
    pub fn set_max_tokens_per_user(&self, max_tokens_per_user: u32) {
        self.max_tokens_per_user.set(max_tokens_per_user);
    }

    /// The cap that [`Self::set_max_tokens_per_user`] set. Its rejection
    /// carries only the error code, so an edge that names the cap reads it
    /// here.
    #[must_use]
    pub const fn max_tokens_per_user(&self) -> u32 {
        self.max_tokens_per_user.get()
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
    #[allow(clippy::too_many_lines)]
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
            {
                let outcome = register_preflight(
                    consensus,
                    &self.client_table,
                    client_id,
                    user_id,
                    message.body(),
                    request_checksum,
                    |credential| {
                        self.accepts_registration_proof(
                            user_id,
                            credential,
                            consensus.clock_realtime_micros(),
                        )
                    },
                );
                apply_preflight_consensus_plane(
                    consensus,
                    self.fence_lifecycle_preflight(outcome, &message),
                    client_id,
                )
                .await
            }
        } else {
            let outcome = request_preflight(
                consensus,
                &self.client_table,
                client_id,
                session,
                request,
                operation,
            );
            apply_preflight_consensus_plane(
                consensus,
                self.fence_lifecycle_preflight(outcome, &message),
                client_id,
            )
            .await
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
        // retries via read-timeout). A pending WAL trim buffers too: the
        // append would wait for it on the pump, see
        // [`Self::checkpoint_trim_pending`]. No await separates this test
        // from the gate acquire in `on_replicate`.
        if consensus.pipeline_is_full() || self.checkpoint_trim_pending() {
            let push_result = consensus.push_queued_request(consensus::RequestEntry::new(message));
            if push_result.is_err() {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    client = client_id,
                    request = request,
                    "on_request: request queue full behind a full prepare queue or a pending WAL trim, dropping"
                );
            }
            return;
        }

        let request_header = *message.header();
        let prepare = match self.prepare_request(message) {
            Ok(prepare) => prepare,
            Err(refusal) if is_admission_refusal(&refusal) => {
                let reply = build_result_rejection_reply(
                    &request_header,
                    consensus.commit_max(),
                    refusal.as_code(),
                );
                if let Err(error) = consensus
                    .message_bus()
                    .send_to_client(client_id, freeze_client_reply(reply.into_generic()))
                    .await
                {
                    debug!(%error, client_id, "admission refusal could not reach client");
                }
                return;
            }
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

        // Fenced by commit: this replica already applied the op, so it is not
        // journaled or forwarded again. A new primary can still need the ack:
        // a `DoViewChange` caps its commit at the sender's head, so the view can
        // restart an op as uncommitted that this replica applied. `send_prepare_ok`
        // acks only a journaled copy with the same checksum. (Mirror of the
        // partition plane's split in `IggyPartition::on_replicate`.)
        #[allow(clippy::cast_possible_truncation)]
        if fence_old_prepare_by_commit(consensus, &header) {
            if consensus.contradicts_applied_log(
                PlaneKind::Metadata,
                ContradictionSource::Prepare,
                header.view,
                [&header],
                |op| self.applied_header(op),
            ) {
                return;
            }
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                commit = consensus.commit_max(),
                operation = ?header.operation,
                "received old prepare (<= commit), re-acking a matching journaled copy"
            );
            self.send_prepare_ok(&header).await;
            return;
        }

        // Durable here but not yet committed, and the primary is retransmitting
        // it: our original PrepareOk was lost (e.g. the primary's inbox
        // overflowed under a client burst). Re-ack only the retransmitted op.
        // The primary's retransmit cycle walks every
        // un-acked op in the window (`prepare_timeout_targets`), so a lost ack
        // for a lower op gets its own retransmit and its own re-ack; re-acking the
        // whole suffix here is O(window^2) PrepareOks per cycle across the
        // backups, which can overflow the primary's inbox -- the very failure
        // this path recovers from. The primary is idempotent on duplicate acks.
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
                "journal already holds prepare, re-acking it"
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
        //
        // On the primary, every submit claims its op and reaches this acquire
        // with no await in between. The gate goes to its waiters in arrival
        // order, so the appends below follow the op order.
        let journal_gate = self.journal_gate.acquire().await;

        // An in-process submit waits on the gate and the checkpoint below on its
        // caller's task, not the pump's, so a later view can conclude meanwhile.
        // That dropped the pipeline entry and reset the sequencer, and the
        // follower path below would take this prepare for the next op of the
        // view's log. Mid view change it still lands: the sequencer a
        // `DoViewChange` reports already counts it, and the reconcile that ends
        // the view change waits for the gate.
        let view_ended = || {
            let ended = header.view < consensus.view() && consensus.is_normal();
            if ended {
                warn!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    view = consensus.view(),
                    prepare_view = header.view,
                    op = header.op,
                    operation = ?header.operation,
                    "dropping prepare of a view that ended before its append"
                );
            }
            ended
        };
        // Before the checkpoint too, or the reconcile of the view waits one out
        // on the gate for a prepare that is then dropped.
        if view_ended() {
            return;
        }

        // Best-effort WAL reclamation. A failed checkpoint must NOT drop the
        // prepare: `pipeline_message` already pushed the pipeline entry and
        // pre-advanced the sequencer, so bailing out here leaves a phantom op
        // that no repair path re-prepares. The commit frontier gaps behind
        // it permanently and the pipeline wedges full. `CHECKPOINT_MARGIN >=
        // PIPELINE_PREPARE_QUEUE_MAX` (static assert above) guarantees the
        // append below still has room after a failed or skipped attempt; a
        // journal that truly wraps is refused by append's slot-collision
        // guard, not here.
        self.checkpoint_if_needed(consensus, journal).await;
        if view_ended() {
            return;
        }

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
                self.prepare_gap_drops
                    .set(self.prepare_gap_drops.get().saturating_add(1));
                return;
            }
        } else {
            debug_assert_eq!(
                header.op, current_op,
                "primary: sequencer pre-advance broken"
            );
        }

        // A backup journals only in a state the superblock holds, as the repair
        // ingest does: a restart then finds the entry under a recorded view, and
        // drops it when that view's log was never proven here.
        if is_backup && !self.persist_superblock_if_needed(consensus).await {
            return;
        }

        // Journal append first; sequencer + checksum after successful append
        // so a failed write doesn't leave state pointing at a phantom entry.
        //
        // Durability BEFORE replicate / PrepareOk: sending an
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

        // Paired with the append, not with the sequencer advance below: a backup
        // repairing under a `StartView` it already adopted is at the announced head
        // already, so the entry arriving moves neither number the suffix snapshot
        // is otherwise tagged by.
        consensus.note_journal_mutation();
        consensus.extend_history_certificate(header.op, header.checksum);

        // Journal mutation done; wire traffic below must not hold the gate.
        // The sends still leave in op order: the next submit runs only after
        // this task yields, and `replicate` queues every copy without yielding.
        drop(journal_gate);

        // Durable; replicate. `replicate` borrows + freezes; we keep
        // message for the sequencer/checksum bookkeeping below.
        self.replicate(&message).await;

        self.observe_prepare_runtime_state(&message);
        // Backup only: advance sequencer + checksum post-append. Primary
        // already advanced in push_prepare_entry; re-setting here would
        // rewind a sibling prepare pipelined during the append await to a
        // stale op + parent, projecting a duplicate next. A prepare of an earlier
        // view was claimed by its primary too, and a view adopted during the
        // append has its own head.
        if is_backup && header.view == consensus.view() {
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
                consensus.handle_late_prepare_ok(header);
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
    /// Not a caught-up primary (its recovery barrier aside), so a client-table
    /// read would not be authoritative.
    NotCaughtUpPrimary,
    /// This shard has no snapshot coordinator, so it never checkpoints.
    NoCoordinator,
    /// No snapshot has ever been persisted. The WAL still holds the full
    /// history, so the requester's journal repair covers its whole gap.
    NoSnapshot,
    /// A checkpoint or an install is replacing `snapshot.bin`, or did so while
    /// the offer read it. Transient: a request after it reads the new file.
    SnapshotReplaced,
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
            Self::SnapshotReplaced => write!(
                f,
                "snapshot is being replaced, or was replaced while it was read"
            ),
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
/// a success: the snapshot, table, frontiers and commit point all land, and
/// only the superblock record of the new pairing is missing. Returning it as an
/// error invites a caller to treat a completed install as a failure and redo it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallOutcome {
    /// The receiver's applied position after the install, `max(snapshot_seq,
    /// local_applied)`. These differ whenever a serving peer offered a
    /// snapshot BEHIND this replica and the local state machine was kept.
    ///
    /// Named apart from [`IggyMetadata::applied_frontier`] deliberately: that
    /// one is the node-wide cell the read gate consults, which the install
    /// raises to `snapshot_seq` alone, so the two carry different numbers
    /// exactly when a behind-snapshot was kept.
    pub installed_frontier: u64,
    /// Whether the transferred checkpoint's `(checkpoint_op, checksum)`
    /// pairing reached the durable superblock.
    ///
    /// `false` leaves the install fully usable: the coordinator already holds
    /// the new pairing, so the next superblock write (view change or
    /// checkpoint) records it. Until then a crash boots the transferred
    /// snapshot under the PREVIOUS pairing, which recovery accepts as a lagging
    /// checkpoint.
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
    /// generation reuses it instead of re-reading the file on shard 0's pump.
    ///
    /// No offer is built while a checkpoint or an install holds
    /// `checkpoint_lock`: the holder trims the WAL to its own op once that op is
    /// durable, and a peer installing the file it replaces could find its repair
    /// range evicted. A read already in flight refuses for the same reason when
    /// a checkpoint captures or a writer returns from its rename, because both
    /// start a new snapshot generation. So an offer never ships a file that a
    /// checkpoint here has begun to replace. The refusal is transient and ends
    /// with the checkpoint.
    ///
    /// A checkpoint can also start after the offer left, while its requester
    /// still pulls it. The job of that checkpoint then leaves the WAL in place,
    /// see [`Self::run_checkpoint_job`].
    ///
    /// Served below the recovery barrier too, see
    /// [`consensus::is_caught_up_primary_except_barrier`]: everything shipped is at
    /// or below `commit_min`, and the requester may be the only replica left that
    /// can ack the recovered suffix the barrier waits for.
    ///
    /// # Errors
    /// [`StateTransferUnavailable`] naming why this replica cannot serve.
    #[allow(clippy::future_not_send)]
    pub async fn state_transfer_offer(
        &self,
    ) -> Result<Rc<StateTransferOffer>, StateTransferUnavailable> {
        #[cfg(feature = "simulator")]
        if self.state_transfer_offers_refused.get() {
            return Err(StateTransferUnavailable::SnapshotReplaced);
        }
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(StateTransferUnavailable::NoCoordinator)?;
        if !is_caught_up_primary_except_barrier(consensus) {
            return Err(StateTransferUnavailable::NotCaughtUpPrimary);
        }
        let coordinator = self
            .coordinator
            .as_ref()
            .ok_or(StateTransferUnavailable::NoCoordinator)?;
        if self.checkpoint_running() {
            return Err(StateTransferUnavailable::SnapshotReplaced);
        }
        if let Some(cached) = self.transfer_offer_cache.borrow().as_ref() {
            return Ok(Rc::clone(cached));
        }
        let path = coordinator.snapshot_path();
        let generation = self.snapshot_generation.get();
        let read = coordinator.io.read(&path).await;
        // Every await is behind this point, so nothing below can see a replacement.
        if self.snapshot_generation.get() != generation {
            return Err(StateTransferUnavailable::SnapshotReplaced);
        }
        let sealed = read.map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                StateTransferUnavailable::NoSnapshot
            } else {
                StateTransferUnavailable::SnapshotUnreadable(source.into())
            }
        })?;
        if !is_caught_up_primary_except_barrier(consensus) {
            return Err(StateTransferUnavailable::NotCaughtUpPrimary);
        }
        // Verifies the trailer and hands back the payload alone.
        let (payload, _) =
            split_trailer(&sealed).map_err(StateTransferUnavailable::SnapshotUnreadable)?;
        // Still read from the file rather than off `last_checkpoint()`: the writer
        // renames before the parent-dir fsync, so a DirSync failure leaves the new
        // file live with that cell stale, and the offer would then under-advertise
        // the frontier it is actually shipping.
        //
        // Only the leading fields are read. A full decode stalls shard 0 for as
        // long as the catalog is large, and only a writer bug produces a payload
        // with a valid trailer that does not decode. Each receiver decodes the
        // whole snapshot at the start of `install_state_transfer`, before it moves
        // any state, and refuses it there.
        let snapshot_seq =
            peek_sequence_number(payload).map_err(StateTransferUnavailable::SnapshotUnreadable)?;

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
        let mut served = self.served_offers.borrow_mut();
        served.retain(|offer| offer.strong_count() > 0);
        served.push(Rc::downgrade(&offer));
        drop(served);
        Ok(offer)
    }

    /// Refuse every later state transfer offer as
    /// [`StateTransferUnavailable::SnapshotReplaced`], as a primary whose
    /// snapshot is replaced during each read would. Simulated snapshot reads
    /// finish within one poll, so no replacement can land inside one.
    #[cfg(feature = "simulator")]
    pub fn refuse_state_transfer_offers(&self) {
        self.state_transfer_offers_refused.set(true);
    }

    /// Whether the WAL here can hold the tail of a state transfer: every op
    /// above the journal floor that the install leaves, up to `table_frontier`.
    ///
    /// No checkpoint is captured below the frontier of a transferred client
    /// table, so the floor stays until the commit walk reaches that frontier,
    /// and an append refuses a slot whose op is above the floor. A replica with
    /// a longer tail stops below the frontier for good. The floor moves to
    /// `snapshot_seq` when the snapshot is ahead of `commit_min`. It stays when
    /// only the table installs.
    ///
    /// # Errors
    /// [`SnapshotError::TransferTailExceedsJournal`], which names
    /// `metadata.journal_slots`.
    pub fn verify_state_transfer_tail(
        &self,
        snapshot_seq: u64,
        table_frontier: u64,
    ) -> Result<(), SnapshotError> {
        let (Some(consensus), Some(journal)) = (self.consensus.as_ref(), self.journal.as_ref())
        else {
            return Ok(());
        };
        let journal = journal.handle();
        let Some(journal_slots) = journal.slot_count() else {
            return Ok(());
        };
        let mut floor = journal.snapshot_op();
        if snapshot_seq > consensus.commit_min() {
            floor = floor.max(snapshot_seq);
        }
        let tail = table_frontier.saturating_sub(floor);
        if tail > journal_slots as u64 {
            return Err(SnapshotError::TransferTailExceedsJournal {
                tail,
                journal_slots,
            });
        }
        Ok(())
    }

    /// Install a fetched state transfer without rewinding either the state
    /// machine or client table, then let tail repair continue above the local
    /// applied frontier.
    ///
    /// Ordering: persist FIRST (a crash mid-install must reboot from the
    /// transferred state, not the pre-transfer one), then the WAL truncate, then
    /// the in-place STM restore (readers observe it on their next read), then the
    /// table + frontier, then commit bookkeeping.
    ///
    /// The partition plane is deliberately untouched: partitions load
    /// whatever their disks hold at boot and repair through their own
    /// consensus groups. Topology changes the snapshot carries below the
    /// receiver's old frontier (topics created/deleted while it was down)
    /// fire no commit notifier -- convergence rests on the partition
    /// reconciler's periodic full diff against the committed STM, which
    /// reads the restored state on its next tick.
    ///
    /// Returns an [`InstallOutcome`]: the installed frontier, plus whether
    /// the transferred checkpoint's pairing reached the durable superblock.
    ///
    /// # Errors
    /// [`SnapshotError`] when the snapshot bytes do not decode, the tail does
    /// not fit the WAL (see [`Self::verify_state_transfer_tail`]), the persist
    /// fails, or the in-place restore is rejected. An `Err` leaves the state
    /// machine, client table and frontiers as they were. The snapshot file, the
    /// superblock record and the WAL suffix above the incoming floor can already
    /// have moved, each in an order a restart recovers from.
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

        // The descriptor gate checked the frontier that the manifest declares
        // for the snapshot, not the op of the decoded snapshot. Refuse rather
        // than install, which drops the caller back to journal repair with the
        // local state untouched.
        verify_state_transfer_manifest(snapshot_seq, commit_op, table_frontier)?;

        // Checkpoints are node-local, so a healthy serving primary can offer
        // a snapshot BEHIND this replica's own applied frontier (each node
        // snapshots at its own watermark; a backup checkpoints an op or two
        // below the primary it later replaces). Restoring such a snapshot
        // would rewind the STM below `commit_min` with no way back: the
        // commit walk never revisits ops it already counted as applied, so
        // the rewound-over effects would be lost until the next transfer.
        // Keep the local STM (it is a superset) and let tail repair cover
        // `(commit_min, commit_op]`. The table is checked separately below:
        // a cached offer can also lag the local table's committed protection.
        // Preliminary read, only to decide whether the gates are needed; the
        // binding decision is re-derived under them below.
        let snapshot_ahead = snapshot_seq > consensus.commit_min();

        // Serialize the whole install against a concurrent checkpoint and every
        // other WAL writer, in the documented lock order (`journal_gate`,
        // `checkpoint_lock`, `superblock_lock`), so none of them can deadlock
        // against it. `journal_gate` covers the WAL truncate, the boundary removal
        // and the floor move below: an append landing inside one of those rewrites
        // is lost from the WAL, and a checkpoint trim fails one of them as busy.
        //
        // Both do the same pair of steps -- rewrite `snapshot.bin`, record
        // `(checkpoint_op, checksum)` -- and a checkpoint, inline or as a job,
        // holds `checkpoint_lock` across BOTH while taking `superblock_lock` only
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
        // `write_superblock` takes no lock of its own.
        let _install_gates = if snapshot_ahead {
            let journal_writes = self.journal_gate.acquire().await;
            let checkpoint = self.checkpoint_lock.acquire().await;
            let superblock = if self.superblock.is_some() {
                Some(self.superblock_lock.acquire().await)
            } else {
                None
            };
            Some((journal_writes, checkpoint, superblock))
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
        let mut local_applied = consensus.commit_min();
        let mut snapshot_ahead = snapshot_seq > local_applied;

        // Ahead of every durable step, so a refusal leaves this replica as it was.
        self.verify_state_transfer_tail(snapshot_seq, table_frontier)?;

        let snapshot_table = if snapshot_ahead {
            let table = snapshot
                .snapshot()
                .client_table
                .clone()
                .ok_or(SnapshotError::MissingClientTable)?;
            Some(ClientTable::from_snapshot(table).map_err(SnapshotError::ClientTable)?)
        } else {
            None
        };
        let mut pairing_durable = true;
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
                // A failed earlier install can leave the durable checkpoint ahead
                // of the STM. Replacing it with an older snapshot would make a
                // crash before the next superblock write refuse recovery.
                let checkpoint_op = coordinator.last_checkpoint().0;
                if snapshot_seq < checkpoint_op {
                    return Err(SnapshotError::InvalidState(
                        "state transfer snapshot is older than the durable checkpoint",
                    ));
                }
                // Nor may another snapshot of the durable checkpoint's op replace
                // it: a superblock write may pair it on disk, also a write that
                // reported an error, and another primary stamps its own
                // `created_at`, so another checksum. The replicated state of one op
                // is the same on every replica, so the file and its pairing stay,
                // and the restore below still reads the received bytes.
                // The change log is node-local, so the two snapshots can differ
                // in its depth. A restart then loads the kept one, and a reader
                // below its floor rebuilds from current state.
                if snapshot_seq == checkpoint_op {
                    tracing::info!(
                        checkpoint_op,
                        "state transfer kept the durable snapshot of its op"
                    );
                } else {
                    let written = coordinator.write_snapshot(snapshot_bytes.to_vec()).await;
                    // Also on an error, which can come after the rename.
                    self.clear_state_transfer_offer_cache();
                    let checksum = written?;
                    coordinator.seed_last_checkpoint(snapshot_seq, checksum);
                    tracing::info!(
                        checkpoint_op = snapshot_seq,
                        "state transfer recorded its checkpoint pairing"
                    );
                }
                if self.superblock.is_none() {
                    coordinator.superblock_checkpoint_op.set(snapshot_seq);
                }
            } else {
                tracing::warn!(
                    snapshot_seq,
                    "installing state transfer without a snapshot coordinator; \
                     the transferred state will not survive a further restart"
                );
            }

            let (truncate_from, remove_boundary) =
                self.journal
                    .as_ref()
                    .map_or((snapshot_seq + 1, false), |journal| {
                        let journal = journal.handle();
                        let head = journal.last_op().unwrap_or(snapshot_seq).max(snapshot_seq);
                        let pending = consensus
                            .pending_view_log()
                            .or_else(|| consensus.adopted_view_log());
                        let log_proven = consensus.is_normal()
                            && consensus.log_view() == consensus.view()
                            && !consensus.view_log_unproven()
                            && consensus.history_certified();
                        let is_vouched = |header: &PrepareHeader| {
                            header.checksum != CHECKSUM_UNSEALED
                                && header.op <= consensus.sequencer().current_sequence()
                                && (log_proven
                                    || pending.as_ref().is_some_and(|pending| {
                                        pending
                                            .headers
                                            .iter()
                                            .chain(pending.committed_elsewhere.iter())
                                            .any(|named| {
                                                named.op == header.op
                                                    && named.checksum == header.checksum
                                            })
                                    }))
                        };
                        let remove_boundary = usize::try_from(snapshot_seq)
                            .ok()
                            .and_then(|slot| journal.header(slot))
                            .is_some_and(|header| !is_vouched(&header));
                        let truncate_from = (snapshot_seq + 1..=head)
                            .find(|op| {
                                usize::try_from(*op)
                                    .ok()
                                    .and_then(|slot| journal.header(slot))
                                    .is_none_or(|header| !is_vouched(&header))
                            })
                            .unwrap_or_else(|| head.saturating_add(1));
                        if remove_boundary && truncate_from == snapshot_seq + 1 {
                            (snapshot_seq, false)
                        } else {
                            (truncate_from, remove_boundary)
                        }
                    });

            // The truncate below can remove the entry the history certificate is
            // bound to, so the binding moves to the highest entry it keeps and
            // reaches the superblock first, in one record with the pairing above.
            // Bound to an entry rather than to the snapshot alone: without a
            // coordinator the record pairs no checkpoint, and only the entry's
            // checksum proves the binding after a restart.
            if let Some(journal) = &self.journal {
                let (mut op, mut checksum) = metadata_wal_head(journal.handle(), truncate_from);
                if remove_boundary && op == snapshot_seq {
                    (op, checksum) = metadata_wal_head(journal.handle(), snapshot_seq);
                }
                consensus.lower_history_certificate(truncate_from, op, checksum);
                if remove_boundary
                    && consensus
                        .history_certificate()
                        .is_some_and(|certificate| certificate.op == snapshot_seq)
                {
                    let (op, checksum) = metadata_wal_head(journal.handle(), snapshot_seq);
                    consensus.lower_history_certificate(snapshot_seq, op, checksum);
                }
            }
            if let Some(superblock) = self.superblock.as_ref() {
                // Already under `_install_gates`, acquired above; re-acquiring here
                // would deadlock on the same non-reentrant gate.
                pairing_durable = self.write_superblock(consensus, superblock.as_ref()).await;
                // A failed write is only DEGRADED while the durable binding survives
                // the truncate: the snapshot on disk is the one the recorded pairing
                // names or a newer one, and `verify_checkpoint_pairing` accepts both.
                if !pairing_durable {
                    if consensus.history_certificate_needs_persist() {
                        return Err(SnapshotError::HistoryBindingNotDurable { snapshot_seq });
                    }
                    tracing::error!(
                        snapshot_seq,
                        commit_op,
                        "state transfer superblock write failed; the transferred \
                         checkpoint is not durable yet"
                    );
                }
            }
            if let Some(journal) = &self.journal {
                // An acknowledged entry may be committed above the offered checkpoint.
                // Keep the prefix whose identities the installed view proves, or a
                // later DoViewChange could falsely nack it. The commit walk matches
                // by op, so the first unvouched entry and everything above still go
                // before the snapshot advances its floor. Appends are excluded while
                // transferring, and the lowered binding is already durable above.
                let removed = journal
                    .handle()
                    .truncate_from(truncate_from)
                    .await
                    .map_err(SnapshotError::Io)?;
                if removed > 0 {
                    // The DVC snapshot's head and commit point do not move when
                    // entries are removed under them; left stale it would advertise
                    // headers this replica can no longer serve.
                    consensus.note_journal_mutation();
                }
                // The checkpoint replaces an unvouched boundary, but a separately
                // vouched suffix may contain the last copy of a committed entry.
                let boundary_removed = if remove_boundary {
                    let removed = journal
                        .handle()
                        .remove_range(snapshot_seq..=snapshot_seq)
                        .await
                        .map_err(SnapshotError::Io)?;
                    if removed > 0 {
                        consensus.note_journal_mutation();
                    }
                    removed
                } else {
                    0
                };
                let removed = removed + boundary_removed;
                if removed > 0 {
                    tracing::warn!(
                        snapshot_seq,
                        truncate_from,
                        removed,
                        "state transfer dropped {removed} unvouched journal entries"
                    );
                }
            }

            // Commits may progress while the snapshot reaches disk. Its bytes remain
            // a valid checkpoint, but must not replace a newer in-memory state.
            local_applied = consensus.commit_min();
            snapshot_ahead = snapshot_seq > local_applied;
            if snapshot_ahead {
                let capacities = self.change_log.capacities().map_err(|_| {
                    SnapshotError::InvalidState("change log restore reached a read-only view")
                })?;
                let change_log = snapshot.snapshot().change_log.restore(capacities)?;
                self.mux_stm
                    .restore_snapshot_in_place(snapshot.snapshot())?;
                // Replaced with the state machines and before the frontier
                // jumps. The kept WAL suffix appends through the commit walk.
                self.change_log.restore(change_log).map_err(|_| {
                    SnapshotError::InvalidState("change log restore reached a read-only view")
                })?;
            }
        } else {
            tracing::info!(
                snapshot_seq,
                local_applied,
                "transferred snapshot at or below the local applied frontier; \
                 keeping the local state machine"
            );
        }

        let protection_frontier = table_frontier.max(self.client_table_frontier.get());
        let replay_table = if snapshot_ahead && protection_frontier > snapshot_seq {
            snapshot_table
        } else if !snapshot_ahead
            && table_frontier > local_applied
            && self.replay_client_table.borrow().is_none()
        {
            let local_table = self.client_table.borrow();
            Some(if local_table.capacity_committed() {
                ClientTable::from_snapshot(local_table.to_snapshot())
                    .map_err(SnapshotError::ClientTable)?
            } else {
                ClientTable::new(local_table.capacity())
            })
        } else {
            None
        };
        if snapshot_ahead || replay_table.is_some() {
            *self.replay_client_table.borrow_mut() = replay_table.map(RefCell::new);
        }

        // Commits can advance while the install awaits. A prior transfer may
        // also have installed a table ahead of the local state machine.
        let local_table_frontier = consensus.commit_min().max(self.client_table_frontier.get());
        if table_frontier >= local_table_frontier {
            let configured_capacity = self.client_table.borrow().capacity();
            if client_table.capacity_committed() && configured_capacity != client_table.capacity() {
                warn!(
                    configured_capacity,
                    committed_capacity = client_table.capacity(),
                    "metadata retry capacity is fixed by committed state; ignoring local configuration"
                );
            }
            *self.client_table.borrow_mut() = client_table;
            self.client_table_frontier.set(table_frontier);
        } else {
            tracing::info!(
                table_frontier,
                local_table_frontier,
                "transferred client table below the local table frontier; keeping the local client table"
            );
        }
        if snapshot_ahead {
            self.applied_frontier
                .record_recovery_revision(self.mux_stm.streams().read(|inner| inner.revision));
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
            // The walk matches WAL entries by op number alone, so every retained
            // entry above the floor must be vouched for by the installed view.
            consensus.set_commit_floor(snapshot_seq);
            if snapshot_seq > consensus.sequencer().current_sequence() {
                consensus.sequencer().set_sequence(snapshot_seq);
            }
            self.advance_applied_frontier(snapshot_seq);
        }
        consensus.advance_commit_max(commit_op);

        Ok(InstallOutcome {
            installed_frontier: snapshot_seq.max(local_applied),
            pairing_durable,
        })
    }

    /// Register a new identity or resolve a matching login retry to its original session.
    ///
    /// A primary behind its committed frontier queues the request and checks
    /// ownership and capacity after catch-up.
    ///
    /// # Errors
    /// Returns a terminal ownership conflict or a transient admission failure.
    ///
    /// # Panics
    /// If called with client zero or outside the metadata consensus shard.
    #[allow(clippy::future_not_send)]
    pub async fn submit_register_in_process(
        &self,
        client_id: u128,
        user_id: u32,
        registration: RegisterRequest,
    ) -> Result<BoundSession, MetadataSubmitError> {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        if message_bus::is_server_internal_client(client_id) {
            return Err(MetadataSubmitError::Unauthenticated);
        }
        let consensus = self
            .consensus
            .as_ref()
            .expect("registration only exists on the metadata shard");
        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return Err(MetadataSubmitError::NotPrimary);
        }
        if is_caught_up_primary(consensus) {
            if !self.accepts_registration_proof(
                user_id,
                &registration.credential,
                consensus.clock_realtime_micros(),
            ) {
                return Err(MetadataSubmitError::Unauthenticated);
            }
            let table = self.client_table.borrow();
            match table.registered_session(
                client_id,
                user_id,
                registration.bind_verifier,
                registration.credential.token_id,
            ) {
                Ok(Some(epoch)) => {
                    return Ok(BoundSession {
                        epoch,
                        watermark: table.get_watermark(client_id).unwrap_or(0),
                    });
                }
                Err(_) => return Err(MetadataSubmitError::ClientIdOwnedByAnotherUser),
                Ok(None) => {}
            }
            if !consensus.has_retry_capacity(&table, client_id) {
                return Err(MetadataSubmitError::PipelineFull);
            }
        }
        if consensus.pipeline_has_message_from_client(client_id) {
            return Err(MetadataSubmitError::InProgress);
        }
        let request = build_register_request_message(consensus, client_id, user_id, registration);
        let outcome = if !is_caught_up_primary(consensus) || consensus.pipeline_is_full() {
            let (entry, receiver) = consensus::RequestEntry::with_subscriber(request);
            if consensus.push_queued_request(entry).is_err() {
                return Err(MetadataSubmitError::PipelineFull);
            }
            receiver.await
        } else {
            let prepare = self
                .prepare_request(request)
                .expect("Register is a client-allowed operation");
            self.dispatch_prepare_and_await(consensus, prepare).await
        };
        if outcome.is_ok_and(|reply| {
            iggy_binary_protocol::result_code(reply.body())
                == Some(IggyError::Unauthenticated.as_code())
        }) {
            return Err(MetadataSubmitError::Unauthenticated);
        }
        self.bound_session(
            client_id,
            user_id,
            registration,
            consensus.clock_realtime_micros(),
        )
    }

    fn accepts_registration_proof(
        &self,
        user_id: u32,
        credential: &CredentialProof,
        now_micros: u64,
    ) -> bool {
        self.mux_stm.users().read(|users| {
            users.accepts_credential_at(
                user_id,
                credential,
                iggy_common::IggyTimestamp::from(now_micros),
            )
        })
    }

    fn bound_session(
        &self,
        client_id: u128,
        user_id: u32,
        registration: RegisterRequest,
        now_micros: u64,
    ) -> Result<BoundSession, MetadataSubmitError> {
        if !self.accepts_registration_proof(user_id, &registration.credential, now_micros) {
            return Err(MetadataSubmitError::Unauthenticated);
        }
        let table = self.client_table.borrow();
        let epoch = table
            .registered_session(
                client_id,
                user_id,
                registration.bind_verifier,
                registration.credential.token_id,
            )
            .map_err(|_| MetadataSubmitError::ClientIdOwnedByAnotherUser)?
            .ok_or(MetadataSubmitError::Canceled)?;
        Ok(BoundSession {
            epoch,
            watermark: table.get_watermark(client_id).unwrap_or(0),
        })
    }

    fn fence_lifecycle_preflight(
        &self,
        outcome: PreflightOutcome,
        request: &Message<RoutedRequestHeader>,
    ) -> PreflightOutcome {
        if let PreflightOutcome::Replay(reply) = &outcome
            && let Some(bytes) = reply.as_slice().get(..size_of::<ReplyHeader>())
            && let Ok(header) = bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes)
            && self.mux_stm.streams().lifecycle_pending(header.op)
        {
            return PreflightOutcome::NotReady;
        }
        if matches!(outcome, PreflightOutcome::Dispatch)
            && self
                .mux_stm
                .streams()
                .request_lifecycle_blocked(request.header().operation, request.body())
        {
            return PreflightOutcome::Reject(IggyError::TransientNotAccepted.as_code());
        }
        outcome
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
        users: &Users,
        request: &Message<RoutedRequestHeader>,
        outcome: PreflightOutcome,
    ) -> Option<Result<Message<GenericHeader>, MetadataSubmitError>> {
        let request_header = request.header();
        let client_id = request_header.client;
        match outcome {
            PreflightOutcome::Dispatch => None,
            PreflightOutcome::Replay(reply) => {
                if let Some(refusal) = unreplayable_secret_refusal(request, &reply, users) {
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
    /// request is canceled before commit. Returns `RequestTooOld` when its
    /// receipt is unavailable, and `OperationMismatch` when the id belongs
    /// to another operation.
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

        if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
            return Err(MetadataSubmitError::NotPrimary);
        }
        // A live commit window is safe to pipeline behind: the per-client
        // guard sees every unapplied request. Inherited history awaiting WAL
        // replay is absent from the pipeline and can hide a newer watermark.
        let committed_in_pipeline = consensus.with_pipeline(|pipeline| {
            (consensus.commit_min()..consensus.commit_max())
                .all(|previous_op| pipeline.entry_by_op(previous_op + 1).is_some())
        });
        if consensus.has_ceded_primaryship()
            || consensus.commit_max() < consensus.recovery_barrier()
            || !committed_in_pipeline
        {
            return Err(MetadataSubmitError::NotCaughtUp);
        }

        if consensus.pipeline_has_message_from_client(client_id) {
            return Err(MetadataSubmitError::InProgress);
        }

        // Epoch guard: only propose a Logout when the slot still holds the
        // exact epoch this logout targets. A late disconnect-logout for a
        // reused client id (slot since rebound to a newer epoch) carries the
        // stale epoch and is dropped here, so it can never wipe the fresh
        // registration. A missing slot also fails the match and short-circuits.
        if self.client_table.borrow().get_epoch(client_id) != Some(session) {
            return Ok(consensus.commit_min());
        }

        match self.client_table.borrow().check_request(
            client_id,
            session,
            request,
            Operation::Logout,
        ) {
            consensus::client_table::RequestStatus::New => {}
            consensus::client_table::RequestStatus::Duplicate(reply) => {
                return Ok(reply.header().commit);
            }
            consensus::client_table::RequestStatus::AlreadyApplied { .. } => {
                return Err(MetadataSubmitError::RequestTooOld);
            }
            consensus::client_table::RequestStatus::OperationMismatch { .. } => {
                return Err(MetadataSubmitError::OperationMismatch);
            }
            consensus::client_table::RequestStatus::NoSession
            | consensus::client_table::RequestStatus::Fenced { .. }
            | consensus::client_table::RequestStatus::EpochAhead { .. } => {
                return Ok(consensus.commit_min());
            }
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

    /// Remove an expired consumer-group member only if its session is unchanged.
    /// The caller must establish timeout expiry on the caught-up metadata primary.
    /// Returns `Ok(None)` when the session changed or membership already ended.
    ///
    /// # Errors
    /// Returns a submission error while consensus cannot accept or commit the logout.
    ///
    /// # Panics
    /// Requires metadata shard 0 and a nonzero client id.
    #[allow(clippy::future_not_send)]
    pub async fn submit_expired_logout_in_process(
        &self,
        client_id: u128,
        expected_session: Option<u64>,
    ) -> Result<Option<u64>, MetadataSubmitError> {
        assert_ne!(client_id, 0, "client_id 0 is reserved for internal use");
        let consensus = self
            .consensus
            .as_ref()
            .expect("session expiry runs on shard 0");
        if !consensus.is_primary() || !consensus.is_normal() || consensus.is_transferring() {
            return Err(MetadataSubmitError::NotPrimary);
        }
        if !is_caught_up_primary(consensus) {
            return Err(MetadataSubmitError::NotCaughtUp);
        }
        if consensus.pipeline_has_message_from_client(client_id) {
            return Err(MetadataSubmitError::InProgress);
        }
        if consensus.pipeline_is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }

        let (user_id, capacity) = {
            let table = self.client_table.borrow();
            if expected_session.is_none() || table.get_epoch(client_id) != expected_session {
                return Ok(None);
            }
            let user_id = table
                .get_user_id(client_id)
                .ok_or(MetadataSubmitError::NotCaughtUp)?;
            (user_id, consensus.retry_capacity(&table))
        };
        let header = RoutedRequestHeader {
            client: client_id,
            session: expected_session.unwrap_or_default(),
            user_id,
            request: EXPIRED_SESSION_REQUEST_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(consensus, &header, Operation::Logout, &[]),
            capacity,
        );
        self.dispatch_prepare_and_await(consensus, prepare)
            .await
            .map(|reply| Some(reply.header().commit))
            .map_err(|Canceled| MetadataSubmitError::Canceled)
    }

    /// Finalize every ended session whose end op is at or below
    /// `retired_through`. Each partition incarnation must have proved all of
    /// them retired on a quorum of its members, or be listed in `debt`, which
    /// its next primary pays before serving sessions.
    ///
    /// # Errors
    /// Returns an admission or consensus error before finalization commits.
    #[allow(clippy::future_not_send)]
    pub async fn submit_session_finalization(
        &self,
        retired_through: u64,
        debt: Vec<RetirementDebt>,
    ) -> Result<Option<u64>, MetadataSubmitError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(MetadataSubmitError::NotPrimary)?;
        if !self.is_caught_up_primary() {
            return Err(MetadataSubmitError::NotCaughtUp);
        }
        if !self
            .client_table
            .borrow()
            .ended_sessions()
            .any(|ended| ended.metadata_watermark <= retired_through)
        {
            return Ok(None);
        }
        if consensus.pipeline_is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }
        let capacity = consensus.retry_capacity(&self.client_table.borrow());
        let header = RoutedRequestHeader {
            client: message_bus::AUTO_COMMIT_CLIENT_ID,
            session: 1,
            request: retired_through,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::FinalizeSession,
                &FinalizeSessionRequest {
                    retired_through,
                    debt,
                }
                .to_bytes(),
            ),
            capacity,
        );
        self.dispatch_prepare_and_await(consensus, prepare)
            .await
            .map(|reply| Some(reply.header().commit))
            .map_err(|Canceled| MetadataSubmitError::Canceled)
    }

    /// Submit a server-originated `CompleteConsumerGroupRevocation` through the
    /// metadata consensus group (shard 0). A partition reconciler, on this node
    /// or on a relaying backup, asks for this to complete a cooperative
    /// revocation once the source has drained the partition (or it timed out).
    /// `namespace` is the partition that `request` completes, and keys its
    /// internal client.
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
        request: CompleteConsumerGroupRevocationRequest,
        namespace: server_common::sharding::IggyNamespace,
    ) -> Result<u64, MetadataSubmitError> {
        self.submit_partition_completion(
            Operation::CompleteConsumerGroupRevocation,
            namespace.inner(),
            &request.to_bytes(),
        )
        .await
        .map(|reply| reply.header().commit)
    }

    /// Commit one batch of lifecycle completions on the metadata primary.
    /// `true` when the batch committed and applied.
    ///
    /// Each reporter shard owns one internal client, so batches of different
    /// shards share the pipeline. Partition namespaces never set
    /// `METADATA_GROUP`, so a reporter client cannot collide with the
    /// per-partition clients of revocations.
    ///
    /// # Errors
    /// Returns the same leadership, pipeline and cancellation errors as
    /// [`Self::submit_complete_revocation_in_process`].
    #[allow(clippy::future_not_send)]
    pub async fn submit_complete_lifecycle_in_process(
        &self,
        request: crate::stm::lifecycle::CompleteLifecycleRequest,
        reporter: u16,
    ) -> Result<bool, MetadataSubmitError> {
        let reply = self
            .submit_partition_completion(
                Operation::CompleteLifecycle,
                server_common::sharding::METADATA_GROUP | u64::from(reporter),
                &request.to_bytes(),
            )
            .await?;
        Ok(iggy_binary_protocol::result_code(reply.body()) == Some(0))
    }

    #[allow(clippy::future_not_send)]
    async fn submit_partition_completion(
        &self,
        operation: Operation,
        internal_client: u64,
        body: &[u8],
    ) -> Result<Message<ReplyHeader>, MetadataSubmitError> {
        // One internal client per key serializes the completions of that key.
        const INTERNAL_REQUEST_ID: u64 = u64::MAX;
        let internal_client_id = message_bus::server_internal_client_id(internal_client);
        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_partition_completion: consensus only exists on shard 0");

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

        let message = build_partition_completion_request_message(
            consensus,
            operation,
            internal_client_id,
            INTERNAL_REQUEST_ID,
            body,
        );
        let prepare = consensus::seal_prepare_capacity(
            message.project(consensus),
            consensus.retry_capacity(&self.client_table.borrow()),
        );

        self.dispatch_prepare_and_await(consensus, prepare)
            .await
            .map_err(|Canceled| MetadataSubmitError::Canceled)
    }

    /// `true` when this node is the caught-up primary of the metadata
    /// consensus group. Gates leader-only maintenance (the PAT cleaner)
    /// off backups and lagging primaries.
    #[must_use]
    pub fn is_caught_up_primary(&self) -> bool {
        self.consensus.as_ref().is_some_and(is_caught_up_primary)
    }

    /// `true` when this node is the primary of the metadata consensus group
    /// in a normal view, however far it is from applying what is committed.
    /// Enough for maintenance that acts only on state already applied here,
    /// and that a commit still to apply must not pause.
    #[must_use]
    pub fn is_settled_primary(&self) -> bool {
        self.consensus.as_ref().is_some_and(is_settled_primary)
    }

    /// Replicate initial root creation with a precomputed password hash.
    ///
    /// # Errors
    /// Returns a submission error if this replica cannot commit the initial root user.
    #[allow(clippy::future_not_send)]
    pub async fn submit_root_user_in_process(
        &self,
        request: &CreateUserRequest,
    ) -> Result<u32, MetadataSubmitError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(MetadataSubmitError::NotPrimary)?;
        if !is_caught_up_primary(consensus) {
            return Err(MetadataSubmitError::NotCaughtUp);
        }
        if !self.mux_stm.users().read(|users| users.items.is_empty()) {
            return Ok(0);
        }
        if consensus.pipeline_has_message_from_client(RESERVED_CLIENT_ID) {
            return Err(MetadataSubmitError::InProgress);
        }
        if consensus.pipeline_is_full() {
            return Err(MetadataSubmitError::PipelineFull);
        }
        let header = RoutedRequestHeader {
            client: RESERVED_CLIENT_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::CreateUser,
                &request.to_bytes(),
            ),
            consensus.retry_capacity(&self.client_table.borrow()),
        );
        self.dispatch_prepare_and_await(consensus, prepare)
            .await
            .map(|reply| reply.header().status)
            .map_err(|Canceled| MetadataSubmitError::Canceled)
    }

    /// Submit a replicated `DeletePersonalAccessToken` originated by the
    /// server (the PAT cleaner), not a client.
    ///
    /// No client session exists, so this skips `request_preflight` (like
    /// the logout precedent) and uses the reserved internal `client` id
    /// `0`: never registered, so the commit path skips receipt caching,
    /// while the preflight and register asserts never run. Delete is
    /// idempotent, so a re-proposal on the next tick is a no-op.
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
            client: RESERVED_CLIENT_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::DeletePersonalAccessToken,
                &body,
            ),
            consensus.retry_capacity(&self.client_table.borrow()),
        );

        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// Submit a replicated `RewrapStreamKeys` originated by the server (the
    /// stream key rewrapper during a key-encryption key rotation).
    ///
    /// Same in-process path as the PAT cleaner's delete: the reserved
    /// internal client `0`, no request preflight. Apply skips any entry that
    /// no longer matches its stream, so a re-proposal on the next tick is
    /// harmless.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot replicate,
    /// `PipelineFull` under pipeline pressure, `Canceled` if the prepare is
    /// dropped before commit.
    #[allow(clippy::future_not_send)]
    pub async fn submit_rewrap_stream_keys_in_process(
        &self,
        request: RewrapStreamKeysRequest,
    ) -> Result<u64, MetadataSubmitError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(MetadataSubmitError::NotPrimary)?;
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
        let header = RoutedRequestHeader {
            client: RESERVED_CLIENT_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::RewrapStreamKeys,
                &request.to_bytes(),
            ),
            consensus.retry_capacity(&self.client_table.borrow()),
        );
        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(reply.header().commit),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// Submit a replicated `TruncatePartition` originated by the server (the
    /// retention of a tiered topic, decided by the partition primary), not by
    /// a client `DeleteSegments`.
    ///
    /// Same in-process path as the PAT cleaner's delete: the reserved
    /// internal client `0`, no request preflight, no receipt. The watermark
    /// only rises, so a re-proposal on the next tick is harmless. `Ok(true)`
    /// when the truncation committed and applied, `Ok(false)` when it
    /// committed as a rejection.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot replicate,
    /// `PipelineFull` under pipeline pressure, `Canceled` if the prepare is
    /// dropped before commit.
    #[allow(clippy::future_not_send)]
    pub async fn submit_truncate_partition_in_process(
        &self,
        request: &TruncatePartitionRequest,
    ) -> Result<bool, MetadataSubmitError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(MetadataSubmitError::NotPrimary)?;
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
        let header = RoutedRequestHeader {
            client: RESERVED_CLIENT_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::TruncatePartition,
                &request.to_bytes(),
            ),
            consensus.retry_capacity(&self.client_table.borrow()),
        );
        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(iggy_binary_protocol::result_code(reply.body()) == Some(0)),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// Submit a replicated `ShadowTopic` originated by the server (the shadow
    /// pass of the metadata primary), not by a client.
    ///
    /// Same in-process path as [`Self::submit_truncate_partition_in_process`].
    /// An `AdoptTopic` names the count of its partitions and its explicit
    /// options only: this fills in what the admission of a topic create
    /// resolves, the derived options with the node's default replication
    /// factor and a consensus group, members and an initial view for each
    /// partition. Returns the committed result code, `0` when the op applied,
    /// or the code of the error that kept an adoption out of the pipeline.
    ///
    /// # Errors
    /// `NotPrimary` / `NotCaughtUp` when this node cannot replicate,
    /// `PipelineFull` under pipeline pressure, `Canceled` if the prepare is
    /// dropped before commit.
    #[allow(clippy::future_not_send)]
    pub async fn submit_shadow_topic_in_process(
        &self,
        mut request: ShadowTopicRequest,
    ) -> Result<u32, MetadataSubmitError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(MetadataSubmitError::NotPrimary)?;
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
        if let ShadowTopicRequest::AdoptTopic { create, .. }
        | ShadowTopicRequest::RestoreTopic { create, .. } = &mut request
            && let Err(error) = self.assign_shadow_partitions(create)
        {
            return Ok(error.as_code());
        }
        let header = RoutedRequestHeader {
            client: RESERVED_CLIENT_ID,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &header,
                Operation::ShadowTopic,
                &request.to_bytes(),
            ),
            consensus.retry_capacity(&self.client_table.borrow()),
        );
        // The partitions of an adoption count toward placement while the
        // prepare waits in the pipeline, as those of a topic create do.
        if let Err(error) = self
            .placement
            .borrow_mut()
            .remember(&prepare, consensus.replica_count())
        {
            return Ok(error.as_code());
        }
        match self.dispatch_prepare_and_await(consensus, prepare).await {
            Ok(reply) => Ok(iggy_binary_protocol::result_code(reply.body()).unwrap_or(0)),
            Err(Canceled) => Err(MetadataSubmitError::Canceled),
        }
    }

    /// Fill in the derived options and the partition assignments of an
    /// adopted shadow topic or a restored geo topic, as the admission of a
    /// topic create does, which drops the keys a geo topic's mode fixes. Its
    /// partitions count toward `[metadata] partitions_max` like any other.
    fn assign_shadow_partitions(
        &self,
        create: &mut PersistedCreateTopicRequest,
    ) -> Result<(), IggyError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(IggyError::TransientNotAccepted)?;
        let partitions_max = self.partitions_max.get();
        if partitions_max != 0 {
            let committed =
                u64::try_from(self.mux_stm.streams().partition_count()).unwrap_or(u64::MAX);
            validate_partitions_limit(
                partitions_max,
                create.request.partitions_count,
                committed.saturating_add(self.pending_partitions()?),
            )?;
        }
        let mut explicit = resource_options_from_wire(&create.request.options, true)?;
        if drop_options_fixed_by_type(&mut explicit) {
            create.request.options = resource_options_to_wire(&explicit, OptionsProvenance::All)?;
        }
        let mut derived = derived_defaults(OptionsScope::Topic, &explicit)?;
        let replication_factor = resolve_replication_factor(
            None,
            self.default_replication_factor(consensus.replica_count()),
            consensus.replica_count(),
            &mut derived,
        )?;
        create.derived_options = resource_options_to_wire(&derived, OptionsProvenance::All)?;
        let mut counts = self.placement_counts()?;
        create.partitions = self
            .allocator
            .allocate_many(create.request.partitions_count as usize)
            .into_iter()
            .enumerate()
            .map(|(partition_id, consensus_group_id)| {
                let (members, initial_view) = self.placement.borrow().select(
                    &mut counts,
                    consensus.view(),
                    Some(replication_factor),
                )?;
                Ok(CreatedPartitionAssignment {
                    partition_id: u32::try_from(partition_id)
                        .map_err(|_| IggyError::InvalidCommand)?,
                    consensus_group_id,
                    initial_view,
                    members,
                })
            })
            .collect::<Result<Vec<_>, IggyError>>()?;
        Ok(())
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
        let operation = request_header.operation;

        let consensus = self
            .consensus
            .as_ref()
            .expect("submit_request_in_process: consensus only exists on shard 0");

        // Reply before admission so the client can retry without a read timeout.
        // This refusal covers only this attempt; earlier copies may still commit.
        // Retrying with their session and request id preserves deduplication.
        //
        // No catch-up gate: a non-register op admitted mid-commit-window
        // simply pipelines behind the in-flight batch (the wire path has
        // always done this); the register-specific invariant is guarded in
        // `submit_register_in_process` / `register_preflight`.
        if consensus.is_normal_backup() {
            return Ok(build_result_rejection_reply(
                &request_header,
                consensus.commit_max(),
                IggyError::NotPrimary.as_code(),
            )
            .into_generic());
        }
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
        // socket by transport id, exactly like a fresh commit. A cached reply
        // that does not decode surfaces as Canceled, and the home shard answers
        // it with `TransientNotCommitted`.
        let outcome = request_preflight(
            consensus,
            &self.client_table,
            client_id,
            session,
            request,
            operation,
        );
        if let Some(answer) = Self::answer_preflight(
            consensus,
            self.mux_stm.users(),
            &message,
            self.fence_lifecycle_preflight(outcome, &message),
        ) {
            return answer;
        }

        // Prepare queue full: backpressure, not failure. Absorb into the
        // request queue with this caller's reply subscriber; the commit
        // path promotes it as slots free up and the await below resolves
        // with the committed reply. A full request queue refuses this attempt
        // before admission, without settling the outcome of earlier copies.
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

        // The acting-user RBAC stamp and the cap admission live in the shared
        // `prepare_request` (op-guarded, fail-closed on an unknown session).
        // `request_preflight` above proved this client's session is live, so it
        // resolves there. It also served a same-id replay of a create that
        // already committed, so that create is never counted twice.
        let prepare = match self.prepare_request(message) {
            Ok(prepare) => prepare,
            Err(refusal) if is_admission_refusal(&refusal) => {
                return Ok(build_result_rejection_reply(
                    &request_header,
                    consensus.commit_max(),
                    refusal.as_code(),
                )
                .into_generic());
            }
            Err(_) => {
                // Malformed requests retain the existing session eviction policy.
                let reason = eviction_reason_for_invalid(request_header.operation);
                let ctx = EvictionContext::from_consensus(consensus);
                return Ok(build_eviction_message(ctx, client_id, reason).into_generic());
            }
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
        // Register and expired-session Logout require the catch-up gate for
        // their epoch checks. Both check-to-dispatch sections are synchronous.
        // Other submits can pipeline behind the in-flight batch.
        debug_assert!(
            prepare.header().operation != Operation::Register || is_caught_up_primary(consensus),
            "dispatch_prepare_and_await: register dispatched with the catch-up gate closed"
        );
        // `on_replicate` awaits: a sibling in-process submit may commit
        // (commit_min advances) or a view change may land (view advances) while
        // parked here. Both are handled downstream - a view change drops the
        // reply_sender so `receiver` resolves `Canceled`, `on_replicate` drops a
        // prepare whose view ended, and loopback acks are op-routed - so no
        // post-await view/commit invariant holds or is needed.
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
    /// gate races the sequencer pre-advance under a client burst), only this
    /// sweep regenerates it. Without it the op sits one vote short of quorum
    /// forever and pins the contiguous commit prefix, so `commit_min` never
    /// catches up to `commit_max` and the cluster wedges.
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
            let reply = self.apply_prepare(prepare);
            consensus.advance_commit_min(prepare_header.op);
            self.start_checkpoint_job_if_due(consensus, journal);
            // Partition primaries on metadata backups act on this op (fences,
            // joins, revocations), so the backups must not wait for the heartbeat.
            consensus.request_eager_commit();
            // Paired with the counter bump, and before the reply leaves: a
            // client that holds this reply may re-home onto any shard and read,
            // and the read gate admits it only once the frontier covers the op.
            self.advance_applied_frontier(prepare_header.op);
            emit_sim_event(SimEventKind::OperationCommitted, &event);

            // Fire subscriber BEFORE wire send. Slot already updated
            // (slot-first ordering, see take_reply_sender). Dropped
            // receiver: ignored. Still inside the sync region, so an
            // in-process awaiter is woken atomically with its commit.
            let reply = if self.mux_stm.streams().lifecycle_pending(prepare_header.op) {
                let pending =
                    crate::stm::result::ApplyReply::err(IggyError::TransientNotCommitted.as_code());
                build_reply_message_with(&prepare_header, pending.reply_body_len(), |dst| {
                    pending.write_reply_body(dst);
                })
                .transmute_header::<ReplyHeader>(|old, header| {
                    *header = old;
                    header.clear_committed_result();
                })
            } else {
                reply
            };
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
            //
            // A server-originated op has no socket either. Its entry loses the
            // in-process sender after a view change or a boot re-pipeline, and
            // a send to the reserved id only fails with a false error.
            if !had_in_process_subscriber
                && prepare_header.client != RESERVED_CLIENT_ID
                && !message_bus::is_server_internal_client(prepare_header.client)
            {
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

            // The production bus refuses it, and a resend gets the cached reply.
            if let Err(e) = consensus
                .message_bus()
                .send_to_client(event.client_id, reply_buffers)
                .await
            {
                debug!(
                    client = event.client_id,
                    op = event.op,
                    request_id = event.request_id,
                    operation = ?event.operation,
                    %e,
                    "client reply has no route by client id",
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
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn drain_request_queue_into_prepares(&self) {
        let consensus = self.consensus.as_ref().unwrap();
        // Promote while prepare slots exist. Requests are queued for two
        // reasons — prepare queue full at arrival, or (in-process register)
        // the catch-up gate was closed — so promotion is bounded by slots,
        // not by how many commits just freed: a whole burst absorbed during
        // one commit window drains the moment the window closes. Promoted
        // prepares are un-quorum'd, so they never re-close the gate here.
        loop {
            // A pending WAL trim ends the promotion as a full pipeline does,
            // and the tick promotes the rest once the trim ended. A request
            // that passes its preflight reaches the gate acquire in
            // `on_replicate` without an await after this test.
            if consensus.pipeline_is_full() || self.checkpoint_trim_pending() {
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
                {
                    let outcome = register_preflight(
                        consensus,
                        &self.client_table,
                        client_id,
                        user_id,
                        req.message.body(),
                        request_checksum,
                        |credential| {
                            self.accepts_registration_proof(
                                user_id,
                                credential,
                                consensus.clock_realtime_micros(),
                            )
                        },
                    );
                    apply_preflight_consensus_plane(
                        consensus,
                        self.fence_lifecycle_preflight(outcome, &req.message),
                        client_id,
                    )
                    .await
                }
            } else {
                let outcome = request_preflight(
                    consensus,
                    &self.client_table,
                    client_id,
                    session,
                    request,
                    operation,
                );
                apply_preflight_consensus_plane(
                    consensus,
                    self.fence_lifecycle_preflight(outcome, &req.message),
                    client_id,
                )
                .await
            };
            if !dispatch {
                continue;
            }

            let request_header = *req.message.header();
            let prepare = match self.prepare_request(req.message) {
                Ok(prepare) => prepare,
                Err(refusal) if is_admission_refusal(&refusal) => {
                    let reply = build_result_rejection_reply(
                        &request_header,
                        consensus.commit_max(),
                        refusal.as_code(),
                    );
                    if let Some(sender) = reply_sender {
                        let _ = sender.send(reply);
                    } else if let Err(error) = consensus
                        .message_bus()
                        .send_to_client(client_id, freeze_client_reply(reply.into_generic()))
                        .await
                    {
                        debug!(%error, client_id, "queued admission refusal could not reach client");
                    }
                    continue;
                }
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
    /// Wait for the journal writes in flight and keep new ones out until the
    /// guard drops, for a shard-side caller that truncates or appends to the
    /// journal.
    ///
    /// An in-process submit appends on its caller's task, so it can run under
    /// the WAL rewrite. Landing after a view's reconcile, its entry would sit
    /// in the view's log as a prepare of an earlier one, at an op the view may
    /// have decided differently.
    pub const fn hold_journal_writes(&self) -> LocalGateAcquire<'_> {
        self.journal_gate.acquire()
    }

    /// Whether a checkpoint job waits for `journal_gate` or holds it for its
    /// WAL trim. A journal write that starts now waits for the whole rewrite.
    ///
    /// The shard 0 pump reads this before each step that appends, and keeps
    /// the step for later while it is set. A short hold of the gate, as by the
    /// append of an in-process submit, does not set it.
    #[must_use]
    pub const fn checkpoint_trim_pending(&self) -> bool {
        self.pending_trims.get() > 0
    }

    /// Counts a WAL trim as pending until the returned guard drops. A
    /// checkpoint job calls this before it waits for `journal_gate`.
    #[must_use]
    pub fn announce_checkpoint_trim(&self) -> PendingTrim<'_> {
        self.pending_trims.set(self.pending_trims.get() + 1);
        PendingTrim(&self.pending_trims)
    }

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
            // Nothing to write, so the in-memory state is the durable one, as on a
            // partition without a store.
            let state = consensus.vsr_state(0, 0);
            consensus.mark_superblock_durable(state.view, state.log_view);
            consensus.mark_adopted_view_durable(state.adopted_view.as_ref());
            consensus.mark_history_certificate_durable(consensus.history_certificate());
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
                if let Some(coordinator) = &self.coordinator {
                    coordinator.superblock_checkpoint_op.set(checkpoint_op);
                }
                consensus.mark_superblock_durable(state.view, state.log_view);
                consensus.mark_adopted_view_durable(state.adopted_view.as_ref());
                consensus.mark_history_certificate_durable(state.history_certificate());
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
    /// Make journal room at the forced-checkpoint margin. Runs under `journal_gate`,
    /// before each append.
    ///
    /// Above the margin it takes no lock. At the margin it waits for
    /// `checkpoint_lock`, so for the durable part of a running job or for an
    /// install, and trims the WAL to the checkpoint the superblock pairs. If the
    /// journal is still at the margin, it checkpoints inline. Without a checkpoint
    /// spawner that is the only path.
    ///
    /// Diagnostic-only outcome: a failure is deliberately not surfaced as control
    /// flow, the prepare being replicated must proceed to its append regardless
    /// (see the phantom-op comment at the call site). A job that failed during the
    /// wait also skips the inline attempt, so that prepare appends into the margin
    /// and the next one retries.
    #[allow(clippy::future_not_send)]
    async fn checkpoint_if_needed(&self, consensus: &VsrConsensus<B, P>, journal: &J) {
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        if !coordinator.should_checkpoint(journal) {
            return;
        }
        let failed_jobs = coordinator.failed_jobs.get();
        let wait_started = Instant::now();
        // In-process submits each run on their own task, so several can reach the
        // margin together. Under the lock the later ones find the margin restored
        // by the first and coalesce away.
        let _checkpoint = self.checkpoint_lock.acquire().await;
        let lock_wait = wait_started.elapsed();
        self.pair_checkpoint_at_commit(consensus, coordinator).await;
        self.trim_to_superblock_checkpoint(
            consensus,
            journal,
            coordinator,
            lock_wait,
            CheckpointTrimmer::Margin,
        )
        .await;
        if !coordinator.should_checkpoint(journal) || coordinator.failed_jobs.get() != failed_jobs {
            return;
        }

        let capture_started = Instant::now();
        let Some(capture) = self.capture_checkpoint(consensus, coordinator) else {
            return;
        };
        let checkpoint_op = capture.sequence_number;
        let Some(timings) = self
            .persist_checkpoint(
                consensus,
                coordinator,
                capture,
                capture_started.elapsed(),
                CheckpointOrigin::Inline,
            )
            .await
        else {
            return;
        };
        #[cfg(test)]
        if checkpoint_boundary(CheckpointBoundary::SuperblockWritten).is_err() {
            return;
        }
        coordinator
            .untrimmed_timings
            .set(Some((checkpoint_op, timings)));
        self.drain_checkpointed_prefix(
            consensus,
            journal,
            coordinator,
            checkpoint_op,
            lock_wait,
            CheckpointTrimmer::Margin,
        )
        .await;
    }

    /// Start a checkpoint job once half the WAL committed above the checkpoint the
    /// superblock pairs. Runs right after `advance_commit_min`, with no await in
    /// between, so the capture belongs to that commit.
    ///
    /// Counts committed ops, not journaled ones: a lagging backup that counted its
    /// WAL would stay over the threshold after each job. The journaled ops are the
    /// margin path's concern, so at the margin no job starts. Nor during a state
    /// transfer: its install waits for a running job's write on the shard-0 pump.
    fn start_checkpoint_job_if_due(&self, consensus: &VsrConsensus<B, P>, journal: &J) {
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        let Some(slot_count) = journal.handle().slot_count() else {
            return;
        };
        let commit_min = consensus.commit_min();
        let committed = commit_min.saturating_sub(coordinator.superblock_checkpoint_op.get());
        if committed < slot_count as u64 / CHECKPOINT_JOB_TRIGGER_DIVISOR
            || commit_min < coordinator.job_retry_op.get()
            || coordinator.should_checkpoint(journal)
            || consensus.is_transferring()
        {
            return;
        }
        let Some(spawner) = self.checkpoint_spawner.borrow().as_ref().map(Rc::clone) else {
            return;
        };
        // Held by a running job or by an install.
        let Some(lease) = self.checkpoint_lock.try_acquire_owned() else {
            return;
        };
        let capture_started = Instant::now();
        let Some(capture) = self.capture_checkpoint(consensus, coordinator) else {
            lease.release();
            return;
        };
        spawner(CheckpointJob {
            lease,
            capture,
            capture_time: capture_started.elapsed(),
            // After the capture, which dropped the cache: only a shard that
            // serves an offer holds one now, and no offer is built before the
            // superblock write of this job.
            keeps_wal: self.offer_needs_wal_above(journal.handle().snapshot_op()),
        });
    }

    /// Make a captured checkpoint durable, then trim the WAL to it. The server
    /// spawns this on shard 0 through [`Self::set_checkpoint_spawner`].
    ///
    /// The job trims nothing when a requester held an offer of the replaced
    /// snapshot at the capture. That requester installs the snapshot and then
    /// repairs from the op after it, also when its offer expires during the
    /// writes of this job. A trim past that op sends it into another transfer,
    /// and under a steady load into one after the other. The trim falls to the
    /// margin, which the journal reaches before the next job is due: that trim
    /// is short, and it bounds the time that the requester has.
    ///
    /// The job holds its lease of `checkpoint_lock` through the snapshot and
    /// superblock writes and releases it before it waits for `journal_gate`. A margin
    /// waiter holds that gate while it waits for the lock, so the job never waits
    /// for the gate while holding the lock.
    #[allow(clippy::future_not_send)]
    pub async fn run_checkpoint_job(&self, job: CheckpointJob) {
        let CheckpointJob {
            lease,
            capture,
            capture_time,
            keeps_wal,
        } = job;
        let (Some(consensus), Some(journal), Some(coordinator)) = (
            self.consensus.as_ref(),
            self.journal.as_ref(),
            self.coordinator.as_ref(),
        ) else {
            lease.release();
            return;
        };
        let checkpoint_op = capture.sequence_number;
        let Some(timings) = self
            .persist_checkpoint(
                consensus,
                coordinator,
                capture,
                capture_time,
                CheckpointOrigin::Job,
            )
            .await
        else {
            let slot_count = journal.handle().slot_count().unwrap_or(0);
            coordinator.record_failed_job(consensus.commit_min(), slot_count as u64);
            lease.release();
            return;
        };
        coordinator
            .untrimmed_timings
            .set(Some((checkpoint_op, timings)));
        lease.release();
        #[cfg(test)]
        if checkpoint_boundary(CheckpointBoundary::SuperblockWritten).is_err() {
            return;
        }
        if keeps_wal {
            return;
        }

        let _trim = self.announce_checkpoint_trim();
        let wait_started = Instant::now();
        let _journal_writes = self.journal_gate.acquire().await;
        // Margin waiters and installs take `journal_gate` first, so only a newer job
        // can hold the lock here. It trims further itself, and a trim here would
        // mark its pending checkpoint complete.
        let Some(_checkpoint) = self.checkpoint_lock.try_acquire() else {
            return;
        };
        self.trim_to_superblock_checkpoint(
            consensus,
            journal,
            coordinator,
            wait_started.elapsed(),
            CheckpointTrimmer::Job,
        )
        .await;
    }

    /// Capture a checkpoint at `commit_min`, or `None` when none is due. The caller
    /// holds `checkpoint_lock`.
    fn capture_checkpoint(
        &self,
        consensus: &VsrConsensus<B, P>,
        coordinator: &SnapshotCoordinator<M>,
    ) -> Option<MetadataCapture> {
        // Use commit_min (locally executed), not commit_max. WAL entries
        // between commit_min+1 and commit_max haven't been applied to the
        // state machine yet, draining them would lose data on crash.
        let snap_op = consensus.commit_min();
        let checkpoint = coordinator.last_checkpoint();
        // The snapshot of the last checkpoint is durable. A second capture of
        // its op gets another `created_at`, so another checksum, and replaces
        // that snapshot. A superblock write may pair the first one on disk, also
        // a write that reported an error. A crash before the next superblock
        // write then leaves a pairing that recovery refuses. So a pending
        // checkpoint retries its superblock write and its trim, never its
        // capture.
        if snap_op <= checkpoint.0 {
            return None;
        }
        // A transferred table may include replies above the restored STM floor.
        // Recovery must never replay that interval against its later protection.
        if snap_op < self.client_table_frontier.get() {
            return None;
        }
        coordinator.checkpoint_pending.set(true);
        // An offer whose read is in flight holds the file that this checkpoint
        // replaces and trims past.
        self.clear_state_transfer_offer_cache();
        // Stamp created_at from the injected consensus clock (seed-derived
        // under the simulator), not the wall clock, so replayed snapshots are
        // byte-identical.
        let created_at = consensus.clock_realtime_micros();
        let client_table = self.client_table.borrow().to_snapshot();
        let change_log = self.change_log.snapshot();
        #[cfg(test)]
        if checkpoint_boundary(CheckpointBoundary::TableCaptured).is_err() {
            return None;
        }
        match coordinator.capture(
            &*self.mux_stm,
            snap_op,
            created_at,
            Some(client_table),
            change_log,
        ) {
            Ok(capture) => Some(capture),
            Err(e) => {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op = snap_op,
                    error = %e,
                    "checkpoint snapshot capture failed"
                );
                None
            }
        }
    }

    /// Persist `capture`, then durably record its pairing in the superblock.
    /// Returns the step timings, or `None` after a logged failure. The caller holds
    /// `checkpoint_lock`.
    ///
    /// Durability ordering, must not be reordered: persist the snapshot, durably
    /// record the `(checkpoint_op, checksum, commit_max)` pairing in the superblock,
    /// THEN drain the snapshotted prefix from the WAL. A crash before the superblock
    /// write recovers the prior checkpoint with the WAL intact; a crash after it
    /// recovers the new one. Draining before the superblock points at the new
    /// snapshot could strand committed ops on a crash. A failure leaves the
    /// checkpoint pending, so a later attempt retries it from the top.
    #[allow(clippy::future_not_send)]
    async fn persist_checkpoint(
        &self,
        consensus: &VsrConsensus<B, P>,
        coordinator: &SnapshotCoordinator<M>,
        capture: MetadataCapture,
        capture_time: Duration,
        origin: CheckpointOrigin,
    ) -> Option<CheckpointTimings> {
        let checkpoint_op = capture.sequence_number;
        let write_started = Instant::now();
        let persisted = coordinator.persist_capture(capture).await;
        // Also on an error, which can come after the rename.
        self.clear_state_transfer_offer_cache();
        let write = write_started.elapsed();
        let checksum = match persisted {
            Ok(checksum) => checksum,
            Err(e) => {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op,
                    error = %e,
                    "checkpoint snapshot persist failed"
                );
                return None;
            }
        };

        let superblock_started = Instant::now();
        if let Some(superblock) = self.superblock.as_ref() {
            // `persist_capture` already recorded the new pairing on the coordinator, so
            // `write_superblock` picks it up from there. A view persist that interleaves
            // between those two steps writes the same new pairing, which only makes it
            // durable sooner.
            debug_assert_eq!(
                coordinator.last_checkpoint(),
                (checkpoint_op, checksum),
                "the checkpoint's pairing must be what the superblock write records"
            );
            let _superblock = self.superblock_lock.acquire().await;
            if !self.write_superblock(consensus, superblock.as_ref()).await {
                error!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    checkpoint_op,
                    "checkpoint superblock write failed; withholding WAL drain"
                );
                return None;
            }
        } else {
            coordinator.superblock_checkpoint_op.set(checkpoint_op);
        }
        Some(CheckpointTimings {
            origin,
            capture: capture_time,
            write,
            superblock: superblock_started.elapsed(),
        })
    }

    /// Write the superblock again for a checkpoint at `commit_min` whose earlier
    /// write failed. No capture replaces the snapshot of that op, see
    /// [`Self::capture_checkpoint`], so this write is its retry. A checkpoint
    /// below `commit_min` is left to the next capture. The caller holds
    /// `checkpoint_lock`.
    #[allow(clippy::future_not_send)]
    async fn pair_checkpoint_at_commit(
        &self,
        consensus: &VsrConsensus<B, P>,
        coordinator: &SnapshotCoordinator<M>,
    ) {
        if !coordinator.checkpoint_pending.get()
            || !coordinator.checkpoint_unpaired()
            || coordinator.last_checkpoint().0 != consensus.commit_min()
        {
            return;
        }
        let Some(superblock) = self.superblock.as_ref() else {
            return;
        };
        let _superblock = self.superblock_lock.acquire().await;
        if !self.write_superblock(consensus, superblock.as_ref()).await {
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                checkpoint_op = coordinator.last_checkpoint().0,
                "checkpoint superblock write failed again; withholding WAL drain"
            );
        }
    }

    /// Trim the WAL to the checkpoint the superblock pairs, if the floor is still
    /// below it. The caller holds `journal_gate` and `checkpoint_lock`.
    #[allow(clippy::future_not_send)]
    async fn trim_to_superblock_checkpoint(
        &self,
        consensus: &VsrConsensus<B, P>,
        journal: &J,
        coordinator: &SnapshotCoordinator<M>,
        gate_wait: Duration,
        trimmed_by: CheckpointTrimmer,
    ) {
        let checkpoint_op = coordinator.superblock_checkpoint_op.get();
        // A failed install can leave the pairing ahead of the applied state, and a
        // drain there would remove this replica's commit point.
        if checkpoint_op > consensus.commit_min() {
            return;
        }
        let Some(drain_to) = checkpoint_op.checked_sub(1) else {
            return;
        };
        if journal.handle().snapshot_op() >= drain_to {
            // An install trims by itself. Nothing is left of a checkpoint that
            // is paired and trimmed.
            if coordinator.last_checkpoint().0 == checkpoint_op {
                coordinator.checkpoint_pending.set(false);
            }
            return;
        }
        self.drain_checkpointed_prefix(
            consensus,
            journal,
            coordinator,
            checkpoint_op,
            gate_wait,
            trimmed_by,
        )
        .await;
    }

    /// Drain the WAL below `checkpoint_op`, whose pairing is durable, and log the
    /// checkpoint as completed. The caller holds `journal_gate` and `checkpoint_lock`.
    #[allow(clippy::future_not_send)]
    async fn drain_checkpointed_prefix(
        &self,
        consensus: &VsrConsensus<B, P>,
        journal: &J,
        coordinator: &SnapshotCoordinator<M>,
        checkpoint_op: u64,
        gate_wait: Duration,
        trimmed_by: CheckpointTrimmer,
    ) {
        let trim_started = Instant::now();
        let drained = coordinator.drain(journal, checkpoint_op).await;
        // On the error path too: a drain that fails part-way still removed
        // whatever it reached, and a snapshot left offering those bodies strands
        // the peer that picks this replica as a repair source.
        consensus.note_journal_mutation();
        if let Err(e) = drained {
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                checkpoint_op,
                error = %e,
                "checkpoint WAL drain failed"
            );
            return;
        }
        if coordinator.last_checkpoint().0 == checkpoint_op {
            coordinator.checkpoint_pending.set(false);
        }

        #[cfg(test)]
        if checkpoint_boundary(CheckpointBoundary::WalDrained).is_err() {
            return;
        }
        let timings = coordinator
            .untrimmed_timings
            .take()
            .filter(|(op, _)| *op == checkpoint_op)
            .map_or_else(CheckpointTimings::default, |(_, timings)| timings);
        #[cfg(test)]
        {
            COMPLETED_CHECKPOINTS.set(COMPLETED_CHECKPOINTS.get() + 1);
            LAST_COMPLETED_CHECKPOINT.set(Some((timings.origin, trimmed_by)));
        }
        // Info, not debug: checkpoints are rare and change what a restart can
        // recover locally, and spec tests pin checkpoint placement by grepping
        // this line off stdout at the default `info` level
        // (`metadata_checkpoint_restart`, `metadata_changes`).
        info!(
            target: "iggy.metadata.diag",
            plane = "metadata",
            replica_id = consensus.replica(),
            checkpoint_op,
            origin = %timings.origin,
            trimmed_by = %trimmed_by,
            capture = ?timings.capture,
            write = ?timings.write,
            superblock = ?timings.superblock,
            trim = ?trim_started.elapsed(),
            gate_wait = ?gate_wait,
            "forced checkpoint completed"
        );
    }

    /// Merge locally published normal views without treating missing reports as
    /// failures. Incarnation checks keep deleted namespaces from transferring load.
    pub fn observe_partition_views(&self, reports: &[PartitionPlacementView]) {
        self.placement
            .borrow_mut()
            .observe(self.mux_stm.streams(), reports);
    }

    /// Initial placement uses readiness only within the metadata view that observed it.
    /// An empty slice clears the evidence when the coordinator loses authority.
    pub fn observe_replica_readiness(&self, eligible: &[bool]) {
        if let Some(consensus) = self.consensus.as_ref() {
            self.placement
                .borrow_mut()
                .observe_replicas(consensus.view(), eligible);
        }
    }

    /// Whether every member of the cluster can tier topics, as the metadata
    /// primary's probes last found. The verdict holds only within the metadata
    /// view that observed it, so one from before a view change admits nothing.
    /// `false` clears it when the coordinator loses authority.
    pub fn observe_tiered_storage_readiness(&self, cluster_ready: bool) {
        self.tiered_storage_ready_view.set(
            self.consensus
                .as_ref()
                .filter(|_| cluster_ready)
                .map(VsrConsensus::view),
        );
    }

    /// Whether a request may turn tiered storage on: the last verdict was that
    /// every member is ready, and it was reached in the current metadata view.
    #[must_use]
    pub fn tiered_storage_cluster_ready(&self) -> bool {
        self.consensus
            .as_ref()
            .is_some_and(|consensus| self.tiered_storage_ready_view.get() == Some(consensus.view()))
    }

    /// Suggest one count-balancing move only after normal failover has settled.
    /// `after` rotates candidates so a recovering target cannot starve other groups.
    /// Each move in `inflight` counts as done, so a caller plans a round of
    /// moves by adding each suggestion to `inflight` before the next call.
    pub fn primary_rebalance_candidate(
        &self,
        eligible: &[bool],
        after: Option<IggyNamespace>,
        inflight: &BTreeMap<IggyNamespace, PartitionPrimaryTransfer>,
    ) -> Option<PartitionPrimaryTransfer> {
        self.placement.borrow_mut().primary_rebalance_candidate(
            self.mux_stm.streams(),
            self.consensus.as_ref()?.replica_count(),
            eligible,
            after,
            inflight,
        )
    }

    /// Return an observed normal view only for the current, unblocked incarnation.
    pub fn partition_placement_view(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
    ) -> Option<PartitionPlacementView> {
        self.placement.borrow().partition_placement_view(
            self.mux_stm.streams(),
            namespace,
            created_revision,
        )
    }

    /// Reconstruct at most one missing reservation per metadata tick. Admission
    /// stays synchronous and refuses creates until every canonical summary exists.
    #[allow(clippy::future_not_send)]
    pub async fn refresh_placement(&self) {
        let (Some(consensus), Some(journal)) = (self.consensus.as_ref(), self.journal.as_ref())
        else {
            return;
        };
        let Ok(headers) = self.pending_placement_headers() else {
            return;
        };
        let Some(header) = headers
            .into_iter()
            .find(|header| !self.placement.borrow().contains(header))
        else {
            return;
        };
        let view = consensus.view();
        let Some(prepare) = journal.handle().entry(&header).await else {
            return;
        };
        if prepare.header().op != header.op
            || prepare.header().checksum != header.checksum
            || verify_prepare_integrity(prepare.header(), prepare.as_slice()).is_err()
            || consensus.view() != view
            || !is_caught_up_primary(consensus)
            || header.op <= consensus.commit_min()
            || !consensus.pipeline_holds_entry(header.op, header.checksum)
        {
            return;
        }
        if let Ok(Some(highest_group_id)) = self
            .placement
            .borrow_mut()
            .remember(&prepare, consensus.replica_count())
        {
            self.allocator.observe(highest_group_id);
        }
    }

    /// `[metadata] partitions_max` admission for a `CreateTopic` or
    /// `CreatePartitions`. Other operations pass.
    ///
    /// The apply must not branch on node config, so the primary checks the cap
    /// in `prepare_request`, right before the create enters the prepare
    /// pipeline with no await between. It counts the committed partitions and
    /// those of every create in that pipeline, so creates admitted before any
    /// of them commits cannot take the node past the cap. A create leaves the
    /// count when it leaves the pipeline: it commits, the apply refuses it, or
    /// a view change drops it. A create parked in the request queue is checked
    /// when it is promoted. The apply can still refuse a create in flight, so a
    /// create that fits the committed partitions alone is refused as transient,
    /// and only one past the cap on committed partitions gets the cap's error.
    ///
    /// A body that does not decode passes here, and `prepare_request` evicts
    /// the session for it. A create that the gated apply refuses, for a
    /// missing target, a missing grant or a topic name already in use, also
    /// passes, so it gets that error and not the cap's. That verdict reads the
    /// committed state, and an op in flight can create the target, grant the
    /// permission or free the name before the create applies, so while any op
    /// is in flight such a create is refused as transient instead.
    fn admit_partitions(&self, message: &Message<RoutedRequestHeader>) -> Result<(), IggyError> {
        let partitions_max = self.partitions_max.get();
        if partitions_max == 0 {
            return Ok(());
        }
        let header = message.header();
        let body = &message.as_slice()[size_of::<RoutedRequestHeader>()..header.size as usize];
        let (requested, stream_id, create) = match header.operation {
            Operation::CreateTopic => match WireCreateTopicRequest::decode_from(body) {
                Ok(request) => (
                    request.partitions_count,
                    request.stream_id,
                    PartitionsCreate::Topic { name: request.name },
                ),
                Err(_) => return Ok(()),
            },
            Operation::CreatePartitions => match WireCreatePartitionsRequest::decode_from(body) {
                Ok(request) => (
                    request.partitions_count,
                    request.stream_id,
                    PartitionsCreate::Partitions {
                        topic_id: request.topic_id,
                    },
                ),
                Err(_) => return Ok(()),
            },
            _ => return Ok(()),
        };
        let committed = u64::try_from(self.mux_stm.streams().partition_count()).unwrap_or(u64::MAX);
        let pending = self.pending_partitions()?;
        let admitted =
            validate_partitions_limit(partitions_max, requested, committed.saturating_add(pending));
        if admitted.is_err() {
            let applies =
                resolve_acting_user_id(header.operation, header.client, &self.client_table)
                    .ok()
                    .flatten()
                    .is_some_and(|user_id| {
                        admits_partitions_create(
                            self.mux_stm.users(),
                            self.mux_stm.streams(),
                            user_id,
                            &stream_id,
                            &create,
                        )
                    });
            if !applies {
                // Any pending metadata may change validity. Conservatively defer
                // even unrelated work; steady traffic can leave this request at
                // 58 until its deadline. A valid create gets the cap error only
                // when the committed partitions alone exceed the cap.
                let consensus = self
                    .consensus
                    .as_ref()
                    .ok_or(IggyError::TransientNotAccepted)?;
                if consensus.sequencer().current_sequence() > consensus.commit_min() {
                    return Err(IggyError::TransientNotAccepted);
                }
                return Ok(());
            }
            if validate_partitions_limit(partitions_max, requested, committed).is_ok() {
                return Err(IggyError::TransientNotAccepted);
            }
            warn!(
                operation = ?header.operation,
                partitions_max,
                committed,
                pending,
                requested,
                "refused a create that would take the node past metadata.partitions_max"
            );
        }
        admitted
    }

    /// `[personal_access_token] max_tokens_per_user` admission for a
    /// `CreatePersonalAccessToken`. Other operations pass, so a user at the
    /// cap can still delete a token to free a slot.
    ///
    /// Checked where and how [`Self::admit_partitions`] checks its cap: the
    /// user's committed tokens plus the user's creates in the prepare
    /// pipeline. A pipeline entry keeps only its header, so a create in flight
    /// counts for its acting user. Every transport rewrites the body for the
    /// session's user, and `prepare_request` stamps that same user into the
    /// header. The apply can still refuse a create in flight, for a taken name
    /// or an expiry already past, so a create that fits the committed tokens
    /// alone is refused as transient. It runs before the apply, so a create
    /// past the cap on committed tokens reports the cap even when its name is
    /// taken, as the legacy server did.
    ///
    /// The PAT rewrite of every transport encodes the body, so the decode
    /// cannot fail for a client request.
    fn admit_personal_access_token(
        &self,
        message: &Message<RoutedRequestHeader>,
    ) -> Result<(), IggyError> {
        let max_tokens_per_user = self.max_tokens_per_user.get();
        if max_tokens_per_user == 0
            || message.header().operation != Operation::CreatePersonalAccessToken
        {
            return Ok(());
        }
        let Ok(request) = CreatePersonalAccessTokenRequest::decode_from(message.body()) else {
            return Ok(());
        };
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(IggyError::TransientNotAccepted)?;
        let pending = pending_headers(consensus, |header| {
            header.operation == Operation::CreatePersonalAccessToken
                && header.user_id == request.user_id
        })?
        .len();
        let committed = self
            .mux_stm
            .users()
            .read(|users| users.pat_count_of(request.user_id));
        if committed.saturating_add(pending) >= max_tokens_per_user as usize {
            if committed < max_tokens_per_user as usize {
                return Err(IggyError::TransientNotAccepted);
            }
            return Err(IggyError::PersonalAccessTokensLimitReached(
                request.user_id,
                max_tokens_per_user,
            ));
        }
        Ok(())
    }

    fn placement_counts(&self) -> Result<PlacementCounts, IggyError> {
        let headers = self.pending_placement_headers()?;
        let consensus = self.consensus.as_ref().unwrap();
        let mut placement = self.placement.borrow_mut();
        let mut counts =
            placement.committed_counts(self.mux_stm.streams(), consensus.replica_count());
        for header in headers {
            placement.add_reservation(&header, &mut counts)?;
        }
        Ok(counts)
    }

    /// The committed topic's replication factor. `None` for a topic created
    /// before replication factors, or one that does not exist. A topic not
    /// committed yet refuses transiently while a create waits in the pipeline:
    /// that create may be the one named, and its factor is unknown until it
    /// commits.
    fn committed_replication_factor(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Result<Option<u8>, IggyError> {
        let committed = self.mux_stm.streams().read(|inner| {
            let stream = inner.resolve_stream_id(stream_id)?;
            let topic = inner.resolve_topic_id(stream, topic_id)?;
            let topic = inner.items.get(stream)?.topics.get(topic)?;
            Some(TopicCreateOptions::from_resource_options(&topic.options).replication_factor)
        });
        let Some(factor) = committed else {
            let creating_topic = self
                .pending_placement_headers()?
                .iter()
                .any(|header| header.operation == Operation::CreateTopicWithAssignments);
            return if creating_topic {
                Err(IggyError::TransientNotAccepted)
            } else {
                Ok(None)
            };
        };
        factor
            .map(|factor| u8::try_from(factor).map_err(|_| IggyError::InvalidReplicationFactor))
            .transpose()
    }

    /// Partitions the creates in the prepare pipeline add when they commit.
    fn pending_partitions(&self) -> Result<u64, IggyError> {
        let headers = self.pending_placement_headers()?;
        let placement = self.placement.borrow();
        headers
            .iter()
            .map(|header| placement.reserved_partitions(header))
            .sum()
    }

    fn pending_placement_headers(&self) -> Result<Vec<PrepareHeader>, IggyError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(IggyError::TransientNotAccepted)?;
        if !is_caught_up_primary(consensus) {
            return Err(IggyError::TransientNotAccepted);
        }
        self.placement.borrow_mut().prune(|op, checksum| {
            op > consensus.commit_min() && consensus.pipeline_holds_entry(op, checksum)
        });
        pending_headers(consensus, |header| {
            matches!(
                header.operation,
                Operation::CreateTopicWithAssignments
                    | Operation::CreatePartitionsWithAssignments
                    | Operation::ShadowTopic
            )
        })
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
        // replica, WAL replay included. Every client-op
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

        // The caps count the creates already in the prepare pipeline, and every
        // caller pushes this prepare before its next await, so no create passes
        // on a count that misses another.
        self.admit_partitions(&message)?;
        self.admit_personal_access_token(&message)?;

        // Must be read AFTER the stamp: the `CreateTopic` / `CreatePartitions`
        // arms hand this `header` copy to `build_prepare_message`, so it has to
        // carry the stamped acting user. Reading it before the stamp would ship
        // those prepares with the untrusted wire `user_id` (0 = root from
        // well-behaved SDKs, attacker-chosen otherwise), silently bypassing the
        // authz gate. The default arm projects the mutated buffer directly and
        // is order-independent.
        let header = *message.header();
        let body = &message.as_slice()[size_of::<RoutedRequestHeader>()..header.size as usize];

        let prepare = match header.operation {
            Operation::CreateTopic => {
                let mut request = WireCreateTopicRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                // Resolve every absent catalog key to its catalog default here,
                // at primary admission, so the replicated payload carries
                // concrete values and every replica commits the same state
                // whatever its build. Resolved defaults ride a separate derived
                // block, preserving per-key provenance for `GetTopic`.
                let mut admitted = admit(
                    OptionsScope::Topic,
                    OptionOperation::Create,
                    &request.options,
                    &[],
                    &OptionAdmissionContext::default(),
                )?;
                // A node that does not understand a critical key must refuse
                // the topic rather than serve it without that key's effect. A
                // geo topic writes in a mode even when the request names none,
                // so its mode is critical either way.
                let geo = TopicCreateOptions::from_resource_options(&admitted.explicit).is_geo();
                let critical: Vec<OptionKey> = admitted
                    .explicit
                    .keys()
                    .filter_map(|key| OptionKey::try_from(key).ok())
                    .filter(|key| key.descriptor().critical)
                    .chain(geo.then_some(OptionKey::GeoMode))
                    .collect();
                if !critical.is_empty() {
                    admitted.explicit.insert(
                        HeaderKey::from(OptionKey::CriticalKeys),
                        OptionValue::explicit(critical_keys_marker(&critical)?),
                    );
                }
                // Re-encode the admitted block rather than forwarding the
                // client's bytes. Admission drops a zero sentinel, so the
                // resolved value goes in the derived block, but apply merges
                // with explicit winning, so a forwarded literal `0` would land
                // back on top as the stored effective value. Re-encoding also
                // canonicalizes kinds (a `"128MiB"` string becomes `Uint64`).
                request.options =
                    resource_options_to_wire(&admitted.explicit, OptionsProvenance::All)?;
                let mut derived = derived_defaults(OptionsScope::Topic, &admitted.explicit)?;
                let replication_factor = resolve_replication_factor(
                    TopicCreateOptions::from_resource_options(&admitted.explicit)
                        .replication_factor,
                    self.default_replication_factor(consensus.replica_count()),
                    consensus.replica_count(),
                    &mut derived,
                )?;
                let mut stored = derived.clone();
                stored.extend(admitted.explicit.clone());
                let resolved = TopicResolvedOptions::from(&stored);
                let resolved_segment_size = resolved.runtime.effective_segment_size();
                // Backstop for the transport-side typed checks: an explicit
                // segment size outside its bounds, or a topic cap below one
                // segment, must never enter the WAL.
                if let Some(segment_size) =
                    TopicCreateOptions::from_resource_options(&admitted.explicit).segment_size
                {
                    validate_topic_segment_size(
                        segment_size.as_bytes_u64(),
                        iggy_common::MAX_TOPIC_SEGMENT_SIZE,
                    )?;
                }
                if resolved.max_topic_size.as_bytes_u64() < resolved_segment_size.as_bytes_u64() {
                    return Err(IggyError::InvalidOptionValue(
                        topic_option_keys::MAX_TOPIC_SIZE.to_string(),
                    ));
                }
                validate_segment_max_age(resolved.runtime)?;
                validate_geo_topic(&admitted.explicit)?;
                admit_geo_topic(&admitted.explicit, self.tiered_storage_cluster_ready())?;
                admit_tiered_storage(&admitted.explicit, self.tiered_storage_cluster_ready())?;
                validate_local_retention(&stored)?;
                let derived_options = resource_options_to_wire(&derived, OptionsProvenance::All)?;
                let mut counts = self.placement_counts()?;
                let partitions = self
                    .allocator
                    .allocate_many(request.partitions_count as usize)
                    .into_iter()
                    .enumerate()
                    .map(|(partition_id, consensus_group_id)| {
                        let (members, initial_view) = self.placement.borrow().select(
                            &mut counts,
                            consensus.view(),
                            Some(replication_factor),
                        )?;
                        Ok(CreatedPartitionAssignment {
                            partition_id: u32::try_from(partition_id)
                                .map_err(|_| IggyError::InvalidCommand)?,
                            consensus_group_id,
                            initial_view,
                            members,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let body = PersistedCreateTopicRequest {
                    request,
                    derived_options,
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
                // validation design doc. The replication factor comes from the
                // committed topic: a missing topic gets no member list, and
                // apply answers it with `TopicNotFound`.
                let replication_factor =
                    self.committed_replication_factor(&request.stream_id, &request.topic_id)?;
                let mut counts = self.placement_counts()?;
                let partitions = self
                    .allocator
                    .allocate_many(request.partitions_count as usize)
                    .into_iter()
                    .enumerate()
                    .map(|(offset, consensus_group_id)| {
                        let (members, initial_view) = self.placement.borrow().select(
                            &mut counts,
                            consensus.view(),
                            replication_factor,
                        )?;
                        Ok(CreatedPartitionAssignment {
                            partition_id: u32::try_from(offset)
                                .map_err(|_| IggyError::InvalidCommand)?,
                            consensus_group_id,
                            initial_view,
                            members,
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
            Operation::CreateStream => {
                let request = WireCreateStreamRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                let options = resource_options_from_wire(&request.options, true)?;
                // Apply refuses a plaintext key too, but only after every replica journaled it.
                if let Some(secret) = options
                    .keys()
                    .filter_map(|key| OptionKey::try_from(key).ok())
                    .find(|key| key.descriptor().visibility == OptionVisibility::Secret)
                {
                    return Err(IggyError::InvalidOptionValue(secret.to_string()));
                }
                // An edge whose keys no peer holds wraps only under its own, and
                // data committed under such a wrap opens nowhere else.
                if let Some(set) = stream_wrap_set(&options)?
                    && self.stream_key_context.open(&set).is_err()
                {
                    return Err(IggyError::StreamEncryptionUnavailable);
                }
                Ok(message.project(consensus))
            }
            Operation::UpdateResource => {
                let request = UpdateResourceRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                let (operation, body) =
                    resolve_update_resource(&request, self.tiered_storage_cluster_ready())?;
                Ok(build_prepare_message(consensus, &header, operation, &body))
            }
            _ => Ok(message.project(consensus)),
        }?;
        let prepare = consensus::seal_prepare_capacity(
            prepare,
            consensus.retry_capacity(&self.client_table.borrow()),
        );
        self.placement
            .borrow_mut()
            .remember(&prepare, consensus.replica_count())?;
        Ok(prepare)
    }

    /// Replicate a prepare message to the backups.
    ///
    /// Only the acting primary sends: it enqueues one copy for every backup
    /// (`replicate_prepare`). Backups never forward a prepare.
    ///
    /// Caller must have already appended `message` to the local journal
    /// before invoking this helper (VSR tail-ahead-of-head). Sending
    /// an un-persisted prepare would leave backup WALs with an op
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
            "replicate: prepare must be durable in local journal before the send"
        );
        if let Err(e) = replicate_prepare(consensus, message).await {
            tracing::warn!(op = header.op, error = ?e, "prepare replication enqueue failed");
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
    ///
    /// Returns why the walk stopped; callers that only drive the walk ignore
    /// it.
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&self) -> CommitWalkStop {
        let consensus = self.consensus.as_ref().unwrap();
        if consensus.view_log_unproven() {
            return CommitWalkStop::ViewLogUnproven;
        }
        let journal = self.journal.as_ref().unwrap();

        let mut applied = 0usize;
        while consensus.commit_min() < consensus.commit_max() {
            if applied == COMMIT_WALK_OPS_MAX {
                debug!(
                    "commit_journal: stopping at op={} after {applied} ops; resuming next tick",
                    consensus.commit_min()
                );
                return CommitWalkStop::WalkCap;
            }
            applied += 1;
            let op = consensus.commit_min() + 1;

            // Never apply the op the pipeline holds: `on_ack` pops that entry
            // and advances past it, so applying it here strands it below
            // `peek_committable_head`'s floor. Nothing pops it after that (the
            // only `pop_committed_prepare` sits inside that loop), so its wire
            // reply is never built and its `reply_sender` neither fires nor drops.
            //
            // Compared per op, not hoisted: the body read below awaits and a
            // sibling driver can move the head. Comparing against `op` also gets
            // the two edge cases right for free -- an absent head is a backup's
            // empty pipeline and caps nothing, and a head at or below `commit_min`
            // is already stranded and must not freeze the walk on top of that.
            if consensus
                .pipeline_head_header()
                .is_some_and(|head| head.op == op)
            {
                return CommitWalkStop::PipelineHead;
            }

            // Copied out of its guard: the body read below awaits, and a
            // checkpoint job that trims the WAL meanwhile borrows the index
            // mutably.
            let Some(header) = journal.handle().header(op as usize).map(|header| *header) else {
                // Gap-stop: the walk halts at the first missing prepare and
                // resumes once it is refilled -- by the primary's retransmit
                // while the op still lacks quorum, otherwise by a
                // `MetadataRepairSession` (shard), armed at recovery, at
                // StartView adoption, or by `tick_metadata`'s gap detector,
                // which is the only one of those a live drop under sustained
                // traffic reaches.
                return CommitWalkStop::HeaderMissing { op };
            };

            let Some(prepare) = journal.handle().entry(&header).await else {
                warn!("commit_journal: prepare body missing for op={op}, stopping");
                return CommitWalkStop::BodyMissing { op };
            };

            // SM apply + client_table mutation BEFORE `advance_commit_min`
            // (see `on_ack` for matching invariant). No await between table
            // mutation and counter bump. The post-commit notifier (e.g. partition
            // reconciler wake-up) fires on backups too, so reconcilers converge
            // after replicated commits, not only quorum-acked ones reached via
            // `on_ack` on the primary.
            //
            // The replay table follows the snapshot while the transferred table
            // retains newer protection for client admission.
            self.apply_prepare(prepare);
            consensus.advance_commit_min(op);
            self.start_checkpoint_job_if_due(consensus, journal);
            self.advance_applied_frontier(op);
            debug!("commit_journal: committed op={op}");
        }
        CommitWalkStop::CaughtUp
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

    /// The header of the prepare this replica applied at `op`, while the journal
    /// still holds it: the `applied_at` of
    /// [`VsrConsensus::contradicts_applied_log`].
    ///
    /// Above the snapshot floor only. A state-transfer install keeps the entries
    /// at or below the floor it installed, and this replica applied the snapshot
    /// there, not those entries.
    #[must_use]
    pub fn applied_header(&self, op: u64) -> Option<PrepareHeader> {
        let handle = self.journal.as_ref()?.handle();
        if op <= handle.snapshot_op() {
            return None;
        }
        usize::try_from(op)
            .ok()
            .and_then(|slot| handle.header(slot))
            .map(|header| *header)
    }
}

/// In-process Register `Message<RoutedRequestHeader>`. Mirrors
/// `SimClient::register`: `session=0`, `request=0` per
/// [`RoutedRequestHeader::validate`]; the body identifies the verified account.
///
/// `cluster` + `view` from `consensus` for self-consistency before
/// `Project::project` overwrites. `release = 0` matches wire today; both
/// paths should switch to `consensus.release()` once
/// `ClientReleaseTooLow/TooHigh` lands.
///
/// `prepare_request` transmutes the header into `PrepareHeader` without reallocating.
fn build_register_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    user_id: u32,
    registration: RegisterRequest,
) -> Message<RoutedRequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let header_size = size_of::<RoutedRequestHeader>();
    let body = registration.to_bytes();
    let message_size = header_size + body.len();
    let mut msg = Message::<RoutedRequestHeader>::new(message_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
        &mut msg.as_mut_slice()[..header_size],
    )
    .expect("zeroed bytes are a valid RoutedRequestHeader");
    *header = RoutedRequestHeader {
        command: Command::Request,
        operation: Operation::Register,
        size: u32::try_from(message_size).expect("Register size fits u32"),
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
    msg.as_mut_slice()[header_size..].copy_from_slice(&body);
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

fn build_partition_completion_request_message<B, P>(
    consensus: &VsrConsensus<B, P>,
    operation: Operation,
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
            operation,
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

/// Build a truncate attributed to the client's original request and session.
/// Committed rejections also advance the client table, so retries deduplicate.
///
/// # Panics
/// If the request size exceeds `u32::MAX`; this fixed-size command cannot.
#[must_use]
pub fn build_truncate_partition_client_message(
    template: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    request: &TruncatePartitionRequest,
) -> Message<RoutedRequestHeader> {
    let body = request.to_bytes();
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
            request_checksum: template.request_checksum,
            group: server_common::sharding::METADATA_GROUP,
            ..RoutedRequestHeader::default()
        };
    }
    msg
}

/// Build a committed rejection for a target that did not resolve locally.
/// Keeping the raw identifiers lets metadata report the missing resource.
///
/// # Panics
/// If the request size exceeds `u32::MAX`; this fixed-size command cannot.
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
    build_truncate_partition_client_message(
        template,
        client_id,
        session,
        &TruncatePartitionRequest {
            stream_id,
            topic_id,
            partition_id,
            up_to_offset,
            expected_history: None,
        },
    )
}

/// A new topic's replication factor: the explicit value, which must fit the
/// cluster's `nodes`, or this node's `default`. The default is written back
/// into `derived`, so the topic stores what placement used.
fn resolve_replication_factor(
    explicit: Option<u32>,
    default: u8,
    nodes: u8,
    derived: &mut ResourceOptions,
) -> Result<u8, IggyError> {
    let factor = explicit.unwrap_or_else(|| {
        let factor = u32::from(default);
        derived.insert(
            HeaderKey::from(OptionKey::ReplicationFactor),
            OptionValue::derived(HeaderValue::from(factor)),
        );
        factor
    });
    match u8::try_from(factor) {
        Ok(factor) if factor >= 1 && factor <= nodes => Ok(factor),
        _ => {
            debug!(
                replication_factor = factor,
                nodes, "refusing a topic whose replication factor the cluster cannot hold"
            );
            Err(IggyError::InvalidReplicationFactor)
        }
    }
}

/// Rewrite a generic patch into the resolved operation of its scope: the
/// column values extracted, the set keys canonical and explicit, each reset key
/// resolved to its catalog default in the derived block.
///
/// The defaults come from the catalog, never from node configuration, so the
/// replicated body is the same whichever primary admits it. The capacity of
/// the patched map is checked at apply, against committed state.
fn resolve_update_resource(
    request: &UpdateResourceRequest,
    tiered_storage_cluster_ready: bool,
) -> Result<(Operation, Bytes), IggyError> {
    let scope = OptionsScope::from_code(request.target.scope_code())?;
    let reset: Vec<String> = request
        .reset
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect();
    let admitted = admit(
        scope,
        OptionOperation::Update,
        &request.options,
        &reset,
        &OptionAdmissionContext::default(),
    )?;
    let set_keys: Vec<OptionKey> = admitted
        .explicit
        .keys()
        .filter_map(|key| OptionKey::try_from(key).ok())
        .collect();
    // No update path wraps a secret, so one would be replicated as sent.
    if let Some(secret) = set_keys
        .iter()
        .find(|key| key.descriptor().visibility == OptionVisibility::Secret)
    {
        return Err(IggyError::OptionNotUpdatable(secret.to_string()));
    }
    if scope == OptionsScope::Topic {
        admit_tiered_storage(&admitted.explicit, tiered_storage_cluster_ready)?;
    }
    let critical: Vec<OptionKey> = set_keys
        .into_iter()
        .chain(admitted.reset.iter().copied())
        .filter(|key| key.descriptor().critical)
        .collect();
    let mut explicit = admitted.explicit;
    if !critical.is_empty() {
        explicit.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(critical_keys_marker(&critical)?),
        );
    }
    let explicit_options = resource_options_to_wire(&explicit, OptionsProvenance::All)?;
    let mut derived = ResourceOptions::new();
    for key in &admitted.reset {
        if let Some(default) = key.descriptor().default_value()? {
            derived.insert(HeaderKey::from(*key), OptionValue::derived(default));
        }
    }
    let derived_options = resource_options_to_wire(&derived, OptionsProvenance::All)?;
    let column_name = |name: Option<String>, error: IggyError| {
        name.map(|name| WireName::new(name).map_err(|_| error))
            .transpose()
    };
    Ok(match &request.target {
        ResourceTarget::Topic {
            stream_id,
            topic_id,
        } => (
            Operation::UpdateTopicResolved,
            UpdateTopicResolvedRequest {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: column_name(admitted.columns.name, IggyError::InvalidTopicName)?,
                explicit_options,
                derived_options,
            }
            .to_bytes(),
        ),
        ResourceTarget::Stream { stream_id } => (
            Operation::UpdateStreamResolved,
            UpdateStreamResolvedRequest {
                stream_id: stream_id.clone(),
                name: column_name(admitted.columns.name, IggyError::InvalidStreamName)?,
                explicit_options,
                derived_options,
            }
            .to_bytes(),
        ),
        ResourceTarget::User { user_id } => (
            Operation::UpdateUserResolved,
            UpdateUserResolvedRequest {
                user_id: user_id.clone(),
                username: column_name(admitted.columns.username, IggyError::InvalidUsername)?,
                status: admitted.columns.status.map(|status| status.as_code()),
                explicit_options,
                derived_options,
            }
            .to_bytes(),
        ),
    })
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
        session: request.session,
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

fn log_commit_reply_outcome(outcome: CommitReply, client_id: u128, op: u64) {
    match outcome {
        CommitReply::Cached => {}
        CommitReply::SessionEnded { ended_op } => debug!(
            client_id,
            op,
            ended_op,
            "metadata op committed after its session ended, so its outcome stays uncertain"
        ),
        CommitReply::NoEntry | CommitReply::SkippedRegression { .. } => fatal(
            FatalReason::UnreconcilableLogFrontier,
            &format!(
                "cannot retain committed metadata receipt for client={client_id} op={op}: {outcome:?}"
            ),
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
/// Meanwhile the binary ingress rewrite has already minted a FRESH secret for
/// the replayed frame whose hash never reached consensus, so serving the cached
/// success would splice that orphan onto it and hand the caller a credential
/// that authenticates against nothing.
///
/// A replay that still carries the committed hash is served from the cache.
/// The HTTP write path resends the frame it minted after a transient, so its
/// caller holds the matching secret and splices it into the cached success.
///
/// `PersonalAccessTokenAlreadyExists` is the honest code: the original create
/// committed, so the name IS taken, and the remedy it implies (delete by name,
/// then recreate) is exactly right. The refusal is the cached reply with only
/// its result section replaced. It keeps the commit marker, the original op
/// and the request identity, so a client that holds the create as uncertain
/// gets a committed outcome for it.
///
/// A cached REJECTION replays untouched: it carries no secret, so serving it
/// is both safe and useful.
fn unreplayable_secret_refusal(
    request: &Message<RoutedRequestHeader>,
    cached: &Frozen<{ server_common::MESSAGE_ALIGN }>,
    users: &Users,
) -> Option<Message<GenericHeader>> {
    let request_header = request.header();
    if request_header.operation != Operation::CreatePersonalAccessToken {
        return None;
    }
    let (cached_header, cached_body) = cached
        .as_slice()
        .split_at_checked(size_of::<ReplyHeader>())?;
    if iggy_binary_protocol::result_code(cached_body) != Some(0)
        || carries_committed_token_hash(request.body(), users)
    {
        return None;
    }
    let committed = bytemuck::checked::try_from_bytes::<ReplyHeader>(cached_header).ok()?;
    warn!(
        target: "iggy.metadata.diag",
        client_id = request_header.client,
        request = request_header.request,
        "refusing replayed CreatePersonalAccessToken: the committed secret is \
         unrecoverable and a re-minted one would not match the stored hash"
    );
    let mut refusal = build_result_rejection_reply(
        request_header,
        committed.commit,
        IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code(),
    );
    let header = ReplyHeader {
        size: refusal.header().size,
        checksum: 0,
        checksum_body: 0,
        ..*committed
    };
    refusal.as_mut_slice()[..size_of::<ReplyHeader>()].copy_from_slice(bytemuck::bytes_of(&header));
    Some(refusal.into_generic())
}

/// Whether a replicated `CreatePersonalAccessToken` body carries the hash
/// stored for its token, so its sender still holds the committed secret.
fn carries_committed_token_hash(body: &[u8], users: &Users) -> bool {
    let Ok(request) = CreatePersonalAccessTokenRequest::decode_from(body) else {
        return false;
    };
    users.read(|users| {
        users
            .personal_access_tokens
            .get(&request.user_id)
            .and_then(|tokens| tokens.get(request.name.as_str()))
            .is_some_and(|token| token.token.as_bytes() == request.token_hash.as_slice())
    })
}

/// A `prepare_request` error that the client gets as a reply. Any other error
/// marks a malformed request, and its session is evicted.
const fn is_admission_refusal(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::TransientNotAccepted
            | IggyError::FeatureUnavailable
            | IggyError::PartitionsLimitReached
            | IggyError::PersonalAccessTokensLimitReached(..)
    )
}

/// Headers that `keep` selects among the ops prepared after `commit_min`,
/// oldest first. Refuses while one of those ops is missing from the pipeline:
/// it could be a create that the caller has to count.
fn pending_headers<B, P>(
    consensus: &VsrConsensus<B, P>,
    keep: impl Fn(&PrepareHeader) -> bool,
) -> Result<Vec<PrepareHeader>, IggyError>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    consensus.with_pipeline(|pipeline| {
        let committed = consensus.commit_min();
        let sequence = consensus.sequencer().current_sequence();
        if sequence < committed || sequence - committed > pipeline.prepare_queue_max() as u64 {
            return Err(IggyError::TransientNotAccepted);
        }
        let mut headers = Vec::new();
        for op in committed.saturating_add(1)..=sequence {
            let header = pipeline
                .entry_by_op(op)
                .ok_or(IggyError::TransientNotAccepted)?
                .header;
            if keep(&header) {
                headers.push(header);
            }
        }
        Ok(headers)
    })
}

/// `requested` more partitions on top of the `held` ones, committed or in
/// flight, against a nonzero `partitions_max` cap. A create of zero partitions
/// adds nothing, so it passes also on a node already past the cap, for example
/// after the cap was lowered.
fn validate_partitions_limit(
    partitions_max: u32,
    requested: u32,
    held: u64,
) -> Result<(), IggyError> {
    if requested == 0 {
        return Ok(());
    }
    if held.saturating_add(u64::from(requested)) > u64::from(partitions_max) {
        return Err(IggyError::PartitionsLimitReached);
    }
    Ok(())
}
#[cfg(test)]
mod tests;
