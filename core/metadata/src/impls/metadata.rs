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
mod tests {
    use super::*;
    use crate::impls::recovery::{ReplicaIdentity, recover};
    use crate::stm::StateHandler;
    use crate::stm::authz::GatedApply;
    use crate::stm::consumer_group::JoinConsumerGroupRequest;
    use crate::stm::lifecycle::{
        CompleteLifecycleRequest, LifecycleCompletion, apply_with_lifecycle_completion,
    };
    use crate::stm::stream::{Streams, StreamsInner};
    use crate::stm::user::{PAT_TOKEN_HASH_BYTES, Users, UsersInner};
    use consensus::{HistoryRecovery, LocalPipeline, Status};
    use iggy_binary_protocol::codes::CREATE_USER_CODE;
    use iggy_binary_protocol::primitives::metadata_changes::{
        WireMetadataChangeKind, WireMetadataChangeStatus,
    };
    use iggy_binary_protocol::primitives::permissions::{
        WireGlobalPermissions, WirePermissions, WireStreamPermissions,
    };
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{CreateTopicRequest, DeleteTopicRequest};
    use iggy_binary_protocol::requests::users::{CreateUserRequest, DeleteUserRequest};
    use iggy_binary_protocol::{
        RequestHeader, StartViewChangeHeader, StartViewHeader, WireOptions,
    };
    use iggy_common::MetadataChangeGroup;
    use iggy_common::{
        Durability, GeoMode, HeaderValue, IggyExpiry, IggyTimestamp, OptionKey, OptionStorage,
        OptionValueCodec, PartitionResizePolicy, ServerKeyContext, StreamEncryptionKey,
        StreamKeyOrigin, TopicType, TopicUpdateOptions, UserStatus, UserUpdateOptions, WrapSet,
        check_critical_options, critical_keys_marker, descriptors_for, stream_wrap_set, variadic,
    };
    use journal::prepare_journal::{DEFAULT_SLOT_COUNT, PrepareJournal};
    use journal::superblock::SuperblockContents;
    use message_bus::{BusMessage, ConnectionLostFn, JoinHandle, ReplicaForwardFn, SendError};
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Frozen;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn given_create_user_when_building_prepare_should_not_store_body_hash_as_request_identifier() {
        let body = CreateUserRequest {
            username: WireName::new("alice").expect("valid username"),
            password: "guessable-password".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        }
        .to_bytes();
        let client =
            RequestHeader::for_request(CREATE_USER_CODE, 7, 1, 3, &body).expect("request header");
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let routed = RoutedRequestHeader {
            request_checksum: client.request_checksum,
            ..RoutedRequestHeader::default()
        };
        let prepare = build_prepare_message(&consensus, &routed, Operation::CreateUser, &body);
        assert_ne!(prepare.header().request_checksum, 0);
        assert_eq!(prepare.header().request_checksum, client.request_checksum);
        assert_ne!(
            prepare.header().request_checksum,
            u128::from(iggy_common::calculate_checksum(&body))
        );
    }

    #[test]
    fn given_topic_patch_when_resolving_should_extract_name_and_resolve_reset_defaults() {
        let patch = TopicUpdateOptions {
            name: Some("renamed".to_owned()),
            message_expiry: Some(IggyExpiry::from(9_000_000u64)),
            reset: BTreeSet::from([OptionKey::MaxTopicSize]),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        let request = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::named("orders").unwrap(),
            },
            reset: patch.reset,
            options: patch.set,
        };

        let (operation, body) = resolve_update_resource(&request, false).unwrap();

        assert_eq!(operation, Operation::UpdateTopicResolved);
        let resolved = UpdateTopicResolvedRequest::decode_from(&body).unwrap();
        let mut explicit = ResourceOptions::new();
        explicit.insert(
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::explicit(IggyExpiry::from(9_000_000u64).encode().unwrap()),
        );
        let mut derived = ResourceOptions::new();
        derived.insert(
            HeaderKey::from(OptionKey::MaxTopicSize),
            OptionValue::derived(
                OptionKey::MaxTopicSize
                    .descriptor()
                    .default_value()
                    .unwrap()
                    .unwrap(),
            ),
        );
        assert_eq!(
            resolved,
            UpdateTopicResolvedRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::named("orders").unwrap(),
                name: Some(WireName::new("renamed").unwrap()),
                explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All)
                    .unwrap(),
                derived_options: resource_options_to_wire(&derived, OptionsProvenance::All)
                    .unwrap(),
            }
        );
    }

    #[test]
    fn given_default_sentinel_when_resolving_patch_should_leave_the_key_unchanged() {
        let patch = TopicUpdateOptions {
            name: Some("renamed".to_owned()),
            raw: BTreeMap::from([(
                OptionKey::MessageExpiry.to_string(),
                "server_default".to_owned(),
            )]),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        let request = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
            },
            reset: patch.reset,
            options: patch.set,
        };

        let (_, body) = resolve_update_resource(&request, false).unwrap();

        let resolved = UpdateTopicResolvedRequest::decode_from(&body).unwrap();
        assert_eq!(resolved.name, Some(WireName::new("renamed").unwrap()));
        assert!(resolved.explicit_options.is_empty());
        assert!(resolved.derived_options.is_empty());
    }

    #[test]
    fn given_create_only_or_foreign_key_when_resolving_patch_should_refuse_before_replication() {
        let topic = ResourceTarget::Topic {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
        };
        let stream = ResourceTarget::Stream {
            stream_id: WireIdentifier::numeric(1),
        };
        let raw = |key: OptionKey, value: &str| TopicUpdateOptions {
            raw: BTreeMap::from([(key.to_string(), value.to_owned())]),
            ..TopicUpdateOptions::default()
        };
        let cases = [
            (
                topic.clone(),
                raw(OptionKey::PartitionResizePolicy, "fixed"),
                IggyError::OptionNotUpdatable(OptionKey::PartitionResizePolicy.to_string()),
            ),
            (
                topic,
                raw(OptionKey::EncryptionKeySource, "stream"),
                IggyError::UnsupportedOptionKey(OptionKey::EncryptionKeySource.to_string()),
            ),
            (
                stream,
                raw(OptionKey::MessageExpiry, "1000"),
                IggyError::UnsupportedOptionKey(OptionKey::MessageExpiry.to_string()),
            ),
        ];
        for (target, options, expected) in cases {
            let patch = options.to_patch().unwrap();
            let request = UpdateResourceRequest {
                target,
                reset: patch.reset,
                options: patch.set,
            };
            assert_eq!(
                resolve_update_resource(&request, false)
                    .unwrap_err()
                    .as_code(),
                expected.as_code()
            );
        }
    }

    #[test]
    fn given_user_patch_when_resolving_should_carry_username_and_status_columns() {
        let patch = UserUpdateOptions {
            username: Some("alicia".to_owned()),
            status: Some(UserStatus::Inactive),
            ..UserUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        let request = UpdateResourceRequest {
            target: ResourceTarget::User {
                user_id: WireIdentifier::numeric(3),
            },
            reset: patch.reset,
            options: patch.set,
        };

        let (operation, body) = resolve_update_resource(&request, false).unwrap();

        assert_eq!(operation, Operation::UpdateUserResolved);
        assert_eq!(
            UpdateUserResolvedRequest::decode_from(&body).unwrap(),
            UpdateUserResolvedRequest {
                user_id: WireIdentifier::numeric(3),
                username: Some(WireName::new("alicia").unwrap()),
                status: Some(UserStatus::Inactive.as_code()),
                explicit_options: WireOptions::empty(),
                derived_options: WireOptions::empty(),
            }
        );
    }

    #[test]
    fn given_cancelled_snapshot_writer_when_replaced_should_keep_the_new_snapshot() {
        let runtime = server_common::executor::create_shard_executor().unwrap();
        runtime.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("snapshot.bin");
            let first = IggySnapshot::create(&TestMux::default(), 1, 1).unwrap();
            let mut cancelled = Box::pin(first.persist(&path));
            assert!(futures::poll!(&mut cancelled).is_pending());
            drop(cancelled);

            let replacement = IggySnapshot::create(&TestMux::default(), 2, 2).unwrap();
            replacement.persist(&path).await.unwrap();
            let (loaded, checksum) = IggySnapshot::load(&path).unwrap();
            assert_eq!(loaded.sequence_number(), 2);
            assert_eq!(
                checksum,
                checkpoint_checksum(&replacement.encode().unwrap())
            );
            assert!(!path.with_extension("bin.tmp").exists());
        });
    }

    #[compio::test]
    async fn given_cached_offer_when_installing_snapshot_should_invalidate_without_rereading() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
        let mut metadata = metadata_plane();
        metadata.coordinator = Some(SnapshotCoordinator::new(
            directory.path().to_path_buf(),
            MetadataCapture::create,
        ));
        let path = metadata.coordinator.as_ref().unwrap().snapshot_path();
        IggySnapshot::create(&TestMux::default(), 0, 1)
            .unwrap()
            .persist(&path)
            .await
            .unwrap();
        let checkpoint = metadata.checkpoint_lock.acquire().await;
        assert!(
            matches!(
                metadata.state_transfer_offer().await,
                Err(StateTransferUnavailable::SnapshotReplaced)
            ),
            "a running checkpoint must refuse an offer of the file it replaces"
        );
        drop(checkpoint);
        let first = metadata.state_transfer_offer().await.unwrap();
        std::fs::remove_file(&path).unwrap();
        let cached = metadata.state_transfer_offer().await.unwrap();
        assert!(
            Rc::ptr_eq(&first, &cached),
            "a cache hit must need no disk read"
        );

        let mut snapshot = IggySnapshot::create(&TestMux::default(), 1, 2).unwrap();
        snapshot.snapshot_mut().client_table = Some(metadata.client_table.borrow().to_snapshot());
        let incoming = snapshot.encode().unwrap();
        metadata
            .install_state_transfer(&incoming, ClientTable::new(CLIENTS_TABLE_MAX), 1, 1)
            .await
            .unwrap();
        let replaced = metadata.state_transfer_offer().await.unwrap();
        assert_eq!(replaced.snapshot_seq, 1);
        assert!(!Rc::ptr_eq(&first, &replaced));

        metadata.clear_state_transfer_offer_cache();
        let mut damaged = std::fs::read(&path).unwrap();
        damaged[0] ^= 1;
        std::fs::write(&path, damaged).unwrap();
        assert!(matches!(
            metadata.state_transfer_offer().await,
            Err(StateTransferUnavailable::SnapshotUnreadable(_))
        ));
    }

    #[compio::test]
    async fn given_a_snapshot_replacement_when_offering_should_never_serve_the_replaced_file_after_it()
     {
        const OLD_SEQ: u64 = 1;
        const NEW_SEQ: u64 = 2;
        const NEWER_SEQ: u64 = 3;
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
        let mut metadata = metadata_plane();
        metadata.coordinator = Some(SnapshotCoordinator::new(
            directory.path().to_path_buf(),
            MetadataCapture::create,
        ));
        let path = metadata.coordinator.as_ref().unwrap().snapshot_path();
        IggySnapshot::create(&TestMux::default(), OLD_SEQ, 1)
            .unwrap()
            .persist(&path)
            .await
            .unwrap();
        let transferred = |seq| {
            let mut snapshot = IggySnapshot::create(&TestMux::default(), seq, seq).unwrap();
            snapshot.snapshot_mut().client_table =
                Some(metadata.client_table.borrow().to_snapshot());
            snapshot.encode().unwrap()
        };
        let table = || ClientTable::new(CLIENTS_TABLE_MAX);

        // An offer cached before the install is refused while the install writes
        // and dropped at its rename.
        let before = metadata.state_transfer_offer().await.unwrap();
        assert_eq!(before.snapshot_seq, OLD_SEQ);
        let new_bytes = transferred(NEW_SEQ);
        let held = SNAPSHOT_IO.lock().await;
        let mut installing =
            Box::pin(metadata.install_state_transfer(&new_bytes, table(), NEW_SEQ, NEW_SEQ));
        assert!(futures::poll!(&mut installing).is_pending());
        assert!(matches!(
            metadata.state_transfer_offer().await,
            Err(StateTransferUnavailable::SnapshotReplaced)
        ));
        drop(held);
        installing.await.unwrap();
        assert_eq!(
            metadata.state_transfer_offer().await.unwrap().snapshot_seq,
            NEW_SEQ,
            "the cache must not outlive the file it was read from"
        );

        // A read that spans a rename is refused and leaves the cache empty.
        metadata.clear_state_transfer_offer_cache();
        let newer_bytes = transferred(NEWER_SEQ);
        let mut offer = Box::pin(metadata.state_transfer_offer());
        assert!(
            futures::poll!(&mut offer).is_pending(),
            "the read must suspend"
        );
        metadata
            .install_state_transfer(&newer_bytes, table(), NEWER_SEQ, NEWER_SEQ)
            .await
            .unwrap();
        assert!(matches!(
            offer.await,
            Err(StateTransferUnavailable::SnapshotReplaced)
        ));
        assert!(
            metadata.transfer_offer_cache.borrow().is_none(),
            "a refused offer must not reach the cache"
        );
        assert_eq!(
            metadata.state_transfer_offer().await.unwrap().snapshot_seq,
            NEWER_SEQ
        );
    }

    #[test]
    fn given_log_and_frontier_when_reading_changes_should_answer_only_a_coherent_image() {
        let metadata = metadata_plane();
        let latest = MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            ..MetadataChangesQuery::default()
        };
        let header = |op| PrepareHeader {
            op,
            checksum: 0x5eed,
            timestamp: op,
            ..PrepareHeader::default()
        };
        assert_eq!(
            metadata.metadata_changes(&latest).unwrap().status,
            WireMetadataChangeStatus::Behind,
            "no op 1 applied yet"
        );

        metadata
            .change_log()
            .append(
                &header(1),
                vec![AppliedChange {
                    kind: WireMetadataChangeKind::UserCreated,
                    actor_user_id: None,
                    body: Bytes::new(),
                }],
            )
            .unwrap();
        assert_eq!(
            metadata.metadata_changes(&latest),
            Err(IggyError::TransientNotAccepted),
            "a log ahead of the frontier is not a coherent image"
        );
        metadata.advance_applied_frontier(1);
        let page = metadata.metadata_changes(&latest).unwrap();
        assert_eq!(page.status, WireMetadataChangeStatus::Ok);
        assert_eq!(page.head_op, 1);
        assert_eq!(page.next.map(|next| (next.op, next.ordinal)), Some((1, 0)));

        let current = MetadataChangesQuery {
            start: MetadataChangeStart::Current(None),
            groups: vec![MetadataChangeGroup::Streams],
            ..MetadataChangesQuery::default()
        };
        assert_eq!(
            metadata.metadata_changes(&current),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            metadata.metadata_current_state(&current).unwrap().status,
            WireMetadataChangeStatus::Ok
        );
        assert_eq!(
            peer_metadata().metadata_current_state(&current),
            Err(IggyError::InvalidConfiguration),
            "only the metadata shard holds the client table"
        );
    }

    #[compio::test]
    async fn given_state_transfer_when_installing_should_replace_the_change_log_only_with_the_state_machines()
     {
        const TRANSFER_OP: u64 = 2;
        let metadata = metadata_plane();
        let local = |op, checksum| PrepareHeader {
            op,
            checksum,
            timestamp: op,
            ..PrepareHeader::default()
        };
        metadata
            .change_log()
            .append(&local(1, 0xaa), Vec::new())
            .unwrap();
        let before = metadata.change_log().snapshot();
        let transferred = |sequence_number, log_id| {
            let mut snapshot =
                IggySnapshot::create(&TestMux::default(), sequence_number, 1).unwrap();
            snapshot.snapshot_mut().client_table =
                Some(ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot());
            let log = MetadataChangeLog::new(ChangeLogCapacities::default());
            log.append(&local(1, log_id), Vec::new()).unwrap();
            log.append(
                &local(sequence_number, log_id),
                vec![AppliedChange {
                    kind: WireMetadataChangeKind::StreamCreated,
                    actor_user_id: None,
                    body: Bytes::from_static(b"body"),
                }],
            )
            .unwrap();
            snapshot.snapshot_mut().change_log = log.snapshot();
            snapshot
        };

        let incoherent = transferred(TRANSFER_OP, 0xbb).encode().unwrap();
        assert!(matches!(
            metadata
                .install_state_transfer(
                    &incoherent,
                    ClientTable::new(CLIENTS_TABLE_MAX),
                    TRANSFER_OP,
                    TRANSFER_OP - 1,
                )
                .await,
            Err(SnapshotError::IncoherentManifest { .. })
        ));
        assert_eq!(
            metadata.change_log().snapshot(),
            before,
            "a refused transfer leaves the log untouched"
        );

        let ahead = transferred(TRANSFER_OP, 0xbb);
        metadata
            .install_state_transfer(
                &ahead.encode().unwrap(),
                ClientTable::new(CLIENTS_TABLE_MAX),
                TRANSFER_OP,
                TRANSFER_OP,
            )
            .await
            .unwrap();
        assert_eq!(
            metadata.change_log().snapshot(),
            ahead.snapshot().change_log,
            "the transfer replaces the log with the state machines"
        );

        let behind = transferred(TRANSFER_OP, 0xcc);
        metadata
            .install_state_transfer(
                &behind.encode().unwrap(),
                ClientTable::new(CLIENTS_TABLE_MAX),
                TRANSFER_OP,
                TRANSFER_OP,
            )
            .await
            .unwrap();
        assert_eq!(
            metadata.change_log().snapshot(),
            ahead.snapshot().change_log,
            "a transfer at the applied frontier keeps the local state and log"
        );
    }

    /// The offer ships state at or below `commit_min`, which a recovered suffix
    /// above `commit_max` cannot change, so the barrier waiting on that suffix must
    /// not withhold it.
    #[compio::test]
    async fn given_a_primary_below_its_recovery_barrier_when_a_peer_asks_for_state_should_serve_its_checkpoint()
     {
        const SNAPSHOT_OP: u64 = 3;
        const COMMITTED: u64 = 5;
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
        let mut metadata = metadata_plane();
        metadata.coordinator = Some(SnapshotCoordinator::new(
            directory.path().to_path_buf(),
            MetadataCapture::create,
        ));
        let path = metadata.coordinator.as_ref().unwrap().snapshot_path();
        IggySnapshot::create(&TestMux::default(), SNAPSHOT_OP, 1)
            .unwrap()
            .persist(&path)
            .await
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.restore_commit_state(COMMITTED, COMMITTED);
        consensus.set_recovery_barrier(COMMITTED + 1);

        let offer = metadata
            .state_transfer_offer()
            .await
            .expect("a primary below its recovery barrier must still serve committed state");
        assert_eq!(offer.snapshot_seq, SNAPSHOT_OP);
        assert_eq!(
            offer.commit_op, COMMITTED,
            "the offer must stop at the applied commit point, below the barrier"
        );
    }

    #[compio::test]
    async fn given_commits_during_snapshot_io_when_installing_should_preserve_the_newer_frontier() {
        const SNAPSHOT_OP: u64 = 1;
        const LOCAL_OP: u64 = 2;
        const TRANSFER_OP: u64 = 3;
        const CLIENT: u128 = 9;
        const USER: u32 = 7;
        const BIND_VERIFIER: [u8; 32] = [0x5a; 32];
        for table_frontier in [SNAPSHOT_OP, TRANSFER_OP] {
            let directory = tempfile::tempdir().unwrap();
            std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
            let mut metadata = metadata_plane();
            metadata.coordinator = Some(SnapshotCoordinator::new(
                directory.path().to_path_buf(),
                MetadataCapture::create,
            ));
            let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
            table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
            let mut snapshot = IggySnapshot::create(&TestMux::default(), SNAPSHOT_OP, 1).unwrap();
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            let incoming = snapshot.encode().unwrap();
            let held = SNAPSHOT_IO.lock().await;
            let mut install = Box::pin(metadata.install_state_transfer(
                &incoming,
                table,
                table_frontier,
                table_frontier,
            ));
            assert!(futures::poll!(&mut install).is_pending());
            let consensus = metadata.consensus.as_ref().unwrap();
            consensus.advance_commit_max(LOCAL_OP);
            consensus.set_commit_floor(LOCAL_OP);
            {
                let mut local_table = metadata.client_table.borrow_mut();
                local_table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
                local_table
                    .commit_register(
                        CLIENT,
                        USER,
                        BIND_VERIFIER,
                        register_reply(CLIENT, LOCAL_OP),
                    )
                    .unwrap();
            }
            drop(held);
            let outcome = install.await.unwrap();
            assert_eq!(outcome.installed_frontier, LOCAL_OP);
            assert_eq!(consensus.commit_min(), LOCAL_OP);
            if table_frontier > LOCAL_OP {
                assert_eq!(metadata.client_table.borrow().get_epoch(CLIENT), None);
                let replay = metadata.replay_client_table.borrow();
                assert_eq!(
                    replay.as_ref().unwrap().borrow().get_epoch(CLIENT),
                    Some(LOCAL_OP),
                    "tail replay must retain the session committed during snapshot I/O"
                );
            } else {
                assert_eq!(
                    metadata.client_table.borrow().get_epoch(CLIENT),
                    Some(LOCAL_OP)
                );
                assert!(metadata.replay_client_table.borrow().is_none());
            }
        }
    }

    #[compio::test]
    async fn given_failed_snapshot_replacement_when_retrying_should_publish_valid_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.bin");
        std::fs::create_dir(&path).unwrap();
        let snapshot = IggySnapshot::create(&TestMux::default(), 1, 1).unwrap();
        assert!(matches!(
            snapshot.persist(&path).await,
            Err(SnapshotError::Persist {
                stage: PersistStage::Rename,
                ..
            })
        ));
        std::fs::remove_dir(&path).unwrap();
        snapshot.persist(&path).await.unwrap();
        let (loaded, _) = IggySnapshot::load(&path).unwrap();
        assert_eq!(loaded.sequence_number(), 1);
    }

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
        snapshot.snapshot.client_table = Some(two_session_client_table());

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

    fn two_session_client_table() -> consensus::ClientTableSnapshot {
        consensus::ClientTableSnapshot {
            capacity: 4,
            slots: vec![
                (
                    0,
                    consensus::ClientEntrySnapshot {
                        bind_verifier: [0x5a; 32],
                        ended_op: None,
                        client_id: 1,
                        epoch: 10,
                        user_id: 1,
                        token_id: 0,
                        watermark: 3,
                        reply: vec![1, 2, 3],
                    },
                ),
                (
                    2,
                    consensus::ClientEntrySnapshot {
                        bind_verifier: [0x5a; 32],
                        ended_op: None,
                        client_id: 2,
                        epoch: 20,
                        user_id: 2,
                        token_id: 9,
                        watermark: 0,
                        reply: vec![4, 5],
                    },
                ),
            ],
        }
    }

    /// One user holding one token (more tokens encode in hash order), the
    /// populated stream catalog, two sessions and change log records in both
    /// rings.
    fn populated_checkpoint_state() -> (TestMux, ClientTableSnapshot, ChangeLogSnapshot) {
        let mut users = UsersInner::new();
        let timestamp = IggyTimestamp::from(1);
        // The op of an apply is the access revision of its user and the id of
        // its token, and a checkpoint refuses either at zero.
        users.apply_context.metadata_op = 1;
        let created = StateHandler::apply(
            &CreateUserRequest {
                username: WireName::new("token-owner").unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            },
            &mut users,
            timestamp,
        );
        assert_eq!(created.code, 0);
        let token = CreatePersonalAccessTokenRequest {
            user_id: 0,
            ..token_create(TOKEN_NAME)
        };
        users.apply_context.metadata_op = 3;
        assert_eq!(StateHandler::apply(&token, &mut users, timestamp).code, 0);
        let streams = Streams::from(crate::stm::stream::tests::populated_catalog());

        let change_log = MetadataChangeLog::new(ChangeLogCapacities::default());
        let header = |op| PrepareHeader {
            op,
            checksum: 0x5eed + u128::from(op),
            timestamp: op,
            ..PrepareHeader::default()
        };
        let change = |kind, body: &'static [u8]| AppliedChange {
            kind,
            actor_user_id: Some(0),
            body: Bytes::from_static(body),
        };
        change_log
            .append(
                &header(1),
                vec![change(WireMetadataChangeKind::UserCreated, b"user")],
            )
            .unwrap();
        change_log
            .append(
                &header(2),
                vec![
                    change(WireMetadataChangeKind::StreamCreated, b"stream"),
                    change(WireMetadataChangeKind::SessionRegistered, b"session"),
                ],
            )
            .unwrap();

        (
            TestMux::new((users.into(), (streams, ()))),
            two_session_client_table(),
            change_log.snapshot(),
        )
    }

    #[compio::test]
    async fn given_empty_or_populated_state_when_persisting_a_checkpoint_should_write_the_encoded_snapshot()
     {
        // Above every op the fixtures record, which `IggySnapshot::load` validates.
        const SEQUENCE_NUMBER: u64 = 1_000;
        const CREATED_AT: u64 = 1_700_000_000_000_000;
        let empty = (
            TestMux::default(),
            ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot(),
            ChangeLogSnapshot::default(),
        );
        for (mux, client_table, change_log) in [empty, populated_checkpoint_state()] {
            let directory = tempfile::tempdir().unwrap();
            std::fs::create_dir(directory.path().join(crate::impls::METADATA_DIR)).unwrap();
            let coordinator =
                SnapshotCoordinator::new(directory.path().to_path_buf(), MetadataCapture::create);
            let mut snapshot = IggySnapshot::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
            snapshot.snapshot.client_table = Some(client_table.clone());
            snapshot.snapshot.change_log = change_log.clone();
            let expected = snapshot.encode().unwrap();
            let mut one_step_chunks =
                MetadataCapture::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
            one_step_chunks.client_table = Some(client_table.clone());
            one_step_chunks.change_log = change_log.clone();

            let capture = coordinator
                .capture(
                    &mux,
                    SEQUENCE_NUMBER,
                    CREATED_AT,
                    Some(client_table),
                    change_log,
                )
                .unwrap();
            let checksum = coordinator.persist_capture(capture).await.unwrap();

            let path = coordinator.snapshot_path();
            let file = std::fs::read(&path).unwrap();
            let (payload, trailer) = file.split_at(file.len() - SNAPSHOT_TRAILER_LEN);
            assert_eq!(
                payload, expected,
                "the streamed payload must be the encoded snapshot"
            );
            assert_eq!(checksum, checkpoint_checksum(&expected));
            assert_eq!(trailer, snapshot_trailer(checksum));
            assert_eq!(coordinator.last_checkpoint(), (SEQUENCE_NUMBER, checksum));
            let (loaded, loaded_checksum) = IggySnapshot::load(&path).unwrap();
            assert_eq!(loaded_checksum, checksum);
            assert_eq!(loaded.sequence_number(), SEQUENCE_NUMBER);
            assert!(!path.with_extension("bin.tmp").exists());

            // One byte per chunk cuts after every step. Spawned as the server runs
            // it: a yield whose wake got lost would hang here.
            let chunked_path = directory.path().join("one-step-chunks.bin");
            let chunked_checksum = compio::runtime::spawn({
                let chunked_path = chunked_path.clone();
                async move {
                    write_snapshot_file(
                        &chunked_path,
                        one_step_chunks.into_encoder(),
                        SnapshotIo::Compio,
                        1,
                    )
                    .await
                }
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(chunked_checksum, checksum);
            assert_eq!(
                std::fs::read(&chunked_path).unwrap(),
                file,
                "chunk boundaries must not change a byte"
            );
        }
    }

    /// Synchronous I/O never suspends, so every `Pending` of the write is a yield
    /// after a chunk, and each must wake its own task first or no executor polls it
    /// again.
    #[cfg(feature = "simulator")]
    #[test]
    fn given_one_byte_chunks_when_writing_a_checkpoint_should_yield_after_every_step() {
        const SEQUENCE_NUMBER: u64 = 1_000;
        const CREATED_AT: u64 = 1_700_000_000_000_000;

        struct CountingWaker(std::sync::atomic::AtomicUsize);

        impl std::task::Wake for CountingWaker {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let (mux, client_table, change_log) = populated_checkpoint_state();
        let mut snapshot = IggySnapshot::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
        snapshot.snapshot.client_table = Some(client_table.clone());
        snapshot.snapshot.change_log = change_log.clone();
        let expected = snapshot.encode().unwrap();
        let capture = || {
            let mut capture = MetadataCapture::create(&mux, SEQUENCE_NUMBER, CREATED_AT).unwrap();
            capture.client_table = Some(client_table.clone());
            capture.change_log = change_log.clone();
            capture
        };
        // A step that writes a byte ends its chunk.
        let mut steps = 0;
        let mut encoder = capture().into_encoder();
        let mut written = Vec::new();
        loop {
            let before = written.len();
            let more = encoder.encode_next(&mut written).unwrap();
            steps += usize::from(written.len() > before);
            if !more {
                break;
            }
        }
        let streams = snapshot.snapshot.streams.as_ref().unwrap().items.len();
        assert!(
            steps > streams,
            "the fixture must hold streams of several steps"
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(crate::impls::SNAPSHOT_FILE_NAME);

        let wake_count = Arc::new(CountingWaker(std::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(Arc::clone(&wake_count));
        let mut context = std::task::Context::from_waker(&waker);
        let mut write = std::pin::pin!(write_snapshot_file(
            &path,
            capture().into_encoder(),
            SnapshotIo::Synchronous,
            1
        ));
        let mut pending_polls = 0;
        let checksum = loop {
            match write.as_mut().poll(&mut context) {
                Poll::Ready(result) => break result.unwrap(),
                Poll::Pending => pending_polls += 1,
            }
        };

        assert!(
            pending_polls >= steps,
            "{pending_polls} yields for {steps} steps in chunks of their own"
        );
        assert_eq!(
            wake_count.0.load(std::sync::atomic::Ordering::Relaxed),
            pending_polls
        );
        let file = std::fs::read(&path).unwrap();
        assert_eq!(file[..file.len() - SNAPSHOT_TRAILER_LEN], expected);
        assert_eq!(checksum, checkpoint_checksum(&expected));
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
            capacity: 4,
            slots: vec![(
                0,
                consensus::ClientEntrySnapshot {
                    bind_verifier: [0x5a; 32],
                    ended_op: None,
                    client_id: 1,
                    epoch: 1,
                    user_id: 1,
                    token_id: 0,
                    watermark: 0,
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

    #[derive(Default)]
    struct CheckpointSuperblock {
        fail: Cell<bool>,
        writes: Cell<usize>,
    }

    #[allow(clippy::future_not_send)]
    impl SuperblockStore for CheckpointSuperblock {
        async fn write(&self, _payload: &[u8]) -> io::Result<()> {
            self.writes.set(self.writes.get() + 1);
            if self.fail.get() {
                Err(io::Error::other("injected checkpoint superblock failure"))
            } else {
                Ok(())
            }
        }

        async fn read_latest(&self) -> io::Result<SuperblockContents> {
            Ok(SuperblockContents::Empty)
        }
    }

    /// Build a peer-shard-style `IggyMetadata` with `consensus`,
    /// `journal`, and `snapshot` all `None`. Enough to test the
    /// commit-notifier slot without standing up VSR / WAL infrastructure:
    /// the test picks `()` for `C` / `J` / `S` since no notifier code path
    /// touches their methods.
    fn peer_metadata() -> IggyMetadata<(), (), (), TestMux> {
        IggyMetadata::new(None, None, None, None, TestMux::default(), None)
    }

    #[test]
    fn take_prepare_gap_drops_drains_the_count() {
        let md = peer_metadata();
        assert_eq!(md.take_prepare_gap_drops(), 0);

        md.prepare_gap_drops.set(2);
        assert_eq!(md.take_prepare_gap_drops(), 2);
        assert_eq!(
            md.take_prepare_gap_drops(),
            0,
            "a second drain must not re-report drops the metrics already counted"
        );
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

    fn register_reply(client: u128, session: u64) -> Message<ReplyHeader> {
        build_reply_message(
            &PrepareHeader {
                client,
                op: session,
                operation: Operation::Register,
                ..Default::default()
            },
            &bytes::Bytes::new(),
        )
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

    #[compio::test]
    async fn given_a_token_expired_at_the_prepare_timestamp_when_registering_should_return_unauthenticated()
     {
        let directory = tempfile::tempdir().unwrap();
        let mut metadata = metadata_plane();
        metadata.journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.mux_stm = Rc::new(TestMux::new((session_users(), (Streams::default(), ()))));
        let consensus = metadata.consensus.as_ref().unwrap();
        let now = consensus.clock_realtime_micros();
        let lifetime = 60_000_000;
        let token = CreatePersonalAccessTokenRequest {
            expiry: lifetime,
            ..token_create(TOKEN_NAME)
        };
        let create = build_prepare_message(
            consensus,
            &RoutedRequestHeader::default(),
            Operation::CreatePersonalAccessToken,
            &token.to_bytes(),
        );
        let id = create.header().op;
        metadata.mux_stm.update(create).unwrap();
        consensus.observe_prepare_timestamp(now + lifetime * 2);
        let registration = RegisterRequest {
            credential: CredentialProof::token(1, id),
            bind_verifier: [0x5a; 32],
        };
        assert!(metadata.accepts_registration_proof(TOKEN_OWNER, &registration.credential, now));
        assert_eq!(
            metadata
                .submit_register_in_process(77, TOKEN_OWNER, registration)
                .await,
            Err(MetadataSubmitError::Unauthenticated)
        );
        assert!(!metadata.client_table.borrow().contains(77));
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
                TestMux::new((session_users(), (Streams::default(), ()))),
                None,
            );
        md.client_table
            .borrow_mut()
            .commit_register(CLIENT, OWNER, [0x5a; 32], register_reply(CLIENT, 1))
            .unwrap();

        assert_eq!(
            md.submit_register_in_process(
                CLIENT,
                IMPOSTOR,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32]
                }
            )
            .await,
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

        assert_eq!(
            md.submit_register_in_process(
                CLIENT,
                OWNER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32]
                }
            )
            .await,
            Ok(BoundSession {
                epoch: 1,
                watermark: 0
            })
        );
    }

    #[compio::test]
    async fn given_reserved_client_id_when_registering_in_process_should_reject_before_queueing() {
        const REGISTRATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
        let metadata = metadata_plane();
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let consensus = metadata.consensus.as_ref().unwrap();
        for commit_max in [0, 1] {
            consensus.advance_commit_max(commit_max);
            for client in [
                message_bus::server_internal_client_id(server_common::sharding::METADATA_GROUP),
                message_bus::AUTO_COMMIT_CLIENT_ID,
            ] {
                let result = compio::time::timeout(
                    REGISTRATION_TIMEOUT,
                    metadata.submit_register_in_process(
                        client,
                        0,
                        RegisterRequest {
                            credential: CredentialProof::account(1, 1),
                            bind_verifier: [0x5a; 32],
                        },
                    ),
                )
                .await
                .expect("reserved IDs must be rejected without waiting for consensus");
                let error = result.expect_err("reserved IDs cannot acquire a session");
                assert_eq!(error, MetadataSubmitError::Unauthenticated);
                assert!(!error.is_transient());
                assert_eq!(metadata.client_table.borrow().get_epoch(client), None);
                assert!(!consensus.pipeline_has_message_from_client(client));
            }
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
        table
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();
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
            _data: impl Into<BusMessage>,
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

    #[compio::test]
    async fn given_committed_lifecycle_intent_when_waiting_for_completion_should_not_prove_final_result()
     {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ROOT_USER: u32 = 0;
        let directory = tempfile::tempdir().unwrap();
        let mut metadata = metadata_plane();
        metadata.journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ROOT_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let request_header = RoutedRequestHeader {
            client: CLIENT,
            session: SESSION,
            request: 1,
            user_id: ROOT_USER,
            group: server_common::sharding::METADATA_GROUP,
            ..Default::default()
        };
        let body = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        }
        .to_bytes();
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(consensus, &request_header, Operation::DeleteTopic, &body),
            consensus.retry_capacity(&metadata.client_table.borrow()),
        );
        let intent_op = prepare.header().op;
        let pending = metadata
            .dispatch_prepare_and_await(consensus, prepare)
            .await
            .unwrap();
        assert!(metadata.mux_stm.streams().lifecycle_pending(intent_op));
        assert_eq!(
            iggy_binary_protocol::result_code(pending.body()),
            Some(IggyError::TransientNotCommitted.as_code())
        );
        assert!(
            !pending.header().has_committed_result(),
            "a committed intent cannot prove its final outcome before every partition finishes"
        );

        metadata
            .submit_complete_lifecycle_in_process(
                CompleteLifecycleRequest::new(
                    intent_op,
                    0,
                    vec![LifecycleCompletion {
                        topic_id: 0,
                        partition_id: 0,
                        partition_op: 1,
                    }],
                )
                .unwrap(),
                0,
            )
            .await
            .unwrap();
        assert!(!metadata.mux_stm.streams().lifecycle_pending(intent_op));
        let final_reply = metadata
            .client_table
            .borrow()
            .get_reply(CLIENT)
            .unwrap()
            .clone()
            .into_message();
        assert!(final_reply.header().has_committed_result());
        assert_eq!(
            iggy_binary_protocol::result_code(final_reply.body()),
            Some(0)
        );
    }

    fn create_topic_request(client: u128, wire_user_id: u32) -> Message<RoutedRequestHeader> {
        create_named_topic_request(client, wire_user_id, WireIdentifier::numeric(0), "t")
    }

    fn create_named_topic_request(
        client: u128,
        wire_user_id: u32,
        stream_id: WireIdentifier,
        name: &str,
    ) -> Message<RoutedRequestHeader> {
        create_topic_request_with_count(client, wire_user_id, stream_id, name, 1)
    }

    fn create_topic_request_with_count(
        client: u128,
        wire_user_id: u32,
        stream_id: WireIdentifier,
        name: &str,
        partitions_count: u32,
    ) -> Message<RoutedRequestHeader> {
        create_topic_request_with_options(
            client,
            wire_user_id,
            stream_id,
            name,
            partitions_count,
            WireOptions::empty(),
        )
    }

    fn create_topic_request_with_options(
        client: u128,
        wire_user_id: u32,
        stream_id: WireIdentifier,
        name: &str,
        partitions_count: u32,
        options: WireOptions,
    ) -> Message<RoutedRequestHeader> {
        let body = CreateTopicRequest {
            stream_id,
            partitions_count,
            name: WireName::new(name).unwrap(),
            options,
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

    fn update_resource_request(client: u128, body: &[u8]) -> Message<RoutedRequestHeader> {
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        let slice = message.as_mut_slice();
        slice[header_size..total].copy_from_slice(body);
        *bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]) =
            RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::UpdateResource,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request: 1,
                group: server_common::sharding::METADATA_GROUP,
                ..Default::default()
            };
        message
    }

    fn placement_metadata() -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
        placement_metadata_with_bus(NoopBus)
    }

    fn placement_metadata_with_bus<B: MessageBus>(
        bus: B,
    ) -> IggyMetadata<VsrConsensus<B>, PrepareJournal, (), TestMux> {
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            server_common::sharding::METADATA_GROUP,
            bus,
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata =
            IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None);
        for client in 1..=3 {
            metadata
                .client_table
                .borrow_mut()
                .commit_register(client, 0, [0x5a; 32], register_reply(client, 1))
                .unwrap();
        }
        metadata
    }

    fn placement_request(client: u128, name: &str, count: u32) -> Message<RoutedRequestHeader> {
        create_topic_request_with_count(client, 0, WireIdentifier::numeric(0), name, count)
    }

    fn assigned_views(prepare: &Message<PrepareHeader>) -> Vec<u32> {
        PersistedCreateTopicRequest::decode_from(prepare.body())
            .unwrap()
            .partitions
            .into_iter()
            .map(|partition| partition.initial_view)
            .collect()
    }

    #[test]
    fn given_a_configured_default_factor_when_admitting_a_create_without_one_should_place_that_many_members()
     {
        for (configured, expected) in [(None, 3), (Some(2), 2), (Some(5), 3)] {
            let metadata = placement_metadata();
            metadata.set_default_replication_factor(configured);
            assert_eq!(metadata.default_replication_factor(3), expected);
            let prepare = metadata
                .prepare_request(placement_request(1, "topic", 2))
                .unwrap();
            let request = PersistedCreateTopicRequest::decode_from(prepare.body()).unwrap();
            assert!(
                request
                    .partitions
                    .iter()
                    .all(|partition| partition.members.len() == usize::from(expected)),
                "configured {configured:?}"
            );
        }
    }

    #[test]
    fn given_an_offline_replica_when_admitting_creates_should_spread_over_responsive_replicas() {
        let metadata = placement_metadata();
        metadata.observe_replica_readiness(&[false, true, true]);
        let first = metadata
            .prepare_request(placement_request(1, "first", 4))
            .unwrap();
        assert_eq!(assigned_views(&first), [1, 2, 1, 2]);
        metadata
            .consensus
            .as_ref()
            .unwrap()
            .pipeline_message(PlaneKind::Metadata, &first);
        let second = metadata
            .prepare_request(placement_request(2, "second", 2))
            .unwrap();
        assert_eq!(assigned_views(&second), [1, 2]);
        metadata.observe_replica_readiness(&[true; 3]);
        let returned = metadata
            .prepare_request(placement_request(3, "returned", 2))
            .unwrap();
        assert_eq!(assigned_views(&returned), [0, 0]);
    }

    #[test]
    fn given_pending_placement_when_admitting_another_batch_should_spread_before_wal_append() {
        let metadata = placement_metadata();
        let consensus = metadata.consensus.as_ref().unwrap();
        let first = metadata
            .prepare_request(placement_request(1, "first", 4))
            .unwrap();
        assert_eq!(assigned_views(&first), [0, 1, 2, 0]);
        consensus.pipeline_message(PlaneKind::Metadata, &first);
        let second = metadata
            .prepare_request(placement_request(2, "second", 2))
            .unwrap();
        assert_eq!(
            assigned_views(&second),
            [1, 2],
            "admitted assignments reserve load before append can yield"
        );
        assert!(metadata.journal.is_none());
    }

    #[test]
    fn given_a_pending_create_topic_when_admitting_create_partitions_should_refuse_transiently() {
        let add_partitions = || {
            let body = WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::named("pending").unwrap(),
                partitions_count: 1,
            }
            .to_bytes();
            pat_request(2, 1, Operation::CreatePartitions, &body)
        };
        let metadata = placement_metadata();
        assert!(
            metadata.prepare_request(add_partitions()).is_ok(),
            "a topic nobody is creating is apply's TopicNotFound"
        );

        let metadata = placement_metadata();
        let create = metadata
            .prepare_request(placement_request(1, "pending", 2))
            .unwrap();
        metadata
            .consensus
            .as_ref()
            .unwrap()
            .pipeline_message(PlaneKind::Metadata, &create);
        assert!(matches!(
            metadata.prepare_request(add_partitions()),
            Err(IggyError::TransientNotAccepted)
        ));
    }

    #[test]
    fn given_rolled_back_placement_when_admitting_another_batch_should_release_reservations() {
        let metadata = placement_metadata();
        let consensus = metadata.consensus.as_ref().unwrap();
        let first = metadata
            .prepare_request(placement_request(1, "first", 2))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &first);
        assert_eq!(
            consensus.rollback_pipelined_prepare(first.header()),
            PrepareRollback::Unwound
        );
        let second = metadata
            .prepare_request(placement_request(2, "second", 1))
            .unwrap();
        assert_eq!(assigned_views(&second), [0]);
    }

    #[test]
    fn given_rejected_create_when_committed_should_release_its_placement_reservations() {
        let metadata = placement_metadata();
        let consensus = metadata.consensus.as_ref().unwrap();
        let first = metadata
            .prepare_request(placement_request(1, "missing-stream", 2))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &first);
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.pop().unwrap();
        });
        let op = first.header().op;
        let reply = metadata.apply_prepare(first);
        assert_ne!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        consensus.advance_commit_max(op);
        consensus.advance_commit_min(op);
        let second = metadata
            .prepare_request(placement_request(2, "next", 1))
            .unwrap();
        assert_eq!(
            assigned_views(&second),
            [0],
            "a committed rejection leaves no partition load"
        );
    }

    #[test]
    fn given_committed_create_when_admitting_another_should_count_it_once() {
        let mut metadata = placement_metadata();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        let consensus = metadata.consensus.as_ref().unwrap();
        let first = metadata
            .prepare_request(placement_request(1, "new-topic", 2))
            .unwrap();
        assert_eq!(assigned_views(&first), [1, 2]);
        consensus.pipeline_message(PlaneKind::Metadata, &first);
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.pop().unwrap();
        });
        let op = first.header().op;
        let reply = metadata.apply_prepare(first);
        assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        consensus.advance_commit_max(op);
        consensus.advance_commit_min(op);
        let second = metadata
            .prepare_request(placement_request(2, "next", 1))
            .unwrap();
        assert_eq!(assigned_views(&second), [0]);
    }

    #[test]
    fn given_a_shadow_adoption_when_assigning_its_partitions_should_mint_each_within_the_cap() {
        let mut metadata = placement_metadata();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        let adoption = |partitions_count| PersistedCreateTopicRequest {
            request: WireCreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count,
                name: WireName::new("payments").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        };

        let mut create = adoption(2);
        metadata.assign_shadow_partitions(&mut create).unwrap();
        let ids: Vec<u32> = create
            .partitions
            .iter()
            .map(|partition| partition.partition_id)
            .collect();
        assert_eq!(ids, [0, 1]);
        assert_ne!(
            create.partitions[0].consensus_group_id,
            create.partitions[1].consensus_group_id
        );
        assert!(
            PersistedCreateTopicRequest::decode_from(&create.to_bytes()).is_ok(),
            "the assigned create encodes as a topic create does"
        );

        metadata.set_partitions_max(2);
        assert!(matches!(
            metadata.assign_shadow_partitions(&mut adoption(2)),
            Err(IggyError::PartitionsLimitReached)
        ));
    }

    #[test]
    fn given_a_restored_geo_topic_when_assigning_its_partitions_should_store_persisted_as_derived()
    {
        let mut metadata = placement_metadata();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        let create = |options: &TopicCreateOptions| PersistedCreateTopicRequest {
            request: WireCreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("payments").unwrap(),
                options: options.to_wire().unwrap(),
            },
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        };
        let durability = HeaderKey::from(OptionKey::Durability);
        let replicated = OptionValue::explicit(Durability::Replicated.encode().unwrap());

        let mut restored = create(&TopicCreateOptions {
            topic_type: Some(TopicType::Geo),
            geo_mode: Some(GeoMode::Geo),
            ..TopicCreateOptions::default()
        });
        metadata.assign_shadow_partitions(&mut restored).unwrap();
        let explicit = resource_options_from_wire(&restored.request.options, true).unwrap();
        let derived = resource_options_from_wire(&restored.derived_options, false).unwrap();
        assert!(!explicit.contains_key(&durability));
        assert_eq!(
            derived.get(&durability),
            Some(&OptionValue::derived(
                Durability::Persisted.encode().unwrap()
            ))
        );

        // A shadow adoption is stored as a normal topic and keeps its block.
        let mut adopted = create(&TopicCreateOptions::default());
        metadata.assign_shadow_partitions(&mut adopted).unwrap();
        let explicit = resource_options_from_wire(&adopted.request.options, true).unwrap();
        assert_eq!(explicit.get(&durability), Some(&replicated));
    }

    #[test]
    fn given_unrelated_metadata_inflight_when_over_cap_create_is_invalid_should_defer_it() {
        let mut metadata = placement_metadata();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        metadata.set_partitions_max(1);
        assert!(matches!(
            metadata.admit_partitions(&placement_request(1, "over-cap", 1)),
            Err(IggyError::PartitionsLimitReached)
        ));
        let mut invalid = placement_request(1, "over-cap", 1);
        let mut request = WireCreateTopicRequest::decode_from(invalid.body()).unwrap();
        request.stream_id = WireIdentifier::numeric(99);
        let body = request.to_bytes();
        invalid.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
        assert!(metadata.admit_partitions(&invalid).is_ok());
        let consensus = metadata.consensus.as_ref().unwrap();
        let body = CreateStreamRequest {
            name: WireName::new("unrelated").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let mut pending = Message::<PrepareHeader>::new(size_of::<PrepareHeader>() + body.len());
        pending.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
        let pending = pending.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::CreateStream;
            header.op = 1;
            header.group = server_common::sharding::METADATA_GROUP;
            header.size = u32::try_from(size_of::<PrepareHeader>() + body.len()).unwrap();
        });
        consensus.pipeline_message(PlaneKind::Metadata, &pending);
        consensus.sequencer().set_sequence(pending.header().op);
        assert!(matches!(
            metadata.admit_partitions(&placement_request(1, "over-cap", 1)),
            Err(IggyError::PartitionsLimitReached)
        ));
        assert!(
            matches!(
                metadata.admit_partitions(&invalid),
                Err(IggyError::TransientNotAccepted)
            ),
            "an unrelated prepared mutation currently defers the missing-stream result"
        );
    }

    #[test]
    fn given_missing_pipeline_entry_when_creating_should_refuse_without_allocating() {
        let metadata = placement_metadata();
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.sequencer().set_sequence(1);
        assert!(matches!(
            metadata.prepare_request(placement_request(1, "missing", 2)),
            Err(IggyError::TransientNotAccepted)
        ));
        consensus.sequencer().set_sequence(0);
        let first = metadata
            .prepare_request(placement_request(1, "ready", 1))
            .unwrap();
        let assignments = PersistedCreateTopicRequest::decode_from(first.body())
            .unwrap()
            .partitions;
        assert_eq!(
            assignments[0].consensus_group_id, 1,
            "refusal cannot consume group ids"
        );
        assert_eq!(assignments[0].initial_view, 0);
    }

    #[test]
    fn given_unknown_session_when_creating_should_not_allocate_groups() {
        let metadata = placement_metadata();
        assert!(matches!(
            metadata.prepare_request(placement_request(99, "unauthorized", 2)),
            Err(IggyError::Unauthenticated)
        ));
        let first = metadata
            .prepare_request(placement_request(1, "authorized", 1))
            .unwrap();
        assert_eq!(
            PersistedCreateTopicRequest::decode_from(first.body())
                .unwrap()
                .partitions[0]
                .consensus_group_id,
            1
        );
    }

    #[test]
    fn given_partition_observations_when_creating_should_keep_highest_matching_incarnation() {
        let mut metadata = placement_metadata();
        let streams = stream_with_one_partition_in_view(2);
        let revision = streams
            .items
            .get(0)
            .unwrap()
            .topics
            .get(0)
            .unwrap()
            .partitions[0]
            .created_revision;
        let mut recreated = streams.clone();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (streams.into(), ()),
        )));
        let namespace = IggyNamespace::new(0, 0, 0);
        metadata.observe_partition_views(&[
            PartitionPlacementView {
                namespace,
                created_revision: revision,
                view: 6,
            },
            PartitionPlacementView {
                namespace,
                created_revision: revision,
                view: 4,
            },
            PartitionPlacementView {
                namespace,
                created_revision: revision - 1,
                view: 100,
            },
            PartitionPlacementView {
                namespace,
                created_revision: revision + 1,
                view: 101,
            },
        ]);
        let first = metadata
            .prepare_request(placement_request(1, "observed", 1))
            .unwrap();
        assert_eq!(
            assigned_views(&first),
            [1],
            "view 6 keeps load on replica 0 despite older reports"
        );
        metadata.observe_partition_views(&[]);
        let unchanged = metadata
            .prepare_request(placement_request(2, "missing-report", 1))
            .unwrap();
        assert_eq!(assigned_views(&unchanged), [1]);

        assert_eq!(
            apply_with_lifecycle_completion(
                &DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                },
                &mut recreated,
                IggyTimestamp::now(),
            )
            .code,
            0
        );
        create_topic_in_view(&mut recreated, 2);
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (recreated.into(), ()),
        )));
        metadata.observe_partition_views(&[PartitionPlacementView {
            namespace,
            created_revision: revision,
            view: 102,
        }]);
        let next = metadata
            .prepare_request(placement_request(3, "recreated", 1))
            .unwrap();
        assert_eq!(
            assigned_views(&next),
            [0],
            "recreation falls back to its own committed view 2"
        );
        metadata.mux_stm = Rc::new(TestMux::default());
        let empty = metadata
            .prepare_request(placement_request(3, "deleted", 1))
            .unwrap();
        assert_eq!(assigned_views(&empty), [0]);
    }

    #[compio::test]
    async fn given_inherited_create_when_summary_missing_should_refuse_then_restore_from_wal() {
        let directory = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let old_primary = placement_metadata();
        let inherited = old_primary
            .prepare_request(placement_request(1, "inherited", 2))
            .unwrap();
        journal.handle().append(inherited.clone()).await.unwrap();
        let mut metadata = placement_metadata();
        metadata.journal = Some(journal);
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &inherited);
        assert!(matches!(
            metadata.prepare_request(placement_request(2, "waiting", 1)),
            Err(IggyError::TransientNotAccepted)
        ));
        let reply = metadata
            .submit_request_in_process(placement_request(2, "waiting", 1))
            .await
            .unwrap()
            .try_into_typed::<ReplyHeader>()
            .unwrap();
        assert_eq!(
            iggy_binary_protocol::result_code(reply.body()),
            Some(IggyError::TransientNotAccepted.as_code())
        );
        assert!(!reply.header().has_committed_result());
        metadata.refresh_placement().await;
        let next = metadata
            .prepare_request(placement_request(2, "ready", 1))
            .unwrap();
        let assignments = PersistedCreateTopicRequest::decode_from(next.body())
            .unwrap()
            .partitions;
        assert_eq!(assignments[0].initial_view, 2);
        assert_eq!(
            assignments[0].consensus_group_id, 3,
            "inherited ids must seed the allocator before admission"
        );
    }

    #[compio::test]
    async fn given_missing_placement_summary_when_wire_or_queue_submits_should_reply_58_without_eviction()
     {
        let old_primary = placement_metadata();
        let inherited = old_primary
            .prepare_request(placement_request(1, "inherited", 2))
            .unwrap();
        let metadata = placement_metadata_with_bus(ClientSendSpyBus::default());
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &inherited);
        metadata.on_request(placement_request(2, "wire", 1)).await;
        {
            let replies = consensus.message_bus().client_replies.borrow();
            assert_eq!(replies.len(), 1);
            let frame = replies[0].first();
            let header =
                bytemuck::checked::from_bytes::<ReplyHeader>(&frame[..size_of::<ReplyHeader>()]);
            assert_eq!(header.command, Command::Reply);
            assert!(!header.has_committed_result());
            assert_eq!(
                iggy_binary_protocol::result_code(&frame[size_of::<ReplyHeader>()..]),
                Some(IggyError::TransientNotAccepted.as_code())
            );
        }
        let (queued, receiver) =
            consensus::RequestEntry::with_subscriber(placement_request(3, "queued", 1));
        assert!(consensus.push_queued_request(queued).is_ok());
        metadata.drain_request_queue_into_prepares().await;
        let reply = receiver.await.unwrap();
        assert_eq!(
            iggy_binary_protocol::result_code(reply.body()),
            Some(IggyError::TransientNotAccepted.as_code())
        );
        assert!(!reply.header().has_committed_result());
        assert_eq!(metadata.allocator.highest(), 0);
        assert_eq!(
            resolve_acting_user_id(Operation::CreateTopic, 2, &metadata.client_table).unwrap(),
            Some(0)
        );
        assert_eq!(
            resolve_acting_user_id(Operation::CreateTopic, 3, &metadata.client_table).unwrap(),
            Some(0)
        );
    }

    #[compio::test]
    async fn given_wrong_canonical_wal_body_when_refreshing_placement_should_keep_admission_closed()
    {
        let directory = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let source = placement_metadata();
        let expected = source
            .prepare_request(placement_request(1, "expected", 2))
            .unwrap();
        let different = source
            .prepare_request(placement_request(2, "different", 1))
            .unwrap();
        assert_eq!(expected.header().op, different.header().op);
        assert_ne!(expected.header().checksum, different.header().checksum);
        journal.handle().append(different).await.unwrap();
        let mut metadata = placement_metadata();
        metadata.journal = Some(journal);
        metadata
            .consensus
            .as_ref()
            .unwrap()
            .pipeline_message(PlaneKind::Metadata, &expected);
        metadata.refresh_placement().await;
        assert!(matches!(
            metadata.prepare_request(placement_request(3, "blocked", 1)),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(
            metadata.allocator.highest(),
            0,
            "an op-only WAL match must not supply assignment ids"
        );
    }

    #[compio::test]
    async fn given_replaced_pipeline_create_when_refreshing_should_discard_old_summary() {
        let directory = tempfile::tempdir().unwrap();
        let mut metadata = placement_metadata();
        metadata.journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        let consensus = metadata.consensus.as_ref().unwrap();
        let old = metadata
            .prepare_request(placement_request(1, "old", 2))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &old);
        assert_eq!(
            consensus.rollback_pipelined_prepare(old.header()),
            PrepareRollback::Unwound
        );
        let source = placement_metadata();
        let replacement = source
            .prepare_request(placement_request(2, "replacement", 1))
            .unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &replacement);
        metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .append(replacement)
            .await
            .unwrap();
        assert!(matches!(
            metadata.prepare_request(placement_request(3, "blocked", 1)),
            Err(IggyError::TransientNotAccepted)
        ));
        metadata.refresh_placement().await;
        let next = metadata
            .prepare_request(placement_request(3, "ready", 1))
            .unwrap();
        assert_eq!(
            assigned_views(&next),
            [1],
            "only the replacement's one assignment remains reserved"
        );
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
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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

    fn prepare_topic_with(
        options: &iggy_common::TopicCreateOptions,
    ) -> Result<PersistedCreateTopicRequest, IggyError> {
        prepare_topic_on(&metadata_plane(), options)
    }

    fn prepare_topic_on(
        plane: &IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
        options: &iggy_common::TopicCreateOptions,
    ) -> Result<PersistedCreateTopicRequest, IggyError> {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .expect("register commits");
        let prepare = plane.prepare_request(create_topic_request_with_options(
            CLIENT,
            ACTING_USER,
            WireIdentifier::numeric(0),
            "t",
            1,
            options.to_wire().expect("options encode"),
        ))?;
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
        Ok(PersistedCreateTopicRequest::decode_from(body)
            .expect("create topic with assignments prepare must decode"))
    }

    const HOUR_MICROS: u64 = 3_600_000_000;

    #[test]
    fn given_an_explicit_critical_key_when_preparing_create_topic_should_record_the_marker() {
        let persisted = prepare_topic_with(&iggy_common::TopicCreateOptions {
            segment_max_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
            ..Default::default()
        })
        .expect("an hour-long age roll is admitted");

        let explicit = resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes");
        assert_eq!(
            explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
                .value
                .as_bytes(),
            b"segment_max_age"
        );
        let stored = TopicResolvedOptions::from(&explicit);
        assert_eq!(
            stored.runtime.effective_segment_max_age(),
            Some(HOUR_MICROS)
        );

        let unmarked = prepare_topic_with(&iggy_common::TopicCreateOptions::default())
            .expect("a keyless topic is admitted");
        assert!(
            !resource_options_from_wire(&unmarked.request.options, true)
                .expect("explicit block decodes")
                .contains_key(&HeaderKey::from(OptionKey::CriticalKeys)),
            "a topic setting no critical key carries no marker"
        );
    }

    #[test]
    fn given_an_age_roll_with_preallocated_segments_when_preparing_should_refuse_it() {
        let refused = prepare_topic_with(&iggy_common::TopicCreateOptions {
            segment_max_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
            preallocate_segments: Some(true),
            ..Default::default()
        });
        assert_eq!(
            refused.err(),
            Some(IggyError::InvalidOptionValue("segment_max_age".to_owned()))
        );

        assert!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                segment_max_age: Some(iggy_common::IggyExpiry::NeverExpire),
                preallocate_segments: Some(true),
                ..Default::default()
            })
            .is_ok(),
            "preallocation with segments rolled by size alone stays admitted"
        );
    }

    #[test]
    fn given_an_age_roll_below_ten_minutes_when_preparing_should_refuse_it() {
        let nine_minutes = iggy_common::MIN_SEGMENT_MAX_AGE - 60_000_000;
        assert_eq!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                segment_max_age: Some(iggy_common::IggyExpiry::from(nine_minutes)),
                ..Default::default()
            })
            .err(),
            Some(IggyError::InvalidOptionValue("segment_max_age".to_owned()))
        );
        assert!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                segment_max_age: Some(iggy_common::IggyExpiry::from(
                    iggy_common::MIN_SEGMENT_MAX_AGE
                )),
                ..Default::default()
            })
            .is_ok()
        );
    }

    #[test]
    fn given_tiered_storage_when_preparing_create_topic_should_admit_on_only_in_a_ready_cluster() {
        let tiered = |enabled: Option<bool>| iggy_common::TopicCreateOptions {
            tiered_storage: enabled,
            ..Default::default()
        };
        for cluster_ready in [false, true] {
            let prepare = |enabled: Option<bool>| {
                let plane = metadata_plane();
                plane.observe_tiered_storage_readiness(cluster_ready);
                prepare_topic_on(&plane, &tiered(enabled))
            };
            match prepare(Some(true)) {
                Ok(persisted) => {
                    assert!(cluster_ready, "an unready cluster admitted a tiered topic");
                    let explicit = resource_options_from_wire(&persisted.request.options, true)
                        .expect("explicit block decodes");
                    assert!(TopicResolvedOptions::from(&explicit).tiered_storage);
                }
                Err(error) => {
                    assert!(!cluster_ready, "a ready cluster refused a tiered topic");
                    assert_eq!(error, IggyError::FeatureUnavailable);
                }
            }
            assert!(prepare(Some(false)).is_ok(), "ready {cluster_ready}");
            assert!(prepare(None).is_ok(), "ready {cluster_ready}");
        }
        assert_eq!(
            prepare_topic_with(&tiered(Some(true))).err(),
            Some(IggyError::FeatureUnavailable),
            "a primary that was told nothing refuses"
        );

        let persisted = prepare_topic_with(&tiered(Some(false)))
            .expect("a topic that states it is not tiered is admitted");
        let explicit = resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes");
        assert_eq!(
            explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
                .value
                .as_bytes(),
            b"tiered_storage"
        );
        assert!(!TopicResolvedOptions::from(&explicit).tiered_storage);
    }

    #[test]
    fn given_a_topic_type_when_preparing_create_topic_should_admit_geo_only_in_a_ready_cluster() {
        let typed = |topic_type: Option<TopicType>, geo_mode: Option<GeoMode>| {
            iggy_common::TopicCreateOptions {
                topic_type,
                geo_mode,
                ..Default::default()
            }
        };
        for cluster_ready in [false, true] {
            let plane = metadata_plane();
            plane.observe_tiered_storage_readiness(cluster_ready);
            for geo_mode in [None, Some(GeoMode::Geo)] {
                let admitted = prepare_topic_on(&plane, &typed(Some(TopicType::Geo), geo_mode));
                if cluster_ready {
                    assert!(admitted.is_ok(), "mode {geo_mode:?}: {admitted:?}");
                } else {
                    assert_eq!(
                        admitted.err(),
                        Some(IggyError::FeatureUnavailable),
                        "mode {geo_mode:?}"
                    );
                }
            }
            assert_eq!(
                prepare_topic_on(
                    &plane,
                    &typed(Some(TopicType::Geo), Some(GeoMode::GeoTiered))
                )
                .err(),
                Some(IggyError::FeatureUnavailable),
                "geo_tiered, ready {cluster_ready}"
            );
            for (refused, key) in [
                (
                    iggy_common::TopicCreateOptions {
                        tiered_storage: Some(true),
                        ..typed(Some(TopicType::Geo), None)
                    },
                    "tiered_storage",
                ),
                (
                    iggy_common::TopicCreateOptions {
                        partition_resize_policy: Some(PartitionResizePolicy::Mutable),
                        ..typed(Some(TopicType::Geo), None)
                    },
                    "partition_resize_policy",
                ),
            ] {
                assert_eq!(
                    prepare_topic_on(&plane, &refused).err(),
                    Some(IggyError::InvalidOptionValue(key.to_owned())),
                    "ready {cluster_ready}"
                );
            }
        }
        for topic_type in [None, Some(TopicType::Normal)] {
            for geo_mode in [GeoMode::Geo, GeoMode::GeoTiered] {
                assert_eq!(
                    prepare_topic_with(&typed(topic_type, Some(geo_mode))).err(),
                    Some(IggyError::InvalidOptionValue("geo_mode".to_owned())),
                    "{topic_type:?} with {geo_mode}"
                );
            }
        }

        let persisted =
            prepare_topic_with(&typed(None, None)).expect("a default topic is admitted");
        let explicit = resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes");
        let derived = resource_options_from_wire(&persisted.derived_options, false)
            .expect("derived block decodes");
        assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::TopicType)));
        assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::CriticalKeys)));
        assert_eq!(
            derived[&HeaderKey::from(OptionKey::TopicType)]
                .value
                .as_bytes(),
            b"normal"
        );
        assert!(!derived.contains_key(&HeaderKey::from(OptionKey::GeoMode)));
        let mut stored = derived;
        stored.extend(explicit);
        let resolved = TopicResolvedOptions::from(&stored);
        assert_eq!(resolved.topic_type, TopicType::Normal);
        assert_eq!(resolved.geo_mode, None);

        let stated = prepare_topic_with(&typed(Some(TopicType::Normal), None))
            .expect("a topic that states it is normal is admitted");
        assert_eq!(
            resource_options_from_wire(&stated.request.options, true)
                .expect("explicit block decodes")[&HeaderKey::from(OptionKey::CriticalKeys)]
                .value
                .as_bytes(),
            b"topic_type"
        );
    }

    #[test]
    fn given_a_ready_cluster_when_preparing_a_geo_topic_should_store_its_mode_fixed_partitions_and_both_critical_keys()
     {
        let plane = metadata_plane();
        plane.observe_tiered_storage_readiness(true);
        let persisted = prepare_topic_on(
            &plane,
            &iggy_common::TopicCreateOptions {
                topic_type: Some(TopicType::Geo),
                ..Default::default()
            },
        )
        .expect("a ready cluster admits a geo topic");
        let explicit = resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes");
        let derived = resource_options_from_wire(&persisted.derived_options, false)
            .expect("derived block decodes");
        assert_eq!(
            explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
                .value
                .as_bytes(),
            b"geo_mode,topic_type"
        );
        assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::GeoMode)));
        assert_eq!(
            derived[&HeaderKey::from(OptionKey::GeoMode)]
                .value
                .as_bytes(),
            b"geo"
        );
        assert_eq!(
            derived[&HeaderKey::from(OptionKey::PartitionResizePolicy)]
                .value
                .as_bytes(),
            b"fixed"
        );
        let mut stored = derived;
        stored.extend(explicit);
        assert_eq!(check_critical_options(&stored), Ok(()));
        let resolved = TopicResolvedOptions::from(&stored);
        assert_eq!(resolved.geo_mode, Some(GeoMode::Geo));
        assert_eq!(
            resolved.partition_resize_policy,
            PartitionResizePolicy::Fixed
        );
        assert!(resolved.runtime.places_batches);
    }

    #[test]
    fn given_a_geo_topic_naming_a_durability_when_preparing_should_ignore_it_and_derive_persisted()
    {
        let durability = HeaderKey::from(OptionKey::Durability);
        for named in [Durability::Replicated, Durability::Persisted] {
            let plane = metadata_plane();
            plane.observe_tiered_storage_readiness(true);
            let persisted = prepare_topic_on(
                &plane,
                &iggy_common::TopicCreateOptions {
                    topic_type: Some(TopicType::Geo),
                    durability: named,
                    ..Default::default()
                },
            )
            .expect("a ready cluster admits a geo topic");
            let explicit = resource_options_from_wire(&persisted.request.options, true)
                .expect("explicit block decodes");
            let derived = resource_options_from_wire(&persisted.derived_options, false)
                .expect("derived block decodes");
            assert!(!explicit.contains_key(&durability), "{named} is ignored");
            assert_eq!(
                derived.get(&durability),
                Some(&OptionValue::derived(
                    Durability::Persisted.encode().unwrap()
                )),
                "{named}"
            );
        }

        let normal = prepare_topic_with(&iggy_common::TopicCreateOptions::default())
            .expect("a normal topic is admitted");
        let explicit = resource_options_from_wire(&normal.request.options, true)
            .expect("explicit block decodes");
        assert_eq!(
            explicit.get(&durability),
            Some(&OptionValue::explicit(
                Durability::Replicated.encode().unwrap()
            )),
            "a normal topic keeps the durability it names"
        );
    }

    #[test]
    fn given_local_retention_above_the_topic_retention_when_preparing_create_topic_should_refuse_it()
     {
        const GIB: u64 = 1 << 30;
        assert_eq!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                message_expiry: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
                local_retention_age: Some(iggy_common::IggyExpiry::from(2 * HOUR_MICROS)),
                ..Default::default()
            })
            .err(),
            Some(IggyError::InvalidOptionValue(
                "local_retention_age".to_owned()
            ))
        );
        assert_eq!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                max_topic_size: Some(iggy_common::MaxTopicSize::from(2 * GIB)),
                local_retention_size: Some(iggy_common::MaxTopicSize::from(4 * GIB)),
                ..Default::default()
            })
            .err(),
            Some(IggyError::InvalidOptionValue(
                "local_retention_size".to_owned()
            ))
        );

        let persisted = prepare_topic_with(&iggy_common::TopicCreateOptions {
            message_expiry: Some(iggy_common::IggyExpiry::from(2 * HOUR_MICROS)),
            local_retention_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
            max_topic_size: Some(iggy_common::MaxTopicSize::from(4 * GIB)),
            local_retention_size: Some(iggy_common::MaxTopicSize::from(2 * GIB)),
            ..Default::default()
        })
        .expect("a local retention within the topic retention is admitted");
        let stored = TopicResolvedOptions::from(
            &resource_options_from_wire(&persisted.request.options, true)
                .expect("explicit block decodes"),
        );
        assert_eq!(
            stored.local_retention_age,
            iggy_common::IggyExpiry::from(HOUR_MICROS)
        );
        assert_eq!(
            stored.local_retention_size,
            iggy_common::MaxTopicSize::from(2 * GIB)
        );

        // The one-day default is above this expiry and yields to it.
        assert!(
            prepare_topic_with(&iggy_common::TopicCreateOptions {
                message_expiry: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
                ..Default::default()
            })
            .is_ok()
        );
    }

    #[test]
    fn given_tiered_storage_when_resolving_a_topic_update_should_admit_on_only_in_a_ready_cluster()
    {
        let update = |set: &[(OptionKey, &str)], cluster_ready: bool| {
            let patch = TopicUpdateOptions {
                raw: set
                    .iter()
                    .map(|(key, value)| (key.to_string(), (*value).to_owned()))
                    .collect(),
                ..TopicUpdateOptions::default()
            }
            .to_patch()
            .unwrap();
            resolve_update_resource(
                &UpdateResourceRequest {
                    target: ResourceTarget::Topic {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                    },
                    reset: patch.reset,
                    options: patch.set,
                },
                cluster_ready,
            )
            .map(|(operation, _)| operation)
        };
        let enable = [(OptionKey::TieredStorage, "true")];

        assert_eq!(update(&enable, false), Err(IggyError::FeatureUnavailable));
        assert_eq!(update(&enable, true), Ok(Operation::UpdateTopicResolved));
        for cluster_ready in [false, true] {
            assert_eq!(
                update(&[(OptionKey::TieredStorage, "false")], cluster_ready),
                Err(IggyError::OptionNotUpdatable("tiered_storage".to_owned())),
                "ready {cluster_ready}"
            );
            assert_eq!(
                update(&[(OptionKey::MessageExpiry, "1h")], cluster_ready),
                Ok(Operation::UpdateTopicResolved),
                "ready {cluster_ready}"
            );
        }
    }

    #[test]
    fn given_a_tiered_storage_patch_when_the_primary_prepares_it_should_follow_the_cluster_verdict()
    {
        const CLIENT: u128 = 1;
        let enable = || {
            let patch = TopicUpdateOptions {
                raw: BTreeMap::from([(OptionKey::TieredStorage.to_string(), "true".to_owned())]),
                ..TopicUpdateOptions::default()
            }
            .to_patch()
            .unwrap();
            update_resource_request(
                CLIENT,
                &UpdateResourceRequest {
                    target: ResourceTarget::Topic {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                    },
                    reset: patch.reset,
                    options: patch.set,
                }
                .to_bytes(),
            )
        };
        let plane = metadata_plane();
        plane
            .client_table
            .borrow_mut()
            .commit_register(CLIENT, 7, [0x5a; 32], register_reply(CLIENT, 10))
            .expect("register commits");

        let refused = plane.prepare_request(enable()).err();
        assert_eq!(refused, Some(IggyError::FeatureUnavailable));
        assert!(
            refused.as_ref().is_some_and(is_admission_refusal),
            "the client gets the refusal as a reply and keeps its session"
        );
        plane.observe_tiered_storage_readiness(true);
        assert_eq!(
            plane
                .prepare_request(enable())
                .map(|prepare| prepare.header().operation),
            Ok(Operation::UpdateTopicResolved)
        );
        plane.observe_tiered_storage_readiness(false);
        assert_eq!(
            plane.prepare_request(enable()).err(),
            Some(IggyError::FeatureUnavailable)
        );
    }

    #[test]
    fn given_a_cluster_verdict_when_the_metadata_view_changes_should_stop_admitting() {
        let mut metadata = placement_metadata();
        assert!(!metadata.tiered_storage_cluster_ready());
        metadata.observe_tiered_storage_readiness(true);
        assert!(metadata.tiered_storage_cluster_ready());

        metadata.consensus.as_mut().unwrap().set_view(1);
        assert!(
            !metadata.tiered_storage_cluster_ready(),
            "a verdict from the previous view admits nothing"
        );
    }

    #[test]
    fn given_keyless_create_topic_when_preparing_should_derive_every_catalog_default() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let plane = metadata_plane();
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .expect("register commits");

        let prepare = plane
            .prepare_request(create_topic_request(CLIENT, ACTING_USER))
            .expect("CreateTopic is client-allowed");
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
        let persisted = PersistedCreateTopicRequest::decode_from(body)
            .expect("create topic with assignments prepare must decode");
        let derived = resource_options_from_wire(&persisted.derived_options, false)
            .expect("derived block decodes");
        let mut expected = ResourceOptions::new();
        for descriptor in descriptors_for(OptionsScope::Topic)
            .filter(|descriptor| descriptor.storage == OptionStorage::Map)
        {
            if let Some(default) = descriptor.default_value().expect("default encodes") {
                expected.insert(
                    HeaderKey::from(descriptor.key),
                    OptionValue::derived(default),
                );
            }
        }
        assert!(!expected.is_empty(), "the topic catalog has defaulted rows");
        // The factor default is lowered to this single-node cluster.
        expected.insert(
            HeaderKey::from(OptionKey::ReplicationFactor),
            OptionValue::derived(HeaderValue::from(1u32)),
        );
        assert_eq!(derived, expected);
        assert!(
            persisted
                .partitions
                .iter()
                .all(|partition| partition.members == [0]),
            "every partition lists the one node"
        );
    }

    #[test]
    fn given_plaintext_stream_key_when_preparing_create_stream_should_refuse_before_replication() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let plane = metadata_plane();
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .expect("register commits");
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKey),
            OptionValue::explicit(
                HeaderValue::try_from([7u8; 32].as_slice()).expect("raw key encodes"),
            ),
        );
        let options =
            resource_options_to_wire(&options, OptionsProvenance::All).expect("options encode");

        let Err(refusal) =
            plane.prepare_request(create_stream_request_with(CLIENT, 1, "secrets", options))
        else {
            panic!("a plaintext stream key must not be sealed into a prepare");
        };

        assert!(
            matches!(
                &refusal,
                IggyError::InvalidOptionValue(key) if *key == OptionKey::EncryptionKey.to_string()
            ),
            "expected the plaintext key refusal, got {refusal:?}"
        );
    }

    #[test]
    fn given_wrap_set_no_primary_key_opens_when_preparing_create_stream_should_refuse_before_replication()
     {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 10;
        const ACTING_USER: u32 = 7;
        let primary_keys = Arc::new(
            ServerKeyContext::from_config(
                "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=",
                &[],
                true,
            )
            .expect("valid key"),
        );
        let stranger_keys = ServerKeyContext::from_config(
            "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=",
            &[],
            true,
        )
        .expect("valid key");
        let plane = metadata_plane().with_stream_key_context(Arc::clone(&primary_keys));
        plane
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .expect("register commits");
        let wrapped_under = |keys: &ServerKeyContext| {
            let set = keys
                .wrap_set(
                    &StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("32 bytes"),
                    StreamKeyOrigin::Server,
                )
                .expect("wraps");
            let mut options = ResourceOptions::new();
            options.insert(
                HeaderKey::from(OptionKey::EncryptionKeyWrapped),
                OptionValue::explicit(HeaderValue::try_from(set.as_ref()).expect("fits")),
            );
            options.insert(
                HeaderKey::from(OptionKey::CriticalKeys),
                OptionValue::explicit(
                    critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).expect("marker"),
                ),
            );
            resource_options_to_wire(&options, OptionsProvenance::All).expect("options encode")
        };

        let Err(refusal) = plane.prepare_request(create_stream_request_with(
            CLIENT,
            1,
            "stranger",
            wrapped_under(&stranger_keys),
        )) else {
            panic!("a wrap set the primary cannot open must not be sealed into a prepare");
        };

        assert_eq!(
            refusal.as_code(),
            IggyError::StreamEncryptionUnavailable.as_code()
        );
        assert!(
            plane
                .prepare_request(create_stream_request_with(
                    CLIENT,
                    2,
                    "own",
                    wrapped_under(&primary_keys),
                ))
                .is_ok()
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
            _data: impl Into<BusMessage>,
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
    }

    /// Bus that records the client id of every `send_to_client`.
    #[derive(Debug, Default)]
    struct ClientSendSpyBus {
        client_sends: RefCell<Vec<u128>>,
        client_replies: RefCell<Vec<BusMessage>>,
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for ClientSendSpyBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            client_id: u128,
            data: impl Into<BusMessage>,
        ) -> Result<(), SendError> {
            self.client_sends.borrow_mut().push(client_id);
            self.client_replies.borrow_mut().push(data.into());
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
    }

    /// A view change or a boot re-pipeline rebuilds a pending expired-token
    /// delete without its in-process sender. Committing it must not send a
    /// reply to the reserved client id, which no connection owns.
    #[compio::test]
    async fn given_repipelined_server_operations_when_committed_should_send_no_client_reply() {
        const USER: u32 = 7;
        const TOKEN: &str = "expired";

        let dir = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            ClientSendSpyBus::default(),
            LocalPipeline::new(),
        );
        consensus.init();
        let md: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
        );
        let consensus = md.consensus.as_ref().unwrap();

        let body = DeletePersonalAccessTokenRequest {
            user_id: USER,
            name: WireName::new(TOKEN).unwrap(),
            only_if_expired: true,
        }
        .to_bytes();
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
            consensus.retry_capacity(&md.client_table.borrow()),
        );
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");
        md.on_ack(ack).await;

        assert_eq!(consensus.commit_min(), 1, "the delete must commit");
        assert!(
            consensus.message_bus().client_sends.borrow().is_empty(),
            "no connection owns the reserved client id, so nothing may be sent to it"
        );

        let body = CompleteLifecycleRequest::new(
            1,
            1,
            vec![LifecycleCompletion {
                topic_id: 1,
                partition_id: 0,
                partition_op: 1,
            }],
        )
        .unwrap()
        .to_bytes();
        let header = RoutedRequestHeader {
            client: message_bus::server_internal_client_id(server_common::sharding::METADATA_GROUP),
            ..header
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(consensus, &header, Operation::CompleteLifecycle, &body),
            consensus.retry_capacity(&md.client_table.borrow()),
        );
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .unwrap()
            .try_into_typed::<PrepareOkHeader>()
            .unwrap();
        md.on_ack(ack).await;
        assert_eq!(consensus.commit_min(), 2, "the completion must commit");
        assert!(
            consensus.message_bus().client_sends.borrow().is_empty(),
            "a completion without its in-process sender must never send a wire reply"
        );
    }

    /// A replayed `CreatePersonalAccessToken` must be refused, not served from
    /// the dedup cache: the committed secret is unrecoverable (never
    /// replicated) and the rewrite has already minted a fresh one whose hash
    /// never reached consensus, so replaying would hand back a credential that
    /// authenticates against nothing. The refusal keeps the committed header,
    /// so the client can still prove that the original create committed. A
    /// cached REJECTION still replays -- it carries no secret.
    #[compio::test]
    async fn replayed_pat_create_is_refused_but_other_replays_pass_through() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;
        const PAT_OP: u64 = 2;
        const PAT_REQUEST_CHECKSUM: u128 = 0x2a;

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
            .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
            .unwrap();

        // Cache a committed SUCCESS for request 1 under the PAT operation,
        // exactly as the commit path would (empty apply body + result section).
        let pat_success = build_reply_message(
            &PrepareHeader {
                client: CLIENT,
                request: 1,
                request_checksum: PAT_REQUEST_CHECKSUM,
                operation: Operation::CreatePersonalAccessToken,
                op: PAT_OP,
                user_id: USER,
                ..PrepareHeader::default()
            },
            &bytes::Bytes::copy_from_slice(&0u32.to_le_bytes()),
        );
        let committed = *pat_success.header();
        md.client_table
            .borrow_mut()
            .commit_reply(CLIENT, USER, pat_success);

        // Replaying request 1 as a PAT create must NOT return the cached
        // success; it must refuse with the name-taken code.
        let refusal = md
            .submit_request_in_process(pat_request(
                CLIENT,
                1,
                Operation::CreatePersonalAccessToken,
                &[],
            ))
            .await
            .expect("refusal is a reply, not a submit error")
            .try_into_typed::<ReplyHeader>()
            .unwrap();
        assert_eq!(
            iggy_binary_protocol::result_code(refusal.body()),
            Some(IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code()),
            "a replayed PAT create must be refused, never answered from cache"
        );
        let header = refusal.header();
        assert!(
            header.has_committed_result(),
            "the refusal must prove that the original create committed"
        );
        assert_eq!((header.op, header.commit), (PAT_OP, PAT_OP));
        let expected = ReplyHeader {
            size: header.size,
            ..committed
        };
        assert_eq!(
            bytemuck::bytes_of(header),
            bytemuck::bytes_of(&expected),
            "the refusal must keep the committed client, request, request checksum and operation"
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
            .submit_request_in_process(pat_request(
                CLIENT,
                2,
                Operation::CreatePersonalAccessToken,
                &[],
            ))
            .await
            .expect("cached rejection replays");
        let body = &reply.as_slice()[size_of::<ReplyHeader>()..];
        assert_eq!(
            iggy_binary_protocol::result_code(body),
            Some(rejected_code),
            "a cached PAT rejection is safe to replay verbatim"
        );
    }

    /// A same-id replay of a `CreateTopic` that already committed must get its
    /// cached reply at the partitions cap. Admitting it again would count the
    /// committed topic's own partitions a second time and deny an op that
    /// succeeded.
    #[compio::test]
    async fn given_committed_create_topic_at_partitions_cap_when_replayed_should_serve_cached_reply()
     {
        const CLIENT: u128 = 1;
        const USER: u32 = TOPIC_MANAGER;
        let mut metadata = metadata_plane();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        metadata.set_partitions_max(1);
        metadata
            .client_table
            .borrow_mut()
            .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
            .unwrap();
        // The commit that created the topic above cached its success.
        metadata.client_table.borrow_mut().commit_reply(
            CLIENT,
            USER,
            committed_reply(CLIENT, 1, Operation::CreateTopicWithAssignments, 0),
        );

        let replay = metadata
            .submit_request_in_process(create_topic_request(CLIENT, USER))
            .await
            .expect("a replay is a reply, not a submit error");
        assert_eq!(
            iggy_binary_protocol::result_code(&replay.as_slice()[size_of::<ReplyHeader>()..]),
            Some(0),
            "the replay must get the cached success, not a cap denial"
        );

        let mut fresh = create_topic_request(CLIENT, USER);
        bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
            &mut fresh.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
        )
        .request = 2;
        let denied = metadata
            .submit_request_in_process(fresh)
            .await
            .expect("a denial is a reply, not a submit error");
        assert_eq!(
            iggy_binary_protocol::result_code(&denied.as_slice()[size_of::<ReplyHeader>()..]),
            Some(IggyError::PartitionsLimitReached.as_code()),
            "a new create past the cap must be denied"
        );
    }

    /// A normal backup names the primary of its view as the place to retry,
    /// and a backup in transfer cannot. Neither admits the mutation.
    #[compio::test]
    async fn given_backup_when_mutation_is_submitted_should_refuse_by_role() {
        const CLIENT: u128 = 1;
        const USER: u32 = TOPIC_MANAGER;
        const BACKUP_REPLICA: u8 = 1;
        const REPLICA_COUNT: u8 = 3;
        for (transferring, expected) in [
            (false, IggyError::NotPrimary),
            (true, IggyError::TransientNotAccepted),
        ] {
            let consensus = VsrConsensus::new(
                1,
                BACKUP_REPLICA,
                REPLICA_COUNT,
                server_common::sharding::METADATA_GROUP,
                NoopBus,
                LocalPipeline::new(),
            );
            consensus.init();
            if transferring {
                consensus.begin_state_transfer_await();
            }
            let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
                Some(consensus),
                None,
                None,
                None,
                TestMux::new((
                    users_with_topic_manager(),
                    (stream_with_one_partition().into(), ()),
                )),
                None,
            );
            metadata
                .client_table
                .borrow_mut()
                .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
                .unwrap();

            let reply = metadata
                .submit_request_in_process(create_topic_request(CLIENT, USER))
                .await
                .expect("a refusal is a reply, not a submit error");
            assert_eq!(
                iggy_binary_protocol::result_code(&reply.as_slice()[size_of::<ReplyHeader>()..]),
                Some(expected.as_code()),
                "transferring {transferring}"
            );
            assert_eq!(
                metadata.consensus.as_ref().unwrap().pipeline_len(),
                0,
                "a backup must not prepare the mutation"
            );
        }
    }

    #[test]
    fn given_no_partitions_cap_when_admitting_create_topic_should_admit() {
        let mut metadata = metadata_plane();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));

        assert!(
            metadata
                .admit_partitions(&create_topic_request(1, TOPIC_MANAGER))
                .is_ok(),
            "a zero partitions_max must admit a create on a node with partitions"
        );
    }

    /// The gated apply answers a create from a user without the grant, for a
    /// stream that does not exist, or of a topic name in use, with
    /// `Unauthorized`, `NotFound` or `TopicNameAlreadyExists`. The cap must not
    /// answer first, or that user could probe the cap state, and a client that
    /// creates a topic only when it is missing would get the wrong error.
    #[test]
    fn given_create_topic_past_partitions_cap_when_apply_would_refuse_should_leave_it_to_apply() {
        const UNGRANTED_CLIENT: u128 = 1;
        const MANAGER_CLIENT: u128 = 2;
        let mut metadata = metadata_plane();
        metadata.mux_stm = Rc::new(TestMux::new((
            users_with_topic_manager(),
            (stream_with_one_partition().into(), ()),
        )));
        metadata.set_partitions_max(1);

        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                UNGRANTED_CLIENT,
                UNGRANTED,
                [0x5a; 32],
                register_reply(UNGRANTED_CLIENT, 1),
            )
            .unwrap();
        assert!(
            metadata
                .admit_partitions(&create_topic_request(UNGRANTED_CLIENT, UNGRANTED))
                .is_ok(),
            "a user without create_topic must get Unauthorized from apply"
        );

        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                MANAGER_CLIENT,
                TOPIC_MANAGER,
                [0x5a; 32],
                register_reply(MANAGER_CLIENT, 1),
            )
            .unwrap();
        let missing_stream = create_named_topic_request(
            MANAGER_CLIENT,
            TOPIC_MANAGER,
            WireIdentifier::numeric(9),
            "t",
        );
        assert!(
            metadata.admit_partitions(&missing_stream).is_ok(),
            "a create for a missing stream must get NotFound from apply"
        );

        let taken_name = create_named_topic_request(
            MANAGER_CLIENT,
            TOPIC_MANAGER,
            WireIdentifier::numeric(0),
            "existing",
        );
        assert!(
            metadata.admit_partitions(&taken_name).is_ok(),
            "a create of a topic name in use must get TopicNameAlreadyExists from apply"
        );

        assert!(
            matches!(
                metadata.admit_partitions(&create_topic_request(MANAGER_CLIENT, TOPIC_MANAGER)),
                Err(IggyError::PartitionsLimitReached)
            ),
            "a create that apply would carry out is denied at the cap"
        );
    }

    /// The cap is graded before the apply grades the name, so a create at the
    /// cap reports the cap even when its name is taken, as the legacy server
    /// did.
    #[test]
    fn given_at_limit_duplicate_name_when_admitting_should_report_limit_not_duplicate() {
        let metadata = metadata_capped_over(users_holding_token());

        let error = metadata
            .admit_personal_access_token(&pat_request(
                1,
                1,
                Operation::CreatePersonalAccessToken,
                &token_create(TOKEN_NAME).to_bytes(),
            ))
            .expect_err("a create at the cap must be denied even for a taken name");

        assert!(
            matches!(
                error,
                IggyError::PersonalAccessTokensLimitReached(TOKEN_OWNER, TOKEN_CAP)
            ),
            "expected the cap error before the name grading, got {error:?}"
        );
    }

    /// A delete is how a user at the cap frees a slot, so capping it too
    /// would lock the account at the cap.
    #[test]
    fn given_user_at_token_cap_when_deleting_should_admit() {
        let metadata = metadata_capped_over(users_holding_token());

        assert!(
            metadata
                .admit_personal_access_token(&pat_request(
                    1,
                    1,
                    Operation::DeletePersonalAccessToken,
                    &token_delete().to_bytes(),
                ))
                .is_ok()
        );
    }

    #[test]
    fn given_deletion_freed_a_slot_when_admitting_should_admit_the_next_create() {
        let mut users = users_holding_token();
        let deleted = StateHandler::apply(&token_delete(), &mut users, IggyTimestamp::now());
        assert_eq!(deleted.code, 0, "the fixture token must be deleted");
        let metadata = metadata_capped_over(users);

        let admitted = metadata.admit_personal_access_token(&pat_request(
            1,
            1,
            Operation::CreatePersonalAccessToken,
            &token_create("replacement").to_bytes(),
        ));

        assert!(
            admitted.is_ok(),
            "a freed slot must admit the next create, got {admitted:?}"
        );
    }

    /// The cap counts the creates in flight, not only the committed tokens, so
    /// creates pipelined at the boundary never take a user past it. A create
    /// in flight holds its slot until it leaves the pipeline: the apply
    /// refuses it, it commits as a token, or a view change drops it. Only the
    /// committed tokens prove that a create cannot fit, so until then a create
    /// waits instead of getting the cap's error.
    #[test]
    fn given_token_creates_in_flight_when_admitting_at_cap_should_count_them_until_they_leave_the_pipeline()
     {
        const TOKENS_MAX: u32 = 3;
        let metadata = primary_with_clients(
            NoopBus,
            users_holding_token().into(),
            StreamsInner::new(),
            TOKEN_OWNER,
            FIRST_CLIENT..=FIRST_CLIENT + 4,
        );
        metadata.set_max_tokens_per_user(TOKENS_MAX);
        let consensus = metadata.consensus.as_ref().unwrap();
        let create = |client: u128, name: &str| {
            metadata.prepare_request(pat_request(
                client,
                1,
                Operation::CreatePersonalAccessToken,
                &token_create(name).to_bytes(),
            ))
        };
        let assert_deferred = |client: u128, name: &str| {
            let refused = create(client, name);
            assert!(
                matches!(refused, Err(IggyError::TransientNotAccepted)),
                "{name} must wait for the creates in flight, got {:?}",
                refused.err()
            );
        };
        // Committed the way `on_ack` commits the pipeline head.
        let commit_head = |prepare: Message<PrepareHeader>| {
            let op = prepare.header().op;
            consensus.with_pipeline_mut(|pipeline| {
                pipeline.pop().unwrap();
            });
            let reply = metadata.apply_prepare(prepare);
            consensus.advance_commit_max(op);
            consensus.advance_commit_min(op);
            iggy_binary_protocol::result_code(reply.body())
        };

        // The committed token leaves two slots. The first create reuses its
        // name, so the apply will refuse it.
        let mut in_flight = Vec::new();
        for (client, name) in (FIRST_CLIENT..).zip([TOKEN_NAME, "second", "third", "fourth"]) {
            if let Ok(prepare) = create(client, name) {
                consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                in_flight.push(prepare);
            }
        }
        assert_eq!(in_flight.len(), 2, "two slots admit two creates in flight");
        assert_deferred(FIRST_CLIENT + 2, "third");

        assert_ne!(
            commit_head(in_flight.remove(0)),
            Some(0),
            "the apply must refuse the taken name"
        );
        let third = create(FIRST_CLIENT + 2, "third").expect("the refused create freed its slot");
        consensus.pipeline_message(PlaneKind::Metadata, &third);
        assert_deferred(FIRST_CLIENT + 3, "fourth");

        assert_eq!(commit_head(in_flight.remove(0)), Some(0));
        assert_deferred(FIRST_CLIENT + 3, "fourth");

        drop_uncommitted_suffix(consensus);
        let fourth =
            create(FIRST_CLIENT + 3, "fourth").expect("a dropped create must free its slot");
        consensus.pipeline_message(PlaneKind::Metadata, &fourth);
        assert_eq!(commit_head(fourth), Some(0));
        let refused = create(FIRST_CLIENT + 4, "fifth");
        assert!(
            matches!(
                refused,
                Err(IggyError::PersonalAccessTokensLimitReached(
                    TOKEN_OWNER,
                    TOKENS_MAX
                ))
            ),
            "a create past the cap on committed tokens keeps the cap error, got {:?}",
            refused.err()
        );
    }

    /// `partitions_max` counts the partitions of the creates in flight the
    /// same way, and frees them when a view change drops those creates.
    #[test]
    fn given_topic_creates_in_flight_when_admitting_at_cap_should_count_them_until_they_leave_the_pipeline()
     {
        const PARTITIONS_MAX: u32 = 4;
        let metadata = primary_with_clients(
            NoopBus,
            users_with_topic_manager(),
            stream_with_one_partition(),
            TOPIC_MANAGER,
            FIRST_CLIENT..=FIRST_CLIENT + 4,
        );
        metadata.set_partitions_max(PARTITIONS_MAX);
        let consensus = metadata.consensus.as_ref().unwrap();
        let create = |client: u128, name: &str| {
            metadata.prepare_request(create_named_topic_request(
                client,
                TOPIC_MANAGER,
                WireIdentifier::numeric(0),
                name,
            ))
        };

        // The committed partition leaves room for three one-partition topics.
        let mut in_flight = 0;
        for (client, name) in (FIRST_CLIENT..).zip(["a", "b", "c", "d", "e"]) {
            match create(client, name) {
                Ok(prepare) => {
                    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                    in_flight += 1;
                }
                Err(error) => assert!(
                    matches!(error, IggyError::TransientNotAccepted),
                    "{name} must wait for the creates in flight, got {error:?}"
                ),
            }
        }
        assert_eq!(in_flight, 3, "three free partitions admit three creates");

        drop_uncommitted_suffix(consensus);
        assert!(
            create(FIRST_CLIENT + 3, "d").is_ok(),
            "a dropped create must free its partitions"
        );
    }

    /// A create in flight that its apply refuses holds a reservation until it
    /// commits. A valid create that fits once that reservation is freed must
    /// not get the terminal cap error before then.
    #[test]
    fn given_refused_create_in_flight_when_a_valid_create_fits_committed_partitions_should_defer_it()
     {
        const PARTITIONS_MAX: u32 = 3;
        let metadata = primary_with_clients(
            NoopBus,
            users_with_topic_manager(),
            stream_with_one_partition(),
            TOPIC_MANAGER,
            FIRST_CLIENT..=FIRST_CLIENT + 2,
        );
        metadata.set_partitions_max(PARTITIONS_MAX);
        let consensus = metadata.consensus.as_ref().unwrap();
        let create = |client: u128, name: &str, partitions_count: u32| {
            metadata.prepare_request(create_topic_request_with_count(
                client,
                TOPIC_MANAGER,
                WireIdentifier::numeric(0),
                name,
                partitions_count,
            ))
        };

        let taken_name = create(FIRST_CLIENT, "existing", 2).unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &taken_name);
        assert!(
            matches!(
                create(FIRST_CLIENT + 1, "fresh", 1),
                Err(IggyError::TransientNotAccepted)
            ),
            "a create that fits the committed partitions must wait"
        );
        assert!(
            matches!(
                create(FIRST_CLIENT + 2, "large", PARTITIONS_MAX),
                Err(IggyError::PartitionsLimitReached)
            ),
            "a create past the cap on committed partitions keeps the cap error"
        );

        let op = taken_name.header().op;
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.pop().unwrap();
        });
        let reply = metadata.apply_prepare(taken_name);
        assert_ne!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        consensus.advance_commit_max(op);
        consensus.advance_commit_min(op);
        assert!(
            create(FIRST_CLIENT + 1, "fresh", 1).is_ok(),
            "the refused create must free its reservation"
        );
    }

    /// The gate that lets a create past the cap reach its own apply error reads
    /// the committed state, and an op in flight can change its verdict before
    /// the create applies: here it creates the create's stream. Such a create
    /// must not take the node past the cap.
    #[test]
    fn given_create_behind_its_uncommitted_stream_when_past_partitions_cap_should_not_pass_the_cap()
    {
        const PARTITIONS_MAX: u32 = 2;
        const STREAM: &str = "pending";
        const ROOT_CLIENT: u128 = 1;
        let metadata = primary_with_clients(
            NoopBus,
            users_with_topic_manager(),
            stream_with_one_partition(),
            TOPIC_MANAGER,
            [],
        );
        metadata.set_partitions_max(PARTITIONS_MAX);
        let consensus = metadata.consensus.as_ref().unwrap();
        let create = || {
            metadata.prepare_request(create_topic_request_with_count(
                ROOT_CLIENT + 1,
                0,
                WireIdentifier::named(STREAM).unwrap(),
                "topic",
                PARTITIONS_MAX,
            ))
        };
        let mut in_flight = vec![
            metadata
                .prepare_request(create_stream_request(ROOT_CLIENT, 1, STREAM))
                .unwrap(),
        ];
        consensus.pipeline_message(PlaneKind::Metadata, &in_flight[0]);

        let behind = create();
        if let Ok(prepare) = &behind {
            consensus.pipeline_message(PlaneKind::Metadata, prepare);
            in_flight.push(prepare.clone());
        }
        for prepare in in_flight {
            let op = prepare.header().op;
            consensus.with_pipeline_mut(|pipeline| {
                pipeline.pop().unwrap();
            });
            metadata.apply_prepare(prepare);
            consensus.advance_commit_max(op);
            consensus.advance_commit_min(op);
        }
        let partitions = metadata.mux_stm.streams().partition_count();
        assert!(
            partitions <= PARTITIONS_MAX as usize,
            "{partitions} partitions committed past the cap of {PARTITIONS_MAX}"
        );
        assert!(
            matches!(behind, Err(IggyError::TransientNotAccepted)),
            "a create whose gate an op in flight can change must wait, got {:?}",
            behind.err()
        );
        assert!(
            matches!(create(), Err(IggyError::PartitionsLimitReached)),
            "once its stream committed, the create must get the cap's error"
        );
    }

    /// A cap refusal answers a well-formed request: the wire ingress and the
    /// promotion of a queued create reply with the cap's code and keep the
    /// session.
    #[compio::test]
    async fn given_create_past_token_cap_when_wire_or_queue_submits_should_reply_52_without_eviction()
     {
        const WIRE_CLIENT: u128 = FIRST_CLIENT;
        const QUEUED_CLIENT: u128 = FIRST_CLIENT + 1;
        let metadata = primary_with_clients(
            ClientSendSpyBus::default(),
            users_holding_token().into(),
            StreamsInner::new(),
            TOKEN_OWNER,
            WIRE_CLIENT..=QUEUED_CLIENT,
        );
        metadata.set_max_tokens_per_user(TOKEN_CAP);
        let consensus = metadata.consensus.as_ref().unwrap();
        let create = |client: u128| {
            pat_request(
                client,
                1,
                Operation::CreatePersonalAccessToken,
                &token_create("next").to_bytes(),
            )
        };
        let cap_code =
            IggyError::PersonalAccessTokensLimitReached(TOKEN_OWNER, TOKEN_CAP).as_code();

        metadata.on_request(create(WIRE_CLIENT)).await;
        {
            let replies = consensus.message_bus().client_replies.borrow();
            assert_eq!(replies.len(), 1, "the wire ingress must answer the refusal");
            let frame = replies[0].first();
            let header =
                bytemuck::checked::from_bytes::<ReplyHeader>(&frame[..size_of::<ReplyHeader>()]);
            assert_eq!(
                header.command,
                Command::Reply,
                "a cap refusal must not evict"
            );
            assert_eq!(
                iggy_binary_protocol::result_code(&frame[size_of::<ReplyHeader>()..]),
                Some(cap_code)
            );
        }

        let (queued, mut receiver) =
            consensus::RequestEntry::with_subscriber(create(QUEUED_CLIENT));
        assert!(consensus.push_queued_request(queued).is_ok());
        metadata.drain_request_queue_into_prepares().await;
        let std::task::Poll::Ready(Ok(reply)) = futures::poll!(&mut receiver) else {
            panic!("the promotion must answer a queued create past the cap");
        };
        assert_eq!(
            iggy_binary_protocol::result_code(reply.body()),
            Some(cap_code)
        );
        assert_eq!(
            consensus.pipeline_len(),
            0,
            "a create past the cap must not be prepared"
        );
    }

    /// Clients the cap tests register, above those `placement_metadata`
    /// registers as root.
    const FIRST_CLIENT: u128 = 11;

    /// A primary of three replicas, so its prepares wait in the pipeline, over
    /// `users` and `streams`, with `clients` registered as `user_id`.
    fn primary_with_clients<B: MessageBus>(
        bus: B,
        users: Users,
        streams: StreamsInner,
        user_id: u32,
        clients: impl IntoIterator<Item = u128>,
    ) -> IggyMetadata<VsrConsensus<B>, PrepareJournal, (), TestMux> {
        let mut metadata = placement_metadata_with_bus(bus);
        metadata.mux_stm = Rc::new(TestMux::new((users, (streams.into(), ()))));
        for client in clients {
            metadata
                .client_table
                .borrow_mut()
                .commit_register(client, user_id, [0x5a; 32], register_reply(client, 1))
                .unwrap();
        }
        metadata
    }

    /// What a view change that truncates the uncommitted suffix leaves on the
    /// old primary: `handle_start_view` clears the pipeline and moves the head
    /// to the announced op, here the commit.
    fn drop_uncommitted_suffix<B: MessageBus>(consensus: &VsrConsensus<B>) {
        consensus.clear_pipeline();
        consensus.sequencer().set_sequence(consensus.commit_min());
    }

    const TOKEN_OWNER: u32 = 7;
    const TOKEN_NAME: &str = "token";
    const TOKEN_CAP: u32 = 1;

    fn token_create(name: &str) -> CreatePersonalAccessTokenRequest {
        CreatePersonalAccessTokenRequest {
            user_id: TOKEN_OWNER,
            name: WireName::new(name).unwrap(),
            expiry: 0,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        }
    }

    fn token_delete() -> DeletePersonalAccessTokenRequest {
        DeletePersonalAccessTokenRequest {
            user_id: TOKEN_OWNER,
            name: WireName::new(TOKEN_NAME).unwrap(),
            only_if_expired: false,
        }
    }

    /// `TOKEN_OWNER` holding `TOKEN_NAME`, which fills `TOKEN_CAP`.
    fn users_holding_token() -> UsersInner {
        let mut users = UsersInner::new();
        let created =
            StateHandler::apply(&token_create(TOKEN_NAME), &mut users, IggyTimestamp::now());
        assert_eq!(created.code, 0, "the fixture token must be created");
        users
    }

    /// A plane over `users` that caps each user at `TOKEN_CAP` tokens.
    fn metadata_capped_over(
        users: UsersInner,
    ) -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
        let mut metadata = metadata_plane();
        metadata.mux_stm = Rc::new(TestMux::new((
            users.into(),
            (StreamsInner::new().into(), ()),
        )));
        metadata.set_max_tokens_per_user(TOKEN_CAP);
        metadata
    }

    /// Slab ids of `users_with_topic_manager`: root takes 0.
    const TOPIC_MANAGER: u32 = 1;
    const UNGRANTED: u32 = 2;

    /// Root, then `TOPIC_MANAGER` with `manage_topics` on stream 0, then
    /// `UNGRANTED` with no permissions.
    fn users_with_topic_manager() -> Users {
        let mut inner = UsersInner::new();
        let timestamp = IggyTimestamp::now();
        let no_grants = WireGlobalPermissions {
            manage_servers: false,
            read_servers: false,
            manage_users: false,
            read_users: false,
            manage_streams: false,
            read_streams: false,
            manage_topics: false,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
        };
        for (username, streams) in [
            ("iggy", Vec::new()),
            (
                "manager",
                vec![WireStreamPermissions {
                    stream_id: 0,
                    manage_stream: false,
                    read_stream: false,
                    manage_topics: true,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                    topics: Vec::new(),
                }],
            ),
            ("ungranted", Vec::new()),
        ] {
            let reply = StateHandler::apply(
                &CreateUserRequest {
                    username: WireName::new(username).unwrap(),
                    password: "hash".to_string(),
                    status: UserStatus::Active.as_code(),
                    permissions: Some(WirePermissions {
                        global: no_grants.clone(),
                        streams,
                    }),
                    options: WireOptions::empty(),
                },
                &mut inner,
                timestamp,
            );
            assert_eq!(reply.code, 0, "fixture user {username} must be created");
        }
        inner.into()
    }

    /// Stream 0 holding one topic with one partition.
    fn stream_with_one_partition() -> StreamsInner {
        stream_with_one_partition_in_view(0)
    }

    fn stream_with_one_partition_in_view(view: u32) -> StreamsInner {
        let mut inner = StreamsInner::new();
        let _ = StateHandler::apply(
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        create_topic_in_view(&mut inner, view);
        inner
    }

    /// Topic "existing" in stream 0, with one partition created in `view`.
    fn create_topic_in_view(inner: &mut StreamsInner, view: u32) {
        let _ = StateHandler::apply(
            &PersistedCreateTopicRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                    name: WireName::new("existing").unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: vec![CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 1,
                    initial_view: view,
                    members: Vec::new(),
                }],
            },
            inner,
            IggyTimestamp::now(),
        );
    }

    #[test]
    fn given_zero_partitions_create_past_partitions_cap_when_validating_should_admit() {
        assert!(validate_partitions_limit(10, 0, 12).is_ok());
    }

    #[test]
    fn given_create_reaching_partitions_cap_when_validating_should_admit() {
        assert!(validate_partitions_limit(10, 4, 6).is_ok());
    }

    #[test]
    fn given_create_past_partitions_cap_when_validating_should_deny() {
        assert!(matches!(
            validate_partitions_limit(10, 5, 6),
            Err(IggyError::PartitionsLimitReached)
        ));
        assert!(matches!(
            validate_partitions_limit(10, 1, 10),
            Err(IggyError::PartitionsLimitReached)
        ));
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

    fn pat_request(
        client: u128,
        request: u64,
        operation: Operation,
        body: &[u8],
    ) -> Message<RoutedRequestHeader> {
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(body);
            let header =
                bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation,
                size: u32::try_from(total).unwrap(),
                client,
                session: 1,
                request,
                group: server_common::sharding::METADATA_GROUP,
                ..Default::default()
            };
        }
        message
    }

    fn create_stream_request(
        client: u128,
        request: u64,
        name: &str,
    ) -> Message<RoutedRequestHeader> {
        create_stream_request_with(client, request, name, WireOptions::empty())
    }

    fn create_stream_request_with(
        client: u128,
        request: u64,
        name: &str,
        options: WireOptions,
    ) -> Message<RoutedRequestHeader> {
        let body = iggy_binary_protocol::requests::streams::CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options,
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

        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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

    /// The walk cap bounds ONE `commit_journal` call, not the backlog.
    ///
    /// The resident `(commit_min, commit_max]` run after a repair window or a
    /// rejoin is the whole backlog, and the walk applies each op with no await
    /// the pump can interleave, so an uncapped call holds the shard for all of
    /// it. Every caller is re-driven every tick, so stopping short loses
    /// nothing; a cap that did NOT resume would pin `commit_min` until the next
    /// op to commit trips `advance_commit_min`'s sequential assert.
    #[compio::test]
    async fn commit_journal_stops_at_the_walk_cap_and_resumes_on_the_next_call() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        /// The cap, as an op count.
        const CAP: u64 = COMMIT_WALK_OPS_MAX as u64;
        /// One op past the cap, so the first call must stop short and the
        /// second must have something left to finish.
        const OPS: u64 = CAP + 1;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        // Replica 1 of 3 at view 0: a backup, so `on_replicate` journals each
        // prepare without the primary's pipeline commit path running under it.
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

        for request in 1..=OPS {
            let prepare = md
                .prepare_request(create_stream_request(
                    CLIENT,
                    request,
                    &format!("s{request}"),
                ))
                .expect("CreateStream is client-allowed");
            md.on_replicate(prepare).await;
        }
        let journal = md.journal.as_ref().unwrap();
        assert_eq!(journal.last_op(), Some(OPS), "every op must be resident");
        assert_eq!(consensus.commit_min(), 0, "no commit heartbeat has landed");

        // What a repair window or a rejoin leaves behind: the whole run
        // committed by the group and resident here, none of it walked.
        consensus.advance_commit_max(OPS);

        assert_eq!(md.commit_journal().await, CommitWalkStop::WalkCap);
        assert_eq!(
            consensus.commit_min(),
            CAP,
            "one call walked the whole backlog; the pump is blocked for as long \
             as the resident run is, however long that is"
        );

        assert_eq!(md.commit_journal().await, CommitWalkStop::CaughtUp);
        assert_eq!(
            consensus.commit_min(),
            OPS,
            "the walk did not resume where it stopped, so `commit_min` is pinned \
             below `commit_max` with no other re-driver"
        );
    }

    /// The walk leaves the pipeline head to `on_ack` and says so.
    ///
    /// A primary-elect inherits a committed head at `commit_min + 1` and the
    /// walk moves nothing over it. `tick_metadata`'s stall detector truncates
    /// on `BodyMissing` only, so this has to come back as `PipelineHead` with
    /// the prepare still journaled for the pipeline driver to read.
    #[compio::test]
    async fn given_committed_pipeline_head_at_next_op_when_walking_should_stop_as_pipeline_head() {
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

        let prepare = md
            .prepare_request(create_stream_request(CLIENT, 1, "s1"))
            .expect("CreateStream is client-allowed");
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        // Committed with no driver to apply it: the inherited-head shape.
        consensus.advance_commit_max(1);

        assert_eq!(md.commit_journal().await, CommitWalkStop::PipelineHead);
        assert_eq!(
            consensus.commit_min(),
            0,
            "the walk applied the pipeline head"
        );
        assert!(
            md.journal.as_ref().unwrap().handle().header(1).is_some(),
            "the head's prepare must stay journaled for the pipeline driver"
        );

        md.resume_stranded_commits().await;
        assert_eq!(
            consensus.commit_min(),
            1,
            "the pipeline driver commits the head the walk left to it"
        );
    }

    /// A committed op with no resident header is a plain gap: the walk names it
    /// as `HeaderMissing`, never as a missing body that needs truncating.
    #[compio::test]
    async fn given_committed_op_without_header_when_walking_should_stop_as_header_missing() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const RESIDENT_OPS: u64 = 2;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        // A backup, so no pipeline head caps the walk.
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

        for request in 1..=RESIDENT_OPS {
            let prepare = md
                .prepare_request(create_stream_request(
                    CLIENT,
                    request,
                    &format!("s{request}"),
                ))
                .expect("CreateStream is client-allowed");
            md.on_replicate(prepare).await;
        }
        consensus.advance_commit_max(RESIDENT_OPS + 1);

        assert_eq!(
            md.commit_journal().await,
            CommitWalkStop::HeaderMissing {
                op: RESIDENT_OPS + 1
            }
        );
        assert_eq!(
            consensus.commit_min(),
            RESIDENT_OPS,
            "the walk applies the resident run before stopping at the gap"
        );
    }

    /// Bus that records every frame sent to a replica.
    #[derive(Debug, Default)]
    struct ReplicaSendSpyBus {
        replica_sends: RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>,
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for ReplicaSendSpyBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: impl Into<BusMessage>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        async fn send_to_replica(
            &self,
            replica: u8,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.replica_sends.borrow_mut().push((replica, data));
            Ok(())
        }
        fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
    }

    /// `(target replica, op, prepare checksum)` of every `PrepareOk` sent on `bus`.
    fn sent_prepare_oks(bus: &ReplicaSendSpyBus) -> Vec<(u8, u64, u128)> {
        bus.replica_sends
            .borrow()
            .iter()
            .filter_map(|(replica, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| (*replica, header.op, header.prepare_checksum))
            })
            .collect()
    }

    /// The ops of the prepares sent to `replica`, in send order.
    fn sent_prepare_ops(bus: &ReplicaSendSpyBus, replica: u8) -> Vec<u64> {
        bus.replica_sends
            .borrow()
            .iter()
            .filter(|(target, _)| *target == replica)
            .filter_map(|(_, frame)| {
                let header = frame.as_slice().get(..size_of::<PrepareHeader>())?;
                bytemuck::checked::try_from_bytes::<PrepareHeader>(header)
                    .ok()
                    .filter(|header| header.command == Command::Prepare)
                    .map(|header| header.op)
            })
            .collect()
    }

    /// A backup re-acks the retransmit of an op it already applied.
    ///
    /// A `DoViewChange` caps its commit at the sender's head, so a backup that
    /// learned a commit without holding the op hands the next primary a lower
    /// commit than the cluster reached. That primary restarts the op as
    /// uncommitted and retransmits it. With the third replica down, this backup's
    /// ack is the only quorum the op can get: dropping the retransmit keeps the
    /// primary below its recovery barrier, refusing every metadata request.
    ///
    /// A prepare naming a different prepare at the applied op is no retransmit:
    /// it contradicts a committed op, so it is refused and counted.
    #[compio::test]
    async fn given_an_applied_op_when_the_primary_retransmits_it_should_ack_only_the_journaled_copy()
     {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const PRIMARY: u8 = 0;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal =
            journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap();
        // Replica 1 of 3 at view 0: a backup of replica 0.
        let consensus = VsrConsensus::new(
            1,
            1,
            3,
            server_common::sharding::METADATA_GROUP,
            ReplicaSendSpyBus::default(),
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

        let prepare = md
            .prepare_request(create_stream_request(CLIENT, 1, "s1"))
            .expect("CreateStream is client-allowed");
        let op = prepare.header().op;
        let checksum = prepare.header().checksum;
        let conflicting = consensus::seal_prepare_checksum(prepare.clone().transmute_header(
            |old, header: &mut PrepareHeader| {
                *header = old;
                header.timestamp += 1;
            },
        ));
        assert!(
            verify_prepare_integrity(conflicting.header(), conflicting.as_slice()).is_ok(),
            "the conflicting prepare must reach the commit fence, not the integrity check"
        );
        md.on_replicate(prepare.clone()).await;
        consensus.advance_commit_max(op);
        md.commit_journal().await;
        assert_eq!(
            consensus.commit_min(),
            op,
            "the backup must apply the op first"
        );
        consensus.message_bus().replica_sends.borrow_mut().clear();

        md.on_replicate(prepare).await;
        assert_eq!(
            sent_prepare_oks(consensus.message_bus()),
            vec![(PRIMARY, op, checksum)],
            "the retransmit of an applied op went unacked, so a primary that restarted \
             it as uncommitted never reaches quorum while the third replica is down"
        );
        consensus.message_bus().replica_sends.borrow_mut().clear();

        md.on_replicate(conflicting).await;
        assert!(
            sent_prepare_oks(consensus.message_bus()).is_empty(),
            "a prepare that conflicts with the journaled op must never be acked"
        );
        assert_eq!(
            consensus.applied_log_contradictions(),
            1,
            "a prepare naming another prepare at an applied op must be reported, not \
             skipped as an old retransmit"
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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

    #[compio::test]
    async fn given_committed_receipts_when_checkpoint_stops_at_each_boundary_should_reopen_them() {
        const CLIENT: u128 = 1;
        for boundary in [
            CheckpointBoundary::TableCaptured,
            CheckpointBoundary::TemporaryCreated,
            CheckpointBoundary::Written,
            CheckpointBoundary::FileSynced,
            CheckpointBoundary::Renamed,
            CheckpointBoundary::DirectorySynced,
            CheckpointBoundary::SuperblockWritten,
            CheckpointBoundary::WalDrained,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
            std::fs::create_dir_all(&metadata_directory).unwrap();
            let journal = PrepareJournal::open(&metadata_directory.join("journal.wal"), 0)
                .await
                .unwrap();
            let superblock = journal::superblock::PingPongSuperblock::open(&metadata_directory)
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
            let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                Some(Rc::new(superblock)),
                TestMux::new((session_users(), (StreamsInner::new().into(), ()))),
                Some(directory.path().to_path_buf()),
            );
            let registration = metadata
                .submit_register_in_process(
                    CLIENT,
                    0,
                    RegisterRequest {
                        credential: CredentialProof::account(1, 1),
                        bind_verifier: [0x5a; 32],
                    },
                )
                .await
                .unwrap();
            let consensus = metadata.consensus.as_ref().unwrap();
            let journal = metadata.journal.as_ref().unwrap();
            metadata.set_checkpoint_margin(usize::MAX);
            metadata.checkpoint_if_needed(consensus, journal).await;
            metadata.set_checkpoint_margin(0);
            for request in 1..=3 {
                let message = create_stream_request(CLIENT, request, &format!("receipt-{request}"))
                    .transmute_header(|old, header: &mut RoutedRequestHeader| {
                        *header = old;
                        header.session = registration.epoch;
                    });
                let reply = metadata.submit_request_in_process(message).await.unwrap();
                assert_eq!(
                    iggy_binary_protocol::result_code(
                        &reply.as_slice()[size_of::<ReplyHeader>()..]
                    ),
                    Some(0)
                );
            }
            let expected = metadata
                .client_table
                .borrow()
                .get_reply(CLIENT)
                .unwrap()
                .clone()
                .into_wire_bytes()
                .as_slice()
                .to_vec();
            let committed = consensus.commit_min();
            metadata.set_checkpoint_margin(usize::MAX);
            STOP_CHECKPOINT_AFTER.set(Some(boundary));
            if boundary == CheckpointBoundary::TableCaptured {
                metadata.checkpoint_if_needed(consensus, journal).await;
            } else {
                metadata.state_transfer_offer().await.unwrap();
                let held = SNAPSHOT_IO.lock().await;
                let mut checkpoint = Box::pin(metadata.checkpoint_if_needed(consensus, journal));
                assert!(futures::poll!(&mut checkpoint).is_pending());
                assert!(
                    matches!(
                        metadata.state_transfer_offer().await,
                        Err(StateTransferUnavailable::SnapshotReplaced)
                    ),
                    "{boundary:?}: the snapshot write must refuse an offer"
                );
                drop(held);
                checkpoint.await;
                assert!(
                    metadata.transfer_offer_cache.borrow().is_none(),
                    "{boundary:?}: an offer cached before the snapshot write must not outlive it"
                );
            }
            assert!(
                STOP_CHECKPOINT_AFTER.get().is_none(),
                "{boundary:?} must be reached"
            );
            drop(metadata);
            let recovered = crate::impls::recovery::recover::<TestMux>(
                directory.path(),
                crate::impls::recovery::ReplicaIdentity {
                    cluster: 1,
                    replica_id: 0,
                    replica_count: 1,
                },
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("{boundary:?}: {error}"));
            let consensus::client_table::RequestStatus::Duplicate(reply) = recovered
                .client_table
                .check_request(CLIENT, registration.epoch, 3, Operation::CreateStream)
            else {
                panic!("{boundary:?}: latest unresolved result must remain replayable");
            };
            assert_eq!(
                reply.into_wire_bytes().as_slice(),
                expected,
                "{boundary:?}: latest receipt identity and body must survive reopen"
            );
            assert_eq!(recovered.client_table.get_user_id(CLIENT), Some(0));
            for request in 1..3 {
                assert!(
                    matches!(
                        recovered.client_table.check_request(
                            CLIENT,
                            registration.epoch,
                            request,
                            Operation::CreateStream
                        ),
                        consensus::client_table::RequestStatus::Duplicate(_)
                            | consensus::client_table::RequestStatus::AlreadyApplied { .. }
                    ),
                    "{boundary:?}: earlier acknowledged request {request} must not execute again"
                );
            }
            assert_eq!(recovered.client_table.get_watermark(CLIENT), Some(3));
            assert!(
                recovered
                    .last_journaled_op
                    .or_else(|| recovered
                        .snapshot
                        .as_ref()
                        .map(IggySnapshot::sequence_number))
                    .is_some_and(|op| op >= committed),
                "{boundary:?}: reopened state must cover the acknowledged prefix"
            );
        }
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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
    /// CHECKPOINT_MARGIN`, i.e. op `SLOTS - MARGIN = 960`) together, and
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
        /// Few enough that the fill below stays short.
        const SLOTS: usize = 1024;
        /// One op below the forced-checkpoint trigger: the journal holds
        /// `SLOTS` slots and forces a checkpoint when 64 or fewer remain.
        const FILL: u64 = 960;

        let dir = tempfile::tempdir().unwrap();
        // Bootstrap creates the metadata dir; the coordinator's snapshot
        // persist expects it to exist.
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal = journal::prepare_journal::PrepareJournal::open_with_slots(
            &dir.path().join("journal.wal"),
            0,
            SLOTS,
        )
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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

    /// Each submit claims its op and then waits on `journal_gate`. A submit that
    /// arrives after a release must not take the gate ahead of the submits that
    /// wait for it: a backup takes only the next op, so it drops a prepare that
    /// comes early, and the op waits for a retransmit. The test holds the gate,
    /// as a slow WAL append does. The submits run in spawned tasks, as in the
    /// server, because a `join!` driver polls its futures in arrival order and
    /// hides the overtake.
    #[compio::test]
    async fn given_a_submit_waiting_on_the_journal_gate_when_another_submit_arrives_should_send_in_op_order()
     {
        let (_dir, metadata) = primary_on_spy_bus([1, 2]).await;
        let submit = |client: u128| {
            let metadata = Rc::clone(&metadata);
            compio::runtime::spawn(async move {
                metadata
                    .on_request(create_stream_request(client, 1, &format!("s{client}")))
                    .await;
            })
        };
        let consensus = metadata.consensus.as_ref().unwrap();

        let held = metadata.journal_gate.try_acquire().unwrap();
        let waiting = submit(1);
        yield_now().await;
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the first submit claims op 1 and waits on the gate"
        );
        let late = submit(2);
        drop(held);
        waiting.await.unwrap();
        late.await.unwrap();

        for backup in 1..=2 {
            assert_eq!(
                sent_prepare_ops(consensus.message_bus(), backup),
                [1, 2],
                "backup {backup} takes only the next op"
            );
        }
    }

    /// An in-process submit runs `on_replicate` on the caller's task, so it can
    /// sit on `journal_gate` while the shard adopts a `StartView`. Granted the
    /// gate afterwards, its prepare is the next op of the adopted journal and
    /// the follower path would take it for one of the new view. A checkpoint it
    /// ran first would keep the reconcile of the view waiting on the gate.
    #[compio::test]
    async fn given_a_submit_waiting_on_the_journal_gate_when_a_view_is_adopted_meanwhile_should_drop_the_prepare()
     {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
        let consensus = metadata.consensus.as_ref().unwrap();
        commit_on_spy_bus_primary(&metadata, create_stream_request(CLIENT, 1, "committed")).await;
        assert_eq!(
            consensus.commit_min(),
            1,
            "op 1 gives a checkpoint a subject"
        );
        // Each prepare from here on finds the journal at the margin.
        metadata.set_checkpoint_margin(usize::MAX);

        let held = metadata.journal_gate.try_acquire().unwrap();
        let waiting = compio::runtime::spawn({
            let metadata = Rc::clone(&metadata);
            async move {
                metadata
                    .submit_request_in_process(create_stream_request(CLIENT, 2, "stale"))
                    .await
            }
        });
        yield_now().await;
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the submit claims op 2 and waits on the gate"
        );
        adopt_view(consensus, 1, 1);
        drop(held);
        let reply = waiting.await.unwrap().unwrap();

        assert_eq!(
            iggy_binary_protocol::result_code(reply.body()),
            Some(IggyError::TransientNotCommitted.as_code()),
            "the caller learns the outcome is unknown and retries"
        );
        assert!(
            metadata
                .journal
                .as_ref()
                .unwrap()
                .handle()
                .header(2)
                .is_none(),
            "a prepare of view 0 must not enter the journal of view 1"
        );
        assert_eq!(
            metadata.coordinator.as_ref().unwrap().last_checkpoint().0,
            0,
            "a prepare of view 0 must not run a checkpoint in view 1"
        );
        for backup in 1..=2 {
            assert_eq!(
                sent_prepare_ops(consensus.message_bus(), backup),
                [1],
                "backup {backup} must not be sent a prepare of the view that ended"
            );
        }
    }

    /// The submit holds `journal_gate` across the forced checkpoint, and the
    /// snapshot write yields to the shard. A `StartView` adopted meanwhile makes
    /// this replica a backup of the new view before the prepare is appended.
    #[compio::test]
    async fn given_a_submit_inside_its_checkpoint_when_a_view_is_adopted_meanwhile_should_drop_the_prepare()
     {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
        let consensus = metadata.consensus.as_ref().unwrap();
        commit_on_spy_bus_primary(&metadata, create_stream_request(CLIENT, 1, "committed")).await;
        assert_eq!(
            consensus.commit_min(),
            1,
            "op 1 gives the checkpoint a subject"
        );
        // Each prepare from here on finds the journal at the margin.
        metadata.set_checkpoint_margin(usize::MAX);

        let checkpointing = compio::runtime::spawn({
            let metadata = Rc::clone(&metadata);
            async move {
                metadata
                    .submit_request_in_process(create_stream_request(CLIENT, 2, "stale"))
                    .await
            }
        });
        yield_now().await;
        assert!(
            metadata.checkpoint_lock.try_acquire().is_none(),
            "the submit must be inside its checkpoint"
        );
        adopt_view(consensus, 1, 1);
        let reply = checkpointing.await.unwrap().unwrap();

        assert_eq!(
            iggy_binary_protocol::result_code(reply.body()),
            Some(IggyError::TransientNotCommitted.as_code()),
            "the caller learns the outcome is unknown and retries"
        );
        assert!(
            metadata
                .journal
                .as_ref()
                .unwrap()
                .handle()
                .header(2)
                .is_none(),
            "a prepare of view 0 must not enter the journal of view 1"
        );
        assert_eq!(
            consensus.sequencer().current_sequence(),
            1,
            "a prepare of view 0 must not move the sequencer of view 1"
        );
        for backup in 1..=2 {
            assert_eq!(
                sent_prepare_ops(consensus.message_bus(), backup),
                [1],
                "backup {backup} must not be sent a prepare of the view that ended"
            );
        }
    }

    /// A prepare granted the gate during a view change still lands, on the
    /// follower path when another replica leads the next view. That view can be
    /// adopted while the prepare appends, and its op is not one of the view's log.
    #[compio::test]
    async fn given_a_submit_inside_its_append_when_a_view_is_adopted_meanwhile_should_keep_the_sequencer_of_the_view()
     {
        const CLIENT: u128 = 1;
        const NEXT_VIEW: u32 = 2;
        let (_dir, metadata) = primary_on_spy_bus([CLIENT]).await;
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();

        let held = metadata.journal_gate.try_acquire().unwrap();
        let appending = compio::runtime::spawn({
            let metadata = Rc::clone(&metadata);
            async move {
                metadata
                    .submit_request_in_process(create_stream_request(CLIENT, 1, "stale"))
                    .await
            }
        });
        yield_now().await;
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the submit claims op 1 and waits on the gate"
        );
        // A primary joins only a vote of a replica that is two views ahead.
        let vote = StartViewChangeHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 1,
            size: u32::try_from(size_of::<StartViewChangeHeader>()).unwrap(),
            view: NEXT_VIEW,
            release: 0,
            command: Command::StartViewChange,
            replica: 1,
            reserved_frame: [0; 66],
            group: server_common::sharding::METADATA_GROUP,
            partition_incarnation: 0,
            reserved: [0; 112],
        };
        consensus.handle_start_view_change(PlaneKind::Metadata, &vote);
        assert_eq!(
            (consensus.status(), consensus.view()),
            (Status::ViewChange, NEXT_VIEW),
            "the replica must be changing to view {NEXT_VIEW}"
        );
        drop(held);
        yield_now().await;
        assert!(
            journal.handle().header(1).is_none(),
            "the write must still be in flight"
        );
        adopt_view(consensus, NEXT_VIEW, 0);
        appending.await.unwrap().unwrap();

        assert!(
            journal.handle().header(1).is_some(),
            "the write in flight lands, for the reconcile of the view to remove"
        );
        assert_eq!(
            consensus.sequencer().current_sequence(),
            0,
            "a prepare of view 0 must not move the sequencer of view {NEXT_VIEW}"
        );
    }

    type JobMetadata =
        IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux, CheckpointSuperblock>;
    type JobHandles = Rc<RefCell<Vec<compio::runtime::JoinHandle<()>>>>;

    /// Journal slots of the checkpoint job tests: a job starts at
    /// [`JOB_TRIGGER_OP`] committed ops, the margin path at [`JOB_MARGIN_OP`]
    /// journaled ones.
    const JOB_SLOTS: usize = 256;
    const JOB_TRIGGER_OP: u64 = JOB_SLOTS as u64 / CHECKPOINT_JOB_TRIGGER_DIVISOR;
    const JOB_MARGIN_OP: u64 = (JOB_SLOTS - SnapshotCoordinator::<()>::CHECKPOINT_MARGIN) as u64;
    const JOB_RETRY_OPS: u64 = JOB_SLOTS as u64 / CHECKPOINT_JOB_RETRY_DIVISOR;
    const JOB_CLIENT: u128 = 1;
    /// Polls before a parked job counts as hung. Each one waits a millisecond.
    const JOB_POLL_BUDGET: usize = 10_000;

    /// Replica `replica` of `replica_count` on a [`JOB_SLOTS`] journal, with a
    /// session for [`JOB_CLIENT`] and a spawner that keeps each job's task so the
    /// test can await it.
    #[allow(clippy::future_not_send)]
    async fn job_metadata(
        dir: &Path,
        replica: u8,
        replica_count: u8,
        superblock: Option<Rc<CheckpointSuperblock>>,
    ) -> (Rc<JobMetadata>, JobHandles) {
        let metadata_dir = dir.join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let journal =
            PrepareJournal::open_with_slots(&metadata_dir.join("journal.wal"), 0, JOB_SLOTS)
                .await
                .unwrap();
        let consensus = VsrConsensus::new(
            1,
            replica,
            replica_count,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let md = Rc::new(IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            superblock,
            TestMux::default(),
            Some(dir.to_path_buf()),
        ));
        md.client_table
            .borrow_mut()
            .commit_register(JOB_CLIENT, 0, [0x5a; 32], register_reply(JOB_CLIENT, 1))
            .unwrap();
        let jobs = JobHandles::default();
        let spawned = Rc::clone(&jobs);
        let weak = Rc::downgrade(&md);
        md.set_checkpoint_spawner(Some(Rc::new(move |job| {
            let weak = weak.clone();
            spawned
                .borrow_mut()
                .push(compio::runtime::spawn(async move {
                    if let Some(md) = weak.upgrade() {
                        md.run_checkpoint_job(job).await;
                    }
                }));
        })));
        COMPLETED_CHECKPOINTS.set(0);
        LAST_COMPLETED_CHECKPOINT.set(None);
        (md, jobs)
    }

    /// Await every job started so far and return how many there were.
    #[allow(clippy::future_not_send)]
    async fn await_jobs(jobs: &JobHandles) -> usize {
        let started: Vec<_> = jobs.borrow_mut().drain(..).collect();
        let count = started.len();
        for job in started {
            job.await.unwrap();
        }
        count
    }

    fn job_prepare(md: &JobMetadata, op: u64) -> Message<PrepareHeader> {
        md.prepare_request(create_stream_request(JOB_CLIENT, op, &format!("s{op}")))
            .unwrap()
    }

    fn primary_prepare(md: &JobMetadata, op: u64) -> Message<PrepareHeader> {
        let prepare = job_prepare(md, op);
        md.consensus
            .as_ref()
            .unwrap()
            .pipeline_message(PlaneKind::Metadata, &prepare);
        prepare
    }

    #[allow(clippy::future_not_send)]
    async fn ack_self(md: &JobMetadata) {
        let mut loopback = Vec::new();
        md.consensus
            .as_ref()
            .unwrap()
            .drain_loopback_into(&mut loopback);
        for message in loopback {
            md.on_ack(message.try_into_typed::<PrepareOkHeader>().unwrap())
                .await;
        }
    }

    /// Replicate and commit `ops` one by one on a single-replica primary.
    #[allow(clippy::future_not_send)]
    async fn commit_on_primary(md: &JobMetadata, ops: std::ops::RangeInclusive<u64>) {
        for op in ops {
            md.on_replicate(primary_prepare(md, op)).await;
            ack_self(md).await;
        }
    }

    fn spawn_replicate(md: &Rc<JobMetadata>, op: u64) -> compio::runtime::JoinHandle<()> {
        let prepare = primary_prepare(md, op);
        let md = Rc::clone(md);
        compio::runtime::spawn(async move { md.on_replicate(prepare).await })
    }

    fn journal_floor(md: &JobMetadata) -> u64 {
        md.journal.as_ref().unwrap().handle().snapshot_op()
    }

    fn journaled(md: &JobMetadata, op: u64) -> bool {
        md.journal
            .as_ref()
            .unwrap()
            .handle()
            .header(usize::try_from(op).unwrap())
            .is_some()
    }

    #[compio::test]
    async fn given_half_the_wal_committed_when_the_job_write_is_parked_should_keep_appending_and_trim_once()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        let coordinator = md.coordinator.as_ref().unwrap();
        let held = SNAPSHOT_IO.lock().await;

        commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
        assert!(jobs.borrow().is_empty(), "no job below half the WAL");
        commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_MARGIN_OP - 1).await;

        assert_eq!(
            md.consensus.as_ref().unwrap().commit_min(),
            JOB_MARGIN_OP - 1
        );
        assert_eq!(jobs.borrow().len(), 1, "one job, started at half the WAL");
        assert!(
            md.checkpoint_lock.try_acquire().is_none(),
            "the parked job holds its lease"
        );
        assert_eq!(journal_floor(&md), 0);
        drop(held);
        assert_eq!(await_jobs(&jobs).await, 1);

        assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
        assert_eq!(coordinator.superblock_checkpoint_op.get(), JOB_TRIGGER_OP);
        assert!(!coordinator.checkpoint_pending.get());
        let (snapshot, _) = IggySnapshot::load(&coordinator.snapshot_path()).unwrap();
        assert_eq!(snapshot.sequence_number(), JOB_TRIGGER_OP);
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
        );
    }

    #[allow(clippy::future_not_send)]
    async fn persist_initial_snapshot(md: &JobMetadata) {
        IggySnapshot::create(&TestMux::default(), 0, 1)
            .unwrap()
            .persist(&md.coordinator.as_ref().unwrap().snapshot_path())
            .await
            .unwrap();
    }

    #[compio::test]
    async fn given_a_parked_checkpoint_job_when_offering_should_refuse_until_the_job_ends() {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        persist_initial_snapshot(&md).await;
        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
        assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");

        assert!(
            matches!(
                md.state_transfer_offer().await,
                Err(StateTransferUnavailable::SnapshotReplaced)
            ),
            "the job trims the WAL past the snapshot that is still on disk"
        );
        drop(held);
        assert_eq!(await_jobs(&jobs).await, 1);

        let offer = md.state_transfer_offer().await.unwrap();
        assert_eq!(offer.snapshot_seq, JOB_TRIGGER_OP);
        assert!(journal_floor(&md) <= offer.snapshot_seq);
    }

    /// A requester that pulls an offer installs its snapshot and then repairs
    /// from the op after it. A job that trims the WAL to its own, newer
    /// checkpoint meanwhile leaves the requester nothing to repair from, and it
    /// starts over. The shard drops a pulled offer after a short time, which
    /// can be before the job ends its writes and before the requester asks.
    #[compio::test]
    async fn given_an_offer_held_at_the_capture_when_it_is_dropped_during_the_job_should_leave_the_wal_to_the_margin()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        let coordinator = md.coordinator.as_ref().unwrap();
        persist_initial_snapshot(&md).await;
        let served = md.state_transfer_offer().await.unwrap();
        let served_op = served.snapshot_seq;
        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
        assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");

        drop(served);
        drop(held);
        assert_eq!(await_jobs(&jobs).await, 1);

        assert_eq!(coordinator.superblock_checkpoint_op.get(), JOB_TRIGGER_OP);
        assert_eq!(
            journal_floor(&md),
            served_op,
            "the requester repairs from the op after the snapshot that it pulled"
        );
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);

        commit_on_primary(&md, JOB_TRIGGER_OP + 1..=JOB_MARGIN_OP + 1).await;

        assert_eq!(
            journal_floor(&md),
            JOB_TRIGGER_OP - 1,
            "the journal must not run out of slots for a requester"
        );
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Job, CheckpointTrimmer::Margin))
        );
        assert!(jobs.borrow().is_empty(), "the margin trims without a job");
    }

    /// An offer that nobody holds at the capture needs no WAL.
    #[compio::test]
    async fn given_an_offer_dropped_before_the_capture_when_a_checkpoint_job_ends_should_trim_the_wal()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        persist_initial_snapshot(&md).await;
        drop(md.state_transfer_offer().await.unwrap());

        commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
        assert_eq!(await_jobs(&jobs).await, 1);

        assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
        );
    }

    /// The journal already trimmed past the snapshot of this offer, so its
    /// requester cannot repair here any more, and a kept WAL gives it nothing.
    #[compio::test]
    async fn given_a_held_offer_below_the_journal_floor_when_a_checkpoint_job_ends_should_trim_the_wal()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        persist_initial_snapshot(&md).await;
        let served = md.state_transfer_offer().await.unwrap();
        commit_on_primary(&md, 1..=JOB_TRIGGER_OP).await;
        assert_eq!(await_jobs(&jobs).await, 1);
        commit_on_primary(&md, JOB_TRIGGER_OP + 1..=JOB_MARGIN_OP + 1).await;
        assert!(served.snapshot_seq < journal_floor(&md));

        commit_on_primary(&md, JOB_MARGIN_OP + 2..=2 * JOB_TRIGGER_OP).await;
        assert_eq!(await_jobs(&jobs).await, 1);

        assert_eq!(journal_floor(&md), 2 * JOB_TRIGGER_OP - 1);
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Job, CheckpointTrimmer::Job))
        );
    }

    #[compio::test]
    async fn given_an_offer_read_in_flight_when_a_checkpoint_job_captures_should_refuse_the_offer()
    {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        persist_initial_snapshot(&md).await;
        commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
        let mut offer = Box::pin(md.state_transfer_offer());
        assert!(
            futures::poll!(&mut offer).is_pending(),
            "the read must suspend"
        );
        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
        assert_eq!(jobs.borrow().len(), 1, "the job captured during the read");

        assert!(matches!(
            offer.await,
            Err(StateTransferUnavailable::SnapshotReplaced)
        ));
        assert!(
            md.transfer_offer_cache.borrow().is_none(),
            "a refused offer must not reach the cache"
        );
        drop(held);
        assert_eq!(await_jobs(&jobs).await, 1);
    }

    /// The snapshot file holds the live state at its op, with the client table and
    /// change log of that commit. Call it before anything commits past the op.
    fn assert_snapshot_holds_live_state(md: &JobMetadata) {
        let path = md.coordinator.as_ref().unwrap().snapshot_path();
        let file = std::fs::read(&path).unwrap();
        let (payload, _) = file.split_at(file.len() - SNAPSHOT_TRAILER_LEN);
        let (loaded, _) = IggySnapshot::load(&path).unwrap();
        assert_eq!(
            loaded.sequence_number(),
            md.consensus.as_ref().unwrap().commit_min()
        );
        let mut expected =
            IggySnapshot::create(&*md.mux_stm, loaded.sequence_number(), loaded.created_at())
                .unwrap();
        expected.snapshot.client_table = Some(md.client_table.borrow().to_snapshot());
        expected.snapshot.change_log = md.change_log.snapshot();
        assert_eq!(
            payload,
            expected.encode().unwrap(),
            "the job captured a state other than its commit's"
        );
    }

    /// A job stopped at any crash boundary releases its lease, and an unstopped one
    /// captures exactly the state of the commit that started it.
    #[compio::test]
    async fn given_a_job_when_it_stops_at_each_boundary_should_release_its_lease() {
        for boundary in [
            None,
            Some(CheckpointBoundary::TableCaptured),
            Some(CheckpointBoundary::TemporaryCreated),
            Some(CheckpointBoundary::Written),
            Some(CheckpointBoundary::FileSynced),
            Some(CheckpointBoundary::Renamed),
            Some(CheckpointBoundary::DirectorySynced),
            Some(CheckpointBoundary::SuperblockWritten),
            Some(CheckpointBoundary::WalDrained),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let superblock = Some(Rc::new(CheckpointSuperblock::default()));
            let (md, jobs) = job_metadata(dir.path(), 0, 1, superblock).await;
            let coordinator = md.coordinator.as_ref().unwrap();
            commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
            STOP_CHECKPOINT_AFTER.set(boundary);
            commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
            let started = await_jobs(&jobs).await;

            assert!(
                STOP_CHECKPOINT_AFTER.get().is_none(),
                "{boundary:?} must be reached"
            );
            assert!(
                md.checkpoint_lock.try_acquire().is_some(),
                "{boundary:?}: the lease must be released"
            );
            let captured = boundary != Some(CheckpointBoundary::TableCaptured);
            assert_eq!(started, usize::from(captured), "{boundary:?}");
            let paired = matches!(
                boundary,
                None | Some(CheckpointBoundary::SuperblockWritten | CheckpointBoundary::WalDrained)
            );
            assert_eq!(
                coordinator.superblock_checkpoint_op.get(),
                if paired { JOB_TRIGGER_OP } else { 0 },
                "{boundary:?}"
            );
            let trimmed = matches!(boundary, None | Some(CheckpointBoundary::WalDrained));
            assert_eq!(
                journal_floor(&md),
                if trimmed { JOB_TRIGGER_OP - 1 } else { 0 },
                "{boundary:?}"
            );
            assert_eq!(
                COMPLETED_CHECKPOINTS.get(),
                u64::from(boundary.is_none()),
                "{boundary:?}"
            );
            if boundary.is_none() {
                assert_snapshot_holds_live_state(&md);
            }
        }
    }

    #[compio::test]
    async fn given_a_parked_job_when_appends_reach_the_margin_should_wait_and_trim_to_the_job_checkpoint_once()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, 1..=JOB_MARGIN_OP).await;

        let waiter = spawn_replicate(&md, JOB_MARGIN_OP + 1);
        yield_now().await;
        assert!(
            !journaled(&md, JOB_MARGIN_OP + 1),
            "the append at the margin waits for the job"
        );
        drop(held);
        waiter.await.unwrap();

        assert!(journaled(&md, JOB_MARGIN_OP + 1));
        assert_eq!(
            journal_floor(&md),
            JOB_TRIGGER_OP - 1,
            "the waiter trims to the job's checkpoint instead of checkpointing inline"
        );
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
        assert_eq!(await_jobs(&jobs).await, 1);
        assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
        assert_eq!(
            COMPLETED_CHECKPOINTS.get(),
            1,
            "the job must not trim or log a checkpoint the waiter completed"
        );
        assert_eq!(
            LAST_COMPLETED_CHECKPOINT.get(),
            Some((CheckpointOrigin::Job, CheckpointTrimmer::Margin))
        );
        assert!(!md.coordinator.as_ref().unwrap().checkpoint_pending.get());
    }

    /// A job that fails backs off the next job, and a margin waiter that saw it fail
    /// appends into the margin. Nobody trims, and the next prepare at the margin
    /// checkpoints inline.
    #[compio::test]
    async fn given_a_failed_job_when_commits_continue_should_back_off_and_retry_inline_at_the_margin()
     {
        for superblock_fails in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let superblock = Rc::new(CheckpointSuperblock::default());
            let (md, jobs) = job_metadata(dir.path(), 0, 1, Some(Rc::clone(&superblock))).await;
            let coordinator = md.coordinator.as_ref().unwrap();
            let fail_next_job = || {
                if superblock_fails {
                    superblock.fail.set(true);
                } else {
                    STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::Written));
                }
            };
            let failed_checkpoint = |op| if superblock_fails { op } else { 0 };

            commit_on_primary(&md, 1..=JOB_TRIGGER_OP - 1).await;
            fail_next_job();
            commit_on_primary(&md, JOB_TRIGGER_OP..=JOB_TRIGGER_OP).await;
            assert_eq!(await_jobs(&jobs).await, 1);
            assert_eq!(
                coordinator.last_checkpoint().0,
                failed_checkpoint(JOB_TRIGGER_OP)
            );
            assert_eq!(coordinator.superblock_checkpoint_op.get(), 0);
            assert_eq!(journal_floor(&md), 0, "a failed job trims nothing");
            assert!(coordinator.checkpoint_pending.get());

            let retry_op = JOB_TRIGGER_OP + JOB_RETRY_OPS;
            commit_on_primary(&md, JOB_TRIGGER_OP + 1..=retry_op - 1).await;
            assert!(jobs.borrow().is_empty(), "no job during the backoff");
            let held = SNAPSHOT_IO.lock().await;
            commit_on_primary(&md, retry_op..=JOB_MARGIN_OP).await;
            assert_eq!(
                jobs.borrow().len(),
                1,
                "the next job starts after the backoff"
            );

            let waiter = spawn_replicate(&md, JOB_MARGIN_OP + 1);
            yield_now().await;
            fail_next_job();
            drop(held);
            waiter.await.unwrap();
            assert_eq!(await_jobs(&jobs).await, 1);
            assert!(
                journaled(&md, JOB_MARGIN_OP + 1),
                "the prepare appends into the margin"
            );
            assert_eq!(
                coordinator.last_checkpoint().0,
                failed_checkpoint(retry_op),
                "the waiter must not checkpoint inline after the job failed during its wait"
            );
            assert_eq!(journal_floor(&md), 0, "nobody trims after a failed job");
            assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);

            superblock.fail.set(false);
            ack_self(&md).await;
            commit_on_primary(&md, JOB_MARGIN_OP + 2..=JOB_MARGIN_OP + 2).await;
            assert_eq!(coordinator.last_checkpoint().0, JOB_MARGIN_OP + 1);
            assert_eq!(journal_floor(&md), JOB_MARGIN_OP, "the inline retry trims");
            assert_eq!(COMPLETED_CHECKPOINTS.get(), 1);
            assert_eq!(
                LAST_COMPLETED_CHECKPOINT.get(),
                Some((CheckpointOrigin::Inline, CheckpointTrimmer::Margin))
            );
            assert!(jobs.borrow().is_empty());
        }
    }

    /// The backoff of a failed job counts from the commit at its failure, so
    /// the commits during a slow write do not use it up.
    #[compio::test]
    async fn given_a_job_that_fails_after_more_commits_when_committing_should_back_off_from_the_failure()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 0, 1, None).await;
        let failed_at = JOB_TRIGGER_OP + JOB_RETRY_OPS;
        let retry_op = failed_at + JOB_RETRY_OPS;
        assert!(retry_op < JOB_MARGIN_OP, "the retry must stay a job");

        let held = SNAPSHOT_IO.lock().await;
        commit_on_primary(&md, 1..=failed_at).await;
        assert_eq!(jobs.borrow().len(), 1, "the job waits in its write");
        STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::Written));
        drop(held);
        assert_eq!(await_jobs(&jobs).await, 1);
        assert_eq!(md.coordinator.as_ref().unwrap().failed_jobs.get(), 1);

        commit_on_primary(&md, failed_at + 1..=retry_op - 1).await;
        assert!(jobs.borrow().is_empty(), "no job during the backoff");
        commit_on_primary(&md, retry_op..=retry_op).await;
        assert_eq!(await_jobs(&jobs).await, 1, "a job starts after the backoff");
    }

    /// A backup whose commits lag its journal starts one job per half WAL of
    /// commits, not one per commit while its journal stays over half full.
    #[compio::test]
    async fn given_commits_lagging_the_journal_when_committing_should_start_one_job_per_half_wal() {
        const HEAD: u64 = JOB_MARGIN_OP - 2;
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        for op in 1..=HEAD {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        let mut started = 0;
        for op in 1..=HEAD {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
            started += await_jobs(&jobs).await;
            if op < JOB_TRIGGER_OP {
                assert_eq!(started, 0, "op {op}: a full journal alone starts no job");
            }
            if op == JOB_TRIGGER_OP {
                assert_snapshot_holds_live_state(&md);
            }
        }
        assert_eq!(consensus.commit_min(), HEAD);
        assert_eq!(started, 1);
        assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
    }

    /// A journal at the margin gets its room from the next append, which trims
    /// or checkpoints inline, so a commit there starts no job.
    #[compio::test]
    async fn given_a_journal_at_the_margin_when_half_the_wal_commits_should_start_no_job() {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        for op in 1..=JOB_MARGIN_OP {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        for op in 1..=JOB_TRIGGER_OP {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
        }
        assert_eq!(consensus.commit_min(), JOB_TRIGGER_OP);
        assert!(jobs.borrow().is_empty(), "no job at the margin");
        assert!(!md.checkpoint_running());

        md.on_replicate(job_prepare(&md, JOB_MARGIN_OP + 1)).await;
        assert_eq!(
            journal_floor(&md),
            JOB_TRIGGER_OP - 1,
            "the next append checkpoints inline"
        );
        assert!(jobs.borrow().is_empty());
    }

    /// The install of a state transfer waits for a running job's write on the
    /// shard-0 pump, so no job starts while a transfer is armed.
    #[compio::test]
    async fn given_an_armed_state_transfer_when_half_the_wal_commits_should_start_no_job() {
        const HEAD: u64 = JOB_TRIGGER_OP + 1;
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        for op in 1..=HEAD {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        consensus.begin_state_transfer_await();
        for op in 1..=JOB_TRIGGER_OP {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
        }
        assert_eq!(consensus.commit_min(), JOB_TRIGGER_OP);
        assert!(
            jobs.borrow().is_empty(),
            "no job while the transfer is armed"
        );
        assert!(!md.checkpoint_running());

        consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
        consensus.advance_commit_max(HEAD);
        md.commit_journal().await;
        assert_eq!(
            await_jobs(&jobs).await,
            1,
            "a job starts once the transfer ended"
        );
    }

    /// A commit walk that waits for a prepare body keeps no borrow of the
    /// journal index, so the trim of a checkpoint job can rebuild the index
    /// meanwhile.
    #[compio::test]
    async fn given_a_commit_walk_reading_a_body_when_the_wal_is_trimmed_should_finish_the_walk() {
        const WALKED: u64 = 2;
        const HEAD: u64 = WALKED + 2;
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        for op in 1..=HEAD {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        consensus.advance_commit_max(WALKED);
        assert_eq!(md.commit_journal().await, CommitWalkStop::CaughtUp);

        consensus.advance_commit_max(HEAD);
        let mut walk = std::pin::pin!(md.commit_journal());
        assert!(
            futures::poll!(walk.as_mut()).is_pending(),
            "the walk waits for the body of its first op"
        );
        md.journal
            .as_ref()
            .unwrap()
            .handle()
            .drain(1..=WALKED - 1)
            .await
            .unwrap();

        assert_eq!(walk.await, CommitWalkStop::CaughtUp);
        assert_eq!(consensus.commit_min(), HEAD);
        assert_eq!(journal_floor(&md), WALKED - 1);
        assert!(
            jobs.borrow().is_empty(),
            "no job this far below half the WAL"
        );
    }

    /// A job that finds `checkpoint_lock` taken once it holds `journal_gate`
    /// leaves the trim to the holder.
    #[compio::test]
    async fn given_a_job_waiting_on_the_journal_gate_when_the_checkpoint_lock_is_taken_should_skip_its_trim()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        let coordinator = md.coordinator.as_ref().unwrap();
        for op in 1..=JOB_TRIGGER_OP {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        let gate = md.journal_gate.try_acquire().unwrap();
        for op in 1..=JOB_TRIGGER_OP {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
        }
        for _ in 0..JOB_POLL_BUDGET {
            if coordinator.superblock_checkpoint_op.get() == JOB_TRIGGER_OP {
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            coordinator.superblock_checkpoint_op.get(),
            JOB_TRIGGER_OP,
            "the job's checkpoint must be durable while it waits on the gate"
        );
        let newer_job = md.checkpoint_lock.try_acquire().unwrap();
        drop(gate);

        let mut ended = Box::pin(await_jobs(&jobs));
        let mut started = None;
        for _ in 0..JOB_POLL_BUDGET {
            if let std::task::Poll::Ready(count) = futures::poll!(&mut ended) {
                started = Some(count);
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(started, Some(1), "the job must end without the lock");
        assert_eq!(journal_floor(&md), 0, "the trim is the lock holder's");
        assert_eq!(COMPLETED_CHECKPOINTS.get(), 0);
        drop(newer_job);
    }

    /// The shard 0 pump keeps its appends for later only while a trim is
    /// pending. The gate serves its waiters in arrival order, so the job must
    /// report its trim before it waits: an append that arrives behind the
    /// waiting job waits for the whole rewrite.
    #[compio::test]
    async fn given_a_job_waiting_on_the_journal_gate_when_asked_should_report_a_pending_trim_until_it_ends()
     {
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        for op in 1..=JOB_TRIGGER_OP {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        let gate = md.journal_gate.try_acquire().unwrap();
        for op in 1..=JOB_TRIGGER_OP {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
        }
        assert!(
            !md.checkpoint_trim_pending(),
            "a held gate alone is no pending trim"
        );
        for _ in 0..JOB_POLL_BUDGET {
            if md.checkpoint_trim_pending() {
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            md.checkpoint_trim_pending(),
            "a job that waits for the gate must report its trim"
        );

        drop(gate);
        assert_eq!(await_jobs(&jobs).await, 1);
        assert!(!md.checkpoint_trim_pending());
        assert_eq!(journal_floor(&md), JOB_TRIGGER_OP - 1);
    }

    /// An install waits for a running job's durable part, then moves the floor past
    /// the job's checkpoint, so the job's later trim does nothing.
    #[compio::test]
    async fn given_a_running_job_when_a_snapshot_is_installed_should_wait_for_its_writes_and_skip_its_trim()
     {
        const HEAD: u64 = JOB_MARGIN_OP - 2;
        const INSTALLED: u64 = JOB_TRIGGER_OP + 10;
        let dir = tempfile::tempdir().unwrap();
        let (md, jobs) = job_metadata(dir.path(), 1, 3, None).await;
        let consensus = md.consensus.as_ref().unwrap();
        let coordinator = md.coordinator.as_ref().unwrap();
        for op in 1..=HEAD {
            md.on_replicate(job_prepare(&md, op)).await;
        }
        let held = SNAPSHOT_IO.lock().await;
        for op in 1..=JOB_TRIGGER_OP {
            consensus.advance_commit_max(op);
            md.commit_journal().await;
        }
        assert_eq!(jobs.borrow().len(), 1);

        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::create(&TestMux::default(), INSTALLED, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let bytes = snapshot.encode().unwrap();
        let install = compio::runtime::spawn({
            let md = Rc::clone(&md);
            async move {
                md.install_state_transfer(&bytes, table, INSTALLED, INSTALLED)
                    .await
            }
        });
        yield_now().await;
        assert!(
            md.journal_gate.try_acquire().is_none() && md.checkpoint_lock.try_acquire().is_none(),
            "the install holds the gate and waits for the job's lease"
        );
        drop(held);
        install.await.unwrap().unwrap();
        assert_eq!(await_jobs(&jobs).await, 1);

        assert_eq!(consensus.commit_min(), INSTALLED);
        assert_eq!(journal_floor(&md), INSTALLED);
        assert_eq!(coordinator.last_checkpoint().0, INSTALLED);
        assert_eq!(coordinator.superblock_checkpoint_op.get(), INSTALLED);
        let (snapshot, _) = IggySnapshot::load(&coordinator.snapshot_path()).unwrap();
        assert_eq!(
            snapshot.sequence_number(),
            INSTALLED,
            "the job's rename must not land over the installed snapshot"
        );
        assert_eq!(
            COMPLETED_CHECKPOINTS.get(),
            0,
            "the install moved the floor past the job's checkpoint"
        );
    }

    /// The primary of a three-replica group at view 0, on a bus that records its
    /// sends, with a session for each of `clients` and checkpoints armed.
    #[allow(clippy::future_not_send)]
    async fn primary_on_spy_bus(
        clients: impl IntoIterator<Item = u128>,
    ) -> (
        tempfile::TempDir,
        Rc<IggyMetadata<VsrConsensus<ReplicaSendSpyBus>, PrepareJournal, (), TestMux>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::impls::METADATA_DIR)).unwrap();
        let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            server_common::sharding::METADATA_GROUP,
            ReplicaSendSpyBus::default(),
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata = Rc::new(IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            Some(dir.path().to_path_buf()),
        ));
        for client in clients {
            metadata
                .client_table
                .borrow_mut()
                .commit_register(client, 0, [0x5a; 32], register_reply(client, 1))
                .unwrap();
        }
        (dir, metadata)
    }

    /// Commits `request` as the next op of that primary: its own ack and the
    /// ack of backup 1 make the quorum.
    #[allow(clippy::future_not_send)]
    async fn commit_on_spy_bus_primary(
        metadata: &IggyMetadata<VsrConsensus<ReplicaSendSpyBus>, PrepareJournal, (), TestMux>,
        request: Message<RoutedRequestHeader>,
    ) {
        const BACKUP: u8 = 1;
        let consensus = metadata.consensus.as_ref().unwrap();
        let prepare = metadata.prepare_request(request).unwrap();
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        metadata.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let own_ack = loopback
            .pop()
            .unwrap()
            .try_into_typed::<PrepareOkHeader>()
            .unwrap();
        let backup_ack: Message<PrepareOkHeader> = Message::<PrepareOkHeader>::new(size_of::<
            PrepareOkHeader,
        >())
        .transmute_header(|_, ack| {
            *ack = PrepareOkHeader {
                replica: BACKUP,
                ..*own_ack.header()
            };
        });
        metadata.on_ack(own_ack).await;
        metadata.on_ack(backup_ack).await;
    }

    /// Adopts `view` from its primary with a log that ends at the committed
    /// `head`, as the shard does on a `StartView`.
    fn adopt_view(consensus: &VsrConsensus<ReplicaSendSpyBus>, view: u32, head: u64) {
        let start_view = StartViewHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 1,
            size: u32::try_from(size_of::<StartViewHeader>()).unwrap(),
            view,
            release: 0,
            command: Command::StartView,
            replica: consensus.primary_index(view),
            reserved_frame: [0; 66],
            op: head,
            commit: head,
            group: server_common::sharding::METADATA_GROUP,
            partition_incarnation: 0,
            reserved: [0; 80],
            incarnation: 0,
        };
        let adopted = consensus.handle_start_view(PlaneKind::Metadata, &start_view, &[]);
        assert!(!adopted.is_empty(), "the replica adopts view {view}");
    }

    #[compio::test]
    async fn logout_waits_for_inherited_committed_history_before_checking_the_session() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const USER: u32 = 7;
        let metadata = metadata_plane();
        let consensus = metadata.consensus.as_ref().unwrap();
        metadata
            .client_table
            .borrow_mut()
            .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, SESSION))
            .unwrap();
        consensus.advance_commit_max(1);

        assert_eq!(
            metadata.submit_logout_in_process(CLIENT, SESSION, 2).await,
            Err(MetadataSubmitError::NotCaughtUp)
        );
        assert_eq!(
            metadata
                .submit_logout_in_process(CLIENT + 1, SESSION, 2)
                .await,
            Err(MetadataSubmitError::NotCaughtUp)
        );
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(SESSION)
        );
        assert!(consensus.pipeline_is_empty());
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
            md.client_table
                .borrow_mut()
                .commit_register(
                    client,
                    ACTING_USER,
                    [0x5a; 32],
                    register_reply(client, SESSION),
                )
                .unwrap();
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

    #[compio::test]
    async fn expired_logout_waits_for_catchup_and_preserves_registered_members() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata
            .submit_register_in_process(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap()
            .epoch;
        let consensus = metadata.consensus.as_ref().unwrap();
        consensus.advance_commit_max(consensus.commit_min() + 1);
        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, Some(session))
                .await,
            Err(MetadataSubmitError::NotCaughtUp)
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT)
                .len(),
            1
        );
    }

    #[compio::test]
    async fn expired_logout_removes_only_the_observed_session() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata
            .submit_register_in_process(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap()
            .epoch;
        let logout_op = metadata.consensus.as_ref().unwrap().commit_min() + 1;
        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, Some(session))
                .await
                .unwrap(),
            Some(logout_op)
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT),
            []
        );
        assert_eq!(metadata.client_table.borrow().get_epoch(CLIENT), None);
        assert_eq!(metadata.mux_stm.streams().consumer_group_count(), 1);
    }

    #[compio::test]
    async fn expired_logout_removes_a_member_without_a_client_table_entry() {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        let logout_op = metadata.consensus.as_ref().unwrap().commit_min() + 1;

        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, Some(session))
                .await
                .unwrap(),
            Some(logout_op)
        );
        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, None)
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT),
            []
        );
        assert_eq!(metadata.mux_stm.streams().consumer_group_count(), 1);
        assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), logout_op);
    }

    #[compio::test]
    async fn stale_logout_request_preserves_the_latest_receipt_and_group_membership() {
        const CLIENT: u128 = 1;
        const WATERMARK: u64 = 5;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        let committed_op = metadata.consensus.as_ref().unwrap().commit_min();
        let reply = committed_reply(CLIENT, WATERMARK, Operation::CreateStream, 0);
        assert_eq!(
            metadata
                .client_table
                .borrow_mut()
                .commit_reply(CLIENT, 7, reply),
            consensus::client_table::CommitReply::Cached
        );
        for (request, expected) in [
            (1, MetadataSubmitError::RequestTooOld),
            (WATERMARK, MetadataSubmitError::OperationMismatch),
        ] {
            assert_eq!(
                metadata
                    .submit_logout_in_process(CLIENT, session, request)
                    .await,
                Err(expected)
            );
            assert_eq!(
                metadata.client_table.borrow().get_epoch(CLIENT),
                Some(session)
            );
            assert_eq!(
                metadata
                    .mux_stm
                    .streams()
                    .consumer_group_memberships(CLIENT)
                    .len(),
                1
            );
            assert_eq!(
                metadata.consensus.as_ref().unwrap().commit_min(),
                committed_op,
                "a refused logout must not commit a metadata operation"
            );
        }
    }

    #[compio::test]
    async fn recovery_frontier_records_partition_revisions_separately_from_metadata_ops() {
        let (_dir, metadata) = metadata_with_group_member(1).await;
        for client in 2..=8 {
            metadata
                .submit_register_in_process(
                    client,
                    7,
                    RegisterRequest {
                        credential: CredentialProof::account(1, 1),
                        bind_verifier: [0x5a; 32],
                    },
                )
                .await
                .unwrap();
        }
        let revision = metadata.mux_stm.streams().read(|inner| inner.revision);
        let commit = metadata.consensus.as_ref().unwrap().commit_min();
        assert!(
            commit > revision,
            "registration churn must exceed the partition revision in this fixture"
        );
        metadata.seed_applied_frontier_from_consensus();
        assert_eq!(metadata.applied_frontier.get(), commit);
        assert_eq!(metadata.applied_frontier.recovered_revision(), revision);
    }

    #[compio::test]
    async fn given_group_logout_when_finalizing_through_its_end_should_release_the_session() {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        metadata
            .submit_logout_in_process(CLIENT, session, 1)
            .await
            .unwrap();
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .unwrap();

        assert_eq!(
            metadata
                .submit_session_finalization(identity.metadata_watermark - 1, Vec::new())
                .await
                .unwrap(),
            None,
            "a watermark below every session end has nothing to finalize"
        );
        assert!(
            metadata
                .submit_session_finalization(identity.metadata_watermark, Vec::new())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            metadata.client_table.borrow().ended_sessions().next(),
            None,
            "successful finalization must release the ended session's slot"
        );
    }

    #[compio::test]
    async fn given_a_partition_created_after_a_session_ended_when_finalizing_should_release_it() {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        metadata
            .submit_logout_in_process(CLIENT, session, 1)
            .await
            .unwrap();
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        // A partition created after the end starts its frontier above it, so
        // the finalization does not depend on it.
        let create = build_prepare_message(
            consensus,
            &RoutedRequestHeader::default(),
            Operation::CreatePartitionsWithAssignments,
            &PersistedCreatePartitionsRequest {
                request: WireCreatePartitionsRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                },
                partitions: vec![CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 2,
                    initial_view: 0,
                    members: Vec::new(),
                }],
            }
            .to_bytes(),
        );
        assert_eq!(
            gated_apply(metadata.mux_stm.as_ref(), create).unwrap().code,
            0
        );

        for (retired_through, expected_session) in [
            (identity.metadata_watermark - 1, Some(identity)),
            (identity.metadata_watermark, None),
        ] {
            let prepare = consensus::seal_prepare_capacity(
                build_prepare_message(
                    consensus,
                    &RoutedRequestHeader::default(),
                    Operation::FinalizeSession,
                    &FinalizeSessionRequest {
                        retired_through,
                        debt: Vec::new(),
                    }
                    .to_bytes(),
                ),
                metadata.client_table_capacity(),
            );
            apply_committed_prepare(
                metadata.mux_stm.as_ref(),
                &metadata.client_table,
                true,
                |_| {},
                prepare,
            );
            assert_eq!(
                metadata.client_table.borrow().ended_sessions().next(),
                expected_session,
                "finalization releases exactly the sessions that ended at or below its watermark"
            );
        }
    }

    #[compio::test]
    async fn given_finalization_with_debt_when_applied_should_record_it_for_the_named_incarnation_only()
     {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        metadata
            .submit_logout_in_process(CLIENT, session, 1)
            .await
            .unwrap();
        let identity = metadata
            .client_table
            .borrow()
            .ended_sessions()
            .next()
            .unwrap();
        let namespace = IggyNamespace::new(0, 0, 0);
        let created_revision = metadata
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &RoutedRequestHeader::default(),
                Operation::FinalizeSession,
                &FinalizeSessionRequest {
                    retired_through: identity.metadata_watermark,
                    debt: vec![
                        RetirementDebt {
                            namespace: namespace.inner(),
                            created_revision,
                        },
                        RetirementDebt {
                            namespace: namespace.inner(),
                            created_revision: created_revision + 1,
                        },
                    ],
                }
                .to_bytes(),
            ),
            metadata.client_table_capacity(),
        );
        let finalize_op = prepare.header().op;
        apply_committed_prepare(
            metadata.mux_stm.as_ref(),
            &metadata.client_table,
            true,
            |_| {},
            prepare,
        );
        assert_eq!(metadata.client_table.borrow().ended_sessions().next(), None);
        let streams = metadata.mux_stm.streams();
        assert_eq!(
            streams.retirement_debt_for(namespace, created_revision),
            Some(finalize_op)
        );
        assert_eq!(
            streams.retirement_debt_for(namespace, created_revision + 1),
            None
        );
        assert_eq!(
            streams.retirement_debts_stored_by(0, 1),
            (
                vec![(namespace, created_revision, finalize_op)],
                finalize_op
            )
        );
        assert_eq!(
            streams.retirement_debts_since(0, 0, 1),
            Some((
                vec![(namespace, created_revision, finalize_op)],
                finalize_op
            ))
        );
        assert_eq!(
            streams.retirement_debts_since(finalize_op, 0, 1),
            Some((Vec::new(), finalize_op))
        );
        let restored = <Streams as crate::stm::snapshot::Snapshotable>::from_snapshot(
            crate::stm::snapshot::Snapshotable::to_snapshot(streams),
        )
        .expect("snapshot restores");
        assert_eq!(
            restored.retirement_debt_for(namespace, created_revision),
            Some(finalize_op)
        );
        assert_eq!(
            restored.read(|inner| inner.retirement_debt_revision),
            finalize_op
        );
        assert_eq!(
            restored.retirement_debts_since(0, 0, 1),
            None,
            "a restored reader must rescan"
        );
    }

    #[compio::test]
    async fn stale_logout_preserves_a_newer_session_and_its_membership() {
        const CLIENT: u128 = 1;
        const OLD_SESSION: u64 = 0;
        const USER: u32 = 7;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let new_session = metadata
            .submit_register_in_process(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap()
            .epoch;
        let committed_op = metadata.consensus.as_ref().unwrap().commit_min();

        metadata
            .submit_logout_in_process(CLIENT, OLD_SESSION, EXPIRED_SESSION_REQUEST_ID)
            .await
            .unwrap();
        for expired_session in [None, Some(OLD_SESSION)] {
            assert_eq!(
                metadata
                    .submit_expired_logout_in_process(CLIENT, expired_session)
                    .await
                    .unwrap(),
                None
            );
        }

        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT)
                .len(),
            1
        );
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(new_session)
        );
        assert_eq!(
            metadata.consensus.as_ref().unwrap().commit_min(),
            committed_op,
            "an old session must not advance the committed frontier"
        );
    }

    #[compio::test]
    async fn capacity_pressure_preserves_membership_until_session_expiry() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let session = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        let capacity = metadata.client_table.borrow().capacity();
        {
            let mut table = metadata.client_table.borrow_mut();
            for client in 2..=capacity as u128 {
                table
                    .commit_register(
                        client,
                        USER,
                        [0x5a; 32],
                        register_reply(client, u64::try_from(client).unwrap() + 1),
                    )
                    .unwrap();
            }
            assert!(
                table
                    .commit_register(
                        capacity as u128 + 1,
                        USER,
                        [0x5a; 32],
                        register_reply(capacity as u128 + 1, capacity as u64 + 2)
                    )
                    .is_err()
            );
            assert_eq!(table.get_epoch(CLIENT), Some(session));
        }
        assert_eq!(
            metadata.mux_stm.streams().consumer_group_session(CLIENT),
            Some(session)
        );
        assert_eq!(
            metadata
                .submit_expired_logout_in_process(CLIENT, Some(session - 1))
                .await
                .unwrap(),
            None
        );
        assert!(
            metadata
                .submit_expired_logout_in_process(CLIENT, Some(session))
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(CLIENT),
            []
        );
        assert_eq!(metadata.client_table.borrow().count(), capacity);
    }

    #[compio::test]
    async fn replayed_register_preserves_the_original_epoch_below_the_table_frontier() {
        const CLIENT: u128 = 1;
        let (_dir, metadata) = metadata_with_group_member(CLIENT).await;
        let epoch = metadata.client_table.borrow().get_epoch(CLIENT).unwrap();
        let request = RoutedRequestHeader {
            client: CLIENT,
            user_id: GROUP_MEMBER_USER,
            ..Default::default()
        };
        let prepare = build_prepare_message(
            metadata.consensus.as_ref().unwrap(),
            &request,
            Operation::Register,
            &RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: [0x5a; 32],
            }
            .to_bytes(),
        );
        apply_committed_prepare(
            &*metadata.mux_stm,
            &metadata.client_table,
            false,
            |_| {},
            prepare,
        );
        assert_eq!(
            metadata.client_table.borrow().get_epoch(CLIENT),
            Some(epoch)
        );
        assert_eq!(
            metadata.mux_stm.streams().consumer_group_session(CLIENT),
            Some(epoch)
        );
    }

    #[compio::test]
    async fn given_user_revocation_when_committed_should_release_group_memberships() {
        const CLIENT: u128 = 1;
        const USER: u32 = 7;
        for (transferred, finalized, primary, snapshot_ahead) in [
            (false, false, false, false),
            (true, false, false, false),
            (true, false, true, false),
            (true, true, false, false),
            (true, true, true, false),
            (true, true, false, true),
            (true, true, true, true),
        ] {
            let (_directory, mut metadata) = metadata_with_group_member(CLIENT).await;
            metadata.mux_stm.users().ensure_root_user("root", "hash");
            let consensus = metadata.consensus.as_ref().unwrap();
            let request = RoutedRequestHeader::default();
            let mut snapshot =
                <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, consensus.commit_min(), 1)
                    .unwrap();
            snapshot.snapshot_mut().client_table =
                Some(metadata.client_table.borrow().to_snapshot());
            let deletion = consensus::seal_prepare_capacity(
                build_prepare_message(
                    consensus,
                    &request,
                    Operation::DeleteUser,
                    &DeleteUserRequest {
                        user_id: WireIdentifier::numeric(USER),
                    }
                    .to_bytes(),
                ),
                metadata.client_table_capacity(),
            );
            let mut tail = vec![deletion];
            if transferred {
                let mut table =
                    ClientTable::decode(&metadata.client_table.borrow().encode()).unwrap();
                let ended = table.end_user_sessions(USER, tail[0].header().op);
                if finalized {
                    assert_eq!(
                        table.finalize_sessions_through(ended[0].metadata_watermark),
                        1
                    );
                    consensus.sequencer().set_sequence(tail[0].header().op);
                    tail.push(consensus::seal_prepare_capacity(
                        build_prepare_message(
                            consensus,
                            &request,
                            Operation::FinalizeSession,
                            &FinalizeSessionRequest {
                                retired_through: ended[0].metadata_watermark,
                                debt: Vec::new(),
                            }
                            .to_bytes(),
                        ),
                        metadata.client_table_capacity(),
                    ));
                }
                let frontier = tail.last().unwrap().header().op;
                if snapshot_ahead {
                    let journal = metadata.journal.take();
                    metadata = metadata_plane();
                    metadata.journal = journal;
                }
                metadata
                    .install_state_transfer(&snapshot.encode().unwrap(), table, frontier, frontier)
                    .await
                    .unwrap();
            }
            let transferred_bytes = transferred.then(|| metadata.client_table.borrow().encode());
            let consensus = metadata.consensus.as_ref().unwrap();
            for prepare in tail {
                let op = prepare.header().op;
                if primary {
                    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                    metadata
                        .journal
                        .as_ref()
                        .unwrap()
                        .handle()
                        .append(prepare)
                        .await
                        .unwrap();
                    metadata.resume_stranded_commits().await;
                } else {
                    metadata
                        .journal
                        .as_ref()
                        .unwrap()
                        .handle()
                        .append(prepare)
                        .await
                        .unwrap();
                    consensus.advance_commit_max(op);
                    metadata.commit_journal().await;
                }
            }
            if let Some(expected) = transferred_bytes {
                assert_eq!(
                    metadata.client_table.borrow().encode(),
                    expected,
                    "tail replay must preserve transferred retry protection"
                );
            }
            assert_eq!(
                metadata.client_table.borrow().ended_sessions().count(),
                usize::from(!finalized)
            );
            assert!(
                metadata.replay_client_table.borrow().is_none(),
                "replay state must be released after catch-up"
            );
            assert!(
                metadata
                    .mux_stm
                    .streams()
                    .consumer_group_memberships(CLIENT)
                    .is_empty(),
                "revoked sessions must release their consumer assignments: transferred={transferred} finalized={finalized} primary={primary} snapshot_ahead={snapshot_ahead} committed={}",
                consensus.commit_min()
            );
        }
    }

    /// The primary admits a request of a live session while a revocation of
    /// its user is still uncommitted, so the request commits right behind it.
    /// A checkpoint of the resulting table must restart, and a state transfer
    /// of it must install on a fresh replica.
    #[compio::test]
    async fn given_request_pipelined_behind_user_revocation_when_checkpointed_should_restart_and_transfer()
     {
        const ADMIN: u128 = 1;
        const VICTIM: u128 = 2;
        const ROOT_USER: u32 = 0;
        const VICTIM_USER: u32 = 7;
        const REQUEST: u64 = 1;
        let registration = || RegisterRequest {
            credential: CredentialProof::account(1, 1),
            bind_verifier: [0x5a; 32],
        };
        let with_session = |request: Message<RoutedRequestHeader>, session: u64| {
            request.transmute_header(|old, header: &mut RoutedRequestHeader| {
                *header = old;
                header.session = session;
            })
        };
        for revocation in [Operation::DeleteUser, Operation::UpdateResource] {
            let directory = tempfile::tempdir().unwrap();
            let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
            std::fs::create_dir_all(&metadata_directory).unwrap();
            let journal = PrepareJournal::open(&metadata_directory.join("journal.wal"), 0)
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
            let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                TestMux::new((session_users(), (StreamsInner::new().into(), ()))),
                Some(directory.path().to_path_buf()),
            );
            let admin = metadata
                .submit_register_in_process(ADMIN, ROOT_USER, registration())
                .await
                .unwrap();
            let victim = metadata
                .submit_register_in_process(VICTIM, VICTIM_USER, registration())
                .await
                .unwrap();
            let body = if revocation == Operation::DeleteUser {
                DeleteUserRequest {
                    user_id: WireIdentifier::numeric(VICTIM_USER),
                }
                .to_bytes()
            } else {
                let patch = UserUpdateOptions {
                    status: Some(UserStatus::Inactive),
                    ..UserUpdateOptions::default()
                }
                .to_patch()
                .unwrap();
                UpdateResourceRequest {
                    target: ResourceTarget::User {
                        user_id: WireIdentifier::numeric(VICTIM_USER),
                    },
                    reset: patch.reset,
                    options: patch.set,
                }
                .to_bytes()
            };
            let consensus = metadata.consensus.as_ref().unwrap();
            let journal = metadata.journal.as_ref().unwrap();
            let revocation_op = consensus.commit_min() + 1;
            for request in [
                with_session(pat_request(ADMIN, REQUEST, revocation, &body), admin.epoch),
                with_session(
                    create_stream_request(VICTIM, REQUEST, "pipelined"),
                    victim.epoch,
                ),
            ] {
                let prepare = metadata.prepare_request(request).unwrap();
                let op = prepare.header().op;
                consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                journal.handle().append(prepare).await.unwrap();
                consensus.advance_commit_max(op);
            }
            metadata.resume_stranded_commits().await;
            let pipelined_op = revocation_op + 1;
            assert_eq!(consensus.commit_min(), pipelined_op);
            assert!(
                metadata
                    .mux_stm
                    .streams()
                    .read(|streams| streams.items.is_empty()),
                "{revocation:?}: the op behind the revocation must not apply"
            );
            let ended = vec![SessionIdentity {
                client_id: VICTIM,
                session: victim.epoch,
                metadata_watermark: revocation_op,
            }];
            assert_eq!(
                metadata
                    .client_table
                    .borrow()
                    .ended_sessions()
                    .collect::<Vec<_>>(),
                ended
            );
            let table = metadata.client_table.borrow().encode();
            let transferred =
                ClientTable::decode(&table).expect("the transferred table must decode");

            metadata.set_checkpoint_margin(usize::MAX);
            metadata.checkpoint_if_needed(consensus, journal).await;
            let checkpoint =
                IggySnapshot::load(&metadata.coordinator.as_ref().unwrap().snapshot_path())
                    .unwrap()
                    .0;
            assert_eq!(checkpoint.sequence_number(), pipelined_op);
            let live_log = metadata.change_log().snapshot();
            assert_eq!(
                checkpoint.snapshot().change_log,
                live_log,
                "{revocation:?}: the checkpoint must carry the applied change log"
            );
            assert_eq!(
                live_log.activity.entries.len(),
                3,
                "{revocation:?}: both registrations and the revoked session are recorded"
            );

            let receiver = metadata_plane();
            receiver
                .install_state_transfer(
                    &checkpoint.encode().unwrap(),
                    transferred,
                    pipelined_op,
                    pipelined_op,
                )
                .await
                .expect("the transferred checkpoint must install");
            assert!(
                receiver.client_table.borrow().encode() == table,
                "{revocation:?}: the receiver must hold the transferred table"
            );
            assert_eq!(receiver.change_log().snapshot(), live_log);

            drop(metadata);
            let recovered = crate::impls::recovery::recover::<TestMux>(
                directory.path(),
                crate::impls::recovery::ReplicaIdentity {
                    cluster: 1,
                    replica_id: 0,
                    replica_count: 1,
                },
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .expect("restart must accept the checkpointed table");
            assert_eq!(
                recovered.client_table.ended_sessions().collect::<Vec<_>>(),
                ended
            );
            assert_eq!(recovered.change_log.snapshot(), live_log);
        }
    }

    /// All owner decisions of one revocation share its op as generation, and a
    /// reconciler on another shard installs what it reads. A reader that holds
    /// a guard across the revocation stalls any second publish, so a racing
    /// reader would then see one removal alone.
    #[compio::test]
    async fn given_two_sessions_of_one_user_when_revoked_should_publish_both_removals_at_once() {
        const FIRST: u128 = 1;
        const SECOND: u128 = 2;
        const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
        let (_directory, metadata) = metadata_with_group_member(FIRST).await;
        join_group_member(&metadata, SECOND).await;
        let header = PrepareHeader {
            op: metadata.consensus.as_ref().unwrap().commit_min() + 1,
            operation: Operation::DeleteUser,
            ..Default::default()
        };
        let factory = metadata.mux_stm.streams().factory();
        // One guard for both members: two reads could straddle a publish.
        let memberships = |streams: &Streams| {
            streams.read(|inner| {
                [FIRST, SECOND].map(|client_id| {
                    inner
                        .items
                        .iter()
                        .flat_map(|(_, stream)| stream.topics.iter())
                        .flat_map(|(_, topic)| topic.consumer_groups.values())
                        .filter(|group| {
                            group
                                .members
                                .iter()
                                .any(|(_, member)| member.client_id == client_id)
                        })
                        .count()
                })
            })
        };
        let (held_sender, held_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel::<()>();

        let seen = thread::scope(|scope| {
            let holder_factory = factory.clone();
            let holder = scope.spawn(move || {
                Streams::from_factory(holder_factory)
                    .read(|_| {
                        held_sender.send(()).unwrap();
                        release_receiver.recv()
                    })
                    .unwrap();
            });
            held_receiver.recv().unwrap();
            let observer = scope.spawn(move || {
                let streams = Streams::from_factory(factory);
                let deadline = Instant::now() + PUBLISH_TIMEOUT;
                let seen = loop {
                    let seen = memberships(&streams);
                    if seen != [1, 1] {
                        break seen;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "the revocation never reached readers"
                    );
                    thread::yield_now();
                };
                release_sender.send(()).unwrap();
                seen
            });
            apply_user_revocation(
                &*metadata.mux_stm,
                &metadata.client_table,
                true,
                Revocation::User(WireUserKey {
                    user_id: GROUP_MEMBER_USER,
                    user_created_at: 0,
                }),
                &header,
            );
            let seen = observer.join().unwrap();
            holder.join().unwrap();
            seen
        });

        assert_eq!(
            seen,
            [0, 0],
            "a reader saw one member removal of the revocation without the other"
        );
    }

    #[test]
    fn given_rejected_or_older_user_deletion_when_applying_should_preserve_newer_sessions() {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 100;
        const USER: u32 = 1;
        for (frontier, acting_user) in [(SESSION, 0), (0, u32::MAX)] {
            let metadata = metadata_plane();
            metadata.mux_stm.users().ensure_root_user("root", "hash");
            let consensus = metadata.consensus.as_ref().unwrap();
            let request = RoutedRequestHeader::default();
            let create = CreateUserRequest {
                username: WireName::new("original").unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            };
            metadata
                .mux_stm
                .update(build_prepare_message(
                    consensus,
                    &request,
                    Operation::CreateUser,
                    &create.to_bytes(),
                ))
                .unwrap();
            metadata
                .client_table
                .borrow_mut()
                .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, SESSION))
                .unwrap();
            metadata.client_table_frontier.set(frontier);
            let request = RoutedRequestHeader {
                user_id: acting_user,
                ..request
            };
            let deletion = build_prepare_message(
                consensus,
                &request,
                Operation::DeleteUser,
                &DeleteUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                }
                .to_bytes(),
            )
            .transmute_header(|old, header: &mut PrepareHeader| {
                *header = old;
                header.retry_capacity = u32::try_from(metadata.client_table_capacity()).unwrap();
            });
            apply_committed_prepare(
                &*metadata.mux_stm,
                &metadata.client_table,
                metadata.client_table_mutation_allowed(deletion.header().op),
                |_| {},
                consensus::seal_prepare_checksum(deletion),
            );
            assert_eq!(
                metadata.client_table.borrow().get_epoch(CLIENT),
                Some(SESSION)
            );
            assert_eq!(metadata.client_table.borrow().ended_sessions().count(), 0);
            assert_eq!(
                metadata
                    .mux_stm
                    .users()
                    .read(|users| users.items.get(USER as usize).is_some()),
                acting_user != 0,
                "only the authorized deletion updates the state machine"
            );
        }
    }

    fn session_users() -> Users {
        const LAST_USER: u32 = 9;
        let mut users = UsersInner::new();
        // Created at op 1, the access revision the sessions below prove.
        users.apply_context.metadata_op = 1;
        for user_id in 0..=LAST_USER {
            let created = StateHandler::apply(
                &CreateUserRequest {
                    username: WireName::new(format!("session-user-{user_id}")).unwrap(),
                    password: "hash".to_owned(),
                    status: UserStatus::Active.as_code(),
                    permissions: None,
                    options: WireOptions::empty(),
                },
                &mut users,
                IggyTimestamp::from(1),
            );
            assert_eq!(created.code, 0);
        }
        users.into()
    }

    /// The user of every session that [`join_group_member`] registers.
    const GROUP_MEMBER_USER: u32 = 7;

    #[allow(clippy::future_not_send)]
    async fn metadata_with_group_member(
        client_id: u128,
    ) -> (
        tempfile::TempDir,
        IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
    ) {
        let mut inner = StreamsInner::new();
        let timestamp = IggyTimestamp::now();
        let _ = StateHandler::apply(
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            },
            &mut inner,
            timestamp,
        );
        let _ = StateHandler::apply(
            &PersistedCreateTopicRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                    name: WireName::new("topic").unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: vec![CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 1,
                    initial_view: 0,
                    members: Vec::new(),
                }],
            },
            &mut inner,
            timestamp,
        );
        let _ = StateHandler::apply(
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new("group").unwrap(),
            },
            &mut inner,
            timestamp,
        );
        let dir = tempfile::tempdir().unwrap();
        let mut metadata = metadata_plane();
        metadata.journal = Some(
            PrepareJournal::open(&dir.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.mux_stm = Rc::new(TestMux::new((session_users(), (inner.into(), ()))));
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        join_group_member(&metadata, client_id).await;
        (dir, metadata)
    }

    /// Registers `client_id` as a session of `GROUP_MEMBER_USER` and commits
    /// its join to the group of [`metadata_with_group_member`].
    #[allow(clippy::future_not_send)]
    async fn join_group_member(
        metadata: &IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux>,
        client_id: u128,
    ) {
        metadata
            .submit_register_in_process(
                client_id,
                GROUP_MEMBER_USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let join = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &RoutedRequestHeader::default(),
                Operation::JoinConsumerGroup,
                &JoinConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    group_id: WireIdentifier::numeric(0),
                    client_id,
                    session: metadata.client_table.borrow().get_epoch(client_id).unwrap(),
                }
                .to_bytes(),
            ),
            metadata.client_table_capacity(),
        );
        let join_op = join.header().op;
        consensus.pipeline_message(PlaneKind::Metadata, &join);
        metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .append(join)
            .await
            .unwrap();
        consensus.advance_commit_max(join_op);
        metadata.resume_stranded_commits().await;
        assert_eq!(consensus.commit_min(), join_op);
        assert_eq!(
            metadata
                .mux_stm
                .streams()
                .consumer_group_memberships(client_id)
                .len(),
            1
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
                TestMux::new((session_users(), (Streams::default(), ()))),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT_B,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT_B, SESSION),
            )
            .unwrap();

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
        let mut register = Box::pin(md.submit_register_in_process(
            CLIENT_C,
            ACTING_USER,
            RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: [0x5a; 32],
            },
        ));
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

    #[compio::test]
    async fn given_retried_keyless_create_when_committed_should_store_one_envelope() {
        const CLIENT: u128 = 9;
        const USER: u32 = 0;
        let directory = tempfile::tempdir().unwrap();
        let context = Arc::new(
            ServerKeyContext::from_config(
                "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=",
                &[],
                true,
            )
            .unwrap(),
        );
        let mut metadata = metadata_plane().with_stream_key_context(Arc::clone(&context));
        metadata.journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        metadata
            .submit_register_in_process(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: [0x5a; 32],
                },
            )
            .await
            .unwrap();
        // Two edge rewrites of the same keyless create generate two keys.
        let envelope_block = |seed: u8| {
            let set = WrapSet::from(
                context
                    .wrap(
                        &StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap(),
                        StreamKeyOrigin::Server,
                    )
                    .unwrap(),
            );
            let mut options = ResourceOptions::new();
            options.insert(
                HeaderKey::from(OptionKey::EncryptionKeyWrapped),
                OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
            );
            options.insert(
                HeaderKey::from(OptionKey::CriticalKeys),
                OptionValue::explicit(
                    critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
                ),
            );
            (
                set,
                resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
            )
        };
        let (first_set, first) = envelope_block(1);
        let (_, retry) = envelope_block(2);

        let committed = metadata
            .submit_request_in_process(create_stream_request_with(CLIENT, 1, "orders", first))
            .await
            .unwrap();
        let replayed = metadata
            .submit_request_in_process(create_stream_request_with(CLIENT, 1, "orders", retry))
            .await
            .unwrap();

        assert_eq!(replayed.as_slice(), committed.as_slice());
        let stored = metadata.mux_stm.streams().read(|inner| {
            let (_, stream) = inner.items.iter().next().expect("one stream");
            stream_wrap_set(&stream.options).unwrap()
        });
        assert_eq!(stored, Some(first_set));
        assert_eq!(
            metadata.mux_stm.streams().read(|inner| inner.items.len()),
            1
        );
    }

    #[compio::test]
    async fn given_newer_local_state_when_installing_stale_transfer_should_preserve_sessions() {
        const CLIENT: u128 = 9;
        const NEW_CLIENT: u128 = 10;
        const USER: u32 = 0;
        const BIND_VERIFIER: [u8; 32] = [0x5a; 32];
        let directory = tempfile::tempdir().unwrap();
        let mut metadata = metadata_plane();
        metadata.journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let original_session = metadata
            .submit_register_in_process(
                CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: BIND_VERIFIER,
                },
            )
            .await
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let transfer_op = consensus.commit_min();
        let snapshot = <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, transfer_op, 1)
            .unwrap()
            .encode()
            .unwrap();
        let transferred_table =
            ClientTable::decode(&metadata.client_table.borrow().encode()).unwrap();

        let original_reply = metadata
            .submit_request_in_process(create_stream_request(CLIENT, 1, "original"))
            .await
            .unwrap();
        assert_eq!(
            iggy_binary_protocol::result_code(original_reply.body()),
            Some(0),
        );
        let new_session = metadata
            .submit_register_in_process(
                NEW_CLIENT,
                USER,
                RegisterRequest {
                    credential: CredentialProof::account(1, 1),
                    bind_verifier: BIND_VERIFIER,
                },
            )
            .await
            .unwrap();
        let local_applied = consensus.commit_min();
        let local_table = metadata.client_table.borrow().encode();
        let local_snapshot =
            <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, local_applied, 1)
                .unwrap()
                .encode()
                .unwrap();

        let outcome = metadata
            .install_state_transfer(&snapshot, transferred_table, transfer_op, transfer_op)
            .await
            .unwrap();

        assert_eq!(outcome.installed_frontier, local_applied);
        assert_eq!(consensus.commit_min(), local_applied);
        assert_eq!(consensus.commit_max(), local_applied);
        assert_eq!(
            metadata.client_table.borrow().get_epoch(NEW_CLIENT),
            Some(new_session.epoch),
            "a stale transfer must preserve registrations above its frontier",
        );
        assert_eq!(
            metadata.client_table.borrow().encode(),
            local_table,
            "a stale transfer must preserve committed registrations and receipts",
        );
        assert_eq!(
            <IggySnapshot as Snapshot>::create(&*metadata.mux_stm, local_applied, 1)
                .unwrap()
                .encode()
                .unwrap(),
            local_snapshot,
        );
        assert_eq!(
            metadata
                .bound_session(
                    CLIENT,
                    USER,
                    RegisterRequest {
                        credential: CredentialProof::account(1, 1),
                        bind_verifier: BIND_VERIFIER
                    },
                    consensus.clock_realtime_micros()
                )
                .unwrap()
                .epoch,
            original_session.epoch,
        );
        assert_eq!(
            metadata
                .bound_session(
                    NEW_CLIENT,
                    USER,
                    RegisterRequest {
                        credential: CredentialProof::account(1, 1),
                        bind_verifier: BIND_VERIFIER
                    },
                    consensus.clock_realtime_micros()
                )
                .unwrap()
                .epoch,
            new_session.epoch,
        );
        let replay = metadata
            .submit_request_in_process(create_stream_request(CLIENT, 1, "original"))
            .await
            .unwrap();
        assert_eq!(replay.as_slice(), original_reply.as_slice());
        assert_eq!(consensus.commit_min(), local_applied);

        let mut request = create_stream_request(NEW_CLIENT, 1, "after-transfer");
        bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
            &mut request.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
        )
        .session = new_session.epoch;
        let prepare = metadata.prepare_request(request).unwrap();
        let op = prepare.header().op;
        metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .append(prepare)
            .await
            .unwrap();
        consensus.advance_commit_max(op);
        metadata.commit_journal().await;
        assert_eq!(consensus.commit_min(), op);
        assert_eq!(
            metadata.client_table.borrow().get_watermark(NEW_CLIENT),
            Some(1)
        );
    }

    #[compio::test]
    async fn given_ahead_client_table_when_installing_older_transfer_should_preserve_its_frontier()
    {
        const CLIENT: u128 = 9;
        const OTHER_CLIENT: u128 = 10;
        const NEW_CLIENT: u128 = 11;
        const USER: u32 = 7;
        const SESSION: u64 = 1;
        const TABLE_FRONTIER: u64 = 3;
        const OLDER_FRONTIER: u64 = 2;
        let metadata = metadata_plane();
        let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
        table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
        let mut snapshot =
            <IggySnapshot as Snapshot>::create(&TestMux::default(), SESSION, 1).unwrap();
        let mut older_table = None;
        for (client, epoch) in [
            (CLIENT, SESSION),
            (OTHER_CLIENT, OLDER_FRONTIER),
            (NEW_CLIENT, TABLE_FRONTIER),
        ] {
            let reply = build_reply_message(
                &PrepareHeader {
                    client,
                    user_id: USER,
                    op: epoch,
                    operation: Operation::Register,
                    ..Default::default()
                },
                &bytes::Bytes::new(),
            );
            table
                .commit_register(client, USER, [0x5a; 32], reply)
                .unwrap();
            if epoch == SESSION {
                snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            }
            if epoch == OLDER_FRONTIER {
                older_table = Some(ClientTable::decode(&table.encode()).unwrap());
            }
        }
        let snapshot = snapshot.encode().unwrap();
        let expected_table = table.encode();
        metadata
            .install_state_transfer(&snapshot, table, TABLE_FRONTIER, TABLE_FRONTIER)
            .await
            .unwrap();
        metadata
            .install_state_transfer(
                &snapshot,
                older_table.unwrap(),
                OLDER_FRONTIER,
                OLDER_FRONTIER,
            )
            .await
            .unwrap();

        assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), SESSION);
        assert_eq!(metadata.client_table.borrow().encode(), expected_table);
        assert_eq!(metadata.client_table_frontier.get(), TABLE_FRONTIER);
        assert!(!metadata.client_table_mutation_allowed(TABLE_FRONTIER));
        assert!(metadata.client_table_mutation_allowed(TABLE_FRONTIER + 1));
    }

    #[compio::test]
    async fn given_a_durable_pairing_and_a_failed_trim_when_retrying_at_the_same_op_should_keep_the_paired_snapshot()
     {
        const COMMITTED: u64 = 3;
        let dir = tempfile::tempdir().unwrap();
        let superblock = Rc::new(CheckpointSuperblock::default());
        let (md, _jobs) = job_metadata(dir.path(), 0, 1, Some(Rc::clone(&superblock))).await;
        commit_on_primary(&md, 1..=COMMITTED).await;
        let consensus = md.consensus.as_ref().unwrap();
        let journal = md.journal.as_ref().unwrap();
        let coordinator = md.coordinator.as_ref().unwrap();
        // The journal refuses a trim while another drain is in flight.
        let mut other_drain = std::pin::pin!(journal.handle().drain(1..=1));
        assert!(futures::poll!(other_drain.as_mut()).is_pending());
        md.set_checkpoint_margin(usize::MAX);
        md.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(coordinator.superblock_checkpoint_op.get(), COMMITTED);
        assert!(coordinator.checkpoint_pending.get(), "the trim failed");
        assert_eq!(journal_floor(&md), 0);
        let paired = std::fs::read(coordinator.snapshot_path()).unwrap();

        md.checkpoint_if_needed(consensus, journal).await;

        assert_eq!(
            superblock.writes.get(),
            1,
            "a second capture of the paired op must not start"
        );
        assert_eq!(
            std::fs::read(coordinator.snapshot_path()).unwrap(),
            paired,
            "the superblock pairs the checksum of the first snapshot of this op"
        );

        other_drain.await.unwrap();
        md.checkpoint_if_needed(consensus, journal).await;
        assert!(!coordinator.checkpoint_pending.get());
        assert_eq!(journal_floor(&md), COMMITTED - 1);
        assert_eq!(superblock.writes.get(), 1);
    }

    #[compio::test]
    async fn given_a_saved_snapshot_and_failed_superblock_when_retrying_should_finish_the_same_checkpoint()
     {
        let directory = tempfile::tempdir().unwrap();
        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
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
        let superblock = Rc::new(CheckpointSuperblock::default());
        superblock.fail.set(true);
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux, CheckpointSuperblock> =
            IggyMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                Some(Rc::clone(&superblock)),
                TestMux::default(),
                Some(directory.path().to_path_buf()),
            );
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        let request = CreateUserRequest {
            username: WireName::new("root").unwrap(),
            password: "hash".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        };
        let prepare = consensus::seal_prepare_capacity(
            build_prepare_message(
                consensus,
                &RoutedRequestHeader::default(),
                Operation::CreateUser,
                &request.to_bytes(),
            ),
            consensus.retry_capacity(&metadata.client_table.borrow()),
        );
        assert_eq!(
            metadata
                .mux_stm
                .gated_update(prepare.deep_copy())
                .unwrap()
                .code,
            0
        );
        journal.append(prepare).await.unwrap();
        consensus.sequencer().set_sequence(1);
        consensus.restore_commit_state(1, 1);
        metadata.set_checkpoint_margin(usize::MAX);
        metadata.checkpoint_if_needed(consensus, journal).await;
        let coordinator = metadata.coordinator.as_ref().unwrap();
        assert_eq!(coordinator.last_checkpoint().0, 1);
        assert!(coordinator.snapshot_path().exists());
        assert!(coordinator.checkpoint_pending.get());
        assert_eq!(superblock.writes.get(), 1);
        // A failed write may have reached the disk, so each retry must keep
        // the snapshot that it paired: a capture here would trip the boundary.
        STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::TableCaptured));
        metadata.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(
            superblock.writes.get(),
            2,
            "the retry writes the superblock"
        );
        assert!(coordinator.checkpoint_pending.get());
        superblock.fail.set(false);
        metadata.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(
            STOP_CHECKPOINT_AFTER.take(),
            Some(CheckpointBoundary::TableCaptured),
            "no retry may capture the op again"
        );
        assert!(!coordinator.checkpoint_pending.get());
        assert_eq!(coordinator.superblock_checkpoint_op.get(), 1);
        assert_eq!(superblock.writes.get(), 3);
        metadata.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(superblock.writes.get(), 3);
    }

    #[compio::test]
    async fn given_no_applied_progress_when_forcing_checkpoints_should_not_persist_empty_state() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
            .await
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            1,
            3,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            Some(directory.path().to_path_buf()),
        );
        metadata.set_checkpoint_margin(usize::MAX);
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        for _ in 0..3 {
            metadata.checkpoint_if_needed(consensus, journal).await;
        }
        let coordinator = metadata.coordinator.as_ref().unwrap();
        assert_eq!(coordinator.last_checkpoint(), (0, 0));
        assert!(!coordinator.snapshot_path().exists());
        assert!(!coordinator.checkpoint_pending.get());
    }

    #[compio::test]
    async fn given_state_recovery_would_refuse_when_persisting_a_checkpoint_should_write_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let metadata: IggyMetadata<(), (), (), TestMux> = IggyMetadata::new(
            None,
            None,
            None,
            None,
            TestMux::default(),
            Some(directory.path().to_path_buf()),
        );
        // Root carries the revision of op 1, which a checkpoint at op 0 cannot hold.
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let coordinator = metadata.coordinator.as_ref().unwrap();

        let capture = coordinator
            .capture(
                &*metadata.mux_stm,
                0,
                0,
                None,
                metadata.change_log.snapshot(),
            )
            .unwrap();
        let persisted = coordinator.persist_capture(capture).await;

        assert!(matches!(persisted, Err(SnapshotError::InvalidState(_))));
        assert!(!coordinator.snapshot_path().exists());
        assert_eq!(coordinator.last_checkpoint(), (0, 0));
    }

    #[compio::test]
    async fn transferred_client_table_waits_for_applied_frontier_before_checkpoint() {
        const CLIENT: u128 = 9;
        const USER: u32 = 7;
        const SNAPSHOT_OP: u64 = 1;
        const TABLE_FRONTIER: u64 = 3;
        const JOURNAL_SLOTS: usize = 16;
        let directory = tempfile::tempdir().unwrap();
        let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_directory).unwrap();
        let journal = PrepareJournal::open(&metadata_directory.join("journal.wal"), 0)
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
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            Some(directory.path().to_path_buf()),
        );
        let mut transferred_table = ClientTable::new(CLIENTS_TABLE_MAX);
        transferred_table
            .commit_capacity(CLIENTS_TABLE_MAX)
            .unwrap();
        transferred_table
            .commit_register(
                CLIENT,
                USER,
                [0x5a; 32],
                register_reply(CLIENT, SNAPSHOT_OP),
            )
            .unwrap();
        let mut snapshot =
            <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_OP, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(transferred_table.to_snapshot());
        for request in 1..TABLE_FRONTIER {
            let reply = build_reply_message(
                &PrepareHeader {
                    client: CLIENT,
                    user_id: USER,
                    session: SNAPSHOT_OP,
                    request,
                    op: request + SNAPSHOT_OP,
                    operation: Operation::CreateStream,
                    ..Default::default()
                },
                &bytes::Bytes::new(),
            );
            assert_eq!(
                transferred_table.commit_reply(CLIENT, USER, reply),
                CommitReply::Cached,
            );
        }
        metadata
            .install_state_transfer(
                &snapshot.encode().unwrap(),
                transferred_table,
                TABLE_FRONTIER,
                TABLE_FRONTIER,
            )
            .await
            .unwrap();
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        let coordinator = metadata.coordinator.as_ref().unwrap();
        metadata.set_checkpoint_margin(usize::MAX);
        metadata.checkpoint_if_needed(consensus, journal).await;
        let persisted = IggySnapshot::load(&coordinator.snapshot_path()).unwrap().0;
        assert_eq!(persisted.sequence_number(), SNAPSHOT_OP);
        assert_eq!(
            persisted.snapshot().client_table.as_ref().unwrap().slots[0]
                .1
                .watermark,
            0,
            "ahead retry protection cannot be persisted at an earlier state-machine floor",
        );
        drop(persisted);
        for (request, name) in [(1, "s1"), (2, "s2")] {
            let prepare = metadata
                .prepare_request(create_stream_request(CLIENT, request, name))
                .unwrap();
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            journal.handle().append(prepare).await.unwrap();
        }
        metadata.resume_stranded_commits().await;
        assert_eq!(consensus.commit_min(), TABLE_FRONTIER);
        metadata.checkpoint_if_needed(consensus, journal).await;
        let persisted = IggySnapshot::load(&coordinator.snapshot_path()).unwrap().0;
        assert_eq!(persisted.sequence_number(), TABLE_FRONTIER);
        drop(persisted);
        drop(metadata);
        let recovered = crate::impls::recovery::recover::<TestMux>(
            directory.path(),
            crate::impls::recovery::ReplicaIdentity {
                cluster: 1,
                replica_id: 0,
                replica_count: 1,
            },
            JOURNAL_SLOTS,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.client_table.get_watermark(CLIENT),
            Some(TABLE_FRONTIER - 1)
        );
    }

    const TAIL_CLIENT: u128 = 9;
    const TAIL_USER: u32 = 7;

    /// The snapshot at `snapshot_op` and the client table of a primary that
    /// committed `tail` requests of [`TAIL_CLIENT`] above it.
    fn transfer_with_tail(snapshot_op: u64, tail: u64) -> (Vec<u8>, ClientTable) {
        let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
        table.commit_capacity(CLIENTS_TABLE_MAX).unwrap();
        table
            .commit_register(
                TAIL_CLIENT,
                TAIL_USER,
                [0x5a; 32],
                register_reply(TAIL_CLIENT, snapshot_op),
            )
            .unwrap();
        let mut snapshot =
            <IggySnapshot as Snapshot>::create(&TestMux::default(), snapshot_op, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        for request in 1..=tail {
            let reply = build_reply_message(
                &PrepareHeader {
                    client: TAIL_CLIENT,
                    user_id: TAIL_USER,
                    session: snapshot_op,
                    request,
                    op: snapshot_op + request,
                    operation: Operation::CreateStream,
                    ..Default::default()
                },
                &bytes::Bytes::new(),
            );
            assert_eq!(
                table.commit_reply(TAIL_CLIENT, TAIL_USER, reply),
                CommitReply::Cached
            );
        }
        (snapshot.encode().unwrap(), table)
    }

    /// No checkpoint runs below the frontier of a transferred client table, so
    /// the journal must hold every op from the floor of the install up to it.
    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_a_transfer_tail_when_it_exceeds_the_journal_slots_should_refuse_the_install() {
        const SLOTS: usize = 4;
        const SNAPSHOT_OP: u64 = 1;
        const FRONTIER: u64 = SNAPSHOT_OP + SLOTS as u64;
        const NEXT_SNAPSHOT_OP: u64 = FRONTIER + 2;
        let directory = tempfile::tempdir().unwrap();
        let metadata_directory = directory.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_directory).unwrap();
        let journal =
            PrepareJournal::open_with_slots(&metadata_directory.join("journal.wal"), 0, SLOTS)
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
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            Some(directory.path().to_path_buf()),
        );
        let consensus = metadata.consensus.as_ref().unwrap();
        let journal = metadata.journal.as_ref().unwrap();
        let coordinator = metadata.coordinator.as_ref().unwrap();
        metadata.set_checkpoint_margin(usize::MAX);

        // One op per slot is the largest tail that fits.
        let (snapshot, table) = transfer_with_tail(SNAPSHOT_OP, SLOTS as u64);
        metadata
            .install_state_transfer(&snapshot, table, FRONTIER, FRONTIER)
            .await
            .unwrap();
        for request in 1..=SLOTS as u64 {
            let prepare = metadata
                .prepare_request(create_stream_request(
                    TAIL_CLIENT,
                    request,
                    &format!("s{request}"),
                ))
                .unwrap();
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            journal.handle().append(prepare).await.unwrap();
        }
        metadata.resume_stranded_commits().await;
        assert_eq!(consensus.commit_min(), FRONTIER);
        let above = metadata
            .prepare_request(create_stream_request(
                TAIL_CLIENT,
                SLOTS as u64 + 1,
                "above",
            ))
            .unwrap();
        assert!(
            journal.handle().append(above.clone()).await.is_err(),
            "the tail fills the journal, so one more op needs a checkpoint"
        );
        metadata.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(coordinator.last_checkpoint().0, FRONTIER);
        journal.handle().append(above).await.unwrap();

        let durable_state = || {
            (
                std::fs::read(coordinator.snapshot_path()).unwrap(),
                coordinator.last_checkpoint(),
                coordinator.superblock_checkpoint_op.get(),
                journal.handle().snapshot_op(),
                journal.handle().last_op(),
                metadata.client_table_frontier.get(),
                consensus.commit_min(),
                consensus.commit_max(),
            )
        };
        let before = durable_state();

        // One more op than slots above a snapshot that is ahead.
        let tail = SLOTS as u64 + 1;
        let frontier = NEXT_SNAPSHOT_OP + tail;
        let (snapshot, table) = transfer_with_tail(NEXT_SNAPSHOT_OP, tail);
        let error = metadata
            .install_state_transfer(&snapshot, table, frontier, frontier)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                SnapshotError::TransferTailExceedsJournal {
                    tail: refused,
                    journal_slots: SLOTS,
                } if refused == tail
            ),
            "{error}"
        );
        assert!(error.to_string().contains("`metadata.journal_slots`"));
        assert_eq!(
            durable_state(),
            before,
            "a refused install must leave the replica as it was"
        );

        // A snapshot at the commit point is not ahead and installs its table
        // only, so the floor stays below the entry of the last checkpoint.
        let floor = journal.handle().snapshot_op();
        assert_eq!(floor, FRONTIER - 1);
        let fitting = floor + SLOTS as u64;
        let (snapshot, table) = transfer_with_tail(FRONTIER, fitting + 1 - FRONTIER);
        assert!(matches!(
            metadata
                .install_state_transfer(&snapshot, table, fitting + 1, fitting + 1)
                .await,
            Err(SnapshotError::TransferTailExceedsJournal { .. })
        ));
        assert_eq!(durable_state(), before);
        let (snapshot, table) = transfer_with_tail(FRONTIER, fitting - FRONTIER);
        metadata
            .install_state_transfer(&snapshot, table, fitting, fitting)
            .await
            .unwrap();
        assert_eq!(metadata.client_table_frontier.get(), fitting);
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
    /// bodies as committed. The install discards that suffix and keeps the
    /// boundary header only when the adopted peer log confirms its checksum.
    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_a_state_transfer_when_installing_should_keep_only_a_verified_boundary() {
        for peer_matches in [None, Some(false), Some(true)] {
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
                    None,
                );
            let consensus = md.consensus.as_ref().unwrap();
            md.client_table
                .borrow_mut()
                .commit_register(
                    CLIENT,
                    ACTING_USER,
                    [0x5a; 32],
                    register_reply(CLIENT, SESSION),
                )
                .unwrap();

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
            consensus.fence_view_log(SNAPSHOT_SEQ + 1, u64::MAX);

            // A donor mux fills the snapshot the way a serving primary would, so
            // the restore path sees populated sections rather than a bare envelope.
            for missing in [true, false] {
                for frontier in [SNAPSHOT_SEQ, SNAPSHOT_SEQ + 1] {
                    let mut invalid_snapshot =
                        <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_SEQ, 1)
                            .unwrap();
                    if !missing {
                        let mut table = ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot();
                        table.capacity = 0;
                        invalid_snapshot.snapshot_mut().client_table = Some(table);
                    }
                    assert!(matches!(
                        md.install_state_transfer(
                            &invalid_snapshot.encode().unwrap(),
                            ClientTable::new(CLIENTS_TABLE_MAX),
                            frontier,
                            frontier
                        )
                        .await,
                        Err(SnapshotError::MissingClientTable | SnapshotError::ClientTable(_))
                    ));
                    assert_eq!(
                        journal_handle.last_op(),
                        Some(3),
                        "invalid protection must not truncate the WAL"
                    );
                    assert_eq!(consensus.commit_min(), 0);
                    assert_eq!(md.client_table.borrow().get_epoch(CLIENT), Some(SESSION));
                }
            }
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut snapshot =
                <IggySnapshot as Snapshot>::create(&TestMux::default(), SNAPSHOT_SEQ, 1)
                    .expect("donor snapshot builds");
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            let snapshot_bytes = snapshot.encode().expect("donor snapshot encodes");
            if let Some(matches) = peer_matches {
                let mut peer = *journal_handle
                    .header(usize::try_from(SNAPSHOT_SEQ).unwrap())
                    .unwrap();
                if !matches {
                    peer.timestamp += 1;
                    peer.checksum = peer.identity_checksum();
                }
                let header = StartViewHeader {
                    checksum: 0,
                    checksum_body: 0,
                    cluster: 1,
                    size: u32::try_from(size_of::<StartViewHeader>() + size_of::<PrepareHeader>())
                        .unwrap(),
                    view: 1,
                    release: 0,
                    command: Command::StartView,
                    replica: 1,
                    reserved_frame: [0; 66],
                    op: SNAPSHOT_SEQ,
                    commit: SNAPSHOT_SEQ,
                    group: server_common::sharding::METADATA_GROUP,
                    partition_incarnation: 0,
                    reserved: [0; 80],
                    incarnation: 0,
                };
                let _ = consensus.handle_start_view(
                    PlaneKind::Metadata,
                    &header,
                    bytemuck::bytes_of(&peer),
                );
                assert!(consensus.view_log_is_pending());
            }
            md.install_state_transfer(&snapshot_bytes, table, SNAPSHOT_SEQ, SNAPSHOT_SEQ)
                .await
                .expect("install succeeds");

            assert_eq!(
                journal_handle.last_op(),
                Some(if peer_matches == Some(true) {
                    SNAPSHOT_SEQ
                } else {
                    SNAPSHOT_SEQ - 1
                }),
                "the unverified boundary and relic above it must be gone"
            );
            assert!(
                journal_handle.header(3).is_none(),
                "op 3 was above the floor; the commit walk must never see it again"
            );
            assert!(
                journal_handle.header(1).is_some()
                    && journal_handle.header(2).is_some() == (peer_matches == Some(true)),
                "the snapshot replaces an unverified boundary header too"
            );
            assert_eq!(
                md.applied_frontier().get(),
                SNAPSHOT_SEQ,
                "the snapshot IS ops up to its sequence applied, so the read gate has \
             to admit reads at the floor the install jumped to"
            );
        }
    }

    /// The replica [`transfer_receiver`] builds: replica 0 of a 3-node cluster.
    const TRANSFER_RECEIVER: ReplicaIdentity = ReplicaIdentity {
        cluster: 1,
        replica_id: 0,
        replica_count: 3,
    };
    /// The receiver's WAL head, which its certificate is bound to.
    const TRANSFER_RECEIVER_HEAD: u64 = 4;
    const CERTIFIED_VIEW: u32 = 0;
    /// Recorded but never proven, so a restart proves the WAL through the
    /// certificate rather than through the recorded view.
    const ADOPTED_VIEW: u32 = 1;
    /// The transferred snapshot's op, below the certified head.
    const TRANSFER_FLOOR: u64 = 2;
    /// Polls before a stepped install counts as hung. Each one waits a millisecond.
    const INSTALL_POLL_BUDGET: usize = 10_000;

    /// A state transfer receiver whose superblock records [`ADOPTED_VIEW`] and a
    /// certificate for [`CERTIFIED_VIEW`] bound to op [`TRANSFER_RECEIVER_HEAD`].
    /// `coordinated` gives it a snapshot coordinator. A simulator replica has none,
    /// so its superblock pairs no checkpoint.
    #[allow(clippy::future_not_send)]
    async fn transfer_receiver(
        data_dir: &Path,
        coordinated: bool,
    ) -> IggyMetadata<VsrConsensus<NoopBus>, PrepareJournal, (), TestMux> {
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        let metadata_dir = data_dir.join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
            .await
            .unwrap();
        let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
        let consensus = VsrConsensus::new(
            TRANSFER_RECEIVER.cluster,
            TRANSFER_RECEIVER.replica_id,
            TRANSFER_RECEIVER.replica_count,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        let mut md = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            Some(Rc::new(superblock)),
            TestMux::default(),
            coordinated.then(|| data_dir.to_path_buf()),
        );
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();
        {
            let consensus = md.consensus.as_ref().unwrap();
            for request in 1..=TRANSFER_RECEIVER_HEAD {
                let prepare = md
                    .prepare_request(create_stream_request(
                        CLIENT,
                        request,
                        &format!("s{request}"),
                    ))
                    .expect("CreateStream is client-allowed");
                consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                let (op, checksum) = (prepare.header().op, prepare.header().checksum);
                md.journal
                    .as_ref()
                    .unwrap()
                    .handle()
                    .append(prepare)
                    .await
                    .unwrap();
                consensus.extend_history_certificate(op, checksum);
            }
        }
        let consensus = md.consensus.as_mut().unwrap();
        consensus.set_view(ADOPTED_VIEW);
        consensus.set_log_view(ADOPTED_VIEW);
        assert!(
            md.persist_superblock_if_needed(md.consensus.as_ref().unwrap())
                .await
        );
        md
    }

    /// The metadata directory as a crash at this instant leaves it.
    fn crash_image(data_dir: &Path) -> tempfile::TempDir {
        let image = tempfile::tempdir().unwrap();
        let image_dir = image.path().join(crate::impls::METADATA_DIR);
        std::fs::create_dir_all(&image_dir).unwrap();
        for entry in std::fs::read_dir(data_dir.join(crate::impls::METADATA_DIR)).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), image_dir.join(entry.file_name())).unwrap();
            }
        }
        image
    }

    /// What a restart from `image` decides about its WAL, and the WAL head left
    /// after that decision.
    #[allow(clippy::future_not_send)]
    async fn restarted_history(image: &Path) -> (HistoryRecovery, Option<u64>) {
        let recovered = recover::<crate::MuxStateMachine<()>>(
            image,
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        (recovered.history, recovered.journal.last_op())
    }

    #[compio::test]
    async fn given_an_adopted_log_when_persistence_fails_or_completes_should_reopen_one_complete_identity()
     {
        const ANNOUNCED_VIEW: u32 = ADOPTED_VIEW + 3;
        const ANNOUNCED_HEAD: u64 = TRANSFER_RECEIVER_HEAD + consensus::DVC_HEADERS_MAX as u64;
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let consensus = md.consensus.as_ref().unwrap();
        let journal = md.journal.as_ref().unwrap().handle();
        let original: Vec<_> = (1..=TRANSFER_RECEIVER_HEAD)
            .map(|op| *journal.header(usize::try_from(op).unwrap()).unwrap())
            .collect();
        let mut headers = Vec::new();
        let mut parent = *original.last().unwrap();
        for op in TRANSFER_RECEIVER_HEAD + 1..=ANNOUNCED_HEAD {
            let mut header = PrepareHeader {
                op,
                view: ANNOUNCED_VIEW,
                timestamp: parent.timestamp + 1,
                request: op,
                parent: parent.checksum,
                ..parent
            };
            header.checksum = header.identity_checksum();
            headers.push(header);
            parent = header;
        }
        headers.reverse();
        let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
            .transmute_header(|_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = TRANSFER_RECEIVER.cluster;
                header.group = server_common::sharding::METADATA_GROUP;
                header.view = ANNOUNCED_VIEW;
                header.replica = consensus.primary_index(ANNOUNCED_VIEW);
                header.op = ANNOUNCED_HEAD;
                header.commit = ANNOUNCED_HEAD - 1;
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            });
        consensus.handle_start_view(
            PlaneKind::Metadata,
            start_view.header(),
            bytemuck::cast_slice(&headers),
        );
        assert_eq!(consensus.log_view_claim(), ANNOUNCED_VIEW);
        assert!(!consensus.history_certified());
        assert!(consensus.needs_superblock_persist());
        let before = crash_image(directory.path());
        {
            let mut write = Box::pin(md.persist_superblock_if_needed(consensus));
            assert!(futures::poll!(&mut write).is_pending());
        }
        let cancelled = crash_image(directory.path());

        // The first write used A. Refuse B's atomic rename without touching A.
        let blocked_slot = directory
            .path()
            .join(crate::impls::METADATA_DIR)
            .join(journal::superblock::SLOT_FILE_NAMES[1]);
        std::fs::create_dir(&blocked_slot).unwrap();
        assert!(!md.persist_superblock_if_needed(consensus).await);
        assert!(consensus.needs_superblock_persist());
        std::fs::remove_dir(&blocked_slot).unwrap();
        let failed = crash_image(directory.path());
        md.superblock_retry_after_micros.set(0);
        assert!(md.persist_superblock_if_needed(consensus).await);
        assert!(!consensus.needs_superblock_persist());
        let completed = crash_image(directory.path());

        for (image, persisted) in [
            (before, false),
            (cancelled, false),
            (failed, false),
            (completed, true),
        ] {
            let recovered = recover::<TestMux>(
                image.path(),
                TRANSFER_RECEIVER,
                DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .unwrap();
            let state = recovered.recovered_state.as_ref().unwrap();
            assert_eq!(state.adopted_view.is_some(), persisted);
            assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
            for header in &original {
                assert_eq!(
                    recovered
                        .journal
                        .header(usize::try_from(header.op).unwrap())
                        .unwrap()
                        .checksum,
                    header.checksum
                );
            }
            let mut restarted = VsrConsensus::new(
                TRANSFER_RECEIVER.cluster,
                TRANSFER_RECEIVER.replica_id,
                TRANSFER_RECEIVER.replica_count,
                server_common::sharding::METADATA_GROUP,
                NoopBus,
                LocalPipeline::new(),
            );
            restarted.set_view(state.view);
            restarted.set_log_view(recovered.history.log_view());
            restarted.sequencer().set_sequence(TRANSFER_RECEIVER_HEAD);
            restarted.restore_history(
                recovered.history,
                (TRANSFER_RECEIVER_HEAD, original.last().unwrap().checksum),
                Some(state),
            );
            restarted.restore_adopted_view(state.adopted_view.as_ref());
            restarted.resume_adopted_view();
            if persisted {
                assert_eq!(
                    restarted.adopted_view_head(),
                    Some((ANNOUNCED_VIEW, ANNOUNCED_HEAD))
                );
                assert_eq!(restarted.log_view_claim(), ANNOUNCED_VIEW);
                assert_eq!(restarted.sequencer().current_sequence(), ANNOUNCED_HEAD);
                assert_eq!(restarted.adopted_view_log().unwrap().headers, headers);
                assert!(restarted.view_log_unproven());
                assert!(!restarted.history_certified());
                assert!(
                    recovered
                        .journal
                        .header(usize::try_from(ANNOUNCED_HEAD).unwrap())
                        .is_none()
                );
            } else {
                assert_eq!(restarted.log_view_claim(), CERTIFIED_VIEW);
                assert_eq!(
                    restarted.sequencer().current_sequence(),
                    TRANSFER_RECEIVER_HEAD
                );
            }
        }
    }

    /// The unvouched boundary is removed too. A durable snapshot can anchor the
    /// certificate through that floor; without it, only the retained WAL can.
    const fn transfer_restart(coordinated: bool) -> (HistoryRecovery, Option<u64>) {
        (
            HistoryRecovery::Certified {
                view: CERTIFIED_VIEW,
                truncate_from: Some(if coordinated {
                    TRANSFER_FLOOR + 1
                } else {
                    TRANSFER_FLOOR
                }),
            },
            Some(TRANSFER_FLOOR - 1),
        )
    }

    /// A crash right after the install truncates the WAL above the incoming floor
    /// must restart certified. The truncate removes the entry the certificate was
    /// bound to, so the superblock must already hold a binding the restart can
    /// prove. One naming a removed entry restarts `Unproven`, which keeps the
    /// replica out of every election until a `StartView` proves its log.
    #[compio::test]
    async fn given_a_crash_after_the_transfer_truncate_when_restarting_should_stay_certified() {
        for coordinated in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let md = transfer_receiver(dir.path(), coordinated).await;
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut snapshot =
                IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            let snapshot_bytes = snapshot.encode().unwrap();
            let journal = md.journal.as_ref().unwrap().handle();
            let mut install = Box::pin(md.install_state_transfer(
                &snapshot_bytes,
                table,
                TRANSFER_FLOOR,
                TRANSFER_FLOOR,
            ));
            let mut image = None;
            let mut outcome = None;
            for _ in 0..INSTALL_POLL_BUDGET {
                let polled = futures::poll!(&mut install);
                if image.is_none() && journal.last_op() == Some(TRANSFER_FLOOR - 1) {
                    image = Some(crash_image(dir.path()));
                }
                if let std::task::Poll::Ready(result) = polled {
                    outcome = Some(result);
                    break;
                }
                compio::time::sleep(Duration::from_millis(1)).await;
            }
            assert!(
                matches!(outcome, Some(Ok(_))),
                "coordinated {coordinated}: the install did not complete: {outcome:?}"
            );
            let image = image.expect("the install truncates the WAL above its floor");
            assert_eq!(
                restarted_history(image.path()).await,
                transfer_restart(coordinated),
                "coordinated {coordinated}"
            );
        }
    }

    #[compio::test]
    async fn given_a_vouched_transfer_suffix_when_installing_and_restarting_should_keep_its_prefix()
    {
        const FIRST_TAIL: u64 = TRANSFER_FLOOR + 1;
        for coordinated in [true, false] {
            for (retained, boundary_matches, bound_at_boundary) in [
                (TRANSFER_FLOOR, true, false),
                (FIRST_TAIL, true, false),
                (TRANSFER_RECEIVER_HEAD, true, false),
                (FIRST_TAIL, false, false),
                (TRANSFER_RECEIVER_HEAD, false, false),
                (FIRST_TAIL, false, true),
                (TRANSFER_RECEIVER_HEAD, false, true),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let md = transfer_receiver(directory.path(), coordinated).await;
                let consensus = md.consensus.as_ref().unwrap();
                consensus.set_log_view(CERTIFIED_VIEW);
                let journal = md.journal.as_ref().unwrap().handle();
                let mut headers: Vec<_> = (TRANSFER_FLOOR..=TRANSFER_RECEIVER_HEAD)
                    .map(|op| *journal.header(usize::try_from(op).unwrap()).unwrap())
                    .collect();
                for index in 1..headers.len() {
                    if headers[index].op > retained {
                        headers[index].timestamp += 1;
                        headers[index].parent = headers[index - 1].checksum;
                        headers[index].checksum = headers[index].identity_checksum();
                    }
                }
                headers.reverse();
                let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
                    .transmute_header(|_, header: &mut StartViewHeader| {
                        header.command = Command::StartView;
                        header.cluster = TRANSFER_RECEIVER.cluster;
                        header.replica = consensus.primary_index(ADOPTED_VIEW);
                        header.view = ADOPTED_VIEW;
                        header.op = TRANSFER_RECEIVER_HEAD;
                        header.commit = TRANSFER_FLOOR;
                        header.group = server_common::sharding::METADATA_GROUP;
                        header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
                    });
                consensus.handle_start_view(
                    PlaneKind::Metadata,
                    start_view.header(),
                    bytemuck::cast_slice(&headers),
                );
                assert!(consensus.view_log_unproven());
                if !boundary_matches {
                    let mut boundary = *journal
                        .header(usize::try_from(TRANSFER_FLOOR).unwrap())
                        .unwrap();
                    let entry = journal.entry_at(&boundary).await.unwrap().unwrap();
                    if bound_at_boundary {
                        consensus.lower_history_certificate(
                            TRANSFER_FLOOR,
                            TRANSFER_FLOOR,
                            boundary.checksum,
                        );
                    }
                    journal
                        .remove_range(TRANSFER_FLOOR..=TRANSFER_FLOOR)
                        .await
                        .unwrap();
                    boundary.timestamp += 1;
                    boundary.checksum = boundary.identity_checksum();
                    let stale =
                        entry.transmute_header(|_, header: &mut PrepareHeader| *header = boundary);
                    let original = *journal
                        .header(usize::try_from(FIRST_TAIL).unwrap())
                        .unwrap();
                    assert_ne!(original.parent, boundary.checksum);
                    journal.append(stale).await.unwrap();
                }
                let expected: Vec<_> = (FIRST_TAIL..=retained)
                    .map(|op| {
                        (
                            op,
                            journal
                                .header(usize::try_from(op).unwrap())
                                .unwrap()
                                .checksum,
                        )
                    })
                    .collect();
                let table = ClientTable::new(CLIENTS_TABLE_MAX);
                let mut snapshot =
                    IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
                snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
                md.install_state_transfer(
                    &snapshot.encode().unwrap(),
                    table,
                    TRANSFER_FLOOR,
                    TRANSFER_FLOOR,
                )
                .await
                .unwrap();
                assert_eq!(
                    journal.last_op(),
                    Some(retained),
                    "coordinated {coordinated}"
                );
                assert_eq!(
                    journal
                        .header(usize::try_from(TRANSFER_FLOOR).unwrap())
                        .is_some(),
                    boundary_matches
                );
                if bound_at_boundary {
                    assert!(consensus.history_certificate().unwrap().op < TRANSFER_FLOOR);
                }
                for (op, checksum) in &expected {
                    assert_eq!(
                        journal
                            .header(usize::try_from(*op).unwrap())
                            .unwrap()
                            .checksum,
                        *checksum
                    );
                }
                let image = crash_image(directory.path());
                assert_eq!(
                    restarted_history(image.path()).await,
                    (
                        HistoryRecovery::Certified {
                            view: CERTIFIED_VIEW,
                            truncate_from: Some(retained + 1)
                        },
                        Some(retained)
                    ),
                    "coordinated {coordinated}, retained {retained}"
                );
            }
        }
    }

    #[compio::test]
    async fn given_a_proven_log_with_a_gap_below_the_snapshot_when_installing_should_keep_the_suffix_across_crashes()
     {
        const MISSING: u64 = TRANSFER_FLOOR;
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let consensus = md.consensus.as_ref().unwrap();
        let journal = md.journal.as_ref().unwrap().handle();
        consensus.certify_view_log(
            TRANSFER_RECEIVER_HEAD,
            journal
                .header(usize::try_from(TRANSFER_RECEIVER_HEAD).unwrap())
                .unwrap()
                .checksum,
        );
        assert!(consensus.history_certified());
        journal.remove_range(MISSING..=MISSING).await.unwrap();
        let expected: Vec<_> = (TRANSFER_FLOOR + 1..=TRANSFER_RECEIVER_HEAD)
            .map(|op| {
                (
                    op,
                    journal
                        .header(usize::try_from(op).unwrap())
                        .unwrap()
                        .checksum,
                )
            })
            .collect();
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let bytes = snapshot.encode().unwrap();
        let mut install =
            Box::pin(md.install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR));
        let mut image = None;
        let mut outcome = None;
        for _ in 0..INSTALL_POLL_BUDGET {
            let polled = futures::poll!(&mut install);
            if image.is_none()
                && let journal::superblock::SuperblockContents::Present(record) =
                    md.superblock.as_ref().unwrap().read_latest().await.unwrap()
                && consensus::VsrState::try_from(record.as_slice())
                    .unwrap()
                    .checkpoint_op
                    == TRANSFER_FLOOR
            {
                image = Some(crash_image(directory.path()));
            }
            if let std::task::Poll::Ready(result) = polled {
                outcome = Some(result);
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(matches!(outcome, Some(Ok(_))), "{outcome:?}");
        assert_eq!(journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
        let image = image.expect("the snapshot pairing reached the superblock");
        for image in [image, crash_image(directory.path())] {
            let recovered = recover::<crate::MuxStateMachine<()>>(
                image.path(),
                TRANSFER_RECEIVER,
                DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .unwrap();
            assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
            for (op, checksum) in &expected {
                assert_eq!(
                    recovered
                        .journal
                        .header(usize::try_from(*op).unwrap())
                        .unwrap()
                        .checksum,
                    *checksum
                );
            }
            assert_eq!(recovered.chain_break_op, None);
        }
    }

    /// An install that fails once the binding moved must also restart certified,
    /// at once and after the next tick persists what the failed install left in
    /// memory. A snapshot with no state machine sections fails the in-place
    /// restore, the last step that can fail.
    #[compio::test]
    async fn given_an_install_error_after_the_binding_moved_when_restarting_should_stay_certified()
    {
        for coordinated in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let md = transfer_receiver(dir.path(), coordinated).await;
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut snapshot = IggySnapshot::new(TRANSFER_FLOOR);
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            let installed = md
                .install_state_transfer(
                    &snapshot.encode().unwrap(),
                    table,
                    TRANSFER_FLOOR,
                    TRANSFER_FLOOR,
                )
                .await;
            assert!(
                matches!(installed, Err(SnapshotError::Deserialize(_))),
                "coordinated {coordinated}: {installed:?}"
            );
            let failed = crash_image(dir.path());
            assert!(
                md.persist_superblock_if_needed(md.consensus.as_ref().unwrap())
                    .await
            );
            let ticked = crash_image(dir.path());
            for (image, moment) in [(failed, "the failed install"), (ticked, "the next tick")] {
                assert_eq!(
                    restarted_history(image.path()).await,
                    transfer_restart(coordinated),
                    "coordinated {coordinated}, crash after {moment}"
                );
            }
        }
    }

    #[compio::test]
    async fn given_a_failed_install_when_an_older_offer_arrives_should_keep_the_durable_checkpoint()
    {
        const OLDER: u64 = TRANSFER_FLOOR - 1;
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let consensus = md.consensus.as_ref().unwrap();
        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        let wal_path = metadata_dir.join("journal.wal");
        let saved_wal = metadata_dir.join("journal.saved");
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let incoming_state = TestMux::default();
        incoming_state.users().ensure_root_user("restored", "hash");
        let mut snapshot = IggySnapshot::create(&incoming_state, TRANSFER_FLOOR, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let bytes = snapshot.encode().unwrap();

        // Keep the open WAL readable but make the truncate's rename fail, after
        // the new snapshot and its superblock pairing have reached disk.
        std::fs::rename(&wal_path, &saved_wal).unwrap();
        std::fs::create_dir(&wal_path).unwrap();
        let failed = md
            .install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
            .await;
        std::fs::remove_dir(&wal_path).unwrap();
        std::fs::rename(&saved_wal, &wal_path).unwrap();
        assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
        assert_eq!(consensus.commit_min(), 0);
        assert_eq!(
            md.coordinator.as_ref().unwrap().last_checkpoint().0,
            TRANSFER_FLOOR
        );
        let before = std::fs::read(metadata_dir.join("snapshot.bin")).unwrap();
        let failed_image = crash_image(directory.path());

        let older_table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut older = IggySnapshot::create(&TestMux::default(), OLDER, 1).unwrap();
        older.snapshot_mut().client_table = Some(older_table.to_snapshot());
        let refused = md
            .install_state_transfer(&older.encode().unwrap(), older_table, OLDER, OLDER)
            .await;
        assert!(
            matches!(refused, Err(SnapshotError::InvalidState(_))),
            "an older offer must not replace a newer durable checkpoint: {refused:?}"
        );
        assert_eq!(
            std::fs::read(metadata_dir.join("snapshot.bin")).unwrap(),
            before
        );
        assert_eq!(consensus.commit_min(), 0);
        let refused_image = crash_image(directory.path());
        for image in [failed_image, refused_image] {
            let recovered = recover::<TestMux>(
                image.path(),
                TRANSFER_RECEIVER,
                DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .unwrap();
            assert_eq!(recovered.snapshot_checkpoint.0, TRANSFER_FLOOR);
            assert_eq!(recovered.journal.last_op(), Some(TRANSFER_FLOOR));
        }

        // The durable floor must not make an equal retry skip the STM restore.
        for retry_floor in [TRANSFER_FLOOR, TRANSFER_FLOOR + 1] {
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut retry = IggySnapshot::create(&incoming_state, retry_floor, 1).unwrap();
            let expected_state = retry.encode().unwrap();
            retry.snapshot_mut().client_table = Some(table.to_snapshot());
            let retried = md
                .install_state_transfer(&retry.encode().unwrap(), table, retry_floor, retry_floor)
                .await
                .unwrap();
            assert_eq!(retried.installed_frontier, retry_floor);
            assert_eq!(consensus.commit_min(), retry_floor);
            assert_eq!(
                IggySnapshot::create(&*md.mux_stm, retry_floor, 1)
                    .unwrap()
                    .encode()
                    .unwrap(),
                expected_state
            );
            let recovered = recover::<TestMux>(
                crash_image(directory.path()).path(),
                TRANSFER_RECEIVER,
                DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                ChangeLogCapacities::default(),
            )
            .await
            .unwrap();
            assert_eq!(recovered.snapshot_checkpoint.0, retry_floor);
        }
    }

    #[compio::test]
    async fn given_a_failed_install_when_an_offer_of_the_same_op_arrives_should_keep_the_durable_snapshot()
     {
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        let wal_path = metadata_dir.join("journal.wal");
        let saved_wal = metadata_dir.join("journal.saved");
        let incoming_state = TestMux::default();
        incoming_state.users().ensure_root_user("restored", "hash");
        let offer = |created_at| {
            let table = ClientTable::new(CLIENTS_TABLE_MAX);
            let mut snapshot =
                IggySnapshot::create(&incoming_state, TRANSFER_FLOOR, created_at).unwrap();
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            (snapshot.encode().unwrap(), table)
        };

        // Keep the open WAL readable but make the truncate's rename fail, after
        // the snapshot and its superblock pairing have reached disk.
        let (first, table) = offer(1);
        std::fs::rename(&wal_path, &saved_wal).unwrap();
        std::fs::create_dir(&wal_path).unwrap();
        let failed = md
            .install_state_transfer(&first, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
            .await;
        std::fs::remove_dir(&wal_path).unwrap();
        std::fs::rename(&saved_wal, &wal_path).unwrap();
        assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
        let durable = std::fs::read(metadata_dir.join("snapshot.bin")).unwrap();

        // The checkpoint of another primary at the same op, stamped at another time.
        let (second, table) = offer(2);
        assert_ne!(first, second);
        // A crash once a rewritten snapshot is durable, before its superblock write.
        STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::DirectorySynced));
        let installed = md
            .install_state_transfer(&second, table, TRANSFER_FLOOR, TRANSFER_FLOOR)
            .await;
        let image = crash_image(directory.path());
        let boundary = STOP_CHECKPOINT_AFTER.take();
        let recovered = recover::<TestMux>(
            image.path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(metadata_dir.join("snapshot.bin")).unwrap(),
            durable
        );
        assert_eq!(
            recovered.snapshot_checkpoint,
            md.coordinator.as_ref().unwrap().last_checkpoint()
        );
        assert_eq!(
            boundary,
            Some(CheckpointBoundary::DirectorySynced),
            "an offer of the durable op must not rewrite its snapshot"
        );
        assert!(installed.is_ok(), "{installed:?}");
        assert_eq!(md.consensus.as_ref().unwrap().commit_min(), TRANSFER_FLOOR);
    }

    #[compio::test]
    async fn given_a_pending_checkpoint_after_failed_transfer_when_retried_should_keep_the_newer_snapshot()
     {
        /// Two ops above the commit point, so a trim to it would drain an op
        /// above that point too.
        const INSTALLED: u64 = TRANSFER_FLOOR + 1;
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let consensus = md.consensus.as_ref().unwrap();
        let journal = md.journal.as_ref().unwrap();
        let coordinator = md.coordinator.as_ref().unwrap();
        consensus.clear_pipeline();
        consensus.advance_commit_max(TRANSFER_FLOOR - 1);
        md.commit_journal().await;
        assert_eq!(consensus.commit_min(), TRANSFER_FLOOR - 1);
        md.set_checkpoint_margin(usize::MAX);
        STOP_CHECKPOINT_AFTER.set(Some(CheckpointBoundary::TableCaptured));
        md.checkpoint_if_needed(consensus, journal).await;
        assert!(STOP_CHECKPOINT_AFTER.get().is_none());
        assert!(coordinator.checkpoint_pending.get());

        let metadata_dir = directory.path().join(crate::impls::METADATA_DIR);
        let wal_path = metadata_dir.join("journal.wal");
        let saved_wal = metadata_dir.join("journal.saved");
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::create(&TestMux::default(), INSTALLED, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        std::fs::rename(&wal_path, &saved_wal).unwrap();
        std::fs::create_dir(&wal_path).unwrap();
        let failed = md
            .install_state_transfer(&snapshot.encode().unwrap(), table, INSTALLED, INSTALLED)
            .await;
        std::fs::remove_dir(&wal_path).unwrap();
        std::fs::rename(&saved_wal, &wal_path).unwrap();
        assert!(matches!(failed, Err(SnapshotError::Io(_))), "{failed:?}");
        assert_eq!(consensus.commit_min(), TRANSFER_FLOOR - 1);
        assert_eq!(coordinator.last_checkpoint().0, INSTALLED);
        assert!(coordinator.checkpoint_pending.get());
        let before = std::fs::read(coordinator.snapshot_path()).unwrap();

        md.checkpoint_if_needed(consensus, journal).await;
        for op in consensus.commit_min()..INSTALLED {
            assert!(
                journal
                    .handle()
                    .header(usize::try_from(op).unwrap())
                    .is_some(),
                "op {op}: the pairing is ahead of the commit point, so no trim may reach it"
            );
        }
        assert_eq!(journal.handle().snapshot_op(), 0);
        assert!(coordinator.checkpoint_pending.get());
        assert_eq!(std::fs::read(coordinator.snapshot_path()).unwrap(), before);
        let recovered = recover::<TestMux>(
            crash_image(directory.path()).path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.snapshot_checkpoint.0, INSTALLED);

        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        md.install_state_transfer(&snapshot.encode().unwrap(), table, INSTALLED, INSTALLED)
            .await
            .unwrap();
        md.checkpoint_if_needed(consensus, journal).await;
        assert_eq!(consensus.commit_min(), INSTALLED);
        assert!(!coordinator.checkpoint_pending.get());
    }

    #[compio::test]
    async fn given_a_vouched_suffix_when_commits_walk_during_snapshot_io_should_preserve_their_receipts()
     {
        const CLIENT: u128 = 1;
        let directory = tempfile::tempdir().unwrap();
        let md = transfer_receiver(directory.path(), true).await;
        let consensus = md.consensus.as_ref().unwrap();
        let journal = md.journal.as_ref().unwrap().handle();
        consensus.clear_pipeline();
        consensus.certify_view_log(
            TRANSFER_RECEIVER_HEAD,
            journal
                .header(usize::try_from(TRANSFER_RECEIVER_HEAD).unwrap())
                .unwrap()
                .checksum,
        );
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        let mut snapshot = IggySnapshot::create(&TestMux::default(), TRANSFER_FLOOR, 1).unwrap();
        snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
        let bytes = snapshot.encode().unwrap();
        let held = SNAPSHOT_IO.lock().await;
        let mut install =
            Box::pin(md.install_state_transfer(&bytes, table, TRANSFER_FLOOR, TRANSFER_FLOOR));
        assert!(futures::poll!(&mut install).is_pending());
        consensus.advance_commit_max(TRANSFER_RECEIVER_HEAD);
        md.commit_journal().await;
        assert_eq!(consensus.commit_min(), TRANSFER_RECEIVER_HEAD);
        assert_eq!(
            md.client_table.borrow().get_watermark(CLIENT),
            Some(TRANSFER_RECEIVER_HEAD)
        );
        let applied = IggySnapshot::create(&*md.mux_stm, TRANSFER_RECEIVER_HEAD, 1)
            .unwrap()
            .encode()
            .unwrap();
        let table_after_walk = md.client_table.borrow().encode();
        drop(held);
        let outcome = install.await.unwrap();
        assert_eq!(outcome.installed_frontier, TRANSFER_RECEIVER_HEAD);
        assert_eq!(journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
        assert_eq!(md.client_table.borrow().encode(), table_after_walk);
        assert_eq!(
            IggySnapshot::create(&*md.mux_stm, TRANSFER_RECEIVER_HEAD, 1)
                .unwrap()
                .encode()
                .unwrap(),
            applied
        );
        let recovered = recover::<TestMux>(
            crash_image(directory.path()).path(),
            TRANSFER_RECEIVER,
            DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            ChangeLogCapacities::default(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.journal.last_op(), Some(TRANSFER_RECEIVER_HEAD));
        assert_eq!(recovered.chain_break_op, None);
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
                TestMux::new((session_users(), (Streams::default(), ()))),
                None,
            );
        let consensus = md.consensus.as_ref().unwrap();
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT_B,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT_B, SESSION),
            )
            .unwrap();

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
        let mut register = Box::pin(md.submit_register_in_process(
            CLIENT_C,
            ACTING_USER,
            RegisterRequest {
                credential: CredentialProof::account(1, 1),
                bind_verifier: [0x5a; 32],
            },
        ));
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();

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

    /// A checkpoint job holds `journal_gate` while it rewrites the WAL. A
    /// request that the primary prepared meanwhile waited for the rewrite on
    /// the shard 0 pump, and so did the promotion of a queued one.
    #[compio::test]
    async fn given_a_pending_wal_trim_when_a_request_arrives_should_queue_it_until_the_trim_ends() {
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
        md.client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                ACTING_USER,
                [0x5a; 32],
                register_reply(CLIENT, SESSION),
            )
            .unwrap();
        let journal = md.journal.as_ref().unwrap().handle();

        // As a checkpoint job: it announces its trim, then takes the gate.
        let trim = md.announce_checkpoint_trim();
        let rewrite = md.hold_journal_writes().await;
        futures::FutureExt::now_or_never(md.on_request(create_stream_request(CLIENT, 1, "s1")))
            .expect("a request must not wait for the WAL trim on the pump");
        assert_eq!(consensus.request_queue_len(), 1);
        futures::FutureExt::now_or_never(md.resume_stranded_commits())
            .expect("the promotion must not wait for the WAL trim on the pump");
        assert_eq!(
            consensus.request_queue_len(),
            1,
            "no request is promoted while the trim is pending"
        );
        assert_eq!(journal.last_op(), None);

        drop(rewrite);
        drop(trim);
        md.resume_stranded_commits().await;

        assert_eq!(consensus.request_queue_len(), 0);
        assert_eq!(
            journal.last_op(),
            Some(consensus.sequencer().current_sequence()),
            "the tick promotes the queued request once the trim ended"
        );
    }
}
