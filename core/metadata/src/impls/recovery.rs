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

use crate::impls::metadata::IggySnapshot;
use crate::stm::StateMachine;
use crate::stm::authz::{GatedApply, reject_stale_session};
use crate::stm::snapshot::{MetadataSnapshot, RestoreSnapshot, Snapshot, SnapshotError};
use consensus::{
    ClientTable, ClientTableDecodeError, CommitReply, HistoryRecovery, VsrState, VsrStateError,
    build_reply_message, build_reply_message_with,
};
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::consensus::{CHECKSUM_UNSEALED, Operation, PrepareHeader};
use iggy_binary_protocol::requests::system::FinalizeSessionRequest;
use iggy_binary_protocol::requests::users::login_register::RegisterRequest;
use iggy_common::IggyError;
use journal::Journal as _;
use journal::prepare_journal::{JournalError, PrepareJournal};
use journal::superblock::{
    PingPongSuperblock, SLOT_FILE_NAMES, SuperblockContents, SuperblockStore,
};
use server_common::Message;
use std::fmt;
use std::path::{Path, PathBuf};

/// Error type for metadata recovery.
#[derive(Debug)]
pub enum RecoveryError {
    Snapshot(SnapshotError),
    Journal(JournalError),
    StateMachine(IggyError),
    Io(std::io::Error),
    DisconnectedSoloJournal {
        path: PathBuf,
        op: u64,
    },
    /// Opening or reading the durable superblock failed. `dir` is the metadata
    /// directory holding [`journal::superblock::SLOT_FILE_NAMES`]: `compio` attaches no
    /// path to its I/O errors, so without this the operator's chain ends in a bare
    /// `ENOENT`.
    Superblock {
        dir: PathBuf,
        source: std::io::Error,
    },
    /// A checkpoint client-table slot held bytes that are not a valid reply
    /// message (a corrupt or torn snapshot).
    ClientTableDecode(ClientTableDecodeError),
    RetryProtection(consensus::ClientTableWireError),
    /// The superblock references a checkpoint newer than the on-disk snapshot, a
    /// lost or reverted snapshot write: recovering from the older snapshot while
    /// trusting the superblock's commit point could skip committed state.
    CheckpointAheadOfSnapshot {
        dir: PathBuf,
        checkpoint_op: u64,
        snapshot_op: u64,
    },
    /// The paired snapshot's checksum does not match the superblock's record (a
    /// corrupt or torn snapshot).
    CheckpointChecksumMismatch {
        dir: PathBuf,
        op: u64,
        expected: u128,
        actual: u128,
    },
    /// A superblock record was present and checksum-clean but its payload did not
    /// decode into a [`VsrState`]: a layout change that skipped a version bump, or
    /// corruption inside the checksummed region. The durable consensus state is
    /// unrecoverable, so refuse boot rather than infer a stale view from the WAL and
    /// risk re-entering a superseded view.
    SuperblockUndecodable {
        dir: PathBuf,
        source: VsrStateError,
    },
    /// The slots yield no record this build can trust: bytes that fail verification
    /// on every copy, or one clean record beside a copy that does not verify, whose
    /// lost `sequence` may have been the newer (`version` names an unrecognized
    /// format version when that was the cause). A superblock was written and its
    /// latest generation cannot be established, so refuse boot rather than treat the
    /// node as fresh or read through to a superseded view. An empty superblock is a
    /// different case, a genuinely fresh or not-yet-checkpointed node, and is NOT an
    /// error.
    SuperblockUnreadable {
        dir: PathBuf,
        version: Option<u16>,
    },
    /// The superblock was written by a different cluster or a different replica
    /// index: a copied, restored-to-the-wrong-host, or otherwise misplaced data
    /// directory. Recovering `(view, log_view)` from another replica's record would
    /// have this node act on consensus numbers it never reached.
    SuperblockIdentityMismatch {
        dir: PathBuf,
        field: IdentityField,
        expected: u128,
        found: u128,
    },
}

/// Which identity field of a durable [`VsrState`] failed to match this replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityField {
    Cluster,
    ReplicaId,
    ReplicaCount,
}

impl fmt::Display for IdentityField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster => write!(f, "cluster"),
            Self::ReplicaId => write!(f, "replica_id"),
            Self::ReplicaCount => write!(f, "replica_count"),
        }
    }
}

/// The identity a replica expects its own durable superblock to carry.
///
/// Bundled rather than passed as three positional scalars, since `cluster`,
/// `replica_id` and `replica_count` are exactly the values a silent misbinding would
/// make unrecoverable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaIdentity {
    pub cluster: u128,
    pub replica_id: u8,
    pub replica_count: u8,
}

impl ReplicaIdentity {
    /// A single-replica cluster: quorum is 1/1, so every journaled op committed the
    /// moment it was written.
    #[must_use]
    pub const fn is_solo(&self) -> bool {
        self.replica_count == 1
    }
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(e) => write!(f, "recovery snapshot error: {e}"),
            Self::Journal(e) => write!(f, "recovery journal error: {e}"),
            Self::StateMachine(e) => write!(f, "recovery state machine error: {e}"),
            Self::Io(e) => write!(f, "recovery I/O error: {e}"),
            Self::DisconnectedSoloJournal { path, op } => write!(
                f,
                "solo metadata journal {} does not connect at op {op}, refusing boot without changing the WAL",
                path.display()
            ),
            Self::Superblock { dir, source } => write!(
                f,
                "recovery superblock error in {} ({} / {}): {source}",
                dir.display(),
                SLOT_FILE_NAMES[0],
                SLOT_FILE_NAMES[1]
            ),
            Self::ClientTableDecode(e) => write!(f, "recovery client-table decode error: {e}"),
            Self::RetryProtection(e) => write!(f, "recovery retry protection error: {e}"),
            Self::CheckpointAheadOfSnapshot {
                dir,
                checkpoint_op,
                snapshot_op,
            } => write!(
                f,
                "superblock in {} references checkpoint op {checkpoint_op} but the on-disk \
                 snapshot is only at op {snapshot_op}: a lost or reverted snapshot write, \
                 refusing boot",
                dir.display()
            ),
            Self::CheckpointChecksumMismatch {
                dir,
                op,
                expected,
                actual,
            } => write!(
                f,
                "snapshot at op {op} in {} does not match the superblock's checkpoint checksum \
                 (superblock {expected:#034x}, snapshot {actual:#034x}): corrupt or torn snapshot, \
                 refusing boot",
                dir.display()
            ),
            Self::SuperblockUndecodable { dir, source } => write!(
                f,
                "superblock record in {} is present and checksum-clean but its payload did not \
                 decode into VSR state ({source}): unrecoverable durable consensus state, \
                 refusing boot",
                dir.display()
            ),
            Self::SuperblockUnreadable {
                dir,
                version: Some(version),
            } => write!(
                f,
                "superblock in {} present but its format version {version} is unrecognized by \
                 this build (a downgrade, or a corrupt version field): refusing boot",
                dir.display()
            ),
            Self::SuperblockUnreadable { dir, version: None } => write!(
                f,
                "superblock present but a copy holds bytes that do not verify (bit-rot or a \
                 checksum failure), so its latest generation cannot be established: refusing \
                 boot; inspect {} and {} in {}",
                SLOT_FILE_NAMES[0],
                SLOT_FILE_NAMES[1],
                dir.display()
            ),
            Self::SuperblockIdentityMismatch {
                dir,
                field,
                expected,
                found,
            } => write!(
                f,
                "superblock {field} in {} is {found} but this replica is configured with \
                 {expected}: the metadata directory belongs to another cluster or replica, or the \
                 cluster was resized without reconfiguration, refusing boot",
                dir.display()
            ),
        }
    }
}

impl std::error::Error for RecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Snapshot(e) => Some(e),
            Self::Journal(e) => Some(e),
            Self::StateMachine(e) => Some(e),
            Self::Io(e) | Self::Superblock { source: e, .. } => Some(e),
            Self::ClientTableDecode(e) => Some(e),
            Self::RetryProtection(e) => Some(e),
            Self::SuperblockUndecodable { source, .. } => Some(source),
            Self::CheckpointAheadOfSnapshot { .. }
            | Self::CheckpointChecksumMismatch { .. }
            | Self::SuperblockUnreadable { .. }
            | Self::SuperblockIdentityMismatch { .. }
            | Self::DisconnectedSoloJournal { .. } => None,
        }
    }
}

impl From<ClientTableDecodeError> for RecoveryError {
    fn from(e: ClientTableDecodeError) -> Self {
        Self::ClientTableDecode(e)
    }
}

impl From<SnapshotError> for RecoveryError {
    fn from(e: SnapshotError) -> Self {
        Self::Snapshot(e)
    }
}

impl From<JournalError> for RecoveryError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}

impl From<IggyError> for RecoveryError {
    fn from(e: IggyError) -> Self {
        Self::StateMachine(e)
    }
}

impl From<std::io::Error> for RecoveryError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Result of a successful metadata recovery.
pub struct RecoveredMetadata<M> {
    pub journal: PrepareJournal,
    pub snapshot: Option<IggySnapshot>,
    /// The durable superblock, opened by recovery so it could cross-check the
    /// snapshot's op and checksum against the checkpoint pairing before restoring the
    /// state machine from it. Handed to the consensus/metadata layer to reuse;
    /// re-opening would fork the ping-pong sequence counter.
    pub superblock: PingPongSuperblock,
    /// The durable VSR state read from the superblock, or `None` when none has been
    /// written yet (a fresh deployment, or a node that took writes but never
    /// checkpointed or changed view). A present but unreadable or undecodable
    /// superblock refuses boot instead ([`RecoveryError::SuperblockUnreadable`] /
    /// [`RecoveryError::SuperblockUndecodable`]). Consensus restores
    /// `(view, log_view)` from it; recovery has already verified the paired snapshot
    /// against it.
    pub recovered_state: Option<VsrState>,
    /// `(snapshot_op, snapshot_checksum)` of the verified on-disk snapshot, `(0, 0)`
    /// when none. Seeds the coordinator's last-checkpoint pairing so the first
    /// post-boot view-change superblock write records the real pairing.
    pub snapshot_checkpoint: (u64, u128),
    pub mux_stm: M,
    /// Client table restored from the checkpoint and advanced by the replayed
    /// committed suffix: registers carry their own commit op as the epoch, so
    /// replay reads fences out of the log rather than deriving them from replay
    /// order, and replies re-cache byte-identically (`build_reply_message*` reads
    /// only the prepare header + deterministic apply output). Sessions whose
    /// register fell below the snapshot floor come back from the checkpoint's
    /// folded table, watermarks included.
    pub client_table: ClientTable,
    /// `None` means no snapshot existed and no journal entries were replayed.
    /// `Some(op)` is the highest op applied, either from the snapshot or journal replay.
    ///
    /// Only the committed prefix is applied: an entry's `commit` header field
    /// carries the primary's commit watermark when the prepare was sent, and
    /// journaled does not imply committed.
    pub last_applied_op: Option<u64>,
    /// Highest durable op above the snapshot floor, `None` when none remain.
    /// Later entries may have committed elsewhere. They stay unapplied until
    /// the cluster proves the adopted log and repairs any missing prefix.
    pub last_journaled_op: Option<u64>,
    /// First op replay could not connect to its predecessor, `None` when the
    /// replayed range is one unbroken chain.
    ///
    /// Entries at and above it remain durable but unapplied until the cluster's
    /// view-log proof reconciles them. The restored head includes this suffix.
    pub chain_break_op: Option<u64>,
    /// What the WAL proves about its original prefix. A retained newer suffix is
    /// only adopted identity until repair proves the complete body log.
    pub history: HistoryRecovery,
}

/// The contiguous WAL run that can prove recovery's committed frontier.
pub struct ReplayPrefix<'a> {
    pub headers: &'a [PrepareHeader],
    pub commit_watermark: u64,
    pub chain_break_op: Option<u64>,
}

/// Select the contiguous parent-chained suffix above a snapshot from sorted headers.
#[must_use]
pub fn replay_prefix(headers: &[PrepareHeader], snapshot_op: u64, solo: bool) -> ReplayPrefix<'_> {
    let start = headers.partition_point(|header| header.op <= snapshot_op);
    let suffix = &headers[start..];
    let mut previous = headers[..start]
        .last()
        .filter(|header| snapshot_op != 0 && header.op == snapshot_op);
    let mut expected = snapshot_op.saturating_add(1).max(1);
    let mut length = 0;
    let mut commit_watermark = snapshot_op;
    let mut chain_break_op = None;
    for header in suffix {
        let broken_chain = previous.is_some_and(|previous| {
            previous.checksum != CHECKSUM_UNSEALED
                && header.checksum != CHECKSUM_UNSEALED
                && header.parent != previous.checksum
        });
        if header.op != expected || broken_chain {
            chain_break_op = Some(header.op);
            break;
        }
        commit_watermark = commit_watermark.max(if solo { header.op } else { header.commit });
        previous = Some(header);
        expected = header.op.saturating_add(1);
        length += 1;
    }
    ReplayPrefix {
        headers: &suffix[..length],
        commit_watermark,
        chain_break_op,
    }
}

/// Recover metadata state from disk.
///
/// 1. Load snapshot from `{data_dir}/metadata/snapshot.bin` if present
/// 2. Restore state machine from snapshot, or initialize empty state
/// 3. Open WAL at `{data_dir}/metadata/journal.wal`, scan and rebuild index
/// 4. Replay journal entries from the first post-snapshot op through the
///    state machine, rebuilding the client table alongside (registers mint
///    epochs, replies re-cache) exactly as the commit paths did live
/// 5. Return the assembled `RecoveredMetadata`
///
/// Only the owning shard (shard 0) should call this. Peer shards receive
/// a `ReadHandleFactory` bundle from shard 0 and skip WAL access entirely.
///
/// # Errors
/// Returns `RecoveryError` if snapshot loading, journal opening, or replay fails.
/// `seed_baseline` reproduces caller-supplied state outside the WAL.
/// Production root creation is journaled. The callback runs on the freshly-defaulted
/// state machine BEFORE journal replay and ONLY when no snapshot exists, so
/// replayed ops land on the same baseline (and the same slab ids) they were
/// originally applied over. A snapshot already contains that baseline.
///
/// `clients_table_max` sizes a fresh registry. Recovery preserves the
/// checkpoint's committed capacity.
///
/// `on_replayed_session` receives registration, logout, and user-revocation
/// effects. For revocation, `client` and `session` identify each affected
/// session; the operation, op, and timestamp still name the committed user mutation.
///
/// `identity` is what this replica expects its own durable superblock to carry, and
/// a mismatch refuses boot ([`RecoveryError::SuperblockIdentityMismatch`]). Its
/// `replica_count == 1` also marks a single-replica cluster: the quorum is 1/1, so
/// every journaled op was committed the moment it was written and replay runs to
/// the journal head. The embedded `commit` stamps cannot be used there: each
/// stamp is the primary's commit point when the prepare was SENT, so a
/// pipelined burst (e.g. create-stream + create-topic on one connection) is
/// stamped entirely below its own ops and would replay as nothing.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub async fn recover<M>(
    data_dir: &Path,
    identity: ReplicaIdentity,
    journal_slots: usize,
    clients_table_max: usize,
    seed_baseline: impl FnOnce(&M),
    on_replayed_session: impl Fn(&M, &PrepareHeader),
) -> Result<RecoveredMetadata<M>, RecoveryError>
where
    M: StateMachine<Input = Message<PrepareHeader>, Error = IggyError>
        + GatedApply
        + RestoreSnapshot<MetadataSnapshot>
        + Default,
{
    let metadata_dir = data_dir.join(super::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir)?;

    let snapshot_path = metadata_dir.join("snapshot.bin");
    // The checksum is over the bytes on disk, so the pairing cross-check below holds
    // across a schema change: see `IggySnapshot::load`.
    let (snapshot, snapshot_checksum) = if snapshot_path.exists() {
        let (snapshot, checksum) = IggySnapshot::load(&snapshot_path)?;
        (Some(snapshot), checksum)
    } else {
        (None, 0)
    };

    // Validate protection before opening storage that may repair a torn WAL.
    let mut client_table = match snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.snapshot().client_table.clone())
    {
        Some(table) => ClientTable::from_snapshot(table)?,
        None if snapshot.is_none() => ClientTable::new(clients_table_max),
        None => {
            return Err(RecoveryError::RetryProtection(
                consensus::ClientTableWireError::EmptyRing,
            ));
        }
    };

    // Open the durable superblock and read the last VSR state. Recovery owns this so
    // it can verify the snapshot against the superblock's checkpoint pairing BEFORE
    // trusting (decoding) it into the state machine and client table. The consensus
    // layer reuses the returned superblock, since re-opening would fork the ping-pong
    // sequence counter, and restores `(view, log_view)` from `recovered_state`.
    let superblock = PingPongSuperblock::open(&metadata_dir)
        .await
        .map_err(|source| RecoveryError::Superblock {
            dir: metadata_dir.clone(),
            source,
        })?;
    // Distinguish the three read outcomes so a lost or corrupt superblock cannot
    // masquerade as a fresh deployment: a present but unreadable or
    // unrecognized-version record refuses boot with a typed error. An EMPTY
    // superblock is genuinely fresh, or a node that took writes but never
    // checkpointed or changed view, and is safe to treat as no durable state: the
    // persist-before-send gate guarantees this replica never externalized a view it
    // cannot re-derive by re-probing, so it recovers `(view, log_view)` from the
    // WAL/init path with no split-brain risk.
    let recovered_state =
        match superblock
            .read_latest()
            .await
            .map_err(|source| RecoveryError::Superblock {
                dir: metadata_dir.clone(),
                source,
            })? {
            SuperblockContents::Present(bytes) => {
                // A checksum-clean record that does not decode is a durability violation,
                // not absent state: refuse boot rather than infer a stale view.
                Some(VsrState::try_from(bytes.as_slice()).map_err(|source| {
                    RecoveryError::SuperblockUndecodable {
                        dir: metadata_dir.clone(),
                        source,
                    }
                })?)
            }
            SuperblockContents::Unreadable { version } => {
                return Err(RecoveryError::SuperblockUnreadable {
                    dir: metadata_dir.clone(),
                    version,
                });
            }
            SuperblockContents::Empty => None,
        };

    // The superblock is the only on-disk identity in the metadata directory, and the
    // wire handshake cannot stand in for it: both peers derive `cluster` from the
    // configured cluster name, so it never sees a foreign disk.
    if let Some(state) = recovered_state.as_ref() {
        verify_identity(state, identity, &metadata_dir)?;
        state
            .validate_group(server_common::sharding::METADATA_GROUP, 0)
            .map_err(|source| RecoveryError::SuperblockUndecodable {
                dir: metadata_dir.clone(),
                source,
            })?;
    }

    // Second of the snapshot's two integrity checks, and the one the superblock owns.
    // Ordering, precisely: `IggySnapshot::load` verified the snapshot's own trailer
    // against its payload before decoding it, and this cross-checks the decoded
    // snapshot's op against the superblock's pairing. The pairing check cannot run
    // first, since it needs `sequence_number` out of the decoded snapshot.
    //
    // A superblock pointing past the snapshot (a lost snapshot write) or disagreeing on
    // its checksum (corrupt or torn) is a durability violation, not a recoverable
    // state. One that merely lags a newer, atomically-complete snapshot is accepted,
    // since `commit_max` is a recovery lower bound and the WAL suffix re-commits.
    //
    // Metadata state transfer exists now, and refusing boot here is still the right
    // answer rather than a gap waiting on it. What this check catches is LOCAL
    // durability damage -- a lost snapshot write, or a torn/corrupt one -- not a
    // replica that merely fell behind the cluster. Transfer repairs the second, and
    // does so post-boot once consensus has a view and a live primary to pull from;
    // wiring it in here would silently re-sync over a failing disk instead of
    // surfacing it. The operator remedy is to clear the metadata directory, after
    // which the node rejoins through the ordinary transfer path (covered by
    // `restart_from_clean_slate` and the fresh-late-join spec).
    let snapshot_op = snapshot.as_ref().map_or(0, IggySnapshot::sequence_number);
    verify_checkpoint_pairing(
        recovered_state.as_ref(),
        snapshot_op,
        snapshot_checksum,
        &metadata_dir,
    )?;

    let mux_stm = if let Some(snapshot) = snapshot.as_ref() {
        M::restore_snapshot(snapshot.snapshot())?
    } else {
        let mux_stm = M::default();
        seed_baseline(&mux_stm);
        mux_stm
    };

    let journal_path = metadata_dir.join("journal.wal");
    let watermark = snapshot.as_ref().map_or(0, IggySnapshot::sequence_number);
    let journal = PrepareJournal::open_with_slots(&journal_path, watermark, journal_slots).await?;

    let history = HistoryRecovery::new(recovered_state.as_ref(), identity.is_solo(), |op| {
        usize::try_from(op)
            .ok()
            .and_then(|slot| journal.header(slot))
            .map(|header| header.checksum)
    });
    let history = recovered_state
        .as_ref()
        .and_then(|state| state.adopted_view.as_ref())
        .map_or(history, |adopted| {
            history.retain_adopted_suffix(adopted, journal.iter_headers_from(0))
        });
    if let HistoryRecovery::Certified {
        truncate_from: Some(from),
        ..
    } = history
        && journal.last_op().is_some_and(|last| last >= from)
    {
        let removed = journal
            .truncate_from(from)
            .await
            .map_err(RecoveryError::Io)?;
        tracing::warn!(
            from,
            removed,
            "dropped metadata WAL entries journaled for a view this replica never proved"
        );
    }

    // The scan replays entries no producer sealed rather than rejecting them, so
    // report the count: an operator gets no other signal that these bodies were
    // replayed on trust. Storage-format admission remains a separate prerequisite.
    let unsealed_entries = journal.unsealed_entry_count();
    if unsealed_entries > 0 {
        tracing::warn!(
            unsealed_entries,
            path = %journal_path.display(),
            "metadata WAL holds entries without a body checksum; replayed unverified \
             (the writer did not seal the frame)"
        );
    }

    let headers = journal.iter_headers_from(snapshot_op);
    let prefix = replay_prefix(&headers, snapshot_op, identity.is_solo());
    let headers_to_replay = prefix.headers;
    let commit_watermark = prefix.commit_watermark;
    let chain_break_op = prefix.chain_break_op;
    if let Some(op) = chain_break_op {
        if identity.is_solo() {
            return Err(RecoveryError::DisconnectedSoloJournal {
                path: journal_path,
                op,
            });
        }
        tracing::error!(
            op,
            snapshot_op,
            last_contiguous_op = headers_to_replay.last().map(|header| header.op),
            "metadata WAL does not connect at this op, stopping replay before the suffix"
        );
    }

    // The superblock's `commit_max` is the highest op this replica knew the cluster had
    // committed when it last wrote. Report a gap against what the WAL can prove, since
    // nothing else surfaces "committed ops this node cannot see".
    //
    // A warning and not a refusal, deliberately: each journaled prepare stamps the
    // primary's commit point as of its SEND, so the derived watermark is a lower bound
    // and trails `commit_max` on a perfectly healthy node. That is also why state
    // transfer landing does not promote this into a trigger -- the comparison cannot
    // separate "behind" from "healthy", so arming on it would make every restart
    // transfer. The triggers that do fire are post-boot and precise: journal repair
    // answering `RangeEvicted` (the gap is provably below a peer's retained floor), a
    // StartView adopted while awaiting a target, and the same-view heartbeat backstop.
    if let Some(state) = recovered_state.as_ref()
        && state.commit_max > commit_watermark
    {
        tracing::warn!(
            durable_commit_max = state.commit_max,
            wal_commit_watermark = commit_watermark,
            snapshot_op,
            "superblock records a commit point above what this replica's WAL can prove; \
             the gap re-commits from the cluster, or needs state transfer if it does not"
        );
    }

    let mut last_applied_op: Option<u64> = None;
    let last_journaled_op = journal.last_op().filter(|op| *op > snapshot_op);
    for header in headers_to_replay {
        if header.op > commit_watermark {
            continue;
        }
        client_table
            .commit_capacity(header.retry_capacity as usize)
            .map_err(RecoveryError::RetryProtection)?;

        // Register/Logout apply their session effects through the callback,
        // mirroring the commit paths (`on_ack` / `commit_journal`).
        //
        // Epochs come back identical on every replica: they are the register's
        // own commit op, so replay reads them out of the log rather than
        // deriving them from replay order.
        //
        // Watermarks do not come out of replay alone: replay starts at THIS
        // node's snapshot floor, and checkpointing is node-local
        // (`checkpoint_if_needed` fires on local journal occupancy), so replicas
        // cross the floor at different ops. A client whose earlier register fell
        // below this node's floor would take `commit_register`'s fresh-entry
        // branch and come back with `watermark = 0`, where a peer that replayed
        // both took the rebind branch and kept it -- the same request id one node
        // answers `Duplicate` the other re-executes, exactly-once degrading to
        // at-least-once. That is why the table is folded into the checkpoint
        // above: the pre-floor watermark is restored from the snapshot, and
        // replay only advances it.
        //
        // Residual gap: a checkpoint written before the fold existed carries no
        // table, so recovery from one still starts empty. It converges once the
        // node takes its next checkpoint.
        if header.operation == Operation::Register {
            let prepare = journal.entry_at(header).await?.ok_or_else(|| {
                RecoveryError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "committed Register is missing from WAL",
                ))
            })?;
            let registration = RegisterRequest::decode_from(prepare.body()).map_err(|_| {
                RecoveryError::RetryProtection(consensus::ClientTableWireError::InvalidReply)
            })?;
            if !mux_stm.is_active_account(header.user_id, registration.user_created_at) {
                last_applied_op = Some(header.op);
                continue;
            }
            let reply = build_reply_message(header, &bytes::Bytes::new());
            client_table
                .commit_register(
                    header.client,
                    header.user_id,
                    registration.bind_verifier,
                    reply,
                )
                .map_err(RecoveryError::RetryProtection)?;
            on_replayed_session(&mux_stm, header);
            last_applied_op = Some(header.op);
            continue;
        }
        if header.operation == Operation::Logout {
            if client_table
                .commit_logout(
                    header.client,
                    header.user_id,
                    header.session,
                    build_reply_message(header, &bytes::Bytes::new()),
                )
                .map_err(RecoveryError::RetryProtection)?
            {
                on_replayed_session(&mux_stm, header);
            }
            last_applied_op = Some(header.op);
            continue;
        }
        if header.operation == Operation::FinalizeSession {
            let prepare = journal.entry_at(header).await?.ok_or_else(|| {
                RecoveryError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "committed finalization is missing from WAL",
                ))
            })?;
            let request = FinalizeSessionRequest::decode_from(prepare.body()).map_err(|_| {
                RecoveryError::RetryProtection(consensus::ClientTableWireError::InvalidReply)
            })?;
            if mux_stm.namespace_revision() == request.namespace_revision {
                client_table.finalize_session(request.identity);
            }
            last_applied_op = Some(header.op);
            continue;
        }

        let entry = journal.entry_at(header).await?.ok_or_else(|| {
            RecoveryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read journal entry for op={}", header.op),
            ))
        })?;
        // WAL replay must recompute authorization denials identically to the
        // primary/backup commit paths, so it goes through the same gate.
        let reply = reject_stale_session(header, &client_table)
            .map_or_else(|| mux_stm.gated_update(entry), Ok)?;
        if let Some(user_id) = reply.revoked_user {
            for identity in client_table.end_user_sessions(user_id, header.op) {
                on_replayed_session(
                    &mux_stm,
                    &PrepareHeader {
                        client: identity.client_id,
                        session: identity.session,
                        ..*header
                    },
                );
            }
        }
        // Every client commit must restore its original receipt before admission.
        if header.client != consensus::client_table::RESERVED_CLIENT_ID
            && !message_bus::is_auto_commit_client(header.client)
            && !matches!(
                header.operation,
                Operation::CompleteConsumerGroupRevocation | Operation::CompleteLifecycle
            )
        {
            let cached = build_reply_message_with(header, reply.reply_body_len(), |dst| {
                reply.write_reply_body(dst);
            });
            if !matches!(
                client_table.commit_reply(header.client, header.user_id, cached),
                CommitReply::Cached | CommitReply::SessionEnded { .. }
            ) {
                return Err(RecoveryError::RetryProtection(
                    consensus::ClientTableWireError::InvalidWatermark {
                        client_id: header.client,
                    },
                ));
            }
        }
        tracing::debug!(
            target: "iggy.metadata.diag",
            op = header.op,
            operation = ?header.operation,
            user_id = header.user_id,
            "recovery replayed op"
        );
        last_applied_op = Some(header.op);
    }

    Ok(RecoveredMetadata {
        journal,
        snapshot,
        superblock,
        recovered_state,
        snapshot_checkpoint: (snapshot_op, snapshot_checksum),
        mux_stm,
        client_table,
        last_applied_op,
        last_journaled_op,
        chain_break_op,
        history,
    })
}

/// Refuse a durable record this replica did not write.
///
/// Scope it honestly: identity catches MISPLACEMENT, not staleness. An older backup
/// of this replica's own directory carries the right `cluster`, `replica_id` and
/// `replica_count` and passes here; only the checkpoint pairing and the WAL constrain
/// how old it may be.
///
/// `replica_count` is checked alongside the identity pair because quorum size and the
/// `view % replica_count` primary mapping both derive from it. There is no VSR
/// reconfiguration yet, so a count that disagrees with the config means the cluster
/// was resized underneath a node, and booting it would have replicas disagree on both
/// quorum and who leads a view.
///
/// # Errors
/// [`RecoveryError::SuperblockIdentityMismatch`] naming the first field that differs
/// and the directory it was read from, since which directory a node was pointed at is
/// the whole diagnosis here.
fn verify_identity(
    state: &VsrState,
    expected: ReplicaIdentity,
    dir: &Path,
) -> Result<(), RecoveryError> {
    let mismatch = |field, expected: u128, found: u128| RecoveryError::SuperblockIdentityMismatch {
        dir: dir.to_path_buf(),
        field,
        expected,
        found,
    };
    if state.cluster != expected.cluster {
        return Err(mismatch(
            IdentityField::Cluster,
            expected.cluster,
            state.cluster,
        ));
    }
    if state.replica_id != expected.replica_id {
        return Err(mismatch(
            IdentityField::ReplicaId,
            expected.replica_id.into(),
            state.replica_id.into(),
        ));
    }
    if state.replica_count != expected.replica_count {
        return Err(mismatch(
            IdentityField::ReplicaCount,
            expected.replica_count.into(),
            state.replica_count.into(),
        ));
    }
    Ok(())
}

/// Cross-check the superblock's checkpoint pairing against the on-disk snapshot
/// BEFORE the snapshot's decoded contents are trusted. The superblock's
/// `(checkpoint_op, checkpoint_checksum)` identify the snapshot its `commit_max` was
/// durable against.
///
/// - `checkpoint_op > snapshot_op`: the superblock references a checkpoint newer than
///   the snapshot on disk, a lost or reverted snapshot write. Recovering from the
///   older snapshot while trusting `commit_max` could skip committed state, so refuse
///   boot.
/// - `checkpoint_op == snapshot_op` with differing checksums: the paired snapshot is
///   corrupt or torn. Refuse boot.
/// - `checkpoint_op < snapshot_op`: the pairing lags a newer, atomically-complete
///   snapshot, from a checkpoint whose superblock update had not yet landed. The
///   snapshot subsumes it and `commit_max` is only a recovery lower bound, so accept.
/// - No recovered state, meaning a fresh or not-yet-checkpointed node: the WAL and
///   snapshot inference stands alone, nothing to cross-check. A present but
///   unreadable superblock refuses boot upstream, so it never arrives here as `None`.
///
/// # Errors
/// [`RecoveryError::CheckpointAheadOfSnapshot`] or
/// [`RecoveryError::CheckpointChecksumMismatch`] on a durability violation.
fn verify_checkpoint_pairing(
    recovered: Option<&VsrState>,
    snapshot_op: u64,
    snapshot_checksum: u128,
    dir: &Path,
) -> Result<(), RecoveryError> {
    let Some(state) = recovered else {
        return Ok(());
    };
    if state.checkpoint_op > snapshot_op {
        return Err(RecoveryError::CheckpointAheadOfSnapshot {
            dir: dir.to_path_buf(),
            checkpoint_op: state.checkpoint_op,
            snapshot_op,
        });
    }
    if state.checkpoint_op == snapshot_op && state.checkpoint_checksum != snapshot_checksum {
        return Err(RecoveryError::CheckpointChecksumMismatch {
            dir: dir.to_path_buf(),
            op: state.checkpoint_op,
            expected: state.checkpoint_checksum,
            actual: snapshot_checksum,
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use crate::impls::metadata::{StreamsFrontend, apply_committed_prepare, checkpoint_checksum};
    use crate::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
    use crate::stm::snapshot::{SNAPSHOT_FORMAT_VERSION, Snapshotable};
    use crate::stm::stream::{Streams, StreamsSnapshot};
    use crate::stm::user::{CreatePersonalAccessTokenRequest, PAT_TOKEN_HASH_BYTES, Users};
    use consensus::{CLIENTS_TABLE_MAX, HistoryCertificate};
    use iggy_binary_protocol::consensus::{Command, Operation};
    use iggy_binary_protocol::primitives::permissions::{WireGlobalPermissions, WirePermissions};
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::users::{
        CreateUserRequest, DeleteUserRequest, UpdateUserRequest,
    };
    use iggy_binary_protocol::{WireEncode, WireIdentifier, WireName, WireOptions, result_code};
    use iggy_common::UserStatus;
    use journal::Journal;
    use server_common::iobuf::Owned;
    use std::cell::RefCell;
    use tempfile::tempdir;

    use crate::MuxStateMachine;

    type TestStm = MuxStateMachine<()>;

    const HEADER_SIZE: usize = size_of::<PrepareHeader>();

    /// Replica 0 of a 3-node cluster 1: quorum 2, so the WAL's embedded commit
    /// stamps bound replay.
    const CLUSTERED: ReplicaIdentity = ReplicaIdentity {
        cluster: 1,
        replica_id: 0,
        replica_count: 3,
    };

    /// The same replica running solo: quorum 1/1, so replay runs to the journal head.
    const SOLO: ReplicaIdentity = ReplicaIdentity {
        cluster: 1,
        replica_id: 0,
        replica_count: 1,
    };

    fn vsr_state_with_checkpoint(checkpoint_op: u64, checkpoint_checksum: u128) -> VsrState {
        VsrState {
            cluster: 1,
            replica_id: 0,
            replica_count: 3,
            view: 7,
            log_view: 7,
            commit_max: 100,
            checkpoint_op,
            checkpoint_checksum,
            offset_frontier: 0,
            offset_reserved: 0,
            adopted_view: None,
        }
    }

    /// The full accept/reject matrix for the checkpoint pairing cross-check, which
    /// decides whether a node boots at all. Grouped because each case is one call
    /// on a pure function and the boundaries only mean something together.
    #[test]
    fn checkpoint_pairing_accepts_consistent_pairings_and_rejects_violations() {
        // The directory only reaches the error message, so one stand-in serves every case.
        let dir = Path::new("/metadata");

        // No superblock yet (fresh or not-yet-checkpointed): nothing to cross-check.
        assert!(verify_checkpoint_pairing(None, 42, 0xabc, dir).is_ok());

        // Exact pairing.
        let matching = vsr_state_with_checkpoint(42, 0xabc);
        assert!(verify_checkpoint_pairing(Some(&matching), 42, 0xabc, dir).is_ok());

        // A checkpoint whose superblock update had not yet landed leaves the
        // superblock behind an atomically-complete newer snapshot: safe, and it must
        // NOT refuse boot on an otherwise healthy node.
        let lagging = vsr_state_with_checkpoint(40, 0xdead);
        assert!(verify_checkpoint_pairing(Some(&lagging), 42, 0xbeef, dir).is_ok());

        // Superblock points past the snapshot on disk: a lost snapshot write.
        let ahead = vsr_state_with_checkpoint(50, 0xabc);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&ahead), 42, 0xabc, dir),
            Err(RecoveryError::CheckpointAheadOfSnapshot {
                checkpoint_op: 50,
                snapshot_op: 42,
                ..
            })
        ));

        // A checkpoint claim with no snapshot on disk is the same violation at op 0.
        let claim_without_snapshot = vsr_state_with_checkpoint(5, 0xabc);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&claim_without_snapshot), 0, 0, dir),
            Err(RecoveryError::CheckpointAheadOfSnapshot {
                checkpoint_op: 5,
                snapshot_op: 0,
                ..
            })
        ));

        // Same op, different content: corrupt or torn snapshot.
        let mismatched = vsr_state_with_checkpoint(42, 0xaaaa);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&mismatched), 42, 0xbbbb, dir),
            Err(RecoveryError::CheckpointChecksumMismatch {
                op: 42,
                expected: 0xaaaa,
                actual: 0xbbbb,
                ..
            })
        ));
    }

    fn protected_snapshot(op: u64) -> IggySnapshot {
        let mut snapshot = IggySnapshot::new(op);
        snapshot.snapshot_mut().client_table =
            Some(ClientTable::new(CLIENTS_TABLE_MAX).to_snapshot());
        snapshot
    }

    fn make_prepare(op: u64, body_size: usize) -> Message<PrepareHeader> {
        make_prepare_with_commit(op, op.saturating_sub(1), body_size)
    }

    /// A prepare as a live primary stamps it: `commit` carries the primary's
    /// commit point when the prepare is sent.
    fn make_prepare_with_commit(op: u64, commit: u64, body_size: usize) -> Message<PrepareHeader> {
        let total_size = HEADER_SIZE + body_size;
        let mut buffer = Owned::<4096>::zeroed(total_size);
        // Seal the body checksum so the WAL scan's integrity check accepts it,
        // matching what the primary seals in `project`.
        let checksum_body = u128::from(iggy_common::calculate_checksum(
            &buffer.as_slice()[HEADER_SIZE..],
        ));
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..HEADER_SIZE],
        );
        header.size = total_size as u32;
        header.command = Command::Prepare;
        header.op = op;
        header.commit = commit;
        header.operation = Operation::CreateStream;
        header.retry_capacity = consensus::CLIENTS_TABLE_MAX as u32;
        header.checksum_body = checksum_body;
        Message::try_from(buffer).unwrap()
    }

    /// A client-attributed prepare (Register / app op / Logout) as the
    /// admission path stamps it.
    fn make_client_prepare(
        op: u64,
        operation: Operation,
        client: u128,
        user_id: u32,
        request: u64,
    ) -> Message<PrepareHeader> {
        let registration = RegisterRequest {
            user_created_at: 1,
            bind_verifier: [0x5a; 32],
        }
        .to_bytes();
        let body: &[u8] = if operation == Operation::Register {
            &registration
        } else {
            &[]
        };
        let total_size = HEADER_SIZE + body.len();
        let mut buffer = Owned::<4096>::zeroed(total_size);
        buffer.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
        let checksum_body = u128::from(iggy_common::calculate_checksum(
            &buffer.as_slice()[HEADER_SIZE..],
        ));
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..HEADER_SIZE],
        );
        header.size = total_size as u32;
        header.command = Command::Prepare;
        header.op = op;
        header.commit = op.saturating_sub(1);
        header.operation = operation;
        header.client = client;
        header.user_id = user_id;
        header.request = request;
        header.session = 1;
        header.retry_capacity = consensus::CLIENTS_TABLE_MAX as u32;
        header.checksum_body = checksum_body;
        Message::try_from(buffer).unwrap()
    }

    #[compio::test]
    async fn recover_empty_state() {
        let dir = tempdir().unwrap();
        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();

        assert_eq!(recovered.last_applied_op, None);
        assert!(recovered.journal.last_op().is_none());
    }

    #[compio::test]
    async fn recover_snapshot_only() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        let snapshot = protected_snapshot(42);
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
            .await
            .unwrap();

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(
            recovered
                .snapshot
                .as_ref()
                .map(IggySnapshot::sequence_number),
            Some(42)
        );
        assert_eq!(recovered.last_applied_op, None);
    }

    #[compio::test]
    async fn recover_journal_only() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal.append(make_prepare(1, 32)).await.unwrap();
            journal.append(make_prepare(2, 32)).await.unwrap();
            journal.append(make_prepare(3, 32)).await.unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert!(recovered.snapshot.is_none());
        // Op 3's entry proves commit=2; op 3 itself is journaled but not
        // provably committed, so it stays journal-only.
        assert_eq!(recovered.last_applied_op, Some(2));
        assert_eq!(recovered.last_journaled_op, Some(3));
        assert_eq!(recovered.journal.last_op(), Some(3));
    }

    /// A prepare sealed the way a live primary seals one: `parent` chains to the
    /// previous op's identity and `checksum` is that identity.
    fn make_chained_prepare(op: u64, commit: u64, parent: u128) -> Message<PrepareHeader> {
        let mut message = make_prepare_with_commit(op, commit, 32);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut message.as_mut_slice()[..HEADER_SIZE],
        );
        header.parent = parent;
        let checksum = header.identity_checksum();
        header.checksum = checksum;
        message
    }

    #[compio::test]
    async fn given_a_snapshot_and_disconnected_wal_when_recovering_should_ignore_its_commit_claims()
    {
        for contiguous in [0, 2] {
            let directory = tempdir().unwrap();
            let metadata_dir = directory.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            protected_snapshot(100)
                .persist(&metadata_dir.join("snapshot.bin"))
                .await
                .unwrap();
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 100)
                    .await
                    .unwrap();
                journal
                    .append(make_prepare_with_commit(100, 100, 32))
                    .await
                    .unwrap();
                for op in 101..101 + contiguous {
                    journal
                        .append(make_prepare_with_commit(op, 100, 32))
                        .await
                        .unwrap();
                }
                for op in 179..=181 {
                    journal
                        .append(make_prepare_with_commit(op, op - 1, 32))
                        .await
                        .unwrap();
                }
            }
            let recovered = recover::<TestStm>(
                directory.path(),
                CLUSTERED,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                |_, _| {},
            )
            .await
            .unwrap();
            assert_eq!(recovered.chain_break_op, Some(179));
            assert_eq!(recovered.last_applied_op, None);
            assert_eq!(recovered.last_journaled_op, Some(181));
            assert_eq!(recovered.journal.last_op(), Some(181));
        }
    }

    #[compio::test]
    async fn given_a_fork_after_a_snapshot_when_recovering_should_stop_at_the_snapshot_parent() {
        let directory = tempdir().unwrap();
        let metadata_dir = directory.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        protected_snapshot(100)
            .persist(&metadata_dir.join("snapshot.bin"))
            .await
            .unwrap();
        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 100)
                .await
                .unwrap();
            let fork = make_chained_prepare(101, 101, 0xdead_beef);
            let child = make_chained_prepare(102, 102, fork.header().checksum);
            journal.append(fork).await.unwrap();
            journal
                .append(make_chained_prepare(100, 100, 0))
                .await
                .unwrap();
            journal.append(child).await.unwrap();
        }
        let recovered = recover::<TestStm>(
            directory.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(recovered.chain_break_op, Some(101));
        assert_eq!(recovered.last_applied_op, None);
        assert_eq!(recovered.last_journaled_op, Some(102));
        assert_eq!(recovered.journal.last_op(), Some(102));
    }

    #[compio::test]
    async fn given_an_uncommitted_root_and_disconnected_tail_when_recovering_should_not_apply_root()
    {
        type UserStm = MuxStateMachine<iggy_common::variadic!(Users, Streams)>;
        let directory = tempdir().unwrap();
        let metadata_dir = directory.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let body = CreateUserRequest {
            username: WireName::new("uncommitted-root").unwrap(),
            password: "hash".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        }
        .to_bytes();
        let mut root = make_prepare_with_commit(1, 0, body.len());
        root.as_mut_slice()[HEADER_SIZE..].copy_from_slice(&body);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut root.as_mut_slice()[..HEADER_SIZE],
        );
        header.operation = Operation::CreateUser;
        header.timestamp = 1;
        header.checksum_body = u128::from(iggy_common::calculate_checksum(&body));
        header.checksum = header.identity_checksum();
        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal.append(root).await.unwrap();
            for op in 179..=181 {
                journal
                    .append(make_prepare_with_commit(op, op - 1, 32))
                    .await
                    .unwrap();
            }
        }
        let recovered = recover::<UserStm>(
            directory.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert!(
            recovered
                .mux_stm
                .users()
                .read(|users| users.items.is_empty())
        );
        assert_eq!(recovered.last_applied_op, None);
        assert_eq!(recovered.last_journaled_op, Some(181));
        assert_eq!(recovered.chain_break_op, Some(179));
        assert_eq!(recovered.journal.last_op(), Some(181));
    }

    #[compio::test]
    async fn given_a_disconnected_solo_wal_when_recovering_should_refuse_without_rewriting_it() {
        for forked in [false, true] {
            let directory = tempdir().unwrap();
            let metadata_dir = directory.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            protected_snapshot(100)
                .persist(&metadata_dir.join("snapshot.bin"))
                .await
                .unwrap();
            let path = metadata_dir.join("journal.wal");
            {
                let journal = PrepareJournal::open(&path, 100).await.unwrap();
                if forked {
                    let fork = make_chained_prepare(101, 101, 0xdead_beef);
                    let child = make_chained_prepare(102, 102, fork.header().checksum);
                    journal.append(fork).await.unwrap();
                    journal
                        .append(make_chained_prepare(100, 100, 0))
                        .await
                        .unwrap();
                    journal.append(child).await.unwrap();
                } else {
                    journal
                        .append(make_prepare_with_commit(100, 100, 32))
                        .await
                        .unwrap();
                    journal
                        .append(make_prepare_with_commit(179, 178, 32))
                        .await
                        .unwrap();
                }
                journal.storage_ref().fsync().await.unwrap();
            }
            let before = std::fs::read(&path).unwrap();
            assert!(matches!(
                recover::<TestStm>(directory.path(), SOLO,
                    journal::prepare_journal::DEFAULT_SLOT_COUNT, CLIENTS_TABLE_MAX,
                    |_| {}, |_, _| {}).await,
                Err(RecoveryError::DisconnectedSoloJournal { op, .. })
                    if op == if forked { 101 } else { 179 }
            ));
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[compio::test]
    async fn given_a_gap_when_recovering_a_cluster_should_preserve_the_durable_suffix() {
        // Ops 1-3 then 5: op 4 never landed. Replaying 5 over a state machine that
        // never saw 4 diverges silently, and the WAL scan waves this through --
        // its chain check only fires on CONSECUTIVE ops, since a gap is also what
        // ordinary compaction leaves behind.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for op in 1..=3u64 {
                journal
                    .append(make_prepare_with_commit(op, op, 32))
                    .await
                    .unwrap();
            }
            journal
                .append(make_prepare_with_commit(5, 5, 32))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();

        assert_eq!(recovered.chain_break_op, Some(5));
        assert_eq!(
            recovered.last_applied_op,
            Some(3),
            "op 5 must not apply across the hole at op 4"
        );
        assert_eq!(
            recovered.last_journaled_op,
            Some(5),
            "the restored head must include every durable prepare for the view change"
        );
        assert_eq!(
            recovered.journal.last_op(),
            Some(5),
            "only a cluster view-log proof may remove a durable prepare"
        );
        assert_eq!(
            recovered.journal.snapshot_op(),
            0,
            "truncating a suffix must leave the watermark, or the ops stop being refillable"
        );
    }

    #[compio::test]
    async fn given_repaired_adopted_bodies_when_restarting_should_retain_them_without_recertifying()
    {
        const CERTIFIED_VIEW: u32 = 5;
        const ANNOUNCED_VIEW: u32 = 7;
        const BINDING: u64 = 2;
        const COMMITTED: u64 = 4;
        const ANNOUNCED_HEAD: u64 = 5;
        for (missing_op, expected_applied, expected_chain_break) in [
            (None, COMMITTED - 1, None),
            (Some(BINDING + 1), BINDING - 1, Some(COMMITTED)),
        ] {
            let directory = tempdir().unwrap();
            let metadata_dir = directory.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            let mut messages = Vec::new();
            let mut parent = 0;
            for op in 1..=ANNOUNCED_HEAD {
                let mut prepare = make_chained_prepare(op, op.saturating_sub(1), parent);
                let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                    &mut prepare.as_mut_slice()[..HEADER_SIZE],
                );
                header.cluster = CLUSTERED.cluster;
                header.group = server_common::sharding::METADATA_GROUP;
                header.view = ANNOUNCED_VIEW;
                header.timestamp = op;
                header.checksum = header.identity_checksum();
                parent = header.checksum;
                messages.push(prepare);
            }
            let certificate = HistoryCertificate::new(
                CERTIFIED_VIEW,
                BINDING,
                messages[BINDING as usize - 1].header().checksum,
            );
            let adopted = consensus::AdoptedView {
                view: ANNOUNCED_VIEW,
                group: server_common::sharding::METADATA_GROUP,
                partition_incarnation: 0,
                op_head: ANNOUNCED_HEAD,
                commit_max: COMMITTED,
                headers: messages
                    .iter()
                    .rev()
                    .map(|prepare| *prepare.header())
                    .collect(),
            };
            let state = VsrState {
                view: ANNOUNCED_VIEW,
                log_view: ANNOUNCED_VIEW,
                commit_max: COMMITTED,
                adopted_view: Some(adopted),
                ..vsr_state_with_checkpoint(0, 0)
            }
            .with_history_certificate(Some(certificate));
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                    .await
                    .unwrap();
                for prepare in &messages[..COMMITTED as usize] {
                    if Some(prepare.header().op) != missing_op {
                        journal.append(prepare.clone()).await.unwrap();
                    }
                }
                // A resident tail with a different identity must still be discarded.
                let mut divergent = messages[ANNOUNCED_HEAD as usize - 1].clone();
                let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                    &mut divergent.as_mut_slice()[..HEADER_SIZE],
                );
                header.request += 1;
                header.checksum = header.identity_checksum();
                journal.append(divergent).await.unwrap();
                journal.storage_ref().fsync().await.unwrap();
                let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
                superblock.write(&state.to_bytes()).await.unwrap();
            }
            // The donor is gone. Both boots must recover the last copy of the
            // committed body, including when an earlier repair is still missing.
            for _ in 0..2 {
                let recovered = recover::<TestStm>(
                    directory.path(),
                    CLUSTERED,
                    journal::prepare_journal::DEFAULT_SLOT_COUNT,
                    CLIENTS_TABLE_MAX,
                    |_| {},
                    |_, _| {},
                )
                .await
                .unwrap();
                assert_eq!(
                    recovered.journal.last_op(),
                    Some(COMMITTED),
                    "gap {missing_op:?}"
                );
                let retained = recovered
                    .journal
                    .entry(messages[COMMITTED as usize - 1].header())
                    .await
                    .unwrap();
                assert_eq!(
                    retained.as_slice(),
                    messages[COMMITTED as usize - 1].as_slice()
                );
                assert!(recovered.journal.header(ANNOUNCED_HEAD as usize).is_none());
                // A retained body beyond a gap cannot establish the replay commit point.
                assert_eq!(recovered.last_applied_op, Some(expected_applied));
                assert_eq!(recovered.chain_break_op, expected_chain_break);
                if let Some(missing) = missing_op {
                    assert!(recovered.journal.header(missing as usize).is_none());
                }
                assert_eq!(
                    recovered
                        .recovered_state
                        .as_ref()
                        .unwrap()
                        .history_certificate(),
                    Some(certificate),
                );
            }
        }
    }

    /// Entries journaled for a view the replica recorded but never proved go before
    /// replay can apply them or count their commit stamps. While no later view is
    /// recorded, every entry stays the certified view's.
    #[compio::test]
    async fn recover_drops_entries_journaled_for_a_view_it_never_proved() {
        const CERTIFIED_VIEW: u32 = 5;
        const ADOPTED_VIEW: u32 = 7;
        const BINDING: u64 = 2;
        const HEAD: u64 = 4;
        for (view, kept) in [(ADOPTED_VIEW, BINDING), (CERTIFIED_VIEW, HEAD)] {
            let dir = tempdir().unwrap();
            let metadata_dir = dir.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                    .await
                    .unwrap();
                let mut parent = 0;
                let mut binding = None;
                for op in 1..=HEAD {
                    let prepare = make_chained_prepare(op, op, parent);
                    parent = prepare.header().checksum;
                    if op == BINDING {
                        binding = Some(HistoryCertificate::new(CERTIFIED_VIEW, op, parent));
                    }
                    journal.append(prepare).await.unwrap();
                }
                journal.storage_ref().fsync().await.unwrap();
                let state = VsrState {
                    view,
                    log_view: view,
                    ..vsr_state_with_checkpoint(0, 0)
                }
                .with_history_certificate(binding);
                let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
                superblock.write(&state.to_bytes()).await.unwrap();
            }

            let recovered = recover::<TestStm>(
                dir.path(),
                CLUSTERED,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                |_, _| {},
            )
            .await
            .unwrap();
            assert_eq!(recovered.history.log_view(), CERTIFIED_VIEW, "view {view}");
            assert_eq!(recovered.journal.last_op(), Some(kept), "view {view}");
            assert_eq!(recovered.last_journaled_op, Some(kept), "view {view}");
            assert_eq!(
                recovered.last_applied_op,
                Some(kept),
                "view {view}: a commit stamp from a dropped entry applied its predecessors"
            );
        }
    }

    #[compio::test]
    async fn recover_stops_at_a_broken_chain_between_consecutive_ops() {
        // Consecutive and sealed on both ends, but op 3 names a parent that is not
        // op 2: a fork left by a crash mid view change. Ops are appended out of
        // ascending file order so the scan's own chain check does not fire first.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            let first = make_chained_prepare(1, 1, 0);
            let first_checksum = first.header().checksum;
            journal.append(first).await.unwrap();
            let second = make_chained_prepare(2, 2, first_checksum);
            journal.append(second).await.unwrap();
            // Parent of a prepare that is not op 2.
            journal
                .append(make_chained_prepare(3, 3, 0xdead_beef))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await;

        // The WAL scan reaches this first and refuses boot: consecutive ops, both
        // sealed, chain broken, with no entry after it is only a tail. Either
        // outcome is a refusal to apply the fork; what must never happen is a
        // clean recovery that replayed op 3.
        match recovered {
            Err(RecoveryError::Journal(_) | RecoveryError::Io(_)) => {}
            Ok(recovered) => {
                assert!(
                    recovered.last_applied_op < Some(3),
                    "op 3 forks the chain and must not be applied"
                );
            }
            Err(other) => panic!("unexpected recovery error: {other:?}"),
        }
    }

    #[compio::test]
    async fn recover_applies_only_the_committed_prefix() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // Ops 1-5 journaled, but the last entry proves commit only up to 3:
        // the primary crashed before 4 and 5 reached quorum.
        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for op in 1..=4u64 {
                journal
                    .append(make_prepare_with_commit(op, op.saturating_sub(1), 32))
                    .await
                    .unwrap();
            }
            journal
                .append(make_prepare_with_commit(5, 3, 32))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(recovered.last_applied_op, Some(3));
        assert_eq!(recovered.last_journaled_op, Some(5));
        assert_eq!(recovered.journal.last_op(), Some(5));
    }

    #[compio::test]
    async fn recover_snapshot_plus_journal() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // Snapshot at op 5
        let snapshot = protected_snapshot(5);
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
            .await
            .unwrap();

        // WAL has ops 1-10
        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for op in 1..=10 {
                journal.append(make_prepare(op, 32)).await.unwrap();
            }
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        // Replays ops 6-9 (snapshot at 5; op 10's entry proves commit=9).
        assert_eq!(recovered.last_applied_op, Some(9));
        assert_eq!(recovered.last_journaled_op, Some(10));
        assert_eq!(
            recovered
                .snapshot
                .as_ref()
                .map(IggySnapshot::sequence_number),
            Some(5)
        );
    }

    #[compio::test]
    async fn recovery_refuses_invalid_session_protection_before_repairing_the_wal() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let wal_path = metadata_dir.join("journal.wal");
        {
            let journal = PrepareJournal::open(&wal_path, 0).await.unwrap();
            journal
                .append(make_client_prepare(3, Operation::Register, 7, 1, 0))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }
        let mut before = std::fs::read(&wal_path).unwrap();
        before.extend_from_slice(b"torn-prepare-header");
        std::fs::write(&wal_path, &before).unwrap();

        let mut malformed = protected_snapshot(2);
        malformed
            .snapshot_mut()
            .client_table
            .as_mut()
            .unwrap()
            .capacity = 0;
        for snapshot in [IggySnapshot::new(2), malformed] {
            snapshot
                .persist(&metadata_dir.join("snapshot.bin"))
                .await
                .unwrap();
            let result = recover::<TestStm>(
                dir.path(),
                SOLO,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                |_, _| {},
            )
            .await;
            assert!(matches!(
                result,
                Err(
                    RecoveryError::RetryProtection(consensus::ClientTableWireError::EmptyRing)
                        | RecoveryError::ClientTableDecode(
                            ClientTableDecodeError::SlotOutOfRange { .. }
                        )
                )
            ));
            assert_eq!(std::fs::read(&wal_path).unwrap(), before);
            for slot in journal::superblock::SLOT_FILE_NAMES {
                assert!(!metadata_dir.join(slot).exists());
            }
        }
    }

    // The fold closes the gap above: same WAL and same floor, but the checkpoint
    // carries the client table as it stood at the floor, so the pre-floor
    // watermark is restored and only advanced by replay. Without this, replicas
    // that checkpoint at different ops disagree on which requests are duplicates
    // and exactly-once silently degrades to at-least-once.
    #[compio::test]
    async fn recover_restores_the_watermark_from_a_folded_checkpoint_table() {
        const CLIENT: u128 = 0x1337;
        const USER: u32 = 7;
        const FLOOR: u64 = 2;

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // The table as the live commit path left it at the floor: registered at
        // op 1, request 1 committed at op 2.
        let mut at_checkpoint = ClientTable::new(CLIENTS_TABLE_MAX);
        let register = make_client_prepare(1, Operation::Register, CLIENT, USER, 0);
        at_checkpoint
            .commit_register(
                CLIENT,
                USER,
                [0x5a; 32],
                build_reply_message(register.header(), &bytes::Bytes::new()),
            )
            .unwrap();
        let app = make_client_prepare(2, Operation::CreateStream, CLIENT, USER, 1);
        at_checkpoint.commit_reply(
            CLIENT,
            USER,
            build_reply_message(app.header(), &bytes::Bytes::new()),
        );

        let mut snapshot = protected_snapshot(FLOOR);
        snapshot.snapshot_mut().client_table = Some(at_checkpoint.to_snapshot());
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
            .await
            .unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for entry in [
                register,
                app,
                // The rebind, above the floor, so replay does see this one.
                make_client_prepare(3, Operation::Register, CLIENT, USER, 0),
            ] {
                journal.append(entry).await.unwrap();
            }
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            SOLO,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();

        let table = &recovered.client_table;
        assert_eq!(
            table.get_epoch(CLIENT),
            Some(1),
            "a matching registration preserves the original epoch"
        );
        assert_eq!(
            table.get_watermark(CLIENT),
            Some(1),
            "the pre-floor watermark comes back from the checkpoint's folded table"
        );
        assert_eq!(
            table.get_user_id(CLIENT),
            Some(USER),
            "the folded entry carries the acting user, so no metadata lookup is needed"
        );
    }

    #[compio::test]
    async fn given_retirement_evidence_when_replaying_after_namespace_change_should_keep_the_session()
     {
        const CLIENT: u128 = 7;
        const USER: u32 = 0;
        const SNAPSHOT_OP: u64 = 2;
        const CURRENT_REVISION: u64 = 2;
        type SessionStm = MuxStateMachine<iggy_common::variadic!(Users, Streams)>;

        for reported_revision in [CURRENT_REVISION - 1, CURRENT_REVISION] {
            let dir = tempdir().unwrap();
            let metadata_dir = dir.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
            let register = make_client_prepare(1, Operation::Register, CLIENT, USER, 0);
            table
                .commit_register(
                    CLIENT,
                    USER,
                    [0x5a; 32],
                    build_reply_message(register.header(), &bytes::Bytes::new()),
                )
                .unwrap();
            let logout = make_client_prepare(SNAPSHOT_OP, Operation::Logout, CLIENT, USER, 1);
            table
                .commit_logout(
                    CLIENT,
                    USER,
                    1,
                    build_reply_message(logout.header(), &bytes::Bytes::new()),
                )
                .unwrap();
            let identity = table.ended_sessions().next().unwrap();
            let mut snapshot = protected_snapshot(SNAPSHOT_OP);
            snapshot.snapshot_mut().client_table = Some(table.to_snapshot());
            snapshot.snapshot_mut().users = Some(Users::default().to_snapshot());
            snapshot.snapshot_mut().streams = Some(StreamsSnapshot {
                lifecycle_intents: std::collections::BTreeMap::default(),
                items: Vec::new(),
                revision: CURRENT_REVISION,
                namespace_revision: CURRENT_REVISION,
            });
            snapshot
                .persist(&metadata_dir.join("snapshot.bin"))
                .await
                .unwrap();

            let body = FinalizeSessionRequest {
                identity,
                namespace_revision: reported_revision,
            }
            .to_bytes();
            let mut prepare = make_prepare(SNAPSHOT_OP + 1, body.len());
            prepare.as_mut_slice()[HEADER_SIZE..].copy_from_slice(&body);
            let prepare = prepare.transmute_header(|old, header: &mut PrepareHeader| {
                *header = old;
                header.operation = Operation::FinalizeSession;
                header.checksum_body = u128::from(iggy_common::calculate_checksum(&body));
            });
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), SNAPSHOT_OP)
                    .await
                    .unwrap();
                journal.append(prepare).await.unwrap();
                journal.storage_ref().fsync().await.unwrap();
            }
            let recovered = recover::<SessionStm>(
                dir.path(),
                SOLO,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                |_, _| {},
            )
            .await
            .unwrap();
            assert_eq!(recovered.last_applied_op, Some(SNAPSHOT_OP + 1));
            assert_eq!(
                recovered.client_table.ended_sessions().next(),
                (reported_revision != CURRENT_REVISION).then_some(identity),
                "WAL replay must preserve protection when the report covered an older partition set"
            );
        }
    }

    // The IGGY-137 restart contract: a rebooted node must remember where
    // each client left off. Replay re-mints the epoch, restores the
    // watermark, and re-caches the reply so a retry of the last committed
    // request id dedups instead of re-executing or silently dropping.
    #[compio::test]
    async fn recover_rebuilds_client_table_from_wal() {
        use consensus::client_table::RequestStatus;

        const CLIENT: u128 = 0x1337;
        const USER: u32 = 7;

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal
                .append(make_client_prepare(1, Operation::Register, CLIENT, USER, 0))
                .await
                .unwrap();
            journal
                .append(make_client_prepare(
                    2,
                    Operation::CreateStream,
                    CLIENT,
                    USER,
                    1,
                ))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        // Solo: every journaled op is committed.
        let recovered = recover::<TestStm>(
            dir.path(),
            SOLO,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();

        let table = &recovered.client_table;
        assert_eq!(table.get_epoch(CLIENT), Some(1), "register minted epoch 1");
        assert_eq!(table.get_user_id(CLIENT), Some(USER));
        assert_eq!(
            table.get_watermark(CLIENT),
            Some(1),
            "committed request 1 restored the watermark"
        );
        match table.check_request(CLIENT, 1, 1, Operation::CreateStream) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1, "retry replays the cached reply");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
        assert!(
            matches!(
                table.check_request(CLIENT, 1, 2, Operation::CreateStream),
                RequestStatus::New
            ),
            "the next request id is admitted"
        );
    }

    #[compio::test]
    async fn recover_retains_the_logout_result_and_ended_session() {
        const CLIENT: u128 = 0x1337;
        const USER: u32 = 7;

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal
                .append(make_client_prepare(1, Operation::Register, CLIENT, USER, 0))
                .await
                .unwrap();
            journal
                .append(make_client_prepare(2, Operation::Logout, CLIENT, USER, 1))
                .await
                .unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let replayed_logouts = std::cell::RefCell::new(Vec::new());
        let recovered = recover::<TestStm>(
            dir.path(),
            SOLO,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, header| {
                if header.operation == Operation::Logout {
                    replayed_logouts.borrow_mut().push(header.client);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.client_table.get_epoch(CLIENT),
            None,
            "an ended session cannot accept another mutation"
        );
        assert_eq!(recovered.client_table.count(), 1);
        assert!(matches!(
            recovered
                .client_table
                .check_request(CLIENT, 1, 1, Operation::Logout),
            consensus::client_table::RequestStatus::Duplicate(_)
        ));
        assert!(matches!(
            recovered
                .client_table
                .check_request(CLIENT, 1, 2, Operation::CreateStream),
            consensus::client_table::RequestStatus::NoSession
        ));
        assert_eq!(
            replayed_logouts.into_inner(),
            vec![CLIENT],
            "replay must hand the logged-out client to the group-removal hook"
        );
        assert_eq!(recovered.last_applied_op, Some(2));
    }

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_reused_user_id_when_pending_operations_commit_should_reject_the_old_session() {
        const CLIENT: u128 = 1;
        const ROOT_CLIENT: u128 = 2;
        const REPLACEMENT_CLIENT: u128 = 3;
        const USER: u32 = 1;
        const ROOT_SESSION: u64 = 1;
        const SESSION: u64 = 3;
        const REPLACEMENT_SESSION: u64 = 8;
        const TOKEN_HASH: [u8; PAT_TOKEN_HASH_BYTES] = [b'a'; PAT_TOKEN_HASH_BYTES];
        type UserStm = MuxStateMachine<iggy_common::variadic!(Users, Streams)>;

        // Admission rejects reserved IDs, but previously committed sessions must
        // still replay and fence their pending operations after user deletion.
        for (client_id, replace_before_token) in [
            CLIENT,
            message_bus::server_internal_client_id(0),
            message_bus::AUTO_COMMIT_CLIENT_ID,
        ]
        .into_iter()
        .flat_map(|client_id| [(client_id, false), (client_id, true)])
        {
            let directory = tempdir().unwrap();
            let metadata_dir = directory.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            let stm = UserStm::default();
            stm.users().ensure_root_user("root", "hash");
            let table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
            let mut account = CreateUserRequest {
                username: WireName::new("original").unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            };
            let original = account.to_bytes();
            account.username = WireName::new("replacement").unwrap();
            account.permissions = Some(WirePermissions {
                global: WireGlobalPermissions {
                    manage_servers: false,
                    read_servers: false,
                    manage_users: false,
                    read_users: false,
                    manage_streams: true,
                    read_streams: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                },
                streams: Vec::new(),
            });
            let token = CreatePersonalAccessTokenRequest {
                user_id: USER,
                name: WireName::new("token").unwrap(),
                expiry: 0,
                token_hash: TOKEN_HASH,
            }
            .to_bytes();
            let stream = CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            }
            .to_bytes();
            let register = |created_at| {
                RegisterRequest {
                    user_created_at: created_at,
                    bind_verifier: [0x5a; 32],
                }
                .to_bytes()
            };
            let replacement = (
                Operation::CreateUser,
                account.to_bytes(),
                ROOT_CLIENT,
                0,
                ROOT_SESSION,
            );
            let pending_token = (
                Operation::CreatePersonalAccessToken,
                token.clone(),
                client_id,
                USER,
                SESSION,
            );
            let (first, second) = if replace_before_token {
                (replacement, pending_token)
            } else {
                (pending_token, replacement)
            };
            let operations = [
                (Operation::Register, register(1), ROOT_CLIENT, 0, 0),
                (
                    Operation::CreateUser,
                    original,
                    ROOT_CLIENT,
                    0,
                    ROOT_SESSION,
                ),
                (Operation::Register, register(2), client_id, USER, 0),
                (
                    Operation::DeleteUser,
                    DeleteUserRequest {
                        user_id: WireIdentifier::numeric(USER),
                    }
                    .to_bytes(),
                    ROOT_CLIENT,
                    0,
                    ROOT_SESSION,
                ),
                first,
                second,
                (
                    Operation::CreateStream,
                    stream.clone(),
                    client_id,
                    USER,
                    SESSION,
                ),
                (
                    Operation::Register,
                    register(if replace_before_token { 5 } else { 6 }),
                    REPLACEMENT_CLIENT,
                    USER,
                    0,
                ),
                (
                    Operation::CreatePersonalAccessToken,
                    token,
                    REPLACEMENT_CLIENT,
                    USER,
                    REPLACEMENT_SESSION,
                ),
                (
                    Operation::CreateStream,
                    stream,
                    REPLACEMENT_CLIENT,
                    USER,
                    REPLACEMENT_SESSION,
                ),
                (
                    Operation::CompleteLifecycle,
                    CompleteLifecycleRequest::new(
                        ROOT_SESSION,
                        0,
                        vec![LifecycleCompletion {
                            topic_id: 0,
                            partition_id: 0,
                            partition_op: 1,
                        }],
                    )
                    .unwrap()
                    .to_bytes(),
                    message_bus::server_internal_client_id(server_common::sharding::METADATA_GROUP),
                    0,
                    1,
                ),
            ];
            let mut parent = 0;
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                    .await
                    .unwrap();
                for (index, (operation, body, client, user_id, session)) in
                    operations.into_iter().enumerate()
                {
                    let op = u64::try_from(index).unwrap() + 1;
                    let mut prepare = make_prepare(op, body.len());
                    prepare.as_mut_slice()[HEADER_SIZE..].copy_from_slice(&body);
                    let prepare = consensus::seal_prepare_checksum(prepare.transmute_header(
                        |old, header: &mut PrepareHeader| {
                            *header = old;
                            header.operation = operation;
                            header.client = client;
                            header.user_id = user_id;
                            header.session = session;
                            header.request = if operation == Operation::Register {
                                0
                            } else {
                                op
                            };
                            header.timestamp = op;
                            header.parent = parent;
                            header.checksum_body =
                                u128::from(iggy_common::calculate_checksum(&body));
                        },
                    ));
                    parent = prepare.header().checksum;
                    let reply =
                        apply_committed_prepare(&stm, &table, true, |_| {}, prepare.clone());
                    let status = if operation == Operation::Register {
                        result_code(reply.body()).unwrap_or(0)
                    } else {
                        result_code(reply.body()).unwrap()
                    };
                    let stale = client == client_id && operation != Operation::Register;
                    assert_eq!(
                        status,
                        if stale {
                            IggyError::StaleClient.as_code()
                        } else {
                            0
                        },
                        "{operation:?} at op {op}, client {client_id}, replace before token: {replace_before_token}"
                    );
                    if stale {
                        assert_eq!(stm.users().read(|users| users.pat_count_of(USER)), 0);
                        assert!(stm.streams().read(|streams| streams.items.is_empty()));
                    }
                    journal.append(prepare).await.unwrap();
                }
                journal.storage_ref().fsync().await.unwrap();
            }
            let recovered = recover::<UserStm>(
                directory.path(),
                SOLO,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |stm| stm.users().ensure_root_user("root", "hash"),
                |_, _| {},
            )
            .await
            .unwrap();
            assert_eq!(recovered.client_table.encode(), table.borrow().encode());
            assert_eq!(recovered.client_table.get_epoch(client_id), None);
            assert_eq!(
                recovered.client_table.get_epoch(REPLACEMENT_CLIENT),
                Some(REPLACEMENT_SESSION)
            );
            assert_eq!(
                recovered.mux_stm.users().read(|users| {
                    let replacement = users.user_by_name("replacement").unwrap();
                    (replacement.id, users.pat_count_of(USER))
                }),
                (USER, 1)
            );
            assert_eq!(
                recovered
                    .mux_stm
                    .streams()
                    .read(|streams| streams.items.len()),
                1
            );
        }
    }

    /// WAL replay must rebuild the table that live apply built. A request the
    /// primary admitted before the revocation of its user committed commits
    /// right behind it as a `StaleClient` no-op, and a checkpoint of that table
    /// must restart.
    #[compio::test]
    async fn given_revoked_user_when_replaying_wal_should_preserve_session_revocation() {
        const CLIENT: u128 = 0x1337;
        const ROOT_CLIENT: u128 = CLIENT + 1;
        const USER: u32 = 1;
        const ROOT_SESSION: u64 = 1;
        // The op of the registration that runs before the revocation.
        const SESSION: u64 = 3;
        const REVOCATION_OP: u64 = 4;
        type UserStm = MuxStateMachine<iggy_common::variadic!(Users, Streams)>;
        for (operation, register_first, replace) in [
            (Operation::DeleteUser, true, false),
            (Operation::UpdateUser, true, false),
            (Operation::DeleteUser, false, false),
            (Operation::UpdateUser, false, false),
            (Operation::DeleteUser, false, true),
        ] {
            let directory = tempdir().unwrap();
            let metadata_dir = directory.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            let register = |user_created_at| {
                RegisterRequest {
                    user_created_at,
                    bind_verifier: [0x5a; 32],
                }
                .to_bytes()
            };
            let create = CreateUserRequest {
                username: WireName::new("user").unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            }
            .to_bytes();
            let revocation = if operation == Operation::DeleteUser {
                DeleteUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                }
                .to_bytes()
            } else {
                UpdateUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                    username: None,
                    status: Some(UserStatus::Inactive.as_code()),
                    options: WireOptions::empty(),
                }
                .to_bytes()
            };
            let registration = (Operation::Register, register(2), CLIENT, USER, 0);
            let mut entries = vec![
                (Operation::Register, register(1), ROOT_CLIENT, 0, 0),
                (
                    Operation::CreateUser,
                    create.clone(),
                    ROOT_CLIENT,
                    0,
                    ROOT_SESSION,
                ),
            ];
            if register_first {
                entries.push(registration.clone());
            }
            entries.push((operation, revocation, ROOT_CLIENT, 0, ROOT_SESSION));
            if register_first {
                let pipelined = CreateStreamRequest {
                    name: WireName::new("stream").unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes();
                entries.push((Operation::CreateStream, pipelined, CLIENT, USER, SESSION));
            }
            if replace {
                entries.push((Operation::CreateUser, create, ROOT_CLIENT, 0, ROOT_SESSION));
            }
            if !register_first {
                entries.push(registration);
            }
            let last_op = entries.len() as u64;
            let stm = UserStm::default();
            stm.users().ensure_root_user("root", "hash");
            let table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
            let mut last_reply = None;
            let mut parent = 0;
            {
                let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                    .await
                    .unwrap();
                for (index, (operation, body, client, user_id, session)) in
                    entries.into_iter().enumerate()
                {
                    let op = index as u64 + 1;
                    let mut prepare = make_prepare(op, body.len());
                    prepare.as_mut_slice()[HEADER_SIZE..].copy_from_slice(&body);
                    let prepare = consensus::seal_prepare_checksum(prepare.transmute_header(
                        |old, header: &mut PrepareHeader| {
                            *header = old;
                            header.operation = operation;
                            header.timestamp = op;
                            header.client = client;
                            header.user_id = user_id;
                            header.session = session;
                            header.request = if operation == Operation::Register {
                                0
                            } else {
                                op
                            };
                            header.parent = parent;
                            header.checksum_body =
                                u128::from(iggy_common::calculate_checksum(&body));
                        },
                    ));
                    parent = prepare.header().checksum;
                    last_reply = Some(apply_committed_prepare(
                        &stm,
                        &table,
                        true,
                        |_| {},
                        prepare.clone(),
                    ));
                    journal.append(prepare).await.unwrap();
                }
                journal.storage_ref().fsync().await.unwrap();
            }
            if register_first {
                assert_eq!(
                    last_reply.and_then(|reply| result_code(reply.body())),
                    Some(IggyError::StaleClient.as_code())
                );
            }
            let live = table.borrow().encode();
            ClientTable::decode(&live).expect("the state transfer form must decode");
            let revoked = RefCell::new(Vec::new());
            let recovered = recover::<UserStm>(
                directory.path(),
                SOLO,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |stm| {
                    stm.users().ensure_root_user("root", "hash");
                },
                |_, header| {
                    if header.operation == operation {
                        revoked
                            .borrow_mut()
                            .push((header.client, header.session, header.op));
                    }
                },
            )
            .await
            .unwrap();
            assert_eq!(recovered.last_applied_op, Some(last_op));
            assert!(
                recovered.client_table.encode() == live,
                "{operation:?}: WAL replay must rebuild the live table"
            );
            assert_eq!(recovered.client_table.get_epoch(CLIENT), None);
            assert_eq!(
                recovered.client_table.get_epoch(ROOT_CLIENT),
                Some(ROOT_SESSION)
            );
            let ended: Vec<_> = recovered.client_table.ended_sessions().collect();
            if register_first {
                assert_eq!(*revoked.borrow(), vec![(CLIENT, SESSION, REVOCATION_OP)]);
                assert_eq!(
                    ended,
                    vec![iggy_binary_protocol::requests::system::SessionIdentity {
                        client_id: CLIENT,
                        session: SESSION,
                        metadata_watermark: REVOCATION_OP,
                    }]
                );
            } else {
                assert!(revoked.borrow().is_empty());
                assert_eq!(
                    ended,
                    [] as [iggy_binary_protocol::requests::system::SessionIdentity; 0]
                );
            }
            drop(recovered);

            let mut checkpoint = <IggySnapshot as Snapshot>::create(&stm, last_op, 0).unwrap();
            checkpoint.snapshot_mut().client_table = Some(table.borrow().to_snapshot());
            checkpoint
                .persist(&metadata_dir.join("snapshot.bin"))
                .await
                .unwrap();
            let restarted = recover::<UserStm>(
                directory.path(),
                SOLO,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |stm| stm.users().ensure_root_user("root", "hash"),
                |_, _| {},
            )
            .await
            .expect("restart must accept the checkpointed table");
            assert_eq!(
                restarted.client_table.ended_sessions().collect::<Vec<_>>(),
                ended
            );
        }
    }

    #[compio::test]
    async fn snapshot_persist_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let snapshot = protected_snapshot(99);
        snapshot.persist(&path).await.unwrap();

        let (loaded, checksum) = IggySnapshot::load(&path).unwrap();
        assert_eq!(loaded.sequence_number(), 99);
        assert_eq!(
            checksum,
            checkpoint_checksum(&snapshot.encode().unwrap()),
            "load must report the checksum of the payload bytes on disk, which is what \
             the superblock pairing recorded"
        );

        // The trailer makes the file self-verifying, so a torn or bit-rotted snapshot is
        // caught even when the superblock's pairing cannot judge it (a crash inside a
        // checkpoint leaves the pairing behind the snapshot on disk).
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            IggySnapshot::load(&path),
            Err(SnapshotError::ChecksumMismatch { .. })
        ));
    }

    #[compio::test]
    async fn snapshot_refuses_absent_truncated_and_damaged_integrity_framing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let snapshot = protected_snapshot(99);
        let payload = snapshot.encode().unwrap();
        snapshot.persist(&path).await.unwrap();
        let sealed = std::fs::read(&path).unwrap();
        assert_eq!(
            IggySnapshot::decode(&payload).unwrap().sequence_number(),
            99
        );
        for trailer_len in 0..sealed.len() - payload.len() {
            std::fs::write(&path, &sealed[..payload.len() + trailer_len]).unwrap();
            assert!(
                IggySnapshot::load(&path).is_err(),
                "accepted {trailer_len} trailer bytes"
            );
        }
        let mut damaged = sealed.clone();
        damaged[payload.len()] ^= u8::MAX;
        std::fs::write(&path, damaged).unwrap();
        assert!(matches!(
            IggySnapshot::load(&path),
            Err(SnapshotError::InvalidTrailer)
        ));
        std::fs::write(&path, sealed).unwrap();
        assert_eq!(IggySnapshot::load(&path).unwrap().0.sequence_number(), 99);
    }

    #[compio::test]
    async fn recover_refuses_boot_on_undecodable_superblock() {
        // A checksum-clean record whose payload is not a valid VsrState: here a short
        // payload, in the field a layout change that skipped a version bump or
        // corruption inside the checksummed region. Refuse boot rather than infer a
        // stale view from the WAL.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        {
            let sb = PingPongSuperblock::open(&metadata_dir).await.unwrap();
            sb.write(b"short").await.unwrap();
        }

        // `matches!` rather than `unwrap_err`: the Ok type `RecoveredMetadata` holds
        // non-Debug handles.
        let result = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await;
        assert!(matches!(
            result,
            Err(RecoveryError::SuperblockUndecodable { .. })
        ));
    }

    #[compio::test]
    async fn recover_refuses_boot_on_corrupt_superblock() {
        // Bytes on disk but no copy verifies: a superblock was written and is now
        // torn, NOT a fresh deployment. Refuse boot rather than degrade to WAL-view
        // inference.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        {
            let sb = PingPongSuperblock::open(&metadata_dir).await.unwrap();
            sb.write(b"durable").await.unwrap();
        }
        let path = metadata_dir.join(journal::superblock::SLOT_FILE_NAMES[0]);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // break the trailing checksum
        std::fs::write(&path, &bytes).unwrap();

        let result = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await;
        assert!(matches!(
            result,
            Err(RecoveryError::SuperblockUnreadable { version: None, .. })
        ));
    }

    #[compio::test]
    async fn recover_treats_absent_superblock_with_wal_as_fresh() {
        // A node that took writes but never checkpointed or changed view has a
        // non-empty WAL and NO superblock. It must recover as fresh, NOT refuse boot,
        // which would brick a healthy node. The persist-before-send gate makes this
        // safe: it never externalized a view it cannot re-derive by re-probing.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal.append(make_prepare(1, 32)).await.unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert!(recovered.recovered_state.is_none());
    }

    #[compio::test]
    async fn recover_refuses_boot_on_foreign_superblock_identity() {
        // The superblock is the only on-disk identity in the metadata directory, and
        // the wire handshake cannot substitute: both peers derive `cluster` from the
        // configured cluster name, so it never sees a foreign DISK. Each field is
        // checked on its own, since a copied directory, a replica index swap, and a
        // cluster resized without reconfiguration are distinct operator mistakes.
        let cases = [
            (
                VsrState {
                    cluster: 99,
                    ..vsr_state_with_checkpoint(0, 0)
                },
                IdentityField::Cluster,
            ),
            (
                VsrState {
                    replica_id: 2,
                    ..vsr_state_with_checkpoint(0, 0)
                },
                IdentityField::ReplicaId,
            ),
            (
                VsrState {
                    replica_count: 5,
                    ..vsr_state_with_checkpoint(0, 0)
                },
                IdentityField::ReplicaCount,
            ),
        ];

        for (state, expected_field) in cases {
            let dir = tempdir().unwrap();
            let metadata_dir = dir.path().join("metadata");
            std::fs::create_dir_all(&metadata_dir).unwrap();
            {
                let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
                superblock.write(&state.to_bytes()).await.unwrap();
            }

            let result = recover::<TestStm>(
                dir.path(),
                CLUSTERED,
                journal::prepare_journal::DEFAULT_SLOT_COUNT,
                CLIENTS_TABLE_MAX,
                |_| {},
                |_, _| {},
            )
            .await;
            match result {
                Err(RecoveryError::SuperblockIdentityMismatch { field, .. }) => {
                    assert_eq!(field, expected_field);
                }
                Err(other) => panic!("expected an identity mismatch, got {other}"),
                Ok(_) => panic!("expected {expected_field} mismatch to refuse boot"),
            }
        }
    }

    #[compio::test]
    async fn recover_refuses_a_snapshot_from_another_format_version() {
        // A snapshot shape is only as trustworthy as the version stamped on it, so a
        // foreign one refuses boot rather than restoring whatever msgpack happens to
        // make of the bytes. Everything else about the directory is healthy: the
        // superblock pairs with the file on disk, so the refusal can only be the
        // version.
        const CHECKPOINT_OP: u64 = 42;
        let mut snapshot = protected_snapshot(CHECKPOINT_OP);
        snapshot.snapshot_mut().version = SNAPSHOT_FORMAT_VERSION + 1;
        let foreign = snapshot.encode().unwrap();

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let state = vsr_state_with_checkpoint(CHECKPOINT_OP, checkpoint_checksum(&foreign));
        IggySnapshot::write_durably(&metadata_dir.join("snapshot.bin"), foreign)
            .await
            .unwrap();
        {
            let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
            superblock.write(&state.to_bytes()).await.unwrap();
        }

        match recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        {
            Err(RecoveryError::Snapshot(SnapshotError::UnsupportedFormatVersion {
                found,
                expected,
            })) => {
                assert_eq!(found, SNAPSHOT_FORMAT_VERSION + 1);
                assert_eq!(expected, SNAPSHOT_FORMAT_VERSION);
            }
            Err(other) => panic!("expected a format-version refusal, got {other}"),
            Ok(_) => panic!("expected a foreign format version to refuse boot"),
        }
    }

    #[compio::test]
    async fn recover_pairs_the_checkpoint_against_the_bytes_on_disk() {
        // The pairing checksum is taken over the file's bytes, never over a re-encode
        // of the decoded snapshot. Re-encoding would tie recovery to
        // decode-then-encode staying byte-identical across every serde and rmp
        // release, and a divergence there would refuse boot on every checkpointed node
        // with its WAL prefix already drained.
        //
        // Canonical bytes cannot show that: they re-encode byte-identically (pinned by
        // `populated_snapshot_reencode_and_checksum_are_stable`), so both candidate
        // checksums agree and the assertion holds either way. This file is instead
        // noncanonical but decodable, carrying the version in a `u32` marker that
        // `read_int` accepts and a re-encode collapses back to a fixint, so hashing a
        // re-encode gives a different checksum and the test can fail.
        const CHECKPOINT_OP: u64 = 42;
        let canonical = protected_snapshot(CHECKPOINT_OP).encode().unwrap();
        let on_disk = with_wide_version_marker(&canonical);

        // Both halves of "noncanonical but decodable", so this cannot pass vacuously.
        assert_ne!(
            on_disk, canonical,
            "the file must not be byte-identical to the canonical encoding"
        );
        let round_tripped = MetadataSnapshot::decode(&on_disk)
            .expect("a wide version marker must still decode")
            .encode()
            .unwrap();
        assert_ne!(
            checkpoint_checksum(&on_disk),
            checkpoint_checksum(&round_tripped),
            "the two candidate checksums must differ, or this proves nothing"
        );

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let checksum = checkpoint_checksum(&on_disk);
        IggySnapshot::write_durably(&metadata_dir.join("snapshot.bin"), on_disk)
            .await
            .unwrap();
        let state = vsr_state_with_checkpoint(CHECKPOINT_OP, checksum);
        {
            let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
            superblock.write(&state.to_bytes()).await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(recovered.snapshot_checkpoint.0, CHECKPOINT_OP);
        assert_eq!(
            recovered.snapshot_checkpoint.1, checksum,
            "the verified pairing must be the checksum of the bytes on disk"
        );
    }

    /// Re-encode a snapshot's leading `version` as an explicit msgpack `u32` marker
    /// instead of the canonical fixint: same value, five bytes instead of one.
    ///
    /// `rmp` reads it back through the same `read_int` the peek uses, and a re-encode
    /// canonicalizes it away, which is what makes it a probe for "did recovery hash
    /// the file or its own re-encode".
    fn with_wide_version_marker(canonical: &[u8]) -> Vec<u8> {
        let mut cursor = canonical;
        rmp::decode::read_array_len(&mut cursor).expect("a snapshot encodes as an array");
        let array_header = canonical.len() - cursor.len();
        let version: u32 =
            rmp::decode::read_int(&mut cursor).expect("version is the first element");
        let version_len = canonical.len() - cursor.len() - array_header;

        let mut wide = Vec::with_capacity(canonical.len() + 4);
        wide.extend_from_slice(&canonical[..array_header]);
        rmp::encode::write_u32(&mut wide, version).expect("writing to a Vec cannot fail");
        wide.extend_from_slice(&canonical[array_header + version_len..]);
        wide
    }

    #[compio::test]
    async fn recover_accepts_matching_superblock_identity() {
        // The fence must not brick the healthy case it guards: the replica's own
        // record boots, so the mismatch arm above is proving identity and not just
        // that any record refuses.
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let state = vsr_state_with_checkpoint(0, 0);
        {
            let superblock = PingPongSuperblock::open(&metadata_dir).await.unwrap();
            superblock.write(&state.to_bytes()).await.unwrap();
        }

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(recovered.recovered_state, Some(state));
    }
}
