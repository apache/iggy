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
use crate::stm::authz::GatedApply;
use crate::stm::snapshot::{MetadataSnapshot, RestoreSnapshot, Snapshot, SnapshotError};
use consensus::{
    ClientTable, ClientTableDecodeError, VsrState, VsrStateError, build_reply_message,
    build_reply_message_with,
};
use iggy_binary_protocol::consensus::{CHECKSUM_UNSEALED, Operation, PrepareHeader};
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
            Self::Superblock { dir, source } => write!(
                f,
                "recovery superblock error in {} ({} / {}): {source}",
                dir.display(),
                SLOT_FILE_NAMES[0],
                SLOT_FILE_NAMES[1]
            ),
            Self::ClientTableDecode(e) => write!(f, "recovery client-table decode error: {e}"),
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
            Self::SuperblockUndecodable { source, .. } => Some(source),
            Self::CheckpointAheadOfSnapshot { .. }
            | Self::CheckpointChecksumMismatch { .. }
            | Self::SuperblockUnreadable { .. }
            | Self::SuperblockIdentityMismatch { .. } => None,
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
    /// Highest op present in the journal, `None` when it is empty. Ops in
    /// `(last_applied_op, last_journaled_op]` are prepared-but-uncommitted:
    /// they stay journal-only until the recovered primary re-replicates them
    /// (or a backup sees the commit point advance past them).
    pub last_journaled_op: Option<u64>,
    /// First op replay could not connect to its predecessor, `None` when the
    /// replayed range is one unbroken chain.
    ///
    /// `Some(op)` means entries at and above `op` were truncated and must come back
    /// from the cluster. `last_journaled_op` stops below it, which keeps the restored
    /// head, the re-pipeline range, and the recovery barrier honest.
    pub chain_break_op: Option<u64>,
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
/// `seed_baseline` reproduces boot-time state that never reaches the WAL
/// (today: the locally-ensured root user). It runs on the freshly-defaulted
/// state machine BEFORE journal replay and ONLY when no snapshot exists, so
/// replayed ops land on the same baseline (and the same slab ids) they were
/// originally applied over. A snapshot already contains that baseline.
///
/// `clients_table_max` sizes the rebuilt client table. It comes from
/// `[metadata] clients_table_max`: the recovered table replaces the one the
/// caller built, so reading the compile-time default here would make the knob
/// inert on every restart.
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

    let replay_from = snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.sequence_number() + 1);

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

    // The scan replays entries no producer sealed rather than rejecting them, so
    // report the count: an operator gets no other signal that these bodies were
    // replayed on trust. Goes away once every node in the cluster seals.
    let unsealed_entries = journal.unsealed_entry_count();
    if unsealed_entries > 0 {
        tracing::warn!(
            unsealed_entries,
            path = %journal_path.display(),
            "metadata WAL holds entries without a body checksum; replayed unverified \
             (written before body sealing, or replicated from a primary that predates it)"
        );
    }

    // Intentional fail-fast: a bad entry aborts recovery and the operator
    // must repair or truncate the WAL before the node can boot again.
    let headers_to_replay = journal.iter_headers_from(replay_from);

    // Committed watermark: the `commit` field of each journaled prepare is
    // the primary's commit point when it was sent, so the highest one is the
    // highest commit this WAL can prove. Ops above it were prepared but not
    // provably committed; applying them here would fabricate commit knowledge
    // (a crashed suffix may have been discarded by a view change) and would
    // hide the suffix from re-replication. A max fold (not `.last()`) so a
    // future non-monotone stamping change cannot silently under-apply the
    // committed prefix.
    let snapshot_floor = snapshot.as_ref().map_or(0, IggySnapshot::sequence_number);
    let commit_watermark = if identity.is_solo() {
        headers_to_replay
            .iter()
            .map(|header| header.op)
            .fold(snapshot_floor, u64::max)
    } else {
        headers_to_replay
            .iter()
            .map(|header| header.commit)
            .fold(snapshot_floor, u64::max)
    };

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

    // Restore the client table from the checkpoint's folded copy, then replay the
    // committed suffix on top, mirroring how the state machine is restored from the
    // snapshot and advanced by the WAL. This is what lets a session registered below
    // the snapshot floor come back with its watermark intact.
    let mut client_table = match snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.snapshot().client_table.clone())
    {
        // Integrity is verified above, so a decode failure here means genuinely
        // corrupt bytes: refuse boot with a typed error rather than panicking.
        Some(table) => ClientTable::from_snapshot(table, clients_table_max)?,
        None => ClientTable::new(clients_table_max),
    };

    let mut last_applied_op: Option<u64> = None;
    let mut last_journaled_op: Option<u64> = None;
    let mut chain_break_op: Option<u64> = None;
    let mut previous: Option<PrepareHeader> = None;
    for header in &headers_to_replay {
        // Stop at the first op that does not connect to the one before it. Applying
        // across a hole replays effects onto a state machine that never saw the
        // missing op, and nothing downstream re-checks it.
        //
        // The WAL scan does not cover this: it only fires on CONSECUTIVE ops with
        // both ends sealed, so a gap reaches here. The first replayed op is exempt,
        // since a snapshot records no checksum for its parent to chain to.
        if let Some(previous) = previous {
            let gap = previous.op + 1 != header.op;
            let broken_chain = previous.checksum != CHECKSUM_UNSEALED
                && header.checksum != CHECKSUM_UNSEALED
                && header.parent != previous.checksum;
            if gap || broken_chain {
                tracing::error!(
                    op = header.op,
                    previous_op = previous.op,
                    gap,
                    broken_chain,
                    "metadata WAL does not connect at this op; stopping replay and dropping the \
                     suffix for VSR repair"
                );
                chain_break_op = Some(header.op);
                break;
            }
        }
        previous = Some(*header);

        last_journaled_op = Some(header.op);
        if header.op > commit_watermark {
            continue;
        }

        // Register/Logout mutate the client table and skip the state
        // machine, mirroring the commit paths (`on_ack` / `commit_journal`).
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
            let reply = build_reply_message(header, &bytes::Bytes::new());
            client_table.commit_register(header.client, header.user_id, reply);
            last_applied_op = Some(header.op);
            continue;
        }
        if header.operation == Operation::Logout {
            client_table.remove_client(header.client);
            // TODO: the commit paths also run `remove_consumer_group_member`
            // here; recovery has no `StreamsFrontend` bound, so replayed
            // logouts leave stale group members (pre-existing, harmless for
            // dead connections but a divergence from the live apply).
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
        let reply = mux_stm.gated_update(entry)?;
        // Re-cache the reply exactly like the commit paths: same prepare
        // header + deterministic apply output = the original bytes. Skipped
        // when the session is absent (server-originated ops, or the client
        // was evicted).
        if client_table.get_epoch(header.client).is_some() {
            let cached = build_reply_message_with(header, reply.reply_body_len(), |dst| {
                reply.write_reply_body(dst);
            });
            // Skips (never panics) on a stale-request replay: capacity
            // eviction is replica-local and unlogged, so a WAL can legitimately
            // replay a lower request id onto a preserved watermark. Recovery
            // must boot from such a WAL, not refuse it.
            let _ = client_table.commit_reply(header.client, cached);
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

    // `truncate_from`, never `drain`: the removed ops must stay refillable, so the
    // snapshot watermark stays put. Leaving them resident would make `append` refuse
    // the slot, failing repair on exactly the ops it exists to fix.
    if let Some(break_op) = chain_break_op {
        let removed = journal
            .truncate_from(break_op)
            .await
            .map_err(RecoveryError::Io)?;
        tracing::warn!(
            break_op,
            removed,
            last_journaled_op,
            "dropped the disconnected metadata WAL suffix; the cluster re-supplies these ops"
        );
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
    use crate::impls::metadata::checkpoint_checksum;
    use crate::stm::snapshot::SNAPSHOT_FORMAT_VERSION;
    use consensus::CLIENTS_TABLE_MAX;
    use iggy_binary_protocol::consensus::{Command2, Operation};
    use journal::Journal;
    use server_common::iobuf::Owned;
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
        header.command = Command2::Prepare;
        header.op = op;
        header.commit = commit;
        header.operation = Operation::CreateStream;
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
        let total_size = HEADER_SIZE;
        let mut buffer = Owned::<4096>::zeroed(total_size);
        let checksum_body = u128::from(iggy_common::calculate_checksum(
            &buffer.as_slice()[HEADER_SIZE..],
        ));
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..HEADER_SIZE],
        );
        header.size = total_size as u32;
        header.command = Command2::Prepare;
        header.op = op;
        header.commit = op.saturating_sub(1);
        header.operation = operation;
        header.client = client;
        header.user_id = user_id;
        header.request = request;
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

        let snapshot = IggySnapshot::new(42);
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
            .unwrap();

        let recovered = recover::<TestStm>(
            dir.path(),
            CLUSTERED,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
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
    async fn recover_stops_at_a_gap_and_drops_the_disconnected_suffix() {
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
            Some(3),
            "the restored head stops below the break, so nothing re-pipelines it"
        );
        assert_eq!(
            recovered.journal.last_op(),
            Some(3),
            "the disconnected entry is dropped so repair can journal the cluster's op 5"
        );
        assert_eq!(
            recovered.journal.snapshot_op(),
            0,
            "truncating a suffix must leave the watermark, or the ops stop being refillable"
        );
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
        let snapshot = IggySnapshot::new(5);
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
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

    // A checkpoint that carries no folded client table still loses the watermark
    // half of table recovery, and this pins that residual as a fact rather than a
    // comment: it is the shape of a snapshot written before the fold existed.
    // Shape: a client registers, commits request 1, a checkpoint lands past that
    // register, then the client rebinds. Replay starts above the floor, so it
    // never sees the first register: `commit_register` takes its fresh-entry
    // branch and the entry comes back with watermark 0, while a peer whose floor
    // sat lower replayed both registers, took the rebind branch, and kept
    // watermark 1.
    //
    // Consequence, once a client re-presents a recovered id: the same request
    // that peer answers `Duplicate` is `New` here and gets re-executed. The
    // fence is unaffected -- epochs are op-derived, so this node still returns
    // the second register's op.
    //
    // The green counterpart, where the checkpoint does carry the table, is
    // `recover_restores_the_watermark_from_a_folded_checkpoint_table`.
    #[compio::test]
    async fn recover_loses_the_watermark_when_a_tableless_checkpoint_hides_the_first_register() {
        const CLIENT: u128 = 0x1337;
        const USER: u32 = 7;
        const FLOOR: u64 = 2;

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // Ops 1..=2 are below the floor and are never replayed: the client's
        // first Register and the request that advanced its watermark.
        IggySnapshot::new(FLOOR)
            .persist(&metadata_dir.join("snapshot.bin"))
            .unwrap();

        {
            let journal = PrepareJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for entry in [
                make_client_prepare(1, Operation::Register, CLIENT, USER, 0),
                make_client_prepare(2, Operation::CreateStream, CLIENT, USER, 1),
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
        )
        .await
        .unwrap();

        let table = &recovered.client_table;
        assert_eq!(
            table.get_epoch(CLIENT),
            Some(3),
            "the fence is op-derived, so it survives the checkpoint intact"
        );
        assert_eq!(
            table.get_watermark(CLIENT),
            Some(0),
            "a tableless checkpoint loses the pre-floor watermark, so request 1 reads \
             as New here while a lower-floor peer answers it as a duplicate"
        );
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
        at_checkpoint.commit_register(
            CLIENT,
            USER,
            build_reply_message(register.header(), &bytes::Bytes::new()),
        );
        let app = make_client_prepare(2, Operation::CreateStream, CLIENT, USER, 1);
        at_checkpoint.commit_reply(
            CLIENT,
            build_reply_message(app.header(), &bytes::Bytes::new()),
        );

        let mut snapshot = IggySnapshot::new(FLOOR);
        snapshot.snapshot_mut().client_table = Some(at_checkpoint.to_snapshot());
        snapshot
            .persist(&metadata_dir.join("snapshot.bin"))
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
        )
        .await
        .unwrap();

        let table = &recovered.client_table;
        assert_eq!(
            table.get_epoch(CLIENT),
            Some(3),
            "replay still advances the fence to the rebind's op"
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
        match table.check_request(CLIENT, 1, 1, 0) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1, "retry replays the cached reply");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
        assert!(
            matches!(table.check_request(CLIENT, 1, 2, 0), RequestStatus::New),
            "the next request id is admitted"
        );
    }

    // A replayed Logout removes the entry, mirroring the commit paths.
    #[compio::test]
    async fn recover_replays_logout_as_session_removal() {
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

        let recovered = recover::<TestStm>(
            dir.path(),
            SOLO,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(
            recovered.client_table.get_epoch(CLIENT),
            None,
            "logged-out session must not be resurrected"
        );
        assert_eq!(recovered.last_applied_op, Some(2));
    }

    #[test]
    fn snapshot_persist_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");

        let snapshot = IggySnapshot::new(99);
        snapshot.persist(&path).unwrap();

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
        let mut snapshot = IggySnapshot::new(CHECKPOINT_OP);
        snapshot.snapshot_mut().version = SNAPSHOT_FORMAT_VERSION + 1;
        let foreign = snapshot.encode().unwrap();

        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        std::fs::write(metadata_dir.join("snapshot.bin"), &foreign).unwrap();
        let state = vsr_state_with_checkpoint(CHECKPOINT_OP, checkpoint_checksum(&foreign));
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
        let canonical = IggySnapshot::new(CHECKPOINT_OP).encode().unwrap();
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
        std::fs::write(metadata_dir.join("snapshot.bin"), &on_disk).unwrap();
        let state = vsr_state_with_checkpoint(CHECKPOINT_OP, checkpoint_checksum(&on_disk));
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
        )
        .await
        .unwrap();
        assert_eq!(recovered.snapshot_checkpoint.0, CHECKPOINT_OP);
        assert_eq!(
            recovered.snapshot_checkpoint.1,
            checkpoint_checksum(&on_disk),
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
        )
        .await
        .unwrap();
        assert_eq!(recovered.recovered_state, Some(state));
    }
}
