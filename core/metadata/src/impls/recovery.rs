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
use iggy_binary_protocol::consensus::{Operation, PrepareHeader};
use iggy_common::IggyError;
use journal::prepare_journal::{JournalError, PrepareJournal};
use journal::superblock::{PingPongSuperblock, SuperblockContents, SuperblockStore};
use server_common::Message;
use std::fmt;
use std::path::Path;

/// Error type for metadata recovery.
#[derive(Debug)]
pub enum RecoveryError {
    Snapshot(SnapshotError),
    Journal(JournalError),
    StateMachine(IggyError),
    Io(std::io::Error),
    /// Opening or reading the durable superblock failed.
    Superblock(std::io::Error),
    /// A checkpoint client-table slot held bytes that are not a valid reply
    /// message (a corrupt or torn snapshot).
    ClientTableDecode(ClientTableDecodeError),
    /// The superblock references a checkpoint newer than the on-disk snapshot, a
    /// lost or reverted snapshot write: recovering from the older snapshot while
    /// trusting the superblock's commit point could skip committed state.
    CheckpointAheadOfSnapshot {
        checkpoint_op: u64,
        snapshot_op: u64,
    },
    /// The paired snapshot's checksum does not match the superblock's record (a
    /// corrupt or torn snapshot).
    CheckpointChecksumMismatch {
        op: u64,
        expected: u128,
        actual: u128,
    },
    /// A superblock record was present and checksum-clean but its payload did not
    /// decode into a [`VsrState`]: a layout change that skipped a version bump, or
    /// corruption inside the checksummed region. The durable consensus state is
    /// unrecoverable, so refuse boot rather than infer a stale view from the WAL and
    /// risk re-entering a superseded view.
    SuperblockUndecodable(VsrStateError),
    /// The slots yield no record this build can trust: bytes that fail verification
    /// on every copy, or one clean record beside a copy that does not verify, whose
    /// lost `sequence` may have been the newer (`version` names an unrecognized
    /// format version when that was the cause). A superblock was written and its
    /// latest generation cannot be established, so refuse boot rather than treat the
    /// node as fresh or read through to a superseded view. An empty superblock is a
    /// different case, a genuinely fresh or not-yet-checkpointed node, and is NOT an
    /// error.
    SuperblockUnreadable {
        version: Option<u16>,
    },
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(e) => write!(f, "recovery snapshot error: {e}"),
            Self::Journal(e) => write!(f, "recovery journal error: {e}"),
            Self::StateMachine(e) => write!(f, "recovery state machine error: {e}"),
            Self::Io(e) => write!(f, "recovery I/O error: {e}"),
            Self::Superblock(e) => write!(f, "recovery superblock error: {e}"),
            Self::ClientTableDecode(e) => write!(f, "recovery client-table decode error: {e}"),
            Self::CheckpointAheadOfSnapshot {
                checkpoint_op,
                snapshot_op,
            } => write!(
                f,
                "superblock references checkpoint op {checkpoint_op} but the on-disk snapshot is \
                 only at op {snapshot_op}: a lost or reverted snapshot write, refusing boot"
            ),
            Self::CheckpointChecksumMismatch {
                op,
                expected,
                actual,
            } => write!(
                f,
                "snapshot at op {op} does not match the superblock's checkpoint checksum \
                 (superblock {expected:#034x}, snapshot {actual:#034x}): corrupt or torn snapshot, \
                 refusing boot"
            ),
            Self::SuperblockUndecodable(e) => write!(
                f,
                "superblock record is present and checksum-clean but its payload did not decode \
                 into VSR state ({e}): unrecoverable durable consensus state, refusing boot"
            ),
            Self::SuperblockUnreadable {
                version: Some(version),
            } => write!(
                f,
                "superblock present but its format version {version} is unrecognized by this build \
                 (a downgrade, or a corrupt version field): refusing boot"
            ),
            Self::SuperblockUnreadable { version: None } => write!(
                f,
                "superblock present but a copy holds bytes that do not verify (bit-rot or a \
                 checksum failure), so its latest generation cannot be established: refusing boot"
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
            Self::Io(e) | Self::Superblock(e) => Some(e),
            Self::ClientTableDecode(e) => Some(e),
            Self::SuperblockUndecodable(e) => Some(e),
            Self::CheckpointAheadOfSnapshot { .. }
            | Self::CheckpointChecksumMismatch { .. }
            | Self::SuperblockUnreadable { .. } => None,
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
    /// The durable superblock, opened by recovery so it could verify the snapshot's
    /// integrity against the checkpoint pairing before decoding it. Handed to the
    /// consensus/metadata layer to reuse; re-opening would fork the ping-pong
    /// sequence counter.
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
/// `solo` marks a single-replica cluster: the quorum is 1/1, so every
/// journaled op was committed the moment it was written and replay runs to
/// the journal head. The embedded `commit` stamps cannot be used there: each
/// stamp is the primary's commit point when the prepare was SENT, so a
/// pipelined burst (e.g. create-stream + create-topic on one connection) is
/// stamped entirely below its own ops and would replay as nothing.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub async fn recover<M>(
    data_dir: &Path,
    solo: bool,
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
    let snapshot = if snapshot_path.exists() {
        Some(IggySnapshot::load(&snapshot_path)?)
    } else {
        None
    };

    // Open the durable superblock and read the last VSR state. Recovery owns this so
    // it can verify the snapshot against the superblock's checkpoint pairing BEFORE
    // trusting (decoding) it into the state machine and client table. The consensus
    // layer reuses the returned superblock, since re-opening would fork the ping-pong
    // sequence counter, and restores `(view, log_view)` from `recovered_state`.
    let superblock = PingPongSuperblock::open(&metadata_dir)
        .await
        .map_err(RecoveryError::Superblock)?;
    // Distinguish the three read outcomes so a lost or corrupt superblock cannot
    // masquerade as a fresh deployment: a present but unreadable or
    // unrecognized-version record refuses boot with a typed error. An EMPTY
    // superblock is genuinely fresh, or a node that took writes but never
    // checkpointed or changed view, and is safe to treat as no durable state: the
    // persist-before-send gate guarantees this replica never externalized a view it
    // cannot re-derive by re-probing, so it recovers `(view, log_view)` from the
    // WAL/init path with no split-brain risk.
    let recovered_state = match superblock
        .read_latest()
        .await
        .map_err(RecoveryError::Superblock)?
    {
        SuperblockContents::Present(bytes) => {
            // A checksum-clean record that does not decode is a durability violation,
            // not absent state: refuse boot rather than infer a stale view.
            Some(
                VsrState::try_from(bytes.as_slice())
                    .map_err(RecoveryError::SuperblockUndecodable)?,
            )
        }
        SuperblockContents::Unreadable { version } => {
            return Err(RecoveryError::SuperblockUnreadable { version });
        }
        SuperblockContents::Empty => None,
    };

    // Verify before decode: a superblock pointing past the snapshot (a lost snapshot
    // write) or disagreeing on its checksum (corrupt or torn) is a durability
    // violation, not a recoverable state. One that merely lags a newer,
    // atomically-complete snapshot is accepted, since `commit_max` is a recovery lower
    // bound and the WAL suffix re-commits.
    //
    // TODO(state-transfer): there is no metadata state transfer yet, so refusing boot
    // is the only sound response. Once it lands, fetch the checkpoint from a healthy
    // replica here instead of erroring.
    let snapshot_op = snapshot.as_ref().map_or(0, IggySnapshot::sequence_number);
    let snapshot_checksum = match snapshot.as_ref() {
        Some(snapshot) => snapshot.checksum()?,
        None => 0,
    };
    verify_checkpoint_pairing(recovered_state.as_ref(), snapshot_op, snapshot_checksum)?;

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
    let commit_watermark = if solo {
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
    for header in &headers_to_replay {
        // TODO: Check hash chain integrity against `previous_header`. On a
        // same-view break, stop replay here and mark the remaining entries for
        // repair via VSR instead of panicking.

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
    })
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
const fn verify_checkpoint_pairing(
    recovered: Option<&VsrState>,
    snapshot_op: u64,
    snapshot_checksum: u128,
) -> Result<(), RecoveryError> {
    let Some(state) = recovered else {
        return Ok(());
    };
    if state.checkpoint_op > snapshot_op {
        return Err(RecoveryError::CheckpointAheadOfSnapshot {
            checkpoint_op: state.checkpoint_op,
            snapshot_op,
        });
    }
    if state.checkpoint_op == snapshot_op && state.checkpoint_checksum != snapshot_checksum {
        return Err(RecoveryError::CheckpointChecksumMismatch {
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
    use consensus::CLIENTS_TABLE_MAX;
    use iggy_binary_protocol::consensus::{Command2, Operation};
    use journal::Journal;
    use server_common::iobuf::Owned;
    use tempfile::tempdir;

    use crate::MuxStateMachine;

    type TestStm = MuxStateMachine<()>;

    const HEADER_SIZE: usize = size_of::<PrepareHeader>();

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
        }
    }

    /// The full accept/reject matrix for the checkpoint pairing cross-check, which
    /// decides whether a node boots at all. Grouped because each case is one call
    /// on a pure function and the boundaries only mean something together.
    #[test]
    fn checkpoint_pairing_accepts_consistent_pairings_and_rejects_violations() {
        // No superblock yet (fresh or not-yet-checkpointed): nothing to cross-check.
        assert!(verify_checkpoint_pairing(None, 42, 0xabc).is_ok());

        // Exact pairing.
        let matching = vsr_state_with_checkpoint(42, 0xabc);
        assert!(verify_checkpoint_pairing(Some(&matching), 42, 0xabc).is_ok());

        // A checkpoint whose superblock update had not yet landed leaves the
        // superblock behind an atomically-complete newer snapshot: safe, and it must
        // NOT refuse boot on an otherwise healthy node.
        let lagging = vsr_state_with_checkpoint(40, 0xdead);
        assert!(verify_checkpoint_pairing(Some(&lagging), 42, 0xbeef).is_ok());

        // Superblock points past the snapshot on disk: a lost snapshot write.
        let ahead = vsr_state_with_checkpoint(50, 0xabc);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&ahead), 42, 0xabc),
            Err(RecoveryError::CheckpointAheadOfSnapshot {
                checkpoint_op: 50,
                snapshot_op: 42,
            })
        ));

        // A checkpoint claim with no snapshot on disk is the same violation at op 0.
        let claim_without_snapshot = vsr_state_with_checkpoint(5, 0xabc);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&claim_without_snapshot), 0, 0),
            Err(RecoveryError::CheckpointAheadOfSnapshot {
                checkpoint_op: 5,
                snapshot_op: 0,
            })
        ));

        // Same op, different content: corrupt or torn snapshot.
        let mismatched = vsr_state_with_checkpoint(42, 0xaaaa);
        assert!(matches!(
            verify_checkpoint_pairing(Some(&mismatched), 42, 0xbbbb),
            Err(RecoveryError::CheckpointChecksumMismatch {
                op: 42,
                expected: 0xaaaa,
                actual: 0xbbbb,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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
            true,
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
            true,
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
            true,
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
            true,
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

        let loaded = IggySnapshot::load(&path).unwrap();
        assert_eq!(loaded.sequence_number(), 99);
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
            false,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
        )
        .await;
        assert!(matches!(
            result,
            Err(RecoveryError::SuperblockUndecodable(_))
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
            false,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
        )
        .await;
        assert!(matches!(
            result,
            Err(RecoveryError::SuperblockUnreadable { version: None })
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
            false,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
            CLIENTS_TABLE_MAX,
            |_| {},
        )
        .await
        .unwrap();
        assert!(recovered.recovered_state.is_none());
    }
}
