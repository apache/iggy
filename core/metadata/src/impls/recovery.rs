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
use consensus::{CLIENTS_TABLE_MAX, ClientTable, build_reply_message, build_reply_message_with};
use iggy_binary_protocol::consensus::{Operation, PrepareHeader};
use iggy_common::IggyError;
use journal::prepare_journal::{JournalError, PrepareJournal};
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
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(e) => write!(f, "recovery snapshot error: {e}"),
            Self::Journal(e) => write!(f, "recovery journal error: {e}"),
            Self::StateMachine(e) => write!(f, "recovery state machine error: {e}"),
            Self::Io(e) => write!(f, "recovery I/O error: {e}"),
        }
    }
}

impl std::error::Error for RecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Snapshot(e) => Some(e),
            Self::Journal(e) => Some(e),
            Self::StateMachine(e) => Some(e),
            Self::Io(e) => Some(e),
        }
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
    pub mux_stm: M,
    /// Client table rebuilt from the replayed committed prefix: registers
    /// re-mint epochs in apply order, replies re-cache byte-identically
    /// (`build_reply_message*` reads only the prepare header + deterministic
    /// apply output). Sessions whose register fell below the snapshot floor
    /// are NOT recovered - the table has no checkpoint artifact yet
    /// (IGGY-137); those clients re-register and their epoch restarts at 1.
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
/// `solo` marks a single-replica cluster: the quorum is 1/1, so every
/// journaled op was committed the moment it was written and replay runs to
/// the journal head. The embedded `commit` stamps cannot be used there: each
/// stamp is the primary's commit point when the prepare was SENT, so a
/// pipelined burst (e.g. create-stream + create-topic on one connection) is
/// stamped entirely below its own ops and would replay as nothing.
#[allow(clippy::future_not_send)]
pub async fn recover<M>(
    data_dir: &Path,
    solo: bool,
    journal_slots: usize,
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

    let mut client_table = ClientTable::new(CLIENTS_TABLE_MAX);
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
        // Watermarks do NOT, and this is a known gap rather than an invariant.
        // Replay starts at THIS node's snapshot floor, and checkpointing is
        // node-local (`checkpoint_if_needed` fires on local journal occupancy),
        // so replicas cross the floor at different ops. If a client's earlier
        // register fell below this node's floor while a later one survived,
        // replay takes `commit_register`'s fresh-entry branch and the entry
        // returns with `watermark = 0`, where a peer that replayed both took
        // the rebind branch and kept it. The fence still passes, so nothing is
        // evicted, and the same request id a peer answers `Duplicate` gets
        // answered `New` here and re-executed -- exactly-once degrading to
        // at-least-once, silently.
        //
        // Unobservable today (no shipping client re-presents a recovered
        // `client_id`), and closed by putting the table in the snapshot so
        // replay no longer has to reconstruct it. Until then a caught-up
        // primary is authoritative for its OWN table only, which is why
        // `request_preflight`'s catch-up gate cannot bridge this (see its
        // rustdoc).
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
        // was evicted / registered below the snapshot floor).
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
            reply = ?reply,
            "recovery replayed op"
        );
        last_applied_op = Some(header.op);
    }

    Ok(RecoveredMetadata {
        journal,
        snapshot,
        mux_stm,
        client_table,
        last_applied_op,
        last_journaled_op,
    })
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use iggy_binary_protocol::consensus::{Command2, Operation};
    use journal::Journal;
    use server_common::iobuf::Owned;
    use tempfile::tempdir;

    use crate::MuxStateMachine;

    type TestStm = MuxStateMachine<()>;

    const HEADER_SIZE: usize = size_of::<PrepareHeader>();

    fn make_prepare(op: u64, body_size: usize) -> Message<PrepareHeader> {
        make_prepare_with_commit(op, op.saturating_sub(1), body_size)
    }

    /// A prepare as a live primary stamps it: `commit` carries the primary's
    /// commit point when the prepare is sent.
    fn make_prepare_with_commit(op: u64, commit: u64, body_size: usize) -> Message<PrepareHeader> {
        let total_size = HEADER_SIZE + body_size;
        let mut buffer = Owned::<4096>::zeroed(total_size);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..HEADER_SIZE],
        );
        header.size = total_size as u32;
        header.command = Command2::Prepare;
        header.op = op;
        header.commit = commit;
        header.operation = Operation::CreateStream;
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
        Message::try_from(buffer).unwrap()
    }

    #[compio::test]
    async fn recover_empty_state() {
        let dir = tempdir().unwrap();
        let recovered = recover::<TestStm>(
            dir.path(),
            false,
            journal::prepare_journal::DEFAULT_SLOT_COUNT,
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

    // The watermark half of table recovery does NOT survive a checkpoint, and
    // this pins that as a fact rather than a comment. Shape: a client registers,
    // commits request 1, a checkpoint lands past that register, then the client
    // rebinds. Replay starts above the floor, so it never sees the first
    // register: `commit_register` takes its fresh-entry branch and the entry
    // comes back with watermark 0, while a peer whose floor sat lower replayed
    // both registers, took the rebind branch, and kept watermark 1.
    //
    // Consequence, once a client re-presents a recovered id: the same request
    // that peer answers `Duplicate` is `New` here and gets re-executed. The
    // fence is unaffected -- epochs are op-derived, so this node still returns
    // the second register's op.
    //
    // Red/green for the table-in-snapshot work: when the table ships in the
    // checkpoint, the watermark assertion below flips to `Some(1)`.
    #[compio::test]
    async fn recover_loses_the_watermark_when_a_checkpoint_hides_the_first_register() {
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
            "KNOWN GAP: the pre-floor watermark is lost, so request 1 reads as New \
             here while a lower-floor peer answers it as a duplicate"
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
}
