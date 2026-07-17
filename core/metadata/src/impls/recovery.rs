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
use iggy_binary_protocol::consensus::PrepareHeader;
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
/// 4. Replay journal entries from the first post-snapshot op through the state machine
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
    let journal = PrepareJournal::open(&journal_path, watermark).await?;

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

        let entry = journal.entry_at(header).await?.ok_or_else(|| {
            RecoveryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read journal entry for op={}", header.op),
            ))
        })?;
        // WAL replay must recompute authorization denials identically to the
        // primary/backup commit paths, so it goes through the same gate.
        let reply = mux_stm.gated_update(entry)?;
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

    #[compio::test]
    async fn recover_empty_state() {
        let dir = tempdir().unwrap();
        let recovered = recover::<TestStm>(dir.path(), false, |_| {}).await.unwrap();

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

        let recovered = recover::<TestStm>(dir.path(), false, |_| {}).await.unwrap();
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

        let recovered = recover::<TestStm>(dir.path(), false, |_| {}).await.unwrap();
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

        let recovered = recover::<TestStm>(dir.path(), false, |_| {}).await.unwrap();
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

        let recovered = recover::<TestStm>(dir.path(), false, |_| {}).await.unwrap();
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
