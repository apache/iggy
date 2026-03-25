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
use crate::stm::snapshot::{MetadataSnapshot, RestoreSnapshot, Snapshot, SnapshotError};
use iggy_binary_protocol::consensus::PrepareHeader;
use iggy_binary_protocol::consensus::message::Message;
use iggy_common::IggyError;
use journal::metadata_journal::{JournalError, MetadataJournal};
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
    pub journal: MetadataJournal,
    pub snapshot: IggySnapshot,
    pub mux_stm: M,
    /// `None` means no snapshot existed and no journal entries were replayed.
    /// `Some(op)` is the highest op applied, either from the snapshot or journal replay.
    pub last_applied_op: Option<u64>,
}

/// Recover metadata state from disk.
///
/// 1. Load snapshot from `{data_dir}/metadata/snapshot.bin` (or empty default)
/// 2. Restore state machine from snapshot
/// 3. Open WAL at `{data_dir}/metadata/journal.wal`, scan and rebuild index
/// 4. Replay journal entries from `snapshot.sequence_number + 1` through the state machine
/// 5. Return the assembled `RecoveredMetadata`
///
/// # Errors
/// Returns `RecoveryError` if snapshot loading, journal opening, or replay fails.
#[allow(clippy::future_not_send)]
pub async fn recover<M>(data_dir: &Path) -> Result<RecoveredMetadata<M>, RecoveryError>
where
    M: StateMachine<Input = Message<PrepareHeader>, Error = IggyError>
        + RestoreSnapshot<MetadataSnapshot>,
{
    let metadata_dir = data_dir.join(super::METADATA_DIR);
    std::fs::create_dir_all(&metadata_dir)?;

    // 1. Load snapshot (or empty default if missing)
    let snapshot_path = metadata_dir.join("snapshot.bin");
    let (snapshot, replay_from) = if snapshot_path.exists() {
        let s = IggySnapshot::load(&snapshot_path)?;
        let from = s.sequence_number() + 1;
        (s, from)
    } else {
        // No snapshot, replay from op 0.
        (IggySnapshot::new(0), 0)
    };

    // 2. Restore state machine from snapshot
    let mux_stm = M::restore_snapshot(snapshot.snapshot())?;

    // 3. Open journal, scan the WAL and build index
    let journal_path = metadata_dir.join("journal.wal");
    let journal = MetadataJournal::open(&journal_path, snapshot.sequence_number()).await?;

    // 4. Replay journal entries after snapshot
    let headers_to_replay = journal.iter_headers_from(replay_from);

    let mut last_applied_op: Option<u64> = None;
    for header in &headers_to_replay {
        // TODO: Check hash chain integrity against `previous_header`. On a
        // same-view break, stop replay here and mark the remaining entries for
        // repair via VSR instead of panicking.

        let entry = journal.entry_at(header).await?.ok_or_else(|| {
            RecoveryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read journal entry for op={}", header.op),
            ))
        })?;
        mux_stm.update(entry)?;
        last_applied_op = Some(header.op);
    }

    Ok(RecoveredMetadata {
        journal,
        snapshot,
        mux_stm,
        last_applied_op,
    })
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use iggy_binary_protocol::consensus::{Command2, Operation};
    use journal::Journal;
    use tempfile::tempdir;

    use crate::MuxStateMachine;

    type TestStm = MuxStateMachine<()>;

    const HEADER_SIZE: usize = size_of::<PrepareHeader>();

    fn make_prepare(op: u64, body_size: usize) -> Message<PrepareHeader> {
        let total_size = HEADER_SIZE + body_size;
        let mut buffer = BytesMut::zeroed(total_size);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut buffer[..HEADER_SIZE]);
        header.size = total_size as u32;
        header.command = Command2::Prepare;
        header.op = op;
        header.operation = Operation::CreateStream;
        Message::from_bytes(buffer.freeze()).unwrap()
    }

    #[compio::test]
    async fn recover_empty_state() {
        let dir = tempdir().unwrap();
        let recovered = recover::<TestStm>(dir.path()).await.unwrap();

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

        let recovered = recover::<TestStm>(dir.path()).await.unwrap();
        assert_eq!(recovered.snapshot.sequence_number(), 42);
        assert_eq!(recovered.last_applied_op, None);
    }

    #[compio::test]
    async fn recover_journal_only() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        {
            let journal = MetadataJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            journal.append(make_prepare(1, 32)).await.unwrap();
            journal.append(make_prepare(2, 32)).await.unwrap();
            journal.append(make_prepare(3, 32)).await.unwrap();
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(dir.path()).await.unwrap();
        assert_eq!(recovered.last_applied_op, Some(3));
        assert_eq!(recovered.journal.last_op(), Some(3));
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
            let journal = MetadataJournal::open(&metadata_dir.join("journal.wal"), 0)
                .await
                .unwrap();
            for op in 1..=10 {
                journal.append(make_prepare(op, 32)).await.unwrap();
            }
            journal.storage_ref().fsync().await.unwrap();
        }

        let recovered = recover::<TestStm>(dir.path()).await.unwrap();
        // Should replay ops 6-10 (snapshot was at 5)
        assert_eq!(recovered.last_applied_op, Some(10));
        assert_eq!(recovered.snapshot.sequence_number(), 5);
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
