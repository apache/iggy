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
use crate::stm::StateMachine;
use crate::stm::snapshot::{FillSnapshot, MetadataSnapshot, Snapshot, SnapshotError};
use consensus::{
    Consensus, Pipeline, PipelineEntry, Plane, PlaneIdentity, Project, Sequencer, VsrConsensus,
    ack_preflight, ack_quorum_reached, build_reply_message, drain_committable_prefix,
    fence_old_prepare_by_commit, panic_if_hash_chain_would_break_in_same_view,
    pipeline_prepare_common, replicate_preflight, replicate_to_next_in_chain,
    send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, GenericHeader, Message, PrepareHeader, PrepareOkHeader,
    RequestHeader,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use std::path::Path;
use tracing::{debug, error, warn};

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct IggySnapshot {
    snapshot: MetadataSnapshot,
}

#[allow(unused)]
impl IggySnapshot {
    #[must_use]
    pub fn new(sequence_number: u64) -> Self {
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

    fn create<T>(stm: &T, sequence_number: u64) -> Result<Self, SnapshotError>
    where
        T: FillSnapshot<MetadataSnapshot>,
    {
        let mut snapshot = MetadataSnapshot::new(sequence_number);

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
    create_snapshot: fn(&M, u64) -> Result<IggySnapshot, SnapshotError>,
}

impl<M> SnapshotCoordinator<M> {
    /// Number of remaining journal slots at which a checkpoint is forced.
    // TODO: tune this margin size
    const CHECKPOINT_MARGIN: usize = 64;

    #[must_use]
    pub fn new(
        data_dir: std::path::PathBuf,
        create_snapshot: fn(&M, u64) -> Result<IggySnapshot, SnapshotError>,
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
    ) -> Result<(), SnapshotError>
    where
        J: JournalHandle,
    {
        let snapshot = (self.create_snapshot)(stm, last_op)?;
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
    ) -> Result<bool, SnapshotError>
    where
        J: JournalHandle,
    {
        let needs_checkpoint = journal
            .handle()
            .remaining_capacity()
            .is_some_and(|c| c <= Self::CHECKPOINT_MARGIN);

        if needs_checkpoint {
            self.checkpoint(stm, journal, commit_op).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub struct IggyMetadata<C, J, S, M> {
    /// Some on shard0, None on other shards
    pub consensus: Option<C>,
    /// Some on shard0, None on other shards
    pub journal: Option<J>,
    /// Some on shard0, None on other shards
    pub snapshot: Option<S>,
    /// State machine - lives on all shards
    pub mux_stm: M,
    /// Snapshot coordinator - present when persistent checkpointing is configured.
    pub coordinator: Option<SnapshotCoordinator<M>>,
}

impl<C, J, S, M> IggyMetadata<C, J, S, M>
where
    M: FillSnapshot<MetadataSnapshot>,
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
        let coordinator = data_dir
            .map(|dir| SnapshotCoordinator::new(dir, |stm, seq| IggySnapshot::create(stm, seq)));
        Self {
            consensus,
            journal,
            snapshot,
            mux_stm,
            coordinator,
        }
    }
}

#[allow(clippy::future_not_send)]
impl<B, J, S, M> Plane<VsrConsensus<B>> for IggyMetadata<VsrConsensus<B>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::Message<RequestHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(consensus);
        pipeline_prepare_common(consensus, prepare, |prepare| self.on_replicate(prepare)).await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = message.header();

        let current_op = match replicate_preflight(consensus, header) {
            Ok(current_op) => current_op,
            Err(reason) => {
                warn!(
                    replica = consensus.replica(),
                    "on_replicate: ignoring ({reason})"
                );
                return;
            }
        };

        // TODO: Handle idx calculation, for now using header.op, but since the journal may get compacted, this may not be correct.
        #[allow(clippy::cast_possible_truncation)]
        let is_old_prepare = fence_old_prepare_by_commit(consensus, header)
            || journal.handle().header(header.op as usize).is_some();
        if is_old_prepare {
            warn!("received old prepare, not replicating");
        } else {
            self.replicate(message.clone()).await;
        }

        // TODO add assertions for valid state here.

        // TODO handle gap in ops.

        // Force a checkpoint if the journal is running low on capacity.
        if let Some(coordinator) = &self.coordinator {
            let snap_op = consensus.commit();
            match coordinator
                .checkpoint_if_needed(&self.mux_stm, journal, snap_op)
                .await
            {
                Ok(true) => {
                    debug!(
                        replica = consensus.replica(),
                        "on_replicate: forced checkpoint at op={snap_op}"
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    error!(
                        replica = consensus.replica(),
                        "on_replicate: forced checkpoint failed: {e}"
                    );
                    return;
                }
            }
        }

        // Verify hash chain integrity.
        if let Some(previous) = journal.handle().previous_header(header) {
            panic_if_hash_chain_would_break_in_same_view(&previous, header);
        }

        assert_eq!(header.op, current_op + 1);

        consensus.sequencer().set_sequence(header.op);
        consensus.set_last_prepare_checksum(header.checksum);

        // Append to journal.
        if let Err(e) = journal.handle().append(message.clone()).await {
            error!(
                replica = consensus.replica(),
                "on_replicate: journal append failed: {e}"
            );
            return;
        }

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal();
        }
    }

    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareOkHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let header = message.header();

        if let Err(reason) = ack_preflight(consensus) {
            warn!("on_ack: ignoring ({reason})");
            return;
        }

        {
            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .message_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
                debug!("on_ack: prepare not in pipeline op={}", header.op);
                return;
            }
        }

        if ack_quorum_reached(consensus, header) {
            let journal = self.journal.as_ref().unwrap();

            debug!("on_ack: quorum received for op={}", header.op);

            let drained = drain_committable_prefix(consensus);
            if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
                debug!(
                    "on_ack: draining committed prefix ops=[{}..={}] count={}",
                    first.header.op,
                    last.header.op,
                    drained.len()
                );
            }

            for entry in drained {
                let prepare_header = entry.header;
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

                let response = self.mux_stm.update(prepare).unwrap_or_else(|err| {
                    warn!(
                        "on_ack: state machine error for op={}: {err}",
                        prepare_header.op
                    );
                    bytes::Bytes::new()
                });
                debug!("on_ack: state applied for op={}", prepare_header.op);

                let generic_reply =
                    build_reply_message(consensus, &prepare_header, response).into_generic();
                debug!(
                    "on_ack: sending reply to client={} for op={}",
                    prepare_header.client, prepare_header.op
                );

                // TODO: Propagate send error instead of panicking; requires bus error design.
                consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, generic_reply)
                    .await
                    .unwrap();
            }
        }
    }
}

impl<B, P, J, S, M> PlaneIdentity<VsrConsensus<B, P>> for IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
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
        message.header().operation().is_metadata()
    }
}

impl<B, P, J, S, M> IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication pattern:
    /// - Primary sends to first backup
    /// - Each backup forwards to the next
    /// - Stops when we would forward back to primary
    #[allow(clippy::future_not_send)]
    async fn replicate(&self, message: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = message.header();

        // TODO: calculate the index;
        #[allow(clippy::cast_possible_truncation)]
        let idx = header.op as usize;
        assert_eq!(header.command, Command2::Prepare);
        assert!(
            journal.handle().header(idx).is_none(),
            "replicate: must not already have prepare"
        );
        replicate_to_next_in_chain(consensus, message).await;
    }

    // TODO: Implement jump_to_newer_op
    // fn jump_to_newer_op(&self, header: &PrepareHeader) {}

    #[allow(clippy::unused_self)]
    const fn commit_journal(&self) {
        // TODO: Implement commit logic
        // Walk through journal from last committed to current commit number
        // Apply each entry to the state machine
    }

    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();
        let persisted = journal.handle().header(header.op as usize).is_some();
        send_prepare_ok_common(consensus, header, Some(persisted)).await;
    }
}
