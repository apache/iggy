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
    CommitLogEvent, Consensus, Pipeline, PipelineEntry, Plane, PlaneIdentity, PlaneKind, Project,
    ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus, ack_preflight,
    ack_quorum_reached, build_reply_message, drain_committable_prefix, emit_sim_event,
    fence_old_prepare_by_commit, panic_if_hash_chain_would_break_in_same_view,
    pipeline_prepare_common, replicate_preflight, replicate_to_next_in_chain, request_preflight,
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

const fn freeze_client_reply(message: Message<GenericHeader>) -> Message<GenericHeader> {
    message
}

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
        let client_id = message.header().client;
        let request = message.header().request;

        // TODO: Add a bounded request queue instead of dropping here.
        // When the prepare queue (8 max) is full, buffer
        // incoming requests in a request queue. On commit, pop the next request
        // from the request queue and begin preparing it. Only drop when both
        // queues are full.
        if consensus.pipeline().borrow().is_full() {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                client = client_id,
                request = request,
                "on_request: pipeline full, dropping request"
            );
            return;
        }

        let Some(_notify) = request_preflight(consensus, client_id, request).await else {
            return;
        };

        emit_sim_event(
            SimEventKind::ClientRequestReceived,
            &RequestLogEvent {
                replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Metadata),
                client_id: message.header().client,
                request_id: message.header().request,
                operation: message.header().operation,
            },
        );
        let prepare = message.project(consensus);
        pipeline_prepare_common(consensus, PlaneKind::Metadata, prepare, |prepare| {
            self.on_replicate(prepare)
        })
        .await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = *message.header();

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

        // TODO: Handle idx calculation, for now using header.op, but since the journal may get compacted, this may not be correct.
        #[allow(clippy::cast_possible_truncation)]
        let is_old_prepare = fence_old_prepare_by_commit(consensus, &header)
            || journal.handle().header(header.op as usize).is_some();
        let message = if is_old_prepare {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                view = consensus.view(),
                op = header.op,
                commit = consensus.commit_max(),
                operation = ?header.operation,
                "received old prepare, skipping replication"
            );
            message
        } else {
            self.replicate(message).await
        };

        // TODO add assertions for valid state here.

        // TODO handle gap in ops.

        // Verify hash chain integrity BEFORE checkpoint. `checkpoint_if_needed`
        // can drain WAL entries, making previous_header return None.
        if let Some(previous) = journal.handle().previous_header(&header) {
            panic_if_hash_chain_would_break_in_same_view(&previous, &header);
        }

        // Force a checkpoint if the journal is running low on capacity.
        if let Some(coordinator) = &self.coordinator {
            // Use commit_min (locally executed), not commit_max. WAL entries
            // between commit_min+1 and commit_max haven't been applied to the
            // state machine yet, draining them would lose data on crash.
            let snap_op = consensus.commit_min();
            match coordinator
                .checkpoint_if_needed(&self.mux_stm, journal, snap_op)
                .await
            {
                Ok(true) => {
                    debug!(
                        target: "iggy.metadata.diag",
                        plane = "metadata",
                        replica_id = consensus.replica(),
                        checkpoint_op = snap_op,
                        "forced checkpoint completed"
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    error!(
                        target: "iggy.metadata.diag",
                        plane = "metadata",
                        replica_id = consensus.replica(),
                        checkpoint_op = snap_op,
                        error = %e,
                        "forced checkpoint failed"
                    );
                    return;
                }
            }
        }

        // TODO: Restore hard assert_eq!(header.op, current_op + 1) once message repair
        // is implemented. Without repair, the network can deliver prepares out of order
        // and the replica has no way to request the missing ones.
        if header.op != current_op + 1 {
            warn!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                expected = current_op + 1,
                "on_replicate: dropping out-of-order prepare (gap)"
            );
            return;
        }

        // Append to journal first. Sequencer and checksum are updated AFTER
        // successful append so a failed write doesn't leave consensus state
        // pointing at a phantom entry.
        if let Err(e) = journal.handle().append(message.clone()).await {
            error!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                operation = ?header.operation,
                error = %e,
                "journal append failed"
            );
            return;
        }

        consensus.sequencer().set_sequence(header.op);
        consensus.set_last_prepare_checksum(header.checksum);

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
            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .entry_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
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

        if ack_quorum_reached(consensus, PlaneKind::Metadata, header) {
            let journal = self.journal.as_ref().unwrap();

            debug!(
                target: "iggy.metadata.diag",
                plane = "metadata",
                replica_id = consensus.replica(),
                op = header.op,
                "ack quorum received"
            );

            let drained = drain_committable_prefix(consensus);
            if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
                debug!(
                    target: "iggy.metadata.diag",
                    plane = "metadata",
                    replica_id = consensus.replica(),
                    first_op = first.header.op,
                    last_op = last.header.op,
                    drained_count = drained.len(),
                    "draining committed metadata prefix"
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

                // Committed ops must be infallible — if the state machine cannot
                // apply a committed op, replicas will diverge.
                let response = self.mux_stm.update(prepare).unwrap_or_else(|err| {
                    panic!(
                        "on_ack: committed metadata op={} failed to apply: {err}",
                        prepare_header.op
                    );
                });
                consensus.advance_commit_min(prepare_header.op);
                let pipeline_depth = consensus.pipeline().borrow().len();
                let event = CommitLogEvent {
                    replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Metadata),
                    op: prepare_header.op,
                    client_id: prepare_header.client,
                    request_id: prepare_header.request,
                    operation: prepare_header.operation,
                    pipeline_depth,
                };
                emit_sim_event(SimEventKind::OperationCommitted, &event);

                let reply = build_reply_message(consensus, &prepare_header, response);
                // Cache reply for duplicate detection:
                consensus
                    .client_table()
                    .borrow_mut()
                    .commit_reply(prepare_header.client, reply.clone());

                let generic_reply = reply.into_generic();
                let reply_buffers = freeze_client_reply(generic_reply);
                emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

                if let Err(e) = consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, reply_buffers)
                    .await
                {
                    warn!(
                        "on_ack: failed to send reply to client={}: {e}",
                        prepare_header.client
                    );
                }
            }
        }
    }
}

impl<B, P, J, S, M> PlaneIdentity<VsrConsensus<B, P>> for IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Entry = PipelineEntry>,
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
    P: Pipeline<Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication pattern:
    /// - Primary sends to first backup
    /// - Each backup forwards to the next
    /// - Stops when we would forward back to primary
    #[allow(clippy::future_not_send)]
    async fn replicate(&self, message: Message<PrepareHeader>) -> Message<PrepareHeader> {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = *message.header();

        // TODO: calculate the index;
        #[allow(clippy::cast_possible_truncation)]
        let idx = header.op as usize;
        assert_eq!(header.command, Command2::Prepare);
        assert!(
            journal.handle().header(idx).is_none(),
            "replicate: must not already have prepare"
        );
        replicate_to_next_in_chain(consensus, message).await
    }

    // TODO: Implement jump_to_newer_op
    // fn jump_to_newer_op(&self, header: &PrepareHeader) {}

    /// Walk ops from `commit_min+1` to `commit_max`, applying the state machine
    /// and updating the client table for each.
    ///
    /// The backup does NOT send replies to clients, only the primary does that.
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&self) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        while consensus.commit_min() < consensus.commit_max() {
            let op = consensus.commit_min() + 1;

            let Some(header) = journal.handle().header(op as usize) else {
                // TODO: Implement message repair: request missing prepare from
                // primary or other replicas. Until then, the backup stalls here.
                break;
            };
            let header = *header;

            let Some(prepare) = journal.handle().entry(&header).await else {
                warn!("commit_journal: prepare body missing for op={op}, stopping");
                break;
            };

            // Committed ops must be infallible (see on_ack comment).
            let response = self.mux_stm.update(prepare).unwrap_or_else(|err| {
                panic!("commit_journal: committed metadata op={op} failed to apply: {err}");
            });

            consensus.advance_commit_min(op);

            let reply = build_reply_message(consensus, &header, response);
            consensus
                .client_table()
                .borrow_mut()
                .commit_reply(header.client, reply);

            debug!("commit_journal: committed op={op}");
        }
    }

    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();
        let persisted = journal.handle().header(header.op as usize).is_some();
        send_prepare_ok_common(consensus, header, Some(persisted)).await;
    }
}
