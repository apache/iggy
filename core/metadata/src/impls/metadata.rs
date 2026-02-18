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
    Consensus, Pipeline, PipelineEntry, Plane, Project, Sequencer, VsrConsensus, ack_preflight,
    ack_quorum_reached, build_reply_message, drain_committable_prefix, fence_old_prepare_by_commit,
    panic_if_hash_chain_would_break_in_same_view, pipeline_prepare_common, replicate_preflight,
    replicate_to_next_in_chain, send_prepare_ok as send_prepare_ok_common,
};
use iggy_common::{
    header::{Command2, GenericHeader, PrepareHeader, PrepareOkHeader, RequestHeader},
    message::Message,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct IggySnapshot {
    snapshot: MetadataSnapshot,
}

#[allow(unused)]
impl IggySnapshot {
    pub fn new(sequence_number: u64) -> Self {
        Self {
            snapshot: MetadataSnapshot::new(sequence_number),
        }
    }

    pub fn snapshot(&self) -> &MetadataSnapshot {
        &self.snapshot
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

#[derive(Debug)]
pub struct IggyMetadata<C, J, S, M> {
    /// Some on shard0, None on other shards
    pub consensus: Option<C>,
    /// Some on shard0, None on other shards
    pub journal: Option<J>,
    /// Some on shard0, None on other shards
    pub snapshot: Option<S>,
    /// State machine - lives on all shards
    pub mux_stm: M,
}

impl<B, P, J, S, M> Plane<VsrConsensus<B, P>> for IggyMetadata<VsrConsensus<B, P>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    P: Pipeline<Message = Message<PrepareHeader>, Entry = PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    async fn on_request(&self, message: <VsrConsensus<B, P> as Consensus>::Message<RequestHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(consensus);
        pipeline_prepare_common(consensus, prepare, |prepare| self.on_replicate(prepare)).await;
    }

    async fn on_replicate(
        &self,
        message: <VsrConsensus<B, P> as Consensus>::Message<PrepareHeader>,
    ) {
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
        let is_old_prepare = fence_old_prepare_by_commit(consensus, header)
            || journal.handle().header(header.op as usize).is_some();
        if !is_old_prepare {
            self.replicate(message.clone()).await;
        } else {
            warn!("received old prepare, not replicating");
        }

        // TODO add assertions for valid state here.

        // TODO verify that the current prepare fits in the WAL.

        // TODO handle gap in ops.

        // Verify hash chain integrity.
        if let Some(previous) = journal.handle().previous_header(header) {
            panic_if_hash_chain_would_break_in_same_view(&previous, header);
        }

        assert_eq!(header.op, current_op + 1);

        consensus.sequencer().set_sequence(header.op);
        consensus.set_last_prepare_checksum(header.checksum);

        // Append to journal.
        journal.handle().append(message.clone()).await;

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal();
        }
    }

    async fn on_ack(&self, message: <VsrConsensus<B, P> as Consensus>::Message<PrepareOkHeader>) {
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

                // Apply the state (consumes prepare)
                // TODO: Handle appending result to response
                let _result = self.mux_stm.update(prepare);
                debug!("on_ack: state applied for op={}", prepare_header.op);

                // Send reply to client
                let generic_reply = build_reply_message(consensus, &prepare_header).into_generic();
                debug!(
                    "on_ack: sending reply to client={} for op={}",
                    prepare_header.client, prepare_header.op
                );

                // TODO: Propagate send error instead of panicking; requires bus error design.
                consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, generic_reply)
                    .await
                    .unwrap()
            }
        }
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
    async fn replicate(&self, message: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = message.header();

        // TODO: calculate the index;
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

    fn commit_journal(&self) {
        // TODO: Implement commit logic
        // Walk through journal from last committed to current commit number
        // Apply each entry to the state machine
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();
        let persisted = journal.handle().header(header.op as usize).is_some();
        send_prepare_ok_common(consensus, header, Some(persisted)).await;
    }
}
