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
use consensus::{Consensus, Project, Sequencer, Status, VsrConsensus};
use iggy_common::{
    header::{Command2, GenericHeader, PrepareHeader, PrepareOkHeader, ReplyHeader},
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

pub trait Metadata<C>
where
    C: Consensus,
{
    /// Handle a request message.
    fn on_request(&self, message: C::RequestMessage) -> impl Future<Output = ()>;

    /// Handle a replicate message (Prepare in VSR).
    fn on_replicate(&self, message: C::ReplicateMessage) -> impl Future<Output = ()>;

    /// Handle an ack message (PrepareOk in VSR).
    fn on_ack(&self, message: C::AckMessage) -> impl Future<Output = ()>;
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

impl<B, J, S, M> Metadata<VsrConsensus<B>> for IggyMetadata<VsrConsensus<B>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    J: JournalHandle,
    J::Target: Journal<
            J::Storage,
            Entry = <VsrConsensus<B> as Consensus>::ReplicateMessage,
            Header = PrepareHeader,
        >,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    async fn on_request(&self, message: <VsrConsensus<B> as Consensus>::RequestMessage) {
        let consensus = self.consensus.as_ref().unwrap();

        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(consensus);
        self.pipeline_prepare(prepare).await;
    }

    async fn on_replicate(&self, message: <VsrConsensus<B> as Consensus>::ReplicateMessage) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = message.header();

        assert_eq!(header.command, Command2::Prepare);

        if !self.fence_old_prepare(&message) {
            self.replicate(message.clone()).await;
        } else {
            warn!("received old prepare, not replicating");
        }

        // If syncing, ignore the replicate message.
        if consensus.is_syncing() {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (sync)"
            );
            return;
        }

        let current_op = consensus.sequencer().current_sequence();

        // If status is not normal, ignore the replicate.
        if consensus.status() != Status::Normal {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (not normal state)"
            );
            return;
        }

        //if message from future view, we ignore the replicate.
        if header.view > consensus.view() {
            warn!(
                replica = consensus.replica(),
                "on_replicate: ignoring (newer view)"
            );
            return;
        }

        // TODO add assertions for valid state here.

        // If we are a follower, we advance the commit number.
        if consensus.is_follower() {
            consensus.advance_commit_number(message.header().commit);
        }

        // TODO verify that the current prepare fits in the WAL.

        // TODO handle gap in ops.

        // Verify hash chain integrity.
        if let Some(previous) = journal.handle().previous_header(header) {
            self.panic_if_hash_chain_would_break_in_same_view(previous, header);
        }

        assert_eq!(header.op, current_op + 1);

        consensus.sequencer().set_sequence(header.op);

        // Append to journal.
        journal.handle().append(message.clone()).await;

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal();
        }
    }

    async fn on_ack(&self, message: <VsrConsensus<B> as Consensus>::AckMessage) {
        let consensus = self.consensus.as_ref().unwrap();
        let header = message.header();

        if !consensus.is_primary() {
            warn!("on_ack: ignoring (not primary)");
            return;
        }

        if consensus.status() != Status::Normal {
            warn!("on_ack: ignoring (not normal)");
            return;
        }

        // Verify checksum by checking pipeline entry exists
        {
            let pipeline = consensus.pipeline().borrow();
            let Some(entry) =
                pipeline.message_by_op_and_checksum(header.op, header.prepare_checksum)
            else {
                debug!("on_ack: prepare not in pipeline op={}", header.op);
                return;
            };

            if entry.message.header().checksum != header.prepare_checksum {
                warn!("on_ack: checksum mismatch");
                return;
            }
        }

        // Let consensus handle the ack increment and quorum check
        if consensus.handle_prepare_ok(header) {
            debug!("on_ack: quorum received for op={}", header.op);
            consensus.advance_commit_number(header.op);

            // Extract the prepare message from the pipeline by op
            // TODO: Commit from the head. ALWAYS
            let entry = consensus.pipeline().borrow_mut().extract_by_op(header.op);
            let Some(entry) = entry else {
                warn!("on_ack: prepare not found in pipeline for op={}", header.op);
                return;
            };

            let prepare = entry.message;
            let prepare_header = *prepare.header();

            // Apply the state (consumes prepare)
            // TODO: Handle appending result to response
            let _result = self.mux_stm.update(prepare);
            debug!("on_ack: state applied for op={}", prepare_header.op);

            // TODO: Figure out better infra for this, its messy.
            let reply = Message::<ReplyHeader>::new(std::mem::size_of::<ReplyHeader>())
                .transmute_header(|_, new| {
                    *new = ReplyHeader {
                        checksum: 0,
                        checksum_body: 0,
                        cluster: consensus.cluster(),
                        size: std::mem::size_of::<ReplyHeader>() as u32,
                        epoch: prepare_header.epoch,
                        view: consensus.view(),
                        release: 0,
                        protocol: 0,
                        command: Command2::Reply,
                        replica: consensus.replica(),
                        reserved_frame: [0; 12],
                        request_checksum: prepare_header.request_checksum,
                        request_checksum_padding: 0,
                        context: 0,
                        context_padding: 0,
                        op: prepare_header.op,
                        commit: consensus.commit(),
                        timestamp: prepare_header.timestamp,
                        request: prepare_header.request,
                        operation: prepare_header.operation,
                        ..Default::default()
                    };
                });

            // Send reply to client
            let generic_reply = reply.into_generic();
            debug!(
                "on_ack: sending reply to client={} for op={}",
                prepare_header.client, prepare_header.op
            );

            // TODO: Error handling
            consensus
                .message_bus()
                .send_to_client(prepare_header.client, generic_reply)
                .await
                .unwrap()
        }
    }
}

impl<B, J, S, M> IggyMetadata<VsrConsensus<B>, J, S, M>
where
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    J: JournalHandle,
    J::Target: Journal<
            J::Storage,
            Entry = <VsrConsensus<B> as Consensus>::ReplicateMessage,
            Header = PrepareHeader,
        >,
    M: StateMachine<Input = Message<PrepareHeader>>,
{
    async fn pipeline_prepare(&self, prepare: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        debug!("inserting prepare into metadata pipeline");
        consensus.verify_pipeline();
        consensus.pipeline_message(prepare.clone());

        self.on_replicate(prepare.clone()).await;
        consensus.post_replicate_verify(&prepare);
    }

    fn fence_old_prepare(&self, prepare: &Message<PrepareHeader>) -> bool {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        let header = prepare.header();
        // TODO: Handle idx calculation, for now using header.op, but since the journal may get compacted, this may not be correct.
        header.op <= consensus.commit() || journal.handle().header(header.op as usize).is_some()
    }

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
        assert!(header.op > consensus.commit());

        let next = (consensus.replica() + 1) % consensus.replica_count();

        let primary = consensus.primary_index(header.view);
        if next == primary {
            debug!(
                replica = consensus.replica(),
                op = header.op,
                "replicate: not replicating (ring complete)"
            );
            return;
        }

        assert_ne!(next, consensus.replica());

        debug!(
            replica = consensus.replica(),
            to = next,
            op = header.op,
            "replicate: forwarding"
        );

        let message = message.into_generic();
        consensus
            .message_bus()
            .send_to_replica(next, message)
            .await
            .unwrap();
    }

    /// Verify hash chain would not break if we add this header.
    fn panic_if_hash_chain_would_break_in_same_view(
        &self,
        previous: &PrepareHeader,
        current: &PrepareHeader,
    ) {
        // If both headers are in the same view, parent must chain correctly
        if previous.view == current.view {
            assert_eq!(
                current.parent, previous.checksum,
                "hash chain broken in same view: op={} parent={} expected={}",
                current.op, current.parent, previous.checksum
            );
        }
    }

    // TODO: Implement jump_to_newer_op
    // fn jump_to_newer_op(&self, header: &PrepareHeader) {}

    fn commit_journal(&self) {
        // TODO: Implement commit logic
        // Walk through journal from last committed to current commit number
        // Apply each entry to the state machine
    }

    /// Send a prepare_ok message to the primary.
    /// Called after successfully writing a prepare to the journal.
    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        let consensus = self.consensus.as_ref().unwrap();
        let journal = self.journal.as_ref().unwrap();

        assert_eq!(header.command, Command2::Prepare);

        if consensus.status() != Status::Normal {
            debug!(
                replica = consensus.replica(),
                status = ?consensus.status(),
                "send_prepare_ok: not sending (not normal)"
            );
            return;
        }

        if consensus.is_syncing() {
            debug!(
                replica = consensus.replica(),
                "send_prepare_ok: not sending (syncing)"
            );
            return;
        }

        // Verify we have the prepare and it's persisted (not dirty).
        if journal.handle().header(header.op as usize).is_none() {
            debug!(
                replica = consensus.replica(),
                op = header.op,
                "send_prepare_ok: not sending (not persisted or missing)"
            );
            return;
        }

        assert!(
            header.view <= consensus.view(),
            "send_prepare_ok: prepare view {} > our view {}",
            header.view,
            consensus.view()
        );

        if header.op > consensus.sequencer().current_sequence() {
            debug!(
                replica = consensus.replica(),
                op = header.op,
                our_op = consensus.sequencer().current_sequence(),
                "send_prepare_ok: not sending (op ahead)"
            );
            return;
        }

        debug!(
            replica = consensus.replica(),
            op = header.op,
            checksum = header.checksum,
            "send_prepare_ok: sending"
        );

        // Use current view, not the prepare's view.
        let prepare_ok_header = PrepareOkHeader {
            command: Command2::PrepareOk,
            cluster: consensus.cluster(),
            replica: consensus.replica(),
            view: consensus.view(),
            epoch: header.epoch,
            op: header.op,
            commit: consensus.commit(),
            timestamp: header.timestamp,
            parent: header.parent,
            prepare_checksum: header.checksum,
            request: header.request,
            operation: header.operation,
            size: std::mem::size_of::<PrepareOkHeader>() as u32,
            ..Default::default()
        };

        let message: Message<PrepareOkHeader> =
            Message::<PrepareOkHeader>::new(std::mem::size_of::<PrepareOkHeader>())
                .transmute_header(|_, new| *new = prepare_ok_header);
        let generic_message = message.into_generic();
        let primary = consensus.primary_index(consensus.view());

        if primary == consensus.replica() {
            debug!(
                replica = consensus.replica(),
                "send_prepare_ok: loopback to self"
            );
            // TODO: Queue for self-processing or call handle_prepare_ok directly
            // TODO: This is temporal, to test simulator, but we should send message to ourselves properly.
            consensus
                .message_bus()
                .send_to_replica(primary, generic_message)
                .await
                .unwrap();
        } else {
            debug!(
                replica = consensus.replica(),
                to = primary,
                op = header.op,
                "send_prepare_ok: sending to primary"
            );

            consensus
                .message_bus()
                .send_to_replica(primary, generic_message)
                .await
                .unwrap();
        }
    }
}
