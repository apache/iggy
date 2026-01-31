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
use consensus::{Consensus, Project, Sequencer, Status, VsrConsensus};
use iggy_common::{
    header::{Command2, PrepareHeader, PrepareOkHeader},
    message::Message,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use tracing::{debug, warn};

#[expect(unused)]
pub trait Metadata<C>
where
    C: Consensus,
{
    /// Handle a request message.
    fn on_request(&self, message: C::RequestMessage);

    /// Handle a replicate message (Prepare in VSR).
    fn on_replicate(&self, message: C::ReplicateMessage) -> impl Future<Output = ()>;

    /// Handle an ack message (PrepareOk in VSR).
    fn on_ack(&self, message: C::AckMessage);
}

#[expect(unused)]
struct IggyMetadata<C, J, S, M> {
    /// Some on shard0, None on other shards
    consensus: Option<C>,
    /// Some on shard0, None on other shards
    journal: Option<J>,
    /// Some on shard0, None on other shards
    snapshot: Option<S>,
    /// State machine - lives on all shards
    mux_stm: M,
}

impl<J, S, M> Metadata<VsrConsensus> for IggyMetadata<VsrConsensus, J, S, M>
where
    J: JournalHandle,
    J::Target: Journal<
            J::Storage,
            Entry = <VsrConsensus as Consensus>::ReplicateMessage,
            Header = PrepareHeader,
        >,
{
    fn on_request(&self, message: <VsrConsensus as Consensus>::RequestMessage) {
        let consensus = self.consensus.as_ref().unwrap();

        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(consensus);
        self.pipeline_prepare(prepare);
    }

    async fn on_replicate(&self, message: <VsrConsensus as Consensus>::ReplicateMessage) {
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

        // Old message (handle as repair). Not replicating.
        if header.view < consensus.view()
            || (consensus.status() == Status::Normal
                && header.view == consensus.view()
                && header.op <= current_op)
        {
            debug!(
                replica = consensus.replica(),
                "on_replicate: ignoring (repair)"
            );
            self.on_repair(message);
            return;
        }

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
        if let Some(previous) = journal.handle().previous_entry(header) {
            self.panic_if_hash_chain_would_break_in_same_view(&previous, header);
        }

        assert_eq!(header.op, current_op + 1);

        consensus.sequencer().set_sequence(header.op);
        journal.handle().set_header_as_dirty(header);

        // Append to journal.
        journal.handle().append(message.clone()).await;

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if consensus.is_follower() {
            self.commit_journal();
        }
    }

    fn on_ack(&self, message: <VsrConsensus as Consensus>::AckMessage) {
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

        // Find the prepare in pipeline
        let mut pipeline = consensus.pipeline().borrow_mut();
        let Some(entry) = pipeline.message_by_op_and_checksum(header.op, header.prepare_checksum)
        else {
            debug!("on_ack: prepare not in pipeline op={}", header.op);
            return;
        };

        // Verify checksum matches
        if entry.message.header().checksum != header.prepare_checksum {
            warn!("on_ack: checksum mismatch");
            return;
        }

        // Record ack
        let count = entry.add_ack(header.replica);

        // Check quorum
        if count >= consensus.quorum() && !entry.ok_quorum_received {
            entry.ok_quorum_received = true;
            debug!("on_ack: quorum received for op={}", header.op);

            // Advance commit number and trigger commit journal
            consensus.advance_commit_number(header.op);
            self.commit_journal();
        }
    }
}

impl<J, S, M> IggyMetadata<VsrConsensus, J, S, M>
where
    J: JournalHandle,
    J::Target: Journal<
            J::Storage,
            Entry = <VsrConsensus as Consensus>::ReplicateMessage,
            Header = PrepareHeader,
        >,
{
    #[expect(unused)]
    fn pipeline_prepare(&self, prepare: Message<PrepareHeader>) {
        let consensus = self.consensus.as_ref().unwrap();

        debug!("inserting prepare into metadata pipeline");
        consensus.verify_pipeline();
        consensus.pipeline_message(prepare.clone());

        self.on_replicate(prepare.clone());
        consensus.post_replicate_verify(&prepare);
    }

    fn fence_old_prepare(&self, prepare: &Message<PrepareHeader>) -> bool {
        let (Some(consensus), Some(journal)) = (&self.consensus, &self.journal) else {
            todo!("dispatch fence_old_prepare to shard0");
        };

        let header = prepare.header();
        header.op <= consensus.commit() || journal.handle().entry(header).is_some()
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

        assert_eq!(header.command, Command2::Prepare);
        assert!(
            journal.handle().entry(header).is_none(),
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

    fn on_repair(&self, _message: Message<PrepareHeader>) {
        todo!()
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
        todo!()
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
        if journal.handle().entry(header).is_none() {
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
