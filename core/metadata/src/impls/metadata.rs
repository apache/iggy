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
use journal::Journal;
use message_bus::MessageBus;
use tracing::{debug, warn};

// TODO: Define a trait (probably in some external crate)
#[expect(unused)]
trait Metadata {
    type Consensus: Consensus;
    type Journal: Journal<Entry = <Self::Consensus as Consensus>::ReplicateMessage>;

    /// Handle a replicate message (Prepare in VSR).
    fn on_request(&self, message: <Self::Consensus as Consensus>::RequestMessage);

    /// Handle an ack message (PrepareOk in VSR).
    fn on_replicate(
        &self,
        message: <Self::Consensus as Consensus>::ReplicateMessage,
    ) -> impl Future<Output = ()>;
    fn on_ack(&self, message: <Self::Consensus as Consensus>::AckMessage);
}

#[expect(unused)]
struct IggyMetadata<M, J, S> {
    consensus: VsrConsensus,
    mux_stm: M,
    journal: J,
    snapshot: S,
}

impl<M, J, S> Metadata for IggyMetadata<M, J, S>
where
    J: Journal<Entry = <VsrConsensus as Consensus>::ReplicateMessage, Header = PrepareHeader>,
{
    type Consensus = VsrConsensus;
    type Journal = J;
    fn on_request(&self, message: <Self::Consensus as Consensus>::RequestMessage) {
        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(&self.consensus);
        self.pipeline_prepare(prepare);
    }

    async fn on_replicate(&self, message: <Self::Consensus as Consensus>::ReplicateMessage) {
        let header = message.header();

        assert_eq!(header.command, Command2::Prepare);

        if !self.fence_old_prepare(&message) {
            self.replicate(message.clone()).await;
        } else {
            warn!("received old prepare, not replicating");
        }

        // If syncing, ignore the replicate message.
        if self.consensus.is_syncing() {
            warn!(
                replica = self.consensus.replica(),
                "on_replicate: ignoring (sync)"
            );
            return;
        }

        let current_op = self.consensus.sequencer().current_sequence();

        // Old message (handle as repair). Not replicating.
        if header.view < self.consensus.view()
            || (self.consensus.status() == Status::Normal
                && header.view == self.consensus.view()
                && header.op <= current_op)
        {
            debug!(
                replica = self.consensus.replica(),
                "on_replicate: ignoring (repair)"
            );
            self.on_repair(message);
            return;
        }

        // If status is not normal, ignore the replicate.
        if self.consensus.status() != Status::Normal {
            warn!(
                replica = self.consensus.replica(),
                "on_replicate: ignoring (not normal state)"
            );
            return;
        }

        //if message from future view, we ignore the replicate.
        if header.view > self.consensus.view() {
            warn!(
                replica = self.consensus.replica(),
                "on_replicate: ignoring (newer view)"
            );
            return;
        }

        // TODO add assertions for valid state here.

        // If we are a follower, we advance the commit number.
        if self.consensus.is_follower() {
            self.consensus
                .advance_commit_number(message.header().commit);
        }

        // TODO verify that the current prepare fits in the WAL.

        // TODO handle gap in ops.

        // Verify hash chain integrity.
        if let Some(previous) = self.journal.previous_entry(header) {
            self.panic_if_hash_chain_would_break_in_same_view(&previous, header);
        }

        assert_eq!(header.op, current_op + 1);

        self.consensus.sequencer().set_sequence(header.op);
        self.journal.set_header_as_dirty(header);

        // Append to journal.
        self.journal.append(message.clone()).await;

        // After successful journal write, send prepare_ok to primary.
        self.send_prepare_ok(header).await;

        // If follower, commit any newly committable entries.
        if self.consensus.is_follower() {
            self.commit_journal();
        }
    }

    fn on_ack(&self, message: <Self::Consensus as Consensus>::AckMessage) {
        let header = message.header();

        if !self.consensus.is_primary() {
            warn!("on_ack: ignoring (not primary)");
            return;
        }

        if self.consensus.status() != Status::Normal {
            warn!("on_ack: ignoring (not normal)");
            return;
        }

        // Find the prepare in pipeline
        let Some(mut pipeline) = self.consensus.pipeline().try_borrow_mut().ok() else {
            warn!("on_ack: could not borrow pipeline (already mutably borrowed)");
            return;
        };

        let Some(entry) = pipeline.prepare_by_op_and_checksum(header.op, header.prepare_checksum)
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
        if count >= self.consensus.quorum() && !entry.ok_quorum_received {
            entry.ok_quorum_received = true;
            debug!("on_ack: quorum received for op={}", header.op);

            // Advance commit number and trigger commit journal
            self.consensus.advance_commit_number(header.op);
            self.commit_journal();
        }
    }
}

impl<M, J, S> IggyMetadata<M, J, S>
where
    J: Journal<Entry = <VsrConsensus as Consensus>::ReplicateMessage, Header = PrepareHeader>,
{
    #[expect(unused)]
    fn pipeline_prepare(&self, prepare: Message<PrepareHeader>) {
        debug!("inserting prepare into metadata pipeline");
        self.consensus.verify_pipeline();
        self.consensus.pipeline_message(prepare.clone());

        self.on_replicate(prepare.clone());
        self.consensus.post_replicate_verify(&prepare);
    }

    fn fence_old_prepare(&self, prepare: &Message<PrepareHeader>) -> bool {
        let header = prepare.header();
        header.op <= self.consensus.commit() || self.journal.has_prepare(header)
    }

    /// Replicate a prepare message to the next replica in the chain.
    ///
    /// Chain replication pattern:
    /// - Primary sends to first backup
    /// - Each backup forwards to the next
    /// - Stops when we would forward back to primary
    async fn replicate(&self, message: Message<PrepareHeader>) {
        let header = message.header();

        assert_eq!(header.command, Command2::Prepare);
        assert!(
            !self.journal.has_prepare(header),
            "replicate: must not already have prepare"
        );
        assert!(header.op > self.consensus.commit());

        let next = (self.consensus.replica() + 1) % self.consensus.replica_count();

        let primary = self.consensus.primary_index(header.view);
        if next == primary {
            debug!(
                replica = self.consensus.replica(),
                op = header.op,
                "replicate: not replicating (ring complete)"
            );
            return;
        }

        assert_ne!(next, self.consensus.replica());

        debug!(
            replica = self.consensus.replica(),
            to = next,
            op = header.op,
            "replicate: forwarding"
        );

        let message = message.into_generic();
        self.consensus
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
        assert_eq!(header.command, Command2::Prepare);

        if self.consensus.status() != Status::Normal {
            debug!(
                replica = self.consensus.replica(),
                status = ?self.consensus.status(),
                "send_prepare_ok: not sending (not normal)"
            );
            return;
        }

        if self.consensus.is_syncing() {
            debug!(
                replica = self.consensus.replica(),
                "send_prepare_ok: not sending (syncing)"
            );
            return;
        }

        // Verify we have the prepare and it's persisted (not dirty).
        if !self.journal.has_prepare(header) {
            debug!(
                replica = self.consensus.replica(),
                op = header.op,
                "send_prepare_ok: not sending (not persisted or missing)"
            );
            return;
        }

        assert!(
            header.view <= self.consensus.view(),
            "send_prepare_ok: prepare view {} > our view {}",
            header.view,
            self.consensus.view()
        );

        if header.op > self.consensus.sequencer().current_sequence() {
            debug!(
                replica = self.consensus.replica(),
                op = header.op,
                our_op = self.consensus.sequencer().current_sequence(),
                "send_prepare_ok: not sending (op ahead)"
            );
            return;
        }

        debug!(
            replica = self.consensus.replica(),
            op = header.op,
            checksum = header.checksum,
            "send_prepare_ok: sending"
        );

        // Use current view, not the prepare's view.
        let prepare_ok_header = PrepareOkHeader {
            command: Command2::PrepareOk,
            cluster: self.consensus.cluster(),
            replica: self.consensus.replica(),
            view: self.consensus.view(),
            epoch: header.epoch,
            op: header.op,
            commit: self.consensus.commit(),
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
                .replace_header(|_prev: &PrepareOkHeader| prepare_ok_header);
        let generic_message = message.into_generic();
        let primary = self.consensus.primary_index(self.consensus.view());

        if primary == self.consensus.replica() {
            debug!(
                replica = self.consensus.replica(),
                "send_prepare_ok: loopback to self"
            );
            // TODO: Queue for self-processing or call handle_prepare_ok directly
        } else {
            debug!(
                replica = self.consensus.replica(),
                to = primary,
                op = header.op,
                "send_prepare_ok: sending to primary"
            );

            self.consensus
                .message_bus()
                .send_to_replica(primary, generic_message)
                .await
                .unwrap();
        }
    }
}

// TODO: Hide with associated types all of those generics, so they are not leaking to the upper layer, or maybe even make of the `Metadata` trait itself.
// Something like this:
// pub trait MetadataHandle {
//     type Consensus: Consensus<Self::Clock>;
//     type Clock: Clock;
//     type MuxStm;
//     type Journal;
//     type Snapshot;
// }

// pub trait Metadata<H: MetadataHandle> {
//     fn on_request(&self, message: <H::Consensus as Consensus<H::Clock>>::RequestMessage); // Create type aliases for those long associated types
//     fn on_replicate(&self, message: <H::Consensus as Consensus<H::Clock>>::ReplicateMessage);
//     fn on_ack(&self, message: <H::Consensus as Consensus<H::Clock>>::AckMessage);
// }

// The error messages can get ugly from those associated types, but I think it's worth the fact that it hides a lot of the generics and their bounds.
