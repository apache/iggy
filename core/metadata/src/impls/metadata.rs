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
    header::{Command2, PrepareHeader},
    message::Message,
};
use journal::Journal;
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
            self.replicate(message.clone());
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
        self.journal.append(message).await;

        // If follower, commit any newly committable entries.
        if self.consensus.is_follower() {
            self.commit_journal();
        }
    }

    fn on_ack(&self, _message: <Self::Consensus as Consensus>::AckMessage) {
        // TODO: Implement on_prepare_ok logic
        todo!()
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

    fn replicate(&self, _prepare: Message<PrepareHeader>) {
        // TODO Forward prepare to next replica in chain.
        todo!()
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
