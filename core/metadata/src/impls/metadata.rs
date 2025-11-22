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
use consensus::{Consensus, Prepare, Project, VsrConsensus};
use tracing::{debug, warn};

// TODO: Define a trait (probably in some external crate)
#[expect(unused)]
trait Metadata {
    type Consensus: Consensus;
    fn on_request(&self, message: <Self::Consensus as Consensus>::RequestMessage);
    fn on_replicate(&self, message: <Self::Consensus as Consensus>::ReplicateMessage);
    fn on_ack(&self, message: <Self::Consensus as Consensus>::AckMessage);
}

#[expect(unused)]
struct IggyMetadata<M, J, S> {
    consensus: VsrConsensus,
    mux_stm: M,
    journal: J,
    snapshot: S,
}

impl<M, J, S> Metadata for IggyMetadata<M, J, S> {
    type Consensus = VsrConsensus;
    fn on_request(&self, message: <Self::Consensus as Consensus>::RequestMessage) {
        // TODO: Bunch of asserts.
        debug!("handling metadata request");
        let prepare = message.project(&self.consensus);
        self.pipeline_prepare(prepare);
    }

    fn on_replicate(&self, message: <Self::Consensus as Consensus>::ReplicateMessage) {
        if !self.fence_old_prepare(&message) {
            self.replicate(message.clone());
        } else {
            warn!("received old prepare, not replicating");
        }
    }

    fn on_ack(&self, _message: <Self::Consensus as Consensus>::AckMessage) {
        todo!()
    }
}

impl<M, J, S> IggyMetadata<M, J, S> {
    #[expect(unused)]
    fn pipeline_prepare(&self, prepare: Prepare) {
        debug!("inserting prepare into metadata pipeline");
        self.consensus.verify_pipeline();
        self.consensus.pipeline_message(prepare.clone());

        self.on_replicate(prepare.clone());
        self.consensus.post_replicate_verify(&prepare);
    }

    fn fence_old_prepare(&self, _prepare: &Prepare) -> bool {
        // TODO
        false
    }

    fn replicate(&self, _prepare: Prepare) {
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
