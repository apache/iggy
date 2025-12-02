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

use crate::{Consensus, Project};
use iggy_common::header::{Command2, PrepareHeader, PrepareOkHeader, RequestHeader};
use iggy_common::message::Message;
use message_bus::IggyMessageBus;
use std::cell::Cell;

pub struct VsrConsensus {
    op: Cell<u64>,
}

impl VsrConsensus {
    pub fn advance_commit_number(&self) {}

    pub fn update_op(&self, op: u64) {
        self.op.set(op);
    }

    pub fn op(&self) -> u64 {
        self.op.get()
    }
}

impl Project<Message<PrepareHeader>> for Message<RequestHeader> {
    type Consensus = VsrConsensus;
    fn project(self, _consensus: &Self::Consensus) -> Message<PrepareHeader> {
        self.replace_header(|prev| {
            PrepareHeader {
                cluster: 0, // TODO: consesus.cluster
                size: prev.size,
                view: 0, // TODO: consesus view
                release: prev.release,
                command: Command2::Prepare,
                replica: 0, // TODO: consesus replica
                parent: 0, // TODO: Get this from the previous entry in the journal (figure out how to pass that ctx here)
                request_checksum: prev.request_checksum,
                request: prev.request,
                commit: 0,    // TODO: consensus.commit
                op: 0,        // TODO: consensus.op
                timestamp: 0, // TODO: consensus timestamp
                operation: prev.operation,
                ..Default::default()
            }
        })
    }
}

impl Project<Message<PrepareOkHeader>> for Message<PrepareHeader> {
    type Consensus = VsrConsensus;
    fn project(self, _consensus: &Self::Consensus) -> Message<PrepareOkHeader> {
        self.replace_header(|prev| {
            PrepareOkHeader {
                command: Command2::PrepareOk,
                parent: prev.parent,
                prepare_checksum: prev.checksum,
                request: prev.request,
                cluster: 0, // TODO: consensus.cluster
                replica: 0, // TODO: consensus replica
                epoch: 0,   // TODO: consensus.epoch
                // It's important to use the view of the replica, not the received prepare!
                view: 0, // TODO: consensus.view
                op: prev.op,
                commit: 0, // TODO: consensus.commit
                timestamp: prev.timestamp,
                operation: prev.operation,
                // PrepareOks are only header no body
                ..Default::default()
            }
        })
    }
}

impl Consensus for VsrConsensus {
    type MessageBus = IggyMessageBus;

    type RequestMessage = Message<RequestHeader>;
    type ReplicateMessage = Message<PrepareHeader>;
    type AckMessage = Message<PrepareOkHeader>;

    fn pipeline_message(&self, _message: Self::ReplicateMessage) {
        todo!()
    }

    fn verify_pipeline(&self) {
        todo!()
    }

    fn post_replicate_verify(&self, _message: &Self::ReplicateMessage) {
        todo!()
    }

    fn is_follower(&self) -> bool {
        todo!()
    }
}
