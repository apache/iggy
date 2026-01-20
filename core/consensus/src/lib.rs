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

use message_bus::MessageBus;

pub trait Project<T> {
    type Consensus: Consensus;
    fn project(self, consensus: &Self::Consensus) -> T;
}

pub trait Pipeline {
    type Message;
    type Entry;

    fn push_message(&mut self, message: Self::Message);

    fn pop_message(&mut self) -> Option<Self::Entry>;

    fn clear(&mut self);

    fn message_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry>;

    fn message_by_op_and_checksum(&mut self, op: u64, checksum: u128) -> Option<&mut Self::Entry>;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn verify(&self);
}

pub trait Consensus {
    type MessageBus: MessageBus;
    // I am wondering, whether we should create a dedicated trait for cloning, so it's explicit that we do ref counting.
    type RequestMessage: Project<Self::ReplicateMessage, Consensus = Self> + Clone;
    type ReplicateMessage: Project<Self::AckMessage, Consensus = Self> + Clone;
    type AckMessage;
    type Sequencer: Sequencer;
    type Pipeline: Pipeline<Message = Self::ReplicateMessage>;

    fn pipeline_message(&self, message: Self::ReplicateMessage);
    fn verify_pipeline(&self);

    // TODO: Figure out how we can achieve that without exposing such methods in the Consensus trait.
    fn post_replicate_verify(&self, message: &Self::ReplicateMessage);

    fn is_follower(&self) -> bool;
    fn is_syncing(&self) -> bool;
}

mod impls;
pub use impls::*;

mod view_change_quorum;
pub use view_change_quorum::*;
mod vsr_timeout;
