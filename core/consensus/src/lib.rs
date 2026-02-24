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

use iggy_common::header::ConsensusHeader;
use iggy_common::message::ConsensusMessage;
use message_bus::MessageBus;

pub trait Project<T, C: Consensus> {
    type Consensus: Consensus;
    fn project(self, consensus: &Self::Consensus) -> T;
}

pub trait Pipeline {
    type Message;
    type Entry;

    fn push_message(&mut self, message: Self::Message);

    fn pop_message(&mut self) -> Option<Self::Entry>;

    fn clear(&mut self);

    fn message_by_op(&self, op: u64) -> Option<&Self::Entry>;

    fn message_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry>;

    fn message_by_op_and_checksum(&self, op: u64, checksum: u128) -> Option<&Self::Entry>;

    fn head(&self) -> Option<&Self::Entry>;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn verify(&self);
}

pub type RequestMessage<C> = <C as Consensus>::Message<<C as Consensus>::RequestHeader>;
pub type ReplicateMessage<C> = <C as Consensus>::Message<<C as Consensus>::ReplicateHeader>;
pub type AckMessage<C> = <C as Consensus>::Message<<C as Consensus>::AckHeader>;

pub trait Consensus: Sized {
    type MessageBus: MessageBus;
    #[rustfmt::skip] // Scuffed formatter.
    type Message<H>: ConsensusMessage<H> where H: ConsensusHeader;

    type RequestHeader: ConsensusHeader;
    type ReplicateHeader: ConsensusHeader;
    type AckHeader: ConsensusHeader;

    type Sequencer: Sequencer;
    type Pipeline: Pipeline<Message = Self::Message<Self::ReplicateHeader>>;

    fn pipeline_message(&self, message: Self::Message<Self::ReplicateHeader>);
    fn verify_pipeline(&self);

    fn is_follower(&self) -> bool;
    fn is_normal(&self) -> bool;
    fn is_syncing(&self) -> bool;
}

/// Shared consensus lifecycle interface for control/data planes.
///
/// This abstracts the VSR message flow:
/// - request -> prepare
/// - replicate (prepare)
/// - ack (prepare_ok)
pub trait Plane<C>
where
    C: Consensus,
{
    fn on_request(&self, message: RequestMessage<C>) -> impl Future<Output = ()>
    where
        RequestMessage<C>: Project<ReplicateMessage<C>, C, Consensus = C> + Clone;

    fn on_replicate(&self, message: ReplicateMessage<C>) -> impl Future<Output = ()>
    where
        ReplicateMessage<C>: Project<AckMessage<C>, C, Consensus = C> + Clone;

    fn on_ack(&self, message: AckMessage<C>) -> impl Future<Output = ()>;
}

pub trait PlaneIdentity<C>
where
    C: Consensus,
{
    fn is_applicable<H>(&self, message: &C::Message<H>) -> bool
    where
        H: ConsensusHeader;
}

mod impls;
pub use impls::*;
mod plane_mux;
pub use plane_mux::*;
mod namespaced_pipeline;
pub use namespaced_pipeline::*;
mod plane_helpers;
pub use plane_helpers::*;

mod view_change_quorum;
pub use view_change_quorum::*;
mod vsr_timeout;
