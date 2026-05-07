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

use iggy_binary_protocol::{ConsensusHeader, ConsensusMessage};
use message_bus::MessageBus;

pub trait Project<T, C: Consensus> {
    type Consensus: Consensus;
    fn project(self, consensus: &Self::Consensus) -> T;
}

pub trait Pipeline {
    type Entry;
    /// Accepted-but-not-yet-prepared client request. For `LocalPipeline`,
    /// `RequestEntry` wrapping `Message<RequestHeader>`.
    type Request;

    fn push(&mut self, entry: Self::Entry);

    fn pop(&mut self) -> Option<Self::Entry>;

    fn clear(&mut self);

    fn entry_by_op(&self, op: u64) -> Option<&Self::Entry>;

    fn entry_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry>;

    fn entry_by_op_and_checksum(&self, op: u64, checksum: u128) -> Option<&Self::Entry>;

    fn head(&self) -> Option<&Self::Entry>;

    /// True iff prepare queue is full. Callers route to
    /// [`Self::push_request`] on `true`.
    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;

    fn verify(&self);

    /// True iff either queue carries `client_id`. Used by metadata-plane
    /// preflight for in-flight dedup. Partition plane is at-least-once
    /// and skips. Default `false`; falls through to slot dedup in
    /// `check_request`.
    fn has_message_from_client(&self, _client_id: u128) -> bool {
        false
    }

    /// Drop reply senders on every entry; receivers wake `Canceled`.
    /// View-change reset uses this to unblock awaiters while preserving
    /// pipeline for DVC reconciliation.
    fn cancel_all_subscribers(&mut self) {}

    /// Drop `request_queue`, preserve `prepare_queue` (DVC reconciliation).
    /// Stale primary-era requests must not outlive the transition;
    /// clients re-send via read-timeout.
    fn clear_request_queue(&mut self) {}

    /// Buffer a request behind a full prepare queue.
    ///
    /// # Errors
    /// `Err(request)` if request queue full (or no queue — default impl).
    /// Caller drops; client retries.
    fn push_request(&mut self, request: Self::Request) -> Result<(), Self::Request> {
        Err(request)
    }

    /// Pop request-queue head. Called when a prepare commits and frees
    /// a slot. Default `None` (no queue).
    fn pop_request(&mut self) -> Option<Self::Request> {
        None
    }
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
    type Pipeline: Pipeline;

    fn pipeline_message(&self, plane: PlaneKind, message: &Self::Message<Self::ReplicateHeader>);
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
/// - ack (`prepare_ok`)
pub trait Plane<C>
where
    C: Consensus,
{
    fn on_request(&self, message: RequestMessage<C>) -> impl Future<Output = ()>
    where
        RequestMessage<C>: Project<ReplicateMessage<C>, C, Consensus = C>;

    fn on_replicate(&self, message: ReplicateMessage<C>) -> impl Future<Output = ()>
    where
        ReplicateMessage<C>: Project<AckMessage<C>, C, Consensus = C>;

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

pub mod client_table;
pub use client_table::{CachedReply, ClientTable};
// One-shot per `PipelineEntry` for in-process commit awaiters.
pub(crate) mod oneshot;
pub use oneshot::{Canceled, Receiver};

mod impls;
pub use impls::*;
mod plane_mux;
pub use plane_mux::*;
mod plane_helpers;
pub use plane_helpers::*;
mod metadata_helpers;
pub use metadata_helpers::*;
mod observability;
pub use observability::*;

mod view_change_quorum;
pub use view_change_quorum::*;
mod vsr_timeout;
