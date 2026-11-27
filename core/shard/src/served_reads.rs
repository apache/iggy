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

//! Disk reads that answer a peer run beside the pump.
//!
//! A repair or a replay can need prepare bodies that the journal released to
//! their segment, and a transfer chunk can need segment bytes. A read on the
//! pump would hold the frames, ticks and replies of every group on the shard
//! until the disk answers. So the pump decides what to serve, a detached task
//! reads the bytes, and the pump serves the request again once they land. That
//! serve checks the state again, so the bytes never answer a request that the
//! state no longer admits.
//!
//! The reads run one at a time per shard, as they did on the pump, and a read
//! starts only after the pump took the bytes of the reads before it. So a
//! shard holds the bytes of two reads at most: the one that the pump serves
//! and the one that runs. A key has at most one read, and a shard holds at
//! most [`SERVED_READS_MAX`] keys. The pump drops a request past either bound,
//! and its requester asks again.

use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use consensus::StateArtifact;
use iggy_binary_protocol::{RequestPreparesHeader, RequestStateChunkHeader};
use journal::local_gate::LocalGate;
use message_bus::MessageBus;
use partitions::ReleasedBodies;
use partitions::state_transfer::SegmentLoadError;
use server_common::Message;

/// Keys that one shard reads or waits to read for. A waiting key holds its
/// request and the headers of one round, but no bytes.
pub const SERVED_READS_MAX: usize = 64;

/// What a read serves. A request whose key reads waits for that read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServedReadKey {
    /// Released bodies that a repair, or a replay when `live`, sends `target`.
    Prepares { group: u64, target: u8, live: bool },
    /// Segment bytes of the transfer chunk that `requester` pulls.
    Chunk { group: u64, requester: u8 },
}

/// A read that landed, with the request that it serves.
pub enum LandedRead {
    Prepares {
        request: Message<RequestPreparesHeader>,
        live: bool,
        bodies: ReleasedBodies,
    },
    Chunk {
        header: RequestStateChunkHeader,
        /// The offered artifact that `load` read.
        artifact: StateArtifact,
        load: Result<Vec<u8>, SegmentLoadError>,
    },
}

/// The pump and the read tasks share one state, so a shard holds one pointer.
#[derive(Default)]
pub struct ServedReads {
    shared: Rc<Shared>,
}

#[derive(Default)]
struct Shared {
    keys: RefCell<HashSet<ServedReadKey>>,
    landed: RefCell<VecDeque<(ServedReadKey, Option<LandedRead>)>>,
    pump: RefCell<Option<Waker>>,
    /// The read whose turn came while the pump had not taken every result.
    taken: RefCell<Option<Waker>>,
    gate: LocalGate,
}

impl Shared {
    /// Ready once the pump took every read that landed.
    fn poll_taken(&self, context: &Context<'_>) -> Poll<()> {
        if self.landed.borrow().is_empty() {
            return Poll::Ready(());
        }
        self.taken.replace(Some(context.waker().clone()));
        Poll::Pending
    }
}

impl ServedReads {
    pub(crate) fn reading(&self, key: ServedReadKey) -> bool {
        self.shared.keys.borrow().contains(&key)
    }

    /// Runs `read` in a detached task once no other read of this shard runs
    /// and the pump took every result, and holds `key` until the pump takes
    /// this one. Runs nothing when `key` already reads or the shard holds
    /// [`SERVED_READS_MAX`] keys.
    pub(crate) fn start<B: MessageBus>(
        &self,
        bus: &B,
        key: ServedReadKey,
        read: impl Future<Output = LandedRead> + 'static,
    ) {
        {
            let mut keys = self.shared.keys.borrow_mut();
            if keys.len() >= SERVED_READS_MAX || !keys.insert(key) {
                return;
            }
        }
        let landing = Landing {
            shared: Rc::clone(&self.shared),
            key,
            read: None,
        };
        let shared = Rc::clone(&self.shared);
        bus.spawn(async move {
            let _turn = shared.gate.acquire().await;
            poll_fn(|context| shared.poll_taken(context)).await;
            landing.land(read.await);
        });
    }

    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut current = self.shared.pump.borrow_mut();
        if current
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }

    pub(crate) fn has_landed(&self) -> bool {
        !self.shared.landed.borrow().is_empty()
    }

    /// Takes the oldest landed read and frees its key. A read that stopped
    /// without a result frees its key and returns `None`.
    pub(crate) fn take_landed(&self) -> Option<LandedRead> {
        let (key, read) = self.shared.landed.borrow_mut().pop_front()?;
        self.shared.keys.borrow_mut().remove(&key);
        if let Some(taken) = self.shared.taken.take() {
            taken.wake();
        }
        read
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.keys.borrow().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn landed_count(&self) -> usize {
        self.shared.landed.borrow().len()
    }

    #[cfg(test)]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn landed(&self) {
        poll_fn(|context| {
            self.register_waker(context.waker());
            if self.has_landed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    /// Holds the turn of this shard's reads, so none runs until the guard drops.
    #[cfg(test)]
    pub(crate) fn hold(&self) -> journal::local_gate::LocalGateGuard<'_> {
        self.shared.gate.try_acquire().expect("no read runs")
    }
}

/// Hands the result of a read to the pump. A task that stops without one, such
/// as one that panicked, hands over nothing, so its key frees either way.
struct Landing {
    shared: Rc<Shared>,
    key: ServedReadKey,
    read: Option<LandedRead>,
}

impl Landing {
    fn land(mut self, read: LandedRead) {
        self.read = Some(read);
    }
}

impl Drop for Landing {
    fn drop(&mut self) {
        self.shared
            .landed
            .borrow_mut()
            .push_back((self.key, self.read.take()));
        if let Some(waker) = self.shared.pump.borrow_mut().take() {
            waker.wake();
        }
    }
}
