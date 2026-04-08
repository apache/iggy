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

use iggy_binary_protocol::{Message, ReplyHeader};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::task::Waker;

/// Identifies a specific request from a specific client.
/// Used as the key for the pending-commit waiter map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientRequest {
    pub client_id: u128,
    pub request: u64,
}

/// Inner state shared between `Notify` clones via `Rc`.
#[derive(Debug)]
struct NotifyInner {
    waker: RefCell<Option<Waker>>,
    notified: std::cell::Cell<bool>,
}

/// Lightweight, single-threaded async notification primitive.
///
/// ## Usage
///
/// ```ignore
/// let notify = Notify::new();
/// let waiter = notify.clone();
///
/// // Producer side (in commit_reply):
/// notify.notify();
///
/// // Consumer side (caller awaiting the commit):
/// waiter.notified().await;
/// ```
#[derive(Debug, Clone)]
pub struct Notify {
    inner: Rc<NotifyInner>,
}

impl Notify {
    /// Create a new `Notify` in the un-notified state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Rc::new(NotifyInner {
                waker: RefCell::new(None),
                notified: std::cell::Cell::new(false),
            }),
        }
    }

    /// Wake the waiter, if any. If `notified()` is polled later, it will
    /// resolve immediately.
    pub fn notify(&self) {
        self.inner.notified.set(true);
        if let Some(waker) = self.inner.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    /// Returns a future that resolves when [`notify()`](Self::notify) is called.
    ///
    /// If `notify()` was already called before this future is polled, it
    /// resolves immediately (permit is consumed).
    #[allow(clippy::future_not_send)]
    pub fn notified(&self) -> impl Future<Output = ()> + '_ {
        std::future::poll_fn(move |cx| {
            if self.inner.notified.get() {
                self.inner.notified.set(false);
                std::task::Poll::Ready(())
            } else {
                *self.inner.waker.borrow_mut() = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-client entry in the clients table (VR paper Section 4, Figure 2).
///
/// Stores the reply for the client's latest committed request. The client ID,
/// request number, and commit number are all read from `reply.header()`.
#[derive(Debug)]
pub struct ClientEntry {
    /// The cached reply for the client's latest committed request (header + body).
    pub reply: Message<ReplyHeader>,
}

/// Result of checking a request against the client table.
#[derive(Debug)]
pub enum RequestStatus {
    /// Request not seen before, proceed with consensus.
    New,
    /// Exact request already committed, re-send cached reply.
    Duplicate(Message<ReplyHeader>),
    /// Request is in the pipeline awaiting commit, drop (client should wait).
    InProgress,
    /// Request number is older than the client's latest committed request.
    /// Already handled in a prior commit cycle, drop silently.
    Stale,
}

/// VSR client-table: tracks per-client request state for duplicate detection,
/// reply caching, and async commit notification.
///
/// Uses a fixed-size slot array as the source of truth, with a `HashMap`
/// as a secondary index for O(1) lookups by client ID.
///
/// ## Committed state (`slots` + `index`)
///
/// Always contains a valid `ClientEntry` with a non-optional reply.
/// Updated by `commit_reply` when a request commits through consensus.
///
/// ## Pending state (`pending`)
///
/// Tracks in-flight requests that have been accepted for consensus but not yet
/// committed. Each entry holds a [`Notify`] that is fired when the corresponding
/// `commit_reply` arrives. Keyed by `ClientRequest` to support future
/// request pipelining (currently at most one per client).
///
/// The `pending` map is local notification state not replicated, not
/// serialized, not part of the deterministic committed state.
///
/// ## Known gaps
///
/// - **Message repair**: If a backup never received a prepare (lost message),
///   `commit_journal` stops at the gap. The client table will be missing
///   entries for ops beyond the gap until the message repair protocol is
///   implemented and the missing prepare is retransmitted.
///
/// - **Checkpoint serialization**: The slot array is laid out for deterministic
///   encode/decode to disk, but serialization is not yet implemented.
#[derive(Debug)]
pub struct ClientTable {
    /// `None` means the slot is free.
    /// Deterministic iteration order for eviction and serialization.
    slots: Vec<Option<ClientEntry>>,
    /// Secondary index: `client_id` → slot index. Rebuilt on decode.
    index: HashMap<u128, usize>,
    /// Pending commit waiters, keyed by `(client_id, request)`.
    /// Keyed by request number (not just client) to support future pipelining.
    /// Currently at most one per client.
    pending: HashMap<ClientRequest, Notify>,
}

impl ClientTable {
    #[must_use]
    pub fn new(max_clients: usize) -> Self {
        let mut slots = Vec::with_capacity(max_clients);
        slots.resize_with(max_clients, || None);
        Self {
            slots,
            index: HashMap::with_capacity(max_clients),
            pending: HashMap::new(),
        }
    }

    /// Check a request against the table.
    ///
    /// Returns:
    /// - [`RequestStatus::New`]: not seen before, proceed with consensus
    /// - [`RequestStatus::Duplicate`]: already committed, re-send cached reply
    /// - [`RequestStatus::InProgress`]: in the pipeline awaiting commit
    /// - [`RequestStatus::Stale`]: older than the client's latest committed request
    ///
    /// # Panics
    /// Panics if the internal index points to an empty slot (invariant violation).
    #[must_use]
    pub fn check_request(&self, client_id: u128, request: u64) -> RequestStatus {
        // TODO: Once client sessions are added (register/evict protocol like
        // validate client_id at the session layer instead of
        // panicking here. Unregistered or invalid clients should be rejected
        // gracefully at ingress, not inside the client table.
        assert!(client_id != 0, "client_id 0 is reserved for internal use");

        // Check if already pending in the pipeline.
        let key = ClientRequest { client_id, request };
        if self.pending.contains_key(&key) {
            return RequestStatus::InProgress;
        }

        let Some(&slot_idx) = self.index.get(&client_id) else {
            return RequestStatus::New;
        };
        let entry = self.slots[slot_idx].as_ref().expect("index/slot mismatch");
        let committed_request = entry.reply.header().request;

        if request < committed_request {
            return RequestStatus::Stale;
        }
        if request == committed_request {
            return RequestStatus::Duplicate(entry.reply.clone());
        }

        RequestStatus::New
    }

    /// Register interest in a pending request's commit.
    ///
    /// Returns a [`Notify`] the caller can `.notified().await` on. The `Notify`
    /// is cloned via `Rc`, so the caller can hold it across `.await` points
    /// without borrowing the `ClientTable`.
    ///
    /// Called after `check_request` returns `New`, before submitting the request
    /// to the consensus pipeline.
    ///
    /// # Panics
    /// Panics if there is already a pending waiter for this `(client_id, request)`.
    pub fn register_pending(&mut self, client_id: u128, request: u64) -> Notify {
        let notify = Notify::new();
        let key = ClientRequest { client_id, request };
        let prev = self.pending.insert(key, notify.clone());
        assert!(
            prev.is_none(),
            "client {client_id} request {request} already has a pending waiter"
        );
        notify
    }

    /// Record a committed reply and cache it.
    ///
    /// If the client already has a slot, updates it in place. Otherwise allocates
    /// a free slot, evicting the client with the oldest commit number if the table
    /// is full.
    ///
    /// Wakes the pending [`Notify`] for this `(client_id, request)` if one exists.
    ///
    /// Called in `on_ack` after `build_reply_message`.
    ///
    /// # Panics
    /// Panics if the internal index points to an empty slot (invariant violation).
    pub fn commit_reply(&mut self, client_id: u128, reply: Message<ReplyHeader>) {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        assert_eq!(
            client_id,
            reply.header().client,
            "commit_reply: client_id mismatch (arg={client_id}, header={})",
            reply.header().client
        );
        let request = reply.header().request;

        if let Some(&slot_idx) = self.index.get(&client_id) {
            let slot = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
            // Monotonicity: both commit (op) and request must not regress.
            assert!(
                reply.header().commit >= slot.reply.header().commit,
                "commit_reply: commit regression for client {client_id}: {} -> {}",
                slot.reply.header().commit,
                reply.header().commit
            );
            assert!(
                reply.header().request >= slot.reply.header().request,
                "commit_reply: request regression for client {client_id}: {} -> {}",
                slot.reply.header().request,
                reply.header().request
            );
            slot.reply = reply;
        } else {
            // Need a free slot. Evict if full.
            if self.index.len() >= self.slots.len() {
                self.evict_oldest();
            }

            let slot_idx = self.first_free_slot().expect("eviction must free a slot");
            self.slots[slot_idx] = Some(ClientEntry { reply });
            self.index.insert(client_id, slot_idx);
        }

        // Wake the waiter, if any.
        let key = ClientRequest { client_id, request };
        if let Some(notify) = self.pending.remove(&key) {
            notify.notify();
        }
    }

    /// Evict the client with the oldest commit number.
    ///
    /// Iterates the fixed-size slot array (deterministic order), so all replicas
    /// with the same committed state evict the same client. Ties on commit number
    /// are broken by slot index (lowest index wins), which is also deterministic.
    ///
    /// **Dedup caveat**: until checkpoint serialization is implemented, eviction
    /// breaks at-most-once semantics for the evicted client — a retransmission
    /// after eviction will be treated as `New` and re-executed.
    fn evict_oldest(&mut self) {
        let mut evictee: Option<(usize, u64)> = None; // (slot_idx, commit)

        for (idx, slot) in self.slots.iter().enumerate() {
            if let Some(entry) = slot {
                let commit = entry.reply.header().commit;
                let should_evict = match evictee {
                    None => true,
                    Some((_, min_commit)) => commit < min_commit,
                };
                if should_evict {
                    evictee = Some((idx, commit));
                }
            }
        }

        if let Some((slot_idx, _)) = evictee {
            let entry = self.slots[slot_idx].take().expect("evictee must exist");
            self.index.remove(&entry.reply.header().client);
        }
    }

    /// Find the first free slot in the array.
    fn first_free_slot(&self) -> Option<usize> {
        self.slots.iter().position(Option::is_none)
    }

    /// Get the cached reply for a client (for duplicate re-sends).
    #[must_use]
    pub fn get_reply(&self, client_id: u128) -> Option<&Message<ReplyHeader>> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| &entry.reply)
    }

    /// Number of active committed client entries.
    #[must_use]
    pub fn count(&self) -> usize {
        self.index.len()
    }

    /// Number of pending (in-flight) requests.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Clear all pending entries (e.g. during view change).
    ///
    /// Stale pending entries from a previous view must not survive into the
    /// new view - `check_request` would return `InProgress` for the orphaned
    /// keys, silently dropping valid client retries.
    pub fn clear_pending(&mut self) {
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, Operation};

    fn make_reply_for(client: u128, request: u64, commit: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            client,
            request,
            commit,
            command: Command2::Reply,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        msg
    }

    fn make_reply(request: u64, commit: u64) -> Message<ReplyHeader> {
        make_reply_for(1, request, commit)
    }

    // Notify tests

    #[test]
    fn notify_after_await_registration() {
        let notify = Notify::new();
        let waiter = notify.clone();

        // Notify before anyone polls, should resolve immediately on first poll.
        notify.notify();

        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut fut = std::pin::pin!(waiter.notified());
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn notify_wakes_pending_poll() {
        let notify = Notify::new();
        let waiter = notify.clone();

        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut fut = std::pin::pin!(waiter.notified());

        // First poll returns Pending (not yet notified).
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        // Notify fires.
        notify.notify();

        // Next poll resolves.
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn notify_consumed_after_ready() {
        let notify = Notify::new();
        notify.notify();

        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        // First notified() consumes the permit.
        let mut fut1 = std::pin::pin!(notify.notified());
        assert!(fut1.as_mut().poll(&mut cx).is_ready());

        // Second notified() should be pending (permit consumed).
        let mut fut2 = std::pin::pin!(notify.notified());
        assert!(fut2.as_mut().poll(&mut cx).is_pending());
    }

    // ClientTable tests

    #[test]
    fn check_request_new() {
        let table = ClientTable::new(10);
        assert!(matches!(table.check_request(1, 1), RequestStatus::New));
    }

    #[test]
    fn check_request_duplicate_after_commit() {
        let mut table = ClientTable::new(10);
        table.commit_reply(1, make_reply(1, 10));

        match table.check_request(1, 1) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1);
            }
            _ => panic!("expected Duplicate"),
        }
    }

    #[test]
    fn check_request_stale() {
        let mut table = ClientTable::new(10);
        table.commit_reply(1, make_reply(5, 10));

        assert!(matches!(table.check_request(1, 3), RequestStatus::Stale));
    }

    #[test]
    fn check_request_in_progress_while_pending() {
        let mut table = ClientTable::new(10);
        let _notify = table.register_pending(1, 1);

        assert!(matches!(
            table.check_request(1, 1),
            RequestStatus::InProgress
        ));
    }

    #[test]
    fn commit_caches_reply() {
        let mut table = ClientTable::new(10);
        table.commit_reply(1, make_reply(1, 10));

        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn commit_updates_existing_entry() {
        let mut table = ClientTable::new(10);
        table.commit_reply(1, make_reply(1, 10));
        table.commit_reply(1, make_reply(2, 20));

        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 2);
        assert_eq!(table.count(), 1);
    }

    #[test]
    fn register_and_commit_notifies() {
        let mut table = ClientTable::new(10);
        let notify = table.register_pending(1, 1);

        assert_eq!(table.pending_count(), 1);

        // Commit fires the notify.
        table.commit_reply(1, make_reply(1, 10));

        assert_eq!(table.pending_count(), 0);

        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut fut = std::pin::pin!(notify.notified());
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn eviction_removes_oldest_commit() {
        let mut table = ClientTable::new(2);

        table.commit_reply(100, make_reply_for(100, 1, 10));
        table.commit_reply(200, make_reply_for(200, 1, 20));
        table.commit_reply(300, make_reply_for(300, 1, 30));

        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
        assert_eq!(table.count(), 2);
    }

    #[test]
    fn eviction_is_deterministic_by_slot_index() {
        let mut table = ClientTable::new(2);

        table.commit_reply(100, make_reply_for(100, 1, 10));
        table.commit_reply(200, make_reply_for(200, 1, 10));
        table.commit_reply(300, make_reply_for(300, 1, 30));

        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    #[test]
    fn new_request_after_commit_is_new() {
        let mut table = ClientTable::new(10);
        table.commit_reply(1, make_reply(1, 10));

        assert!(matches!(table.check_request(1, 2), RequestStatus::New));
    }

    #[test]
    fn slot_reuse_after_eviction() {
        let mut table = ClientTable::new(1);

        table.commit_reply(100, make_reply_for(100, 1, 10));
        table.commit_reply(200, make_reply_for(200, 1, 20));

        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert_eq!(table.count(), 1);
    }

    #[test]
    #[should_panic(expected = "already has a pending waiter")]
    fn register_pending_twice_panics() {
        let mut table = ClientTable::new(10);
        let _n1 = table.register_pending(1, 1);
        let _n2 = table.register_pending(1, 1);
    }
}
