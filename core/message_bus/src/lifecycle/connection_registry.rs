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

//! Keyed connection registry shared by the client and replica TCP paths.
//!
//! Each entry owns:
//! - the `BusSender` used by `MessageBus::send_to_*` (a `try_send` into the
//!   per-peer bounded mpsc),
//! - the `JoinHandle` of the writer task (drains the mpsc and pushes batched
//!   `writev` to the wire),
//! - the `JoinHandle` of the reader task (read loop that hands inbound
//!   messages to the consumer's sync callback).
//!
//! Coordination is via the bus-wide [`ShutdownToken`]: triggering it makes
//! reader/writer tasks observe cancellation and exit. `drain` additionally
//! closes each `Sender` so writer tasks see the channel close and finish
//! any in-flight batch before exiting.
//!
//! [`ShutdownToken`]: crate::lifecycle::ShutdownToken

use compio::runtime::JoinHandle;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::Entry as HmEntry;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Opaque per-insert generation token.
///
/// Each successful [`ConnectionRegistry::insert`] / [`ReplicaRegistry::insert`]
/// mints a fresh monotonically increasing token and stores it alongside the
/// entry. Callers that want to release the slot from the post-loop of the
/// very install they spawned must use the `*_if_token_matches` variants so
/// a late-exiting predecessor cannot evict a later reinstall's slot.
///
/// Minted by a single-threaded `Cell<u64>` counter; single runtime = no
/// atomic needed. u64 means ~585 years of single-ns-per-install wraparound
/// headroom — treat as effectively unique within a process lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceToken(u64);

/// Minimum deadline reserved for reader-handle drain, in addition to the
/// writer drain's remaining budget.
///
/// A slow writer that consumes the shared deadline used to leave zero
/// runway for the reader handle, force-cancelling it even though reader
/// exit is usually immediate once the socket half-closes. Reserving a
/// small floor keeps the reader drain observable (`Clean` vs `Force`)
/// without meaningfully extending total shutdown time.
pub const READER_DRAIN_FLOOR: Duration = Duration::from_millis(250);

/// Payload type carried over every per-peer queue.
///
/// Consensus messages are `Frozen<MESSAGE_ALIGN>` by the time they hit
/// this queue: the dispatch layer freezes once and fan-out becomes a
/// refcount bump per target. The writer task reads `Frozen` out of the
/// queue and passes it straight to `write_vectored_all`, so no
/// conversion happens on the hot path.
pub type BusMessage = Frozen<MESSAGE_ALIGN>;

/// Producer side of a per-peer queue. Cloned out of the registry by
/// `send_to_*` and used with `try_send`.
pub type BusSender = async_channel::Sender<BusMessage>;

/// Consumer side of a per-peer queue. Owned by the writer task.
pub type BusReceiver = async_channel::Receiver<BusMessage>;

/// Rejected payload handed back to the caller when `insert` loses.
///
/// Exposes the writer and reader [`JoinHandle`]s plus the per-connection
/// [`Shutdown`] so the loser can explicitly drain (or force-cancel on
/// deadline) the orphan tasks rather than relying on them to self-exit via
/// `install_aborted`. `compio::runtime::JoinHandle::drop` detaches; without
/// this the loser would leak the handles and a reader looping on
/// `framing::read_message` could outlive the race indefinitely on a
/// half-open socket. Triggering the [`Shutdown`] wakes the reader off its
/// `io_uring` read SQE without waiting for peer EOF.
///
/// [`Shutdown`]: crate::lifecycle::Shutdown
#[derive(Debug)]
pub struct RejectedRegistration {
    /// Producer side of the losing queue. Drop or `close()` to wake the
    /// writer task with `Closed`.
    pub sender: BusSender,
    /// Writer task spawned before `insert` was attempted.
    pub writer_handle: JoinHandle<()>,
    /// Reader task spawned before `insert` was attempted.
    pub reader_handle: JoinHandle<()>,
    /// Per-connection shutdown the caller passed into the failed insert.
    /// Trigger to wake the reader without waiting for peer EOF.
    pub conn_shutdown: super::Shutdown,
}

/// Result of [`ConnectionRegistry::drain`] or [`crate::IggyMessageBus::shutdown`].
///
/// Counts are aggregate across all drained entries. `background_*` apply
/// only to bus-level background tasks (accept loops, reconnect periodic)
/// and stay zero when returned from [`ConnectionRegistry::drain`] directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DrainOutcome {
    /// Writer / reader tasks that exited within the deadline.
    pub clean: usize,
    /// Writer / reader tasks that had to be cancelled when the deadline
    /// elapsed.
    pub force: usize,
    /// Background tasks (accept loops, reconnect periodic) that exited
    /// within the deadline.
    pub background_clean: usize,
    /// Background tasks that had to be cancelled when the deadline
    /// elapsed.
    pub background_force: usize,
}

#[derive(Debug)]
struct Entry {
    sender: BusSender,
    writer_handle: Option<JoinHandle<()>>,
    reader_handle: Option<JoinHandle<()>>,
    /// Generation token minted by the registry on insert. Entries that
    /// must be released only by their originating install compare against
    /// this value; a stale-install cleanup presenting a different token is
    /// a no-op.
    token: InstanceToken,
    /// Per-connection shutdown captured at install time. Only kept alive
    /// here so its `Sender` survives until the entry is removed; dropping
    /// it earlier would close the broadcast channel and falsely wake
    /// readers waiting on `conn_token.wait()`. Triggered explicitly by
    /// [`drain_rejected_registration`](crate::installer) on insert race;
    /// implicitly dropped on entry removal.
    _conn_shutdown: super::Shutdown,
}

/// Map of live connections keyed by some transport-specific id.
///
/// For clients `K = u128` (the minted client id); for replicas `K = u8`
/// (the replica id carried in the Ping handshake).
#[derive(Debug)]
pub struct ConnectionRegistry<K>
where
    K: Eq + Hash + Copy + Debug + 'static,
{
    entries: RefCell<HashMap<K, Entry>>,
    next_token: Cell<u64>,
}

impl<K> Default for ConnectionRegistry<K>
where
    K: Eq + Hash + Copy + Debug + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> ConnectionRegistry<K>
where
    K: Eq + Hash + Copy + Debug + 'static,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RefCell::new(HashMap::new()),
            next_token: Cell::new(1),
        }
    }

    fn mint_token(&self) -> InstanceToken {
        let current = self.next_token.get();
        self.next_token.set(current.wrapping_add(1));
        InstanceToken(current)
    }

    #[must_use]
    pub fn contains(&self, key: K) -> bool {
        self.entries.borrow().contains_key(&key)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    /// Run `f` with the per-peer queue producer without cloning the `Sender`.
    ///
    /// `async_channel::Sender::try_send` takes `&self` so a borrow is
    /// sufficient; cloning would trigger an atomic RMW on the inner
    /// `Arc<State>` on every send.
    ///
    /// Scoping the borrow to `f` keeps the read guard on the stack for
    /// exactly the closure body. Any future edit that sneaks an `.await`
    /// between borrow and send becomes a compile error instead of a
    /// runtime `RefCell` panic on a concurrent `insert` / `remove` /
    /// `close_peer`.
    pub fn with_sender<R>(&self, key: K, f: impl FnOnce(&BusSender) -> R) -> Option<R> {
        let entries = self.entries.borrow();
        entries.get(&key).map(|entry| f(&entry.sender))
    }

    /// Register a new connection. Stores the producer side of the per-peer
    /// queue alongside the writer + reader task handles so graceful shutdown
    /// can drain everything.
    ///
    /// On success returns an [`InstanceToken`] the caller can later feed
    /// into [`remove_if_token_matches`](Self::remove_if_token_matches) /
    /// [`close_peer_if_token_matches`](Self::close_peer_if_token_matches)
    /// to fence stale-install cleanup from evicting a later reinstall.
    ///
    /// `conn_shutdown` is moved into the entry so its `Sender` outlives
    /// the connection: dropping it earlier would close the broadcast
    /// channel and falsely wake the reader's `select!`.
    ///
    /// # Errors
    ///
    /// On duplicate `key` returns the rejected payload (sender +
    /// both [`JoinHandle`]s + the per-connection [`Shutdown`]) so the
    /// caller can trigger the shutdown to wake the reader and explicitly
    /// drain the orphan tasks instead of leaking them on drop.
    ///
    /// [`Shutdown`]: crate::lifecycle::Shutdown
    pub fn insert(
        &self,
        key: K,
        sender: BusSender,
        writer_handle: JoinHandle<()>,
        reader_handle: JoinHandle<()>,
        conn_shutdown: super::Shutdown,
    ) -> Result<InstanceToken, RejectedRegistration> {
        let mut entries = self.entries.borrow_mut();
        if entries.contains_key(&key) {
            return Err(RejectedRegistration {
                sender,
                writer_handle,
                reader_handle,
                conn_shutdown,
            });
        }
        let token = self.mint_token();
        entries.insert(
            key,
            Entry {
                sender,
                writer_handle: Some(writer_handle),
                reader_handle: Some(reader_handle),
                token,
                _conn_shutdown: conn_shutdown,
            },
        );
        Ok(token)
    }

    /// Unregister a connection without awaiting its tasks.
    ///
    /// Returns `true` if an entry was removed. Dropping the entry drops the
    /// `Sender`, which makes the writer task observe `Closed` and exit. The
    /// reader task is independent and will exit on its own (read error) or
    /// when the bus shutdown token fires.
    ///
    /// Prefer [`close_peer`](Self::close_peer) when closing from the reader
    /// task: the explicit close-sender, await-writer sequence prevents a
    /// mid-writev cancellation from landing a truncated frame on the wire.
    ///
    /// Unfenced: callers that need generation-safety must use
    /// [`remove_if_token_matches`](Self::remove_if_token_matches).
    pub fn remove(&self, key: K) -> bool {
        self.entries.borrow_mut().remove(&key).is_some()
    }

    /// Token-fenced variant of [`remove`](Self::remove).
    ///
    /// Removes the entry only if its stored token equals `token`. Returns
    /// `true` when the removal applied. Used by install post-loops so a
    /// lagging predecessor cannot evict a newer reinstall's slot.
    pub fn remove_if_token_matches(&self, key: K, token: InstanceToken) -> bool {
        let mut entries = self.entries.borrow_mut();
        let HmEntry::Occupied(slot) = entries.entry(key) else {
            return false;
        };
        if slot.get().token != token {
            return false;
        }
        slot.remove();
        true
    }

    /// Close the entry keyed by `key` in the correct order: remove the
    /// entry from the registry, close its `BusSender` (so the writer task
    /// observes `Closed` and drains any in-flight batch cleanly), then
    /// await the writer handle up to `timeout`.
    ///
    /// The reader handle is dropped without awaiting because the typical
    /// caller of `close_peer` IS the reader task (self-remove path).
    /// Awaiting your own `JoinHandle` would deadlock.
    ///
    /// Returns a best-effort result: this is a lifecycle convenience, not
    /// a signal of a problem if the writer needed to be force-cancelled
    /// at the deadline.
    ///
    /// Unfenced: callers that need generation-safety must use
    /// [`close_peer_if_token_matches`](Self::close_peer_if_token_matches).
    #[allow(clippy::future_not_send)]
    pub async fn close_peer(&self, key: K, timeout: Duration) {
        let Some(mut entry) = self.entries.borrow_mut().remove(&key) else {
            return;
        };
        entry.sender.close();
        if let Some(writer_handle) = entry.writer_handle.take() {
            let _ = compio::time::timeout(timeout, writer_handle).await;
        }
        drop(entry.reader_handle);
    }

    /// Token-fenced variant of [`close_peer`](Self::close_peer).
    ///
    /// Closes the entry only if its stored token equals `token`. Returns
    /// without effect when the token does not match (stale-install
    /// cleanup races against a newer reinstall).
    #[allow(clippy::future_not_send)]
    pub async fn close_peer_if_token_matches(
        &self,
        key: K,
        token: InstanceToken,
        timeout: Duration,
    ) -> bool {
        let mut entry = {
            let mut entries = self.entries.borrow_mut();
            let HmEntry::Occupied(slot) = entries.entry(key) else {
                return false;
            };
            if slot.get().token != token {
                return false;
            }
            slot.remove()
        };
        entry.sender.close();
        if let Some(writer_handle) = entry.writer_handle.take() {
            let _ = compio::time::timeout(timeout, writer_handle).await;
        }
        drop(entry.reader_handle);
        true
    }

    /// Drain every entry, awaiting each task with a shared deadline.
    ///
    /// Order:
    /// 1. Take the entries out of the map (so concurrent inserts during
    ///    drain go to a fresh registry state - we do not block them).
    /// 2. Close each `Sender` so the writer task sees the channel close.
    ///    Combined with the bus shutdown token, both reader and writer
    ///    will exit cleanly.
    /// 3. Await both task handles per entry against the shared deadline.
    ///    A handle that does not finish in time is force-cancelled by drop.
    ///
    /// Entries are drained concurrently via a `FuturesUnordered`: total
    /// drain time is bounded by the slowest entry, not the sum across
    /// entries. Writer-before-reader sequencing is preserved inside each
    /// entry's future so a mid-writev cancellation cannot truncate a frame.
    #[allow(clippy::future_not_send)]
    pub async fn drain(&self, timeout: Duration) -> DrainOutcome {
        let drained: Vec<(K, Entry)> = self.entries.borrow_mut().drain().collect();
        drain_entries(drained, timeout).await
    }
}

/// Shared parallel-drain routine used by both [`ConnectionRegistry`] and
/// [`ReplicaRegistry`].
///
/// Callers take their entries out of storage in whatever way suits their
/// backing type and hand the pre-collected `Vec` to this helper. Each
/// entry's sender is closed, writer awaited, then reader awaited, all
/// concurrently across entries via `FuturesUnordered`.
#[allow(clippy::future_not_send)]
async fn drain_entries<K>(drained: Vec<(K, Entry)>, timeout: Duration) -> DrainOutcome
where
    K: Debug + 'static,
{
    use futures::stream::{FuturesUnordered, StreamExt};

    if drained.is_empty() {
        return DrainOutcome::default();
    }

    let deadline = Instant::now() + timeout;

    let mut pending: FuturesUnordered<_> = drained
        .into_iter()
        .map(|(key, mut entry)| async move {
            // Closing the sender unblocks the writer task's
            // `recv().await` with `Err(Closed)` so it exits even if
            // the shutdown token has not been triggered. Writer is
            // awaited before the reader handle is dropped so a
            // mid-writev cancellation cannot truncate a frame.
            entry.sender.close();
            // Writer gets the full shared deadline so a legitimately-
            // flushing `write_vectored_all` never loses budget to any
            // other entry in the same drain batch.
            let writer = drain_handle(entry.writer_handle.take(), deadline, &key).await;
            // Reader gets at least half of what is left after the writer
            // returns, or READER_DRAIN_FLOOR, whichever is larger. This
            // prevents a slow writer from force-cancelling the reader
            // even though reader exit is usually immediate on socket
            // half-close.
            let reader_deadline = {
                let now = Instant::now();
                let remaining = deadline.saturating_duration_since(now);
                let reader_budget = std::cmp::max(remaining / 2, READER_DRAIN_FLOOR);
                now + reader_budget
            };
            let reader = drain_handle(entry.reader_handle.take(), reader_deadline, &key).await;
            (writer, reader)
        })
        .collect();

    let mut clean = 0usize;
    let mut force = 0usize;
    let mut tally = |outcome: TaskOutcome| match outcome {
        TaskOutcome::None => {}
        TaskOutcome::Clean => clean += 1,
        TaskOutcome::Force => force += 1,
    };
    while let Some((writer, reader)) = pending.next().await {
        tally(writer);
        tally(reader);
    }

    DrainOutcome {
        clean,
        force,
        background_clean: 0,
        background_force: 0,
    }
}

#[derive(Debug)]
enum TaskOutcome {
    /// No handle to await (already taken).
    None,
    /// Task exited within the deadline.
    Clean,
    /// Task was force-cancelled at or past the deadline.
    Force,
}

#[allow(clippy::future_not_send)]
async fn drain_handle<K: Debug>(
    handle: Option<JoinHandle<()>>,
    deadline: Instant,
    key: &K,
) -> TaskOutcome {
    let Some(handle) = handle else {
        return TaskOutcome::None;
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        debug!(?key, "drain deadline reached, cancelling task");
        drop(handle);
        return TaskOutcome::Force;
    }
    if compio::time::timeout(remaining, handle).await.is_ok() {
        TaskOutcome::Clean
    } else {
        warn!(?key, "task did not exit within deadline");
        TaskOutcome::Force
    }
}

/// Connection registry specialised for the replica keyspace (`u8`).
///
/// The client registry uses `u128` client ids so a `HashMap` backing is
/// the right trade-off. Replicas are capped at 256 by the keyspace, and
/// in practice the cluster has 3-7 replicas, so a fixed
/// `[Option<Entry>; 256]` avoids every send-path lookup paying hash +
/// bucket indirection. Storage is ~10 KB per bus, paid once at
/// construction.
///
/// The API mirrors [`ConnectionRegistry`] exactly; `IggyMessageBus`
/// swaps the backing type transparently to every existing call site.
#[derive(Debug)]
pub struct ReplicaRegistry {
    slots: RefCell<[Option<Entry>; 256]>,
    len: Cell<usize>,
    next_token: Cell<u64>,
}

impl Default for ReplicaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            slots: RefCell::new(std::array::from_fn(|_| None)),
            len: Cell::new(0),
            next_token: Cell::new(1),
        }
    }

    fn mint_token(&self) -> InstanceToken {
        let current = self.next_token.get();
        self.next_token.set(current.wrapping_add(1));
        InstanceToken(current)
    }

    #[must_use]
    pub fn contains(&self, key: u8) -> bool {
        self.slots.borrow()[usize::from(key)].is_some()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len.get()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    /// See [`ConnectionRegistry::with_sender`].
    pub fn with_sender<R>(&self, key: u8, f: impl FnOnce(&BusSender) -> R) -> Option<R> {
        let slots = self.slots.borrow();
        slots[usize::from(key)]
            .as_ref()
            .map(|entry| f(&entry.sender))
    }

    /// See [`ConnectionRegistry::insert`].
    ///
    /// # Errors
    ///
    /// On duplicate `key` returns the rejected payload so the caller
    /// can drain orphan tasks instead of leaking them on drop.
    pub fn insert(
        &self,
        key: u8,
        sender: BusSender,
        writer_handle: JoinHandle<()>,
        reader_handle: JoinHandle<()>,
        conn_shutdown: super::Shutdown,
    ) -> Result<InstanceToken, RejectedRegistration> {
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[usize::from(key)];
        if slot.is_some() {
            return Err(RejectedRegistration {
                sender,
                writer_handle,
                reader_handle,
                conn_shutdown,
            });
        }
        let token = self.mint_token();
        *slot = Some(Entry {
            sender,
            writer_handle: Some(writer_handle),
            reader_handle: Some(reader_handle),
            token,
            _conn_shutdown: conn_shutdown,
        });
        self.len.set(self.len.get() + 1);
        Ok(token)
    }

    /// See [`ConnectionRegistry::remove`].
    pub fn remove(&self, key: u8) -> bool {
        if self.slots.borrow_mut()[usize::from(key)].take().is_some() {
            self.len.set(self.len.get() - 1);
            true
        } else {
            false
        }
    }

    /// See [`ConnectionRegistry::remove_if_token_matches`].
    pub fn remove_if_token_matches(&self, key: u8, token: InstanceToken) -> bool {
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[usize::from(key)];
        if !slot.as_ref().is_some_and(|e| e.token == token) {
            return false;
        }
        *slot = None;
        self.len.set(self.len.get() - 1);
        true
    }

    /// See [`ConnectionRegistry::close_peer`].
    #[allow(clippy::future_not_send)]
    pub async fn close_peer(&self, key: u8, timeout: Duration) {
        let mut entry = {
            let mut slots = self.slots.borrow_mut();
            let Some(entry) = slots[usize::from(key)].take() else {
                return;
            };
            self.len.set(self.len.get() - 1);
            entry
        };
        entry.sender.close();
        if let Some(writer_handle) = entry.writer_handle.take() {
            let _ = compio::time::timeout(timeout, writer_handle).await;
        }
        drop(entry.reader_handle);
    }

    /// See [`ConnectionRegistry::close_peer_if_token_matches`].
    ///
    /// # Panics
    ///
    /// Cannot panic in practice: the `expect` after `slot.take()` is
    /// guarded by a matching `slot.as_ref()` check under the same
    /// single-threaded `RefCell` borrow.
    #[allow(clippy::future_not_send)]
    pub async fn close_peer_if_token_matches(
        &self,
        key: u8,
        token: InstanceToken,
        timeout: Duration,
    ) -> bool {
        let mut entry = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[usize::from(key)];
            if !slot.as_ref().is_some_and(|e| e.token == token) {
                return false;
            }
            let removed_entry = slot
                .take()
                .expect("slot is Some: checked above under the same borrow");
            self.len.set(self.len.get() - 1);
            removed_entry
        };
        entry.sender.close();
        if let Some(writer_handle) = entry.writer_handle.take() {
            let _ = compio::time::timeout(timeout, writer_handle).await;
        }
        drop(entry.reader_handle);
        true
    }

    /// See [`ConnectionRegistry::drain`].
    #[allow(clippy::future_not_send)]
    pub async fn drain(&self, timeout: Duration) -> DrainOutcome {
        let drained: Vec<(u8, Entry)> = {
            let mut slots = self.slots.borrow_mut();
            let mut out = Vec::with_capacity(self.len.get());
            for (idx, slot) in slots.iter_mut().enumerate() {
                if let Some(entry) = slot.take() {
                    // Safe: index comes from iterating a 256-element array.
                    #[allow(clippy::cast_possible_truncation)]
                    out.push((idx as u8, entry));
                }
            }
            self.len.set(0);
            out
        };
        drain_entries(drained, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Shutdown;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};

    #[allow(clippy::cast_possible_truncation)]
    fn make_bus_msg() -> BusMessage {
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = Command2::Ping;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    fn spawn_dummy_writer(rx: BusReceiver) -> JoinHandle<()> {
        compio::runtime::spawn(async move {
            while let Ok(_msg) = rx.recv().await {
                // discard
            }
        })
    }

    fn spawn_dummy_reader(token: crate::lifecycle::ShutdownToken) -> JoinHandle<()> {
        compio::runtime::spawn(async move {
            token.wait().await;
        })
    }

    /// Throwaway per-connection [`Shutdown`] for tests that exercise
    /// `insert` but do not care about the reader-wake plumbing.
    fn dummy_conn_shutdown() -> Shutdown {
        let (s, _t) = Shutdown::new();
        s
    }

    #[compio::test]
    async fn insert_and_get_sender() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        let writer = spawn_dummy_writer(rx);
        let reader = spawn_dummy_reader(token);

        reg.insert(1u8, tx, writer, reader, dummy_conn_shutdown())
            .expect("insert ok");
        assert!(reg.contains(1u8));
        assert_eq!(reg.len(), 1);

        reg.with_sender(1u8, |sender| {
            sender.try_send(make_bus_msg()).expect("queue accepts msg");
        })
        .expect("sender present");
    }

    #[compio::test]
    async fn insert_duplicate_errors() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        let (tx1, rx1) = async_channel::bounded(8);
        let (tx2, rx2) = async_channel::bounded(8);
        let w1 = spawn_dummy_writer(rx1);
        let r1 = spawn_dummy_reader(token.clone());
        let w2 = spawn_dummy_writer(rx2);
        let r2 = spawn_dummy_reader(token);

        reg.insert(1u8, tx1, w1, r1, dummy_conn_shutdown())
            .expect("first insert");
        let err = reg.insert(1u8, tx2, w2, r2, dummy_conn_shutdown());
        assert!(err.is_err());
    }

    #[compio::test]
    async fn drain_after_shutdown_counts_both_tasks_per_entry() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (shutdown, token) = Shutdown::new();

        for k in 0..3u8 {
            let (tx, rx) = async_channel::bounded(8);
            let w = spawn_dummy_writer(rx);
            let r = spawn_dummy_reader(token.clone());
            reg.insert(k, tx, w, r, dummy_conn_shutdown()).unwrap();
        }

        shutdown.trigger();
        let outcome = reg.drain(Duration::from_secs(2)).await;
        // 3 entries * 2 tasks (writer + reader) each = 6 clean exits.
        assert_eq!(outcome.clean, 6);
        assert_eq!(outcome.force, 0);
        assert!(reg.is_empty());
    }

    #[compio::test]
    async fn drain_force_cancels_reader_that_refuses_to_exit() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, _token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        let w = spawn_dummy_writer(rx);
        // Reader ignores shutdown entirely.
        let r = compio::runtime::spawn(async move {
            loop {
                compio::time::sleep(Duration::from_secs(10)).await;
            }
        });
        reg.insert(1u8, tx, w, r, dummy_conn_shutdown()).unwrap();

        let outcome = reg.drain(Duration::from_millis(40)).await;
        // Writer exits because the sender is closed by drain. Reader is
        // force-cancelled.
        assert_eq!(outcome.clean, 1);
        assert_eq!(outcome.force, 1);
    }

    #[compio::test]
    async fn close_peer_closes_sender_awaits_writer() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        let writer = spawn_dummy_writer(rx);
        let reader = spawn_dummy_reader(token);
        reg.insert(7u8, tx, writer, reader, dummy_conn_shutdown())
            .expect("insert ok");

        assert!(reg.contains(7u8));
        reg.close_peer(7u8, Duration::from_secs(1)).await;
        assert!(!reg.contains(7u8));
    }

    #[compio::test]
    async fn close_peer_noop_on_missing_key() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.close_peer(42u8, Duration::from_millis(10)).await;
    }

    /// Writer consumes nearly the whole shared deadline; reader still
    /// gets at least `READER_DRAIN_FLOOR` of runway and exits cleanly.
    /// Before the split the reader would be force-cancelled with zero
    /// remaining time.
    #[compio::test]
    async fn drain_reader_keeps_floor_when_writer_slow() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (shutdown, token) = Shutdown::new();

        let (tx, rx) = async_channel::bounded::<BusMessage>(8);
        // Writer sleeps just under the shared deadline before returning.
        let writer = compio::runtime::spawn(async move {
            while rx.recv().await.is_ok() {}
            compio::time::sleep(Duration::from_millis(180)).await;
        });
        let reader = spawn_dummy_reader(token);
        reg.insert(1u8, tx, writer, reader, dummy_conn_shutdown())
            .unwrap();

        shutdown.trigger();
        // Shared deadline = 200 ms. Writer consumes ~180 ms, leaving
        // ~20 ms. Without the reader floor that would force the reader.
        let outcome = reg.drain(Duration::from_millis(200)).await;
        assert_eq!(outcome.force, 0, "reader should drain cleanly: {outcome:?}");
        assert_eq!(outcome.clean, 2);
    }

    /// Every entry's writer takes ~80 ms to exit (simulating a slow
    /// in-flight batch). With sequential drain that would be N * 80 ms.
    /// With parallel drain it should be bounded by the slowest entry.
    #[compio::test]
    async fn drain_runs_entries_concurrently() {
        const N: u8 = 10;
        const PER_ENTRY_LATENCY: Duration = Duration::from_millis(80);

        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (shutdown, token) = Shutdown::new();

        for k in 0..N {
            let (tx, rx) = async_channel::bounded::<BusMessage>(8);
            // Writer waits PER_ENTRY_LATENCY after observing channel
            // close before returning, mimicking a slow tail batch.
            let writer = compio::runtime::spawn(async move {
                while rx.recv().await.is_ok() {}
                compio::time::sleep(PER_ENTRY_LATENCY).await;
            });
            let reader = spawn_dummy_reader(token.clone());
            reg.insert(k, tx, writer, reader, dummy_conn_shutdown())
                .unwrap();
        }

        // Shutdown so readers (which wait on the token) exit cleanly.
        shutdown.trigger();

        let start = Instant::now();
        let outcome = reg.drain(Duration::from_secs(5)).await;
        let elapsed = start.elapsed();

        assert_eq!(outcome.clean, usize::from(N) * 2);
        assert_eq!(outcome.force, 0);
        // Sequential lower bound would be N * PER_ENTRY_LATENCY.
        // Allow 3x the single-entry latency as headroom for scheduling.
        let parallel_budget = PER_ENTRY_LATENCY * 3;
        assert!(
            elapsed < parallel_budget,
            "drain took {:?}, expected parallel < {:?} (serial would be ~{:?})",
            elapsed,
            parallel_budget,
            PER_ENTRY_LATENCY * u32::from(N),
        );
    }

    /// Each successful insert mints a distinct token.
    #[compio::test]
    async fn insert_mints_unique_tokens() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();

        let (tx1, rx1) = async_channel::bounded(8);
        let tok1 = reg
            .insert(
                1u8,
                tx1,
                spawn_dummy_writer(rx1),
                spawn_dummy_reader(token.clone()),
                dummy_conn_shutdown(),
            )
            .expect("first insert ok");

        assert!(reg.remove(1u8));

        let (tx2, rx2) = async_channel::bounded(8);
        let tok2 = reg
            .insert(
                1u8,
                tx2,
                spawn_dummy_writer(rx2),
                spawn_dummy_reader(token),
                dummy_conn_shutdown(),
            )
            .expect("second insert ok");

        assert_ne!(tok1, tok2, "reinsert after remove must mint a fresh token");
    }

    /// Sequential install -> remove -> reinstall: an old install's stale
    /// `remove_if_token_matches` call must NOT evict the new slot.
    #[compio::test]
    async fn stale_remove_does_not_evict_reinstall() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();

        let (tx_a, rx_a) = async_channel::bounded(8);
        let tok_a = reg
            .insert(
                1u8,
                tx_a,
                spawn_dummy_writer(rx_a),
                spawn_dummy_reader(token.clone()),
                dummy_conn_shutdown(),
            )
            .expect("install A ok");

        reg.remove(1u8);

        let (tx_b, rx_b) = async_channel::bounded(8);
        let tok_b = reg
            .insert(
                1u8,
                tx_b,
                spawn_dummy_writer(rx_b),
                spawn_dummy_reader(token),
                dummy_conn_shutdown(),
            )
            .expect("install B (reinstall) ok");

        // Late-exiting A presents its stale token. Must be a no-op.
        let evicted = reg.remove_if_token_matches(1u8, tok_a);
        assert!(!evicted, "stale token must not evict newer slot");
        assert!(reg.contains(1u8));

        // B's own cleanup releases correctly.
        assert!(reg.remove_if_token_matches(1u8, tok_b));
        assert!(!reg.contains(1u8));
    }

    /// Same invariant on the fixed-array replica registry.
    #[compio::test]
    async fn replica_stale_close_does_not_evict_reinstall() {
        let reg = ReplicaRegistry::new();
        let (_shutdown, token) = Shutdown::new();

        let (tx_a, rx_a) = async_channel::bounded(8);
        let tok_a = reg
            .insert(
                2u8,
                tx_a,
                spawn_dummy_writer(rx_a),
                spawn_dummy_reader(token.clone()),
                dummy_conn_shutdown(),
            )
            .expect("install A ok");

        reg.remove(2u8);

        let (tx_b, rx_b) = async_channel::bounded(8);
        let tok_b = reg
            .insert(
                2u8,
                tx_b,
                spawn_dummy_writer(rx_b),
                spawn_dummy_reader(token),
                dummy_conn_shutdown(),
            )
            .expect("install B ok");

        let closed = reg
            .close_peer_if_token_matches(2u8, tok_a, Duration::from_millis(100))
            .await;
        assert!(!closed, "stale token must not close newer slot");
        assert!(reg.contains(2u8));

        // New slot still owns the writer / reader pair.
        let closed = reg
            .close_peer_if_token_matches(2u8, tok_b, Duration::from_millis(100))
            .await;
        assert!(closed);
        assert!(!reg.contains(2u8));
    }
}
