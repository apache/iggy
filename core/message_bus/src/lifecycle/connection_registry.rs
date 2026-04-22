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
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

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
/// Exposes the writer and reader [`JoinHandle`]s so the loser can
/// explicitly drain (or force-cancel on deadline) the orphan tasks
/// rather than relying on them to self-exit via `install_aborted`.
/// `compio::runtime::JoinHandle::drop` detaches; without this the loser
/// would leak the handles and a reader looping on `framing::read_message`
/// could outlive the race indefinitely on a half-open socket.
#[derive(Debug)]
pub struct RejectedRegistration {
    /// Producer side of the losing queue. Drop or `close()` to wake the
    /// writer task with `Closed`.
    pub sender: BusSender,
    /// Writer task spawned before `insert` was attempted.
    pub writer_handle: JoinHandle<()>,
    /// Reader task spawned before `insert` was attempted.
    pub reader_handle: JoinHandle<()>,
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
        }
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
    /// # Errors
    ///
    /// On duplicate `key` returns the rejected payload (sender +
    /// both [`JoinHandle`]s) so the caller can explicitly drain the
    /// orphan tasks instead of leaking them on drop.
    pub fn insert(
        &self,
        key: K,
        sender: BusSender,
        writer_handle: JoinHandle<()>,
        reader_handle: JoinHandle<()>,
    ) -> Result<(), RejectedRegistration> {
        let mut entries = self.entries.borrow_mut();
        if entries.contains_key(&key) {
            return Err(RejectedRegistration {
                sender,
                writer_handle,
                reader_handle,
            });
        }
        entries.insert(
            key,
            Entry {
                sender,
                writer_handle: Some(writer_handle),
                reader_handle: Some(reader_handle),
            },
        );
        Ok(())
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
    pub fn remove(&self, key: K) -> bool {
        self.entries.borrow_mut().remove(&key).is_some()
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
    #[allow(clippy::future_not_send)] // single-threaded compio
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
    #[allow(clippy::future_not_send)] // single-threaded compio
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
#[allow(clippy::future_not_send)] // single-threaded compio
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

#[allow(clippy::future_not_send)] // single-threaded compio
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
    len: std::cell::Cell<usize>,
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
            len: std::cell::Cell::new(0),
        }
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
    ) -> Result<(), RejectedRegistration> {
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[usize::from(key)];
        if slot.is_some() {
            return Err(RejectedRegistration {
                sender,
                writer_handle,
                reader_handle,
            });
        }
        *slot = Some(Entry {
            sender,
            writer_handle: Some(writer_handle),
            reader_handle: Some(reader_handle),
        });
        self.len.set(self.len.get() + 1);
        Ok(())
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

    /// See [`ConnectionRegistry::close_peer`].
    #[allow(clippy::future_not_send)] // single-threaded compio
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

    /// See [`ConnectionRegistry::drain`].
    #[allow(clippy::future_not_send)] // single-threaded compio
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

    #[compio::test]
    async fn insert_and_get_sender() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        let writer = spawn_dummy_writer(rx);
        let reader = spawn_dummy_reader(token);

        reg.insert(1u8, tx, writer, reader).expect("insert ok");
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

        reg.insert(1u8, tx1, w1, r1).expect("first insert");
        let err = reg.insert(1u8, tx2, w2, r2);
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
            reg.insert(k, tx, w, r).unwrap();
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
        reg.insert(1u8, tx, w, r).unwrap();

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
        reg.insert(7u8, tx, writer, reader).expect("insert ok");

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
        reg.insert(1u8, tx, writer, reader).unwrap();

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
            reg.insert(k, tx, writer, reader).unwrap();
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
}
