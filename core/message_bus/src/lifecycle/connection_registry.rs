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
use futures::channel::oneshot;
use server_common::{MESSAGE_ALIGN, iobuf::Frozen};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::Entry as HmEntry;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};
use thiserror::Error;
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

/// Where a client reply is delivered.
///
/// `Socket` is the wire path taken by every TCP / WS / QUIC client: the
/// reply is pushed into the per-peer queue exactly as before. `InProcess`
/// reifies an in-process reply target keyed by request id so a caller
/// holding the matching [`oneshot::Receiver`] can await its reply without a
/// socket. Both ends are shard-0-local, hence a single-threaded oneshot.
#[derive(Debug)]
enum ReplyTarget {
    Socket(BusSender),
    InProcess {
        by_request: HashMap<u64, oneshot::Sender<BusMessage>>,
    },
}

impl ReplyTarget {
    /// Wire sender for socket targets; `None` for in-process targets.
    const fn as_socket(&self) -> Option<&BusSender> {
        match self {
            Self::Socket(sender) => Some(sender),
            Self::InProcess { .. } => None,
        }
    }
}

/// Outcome of routing a client reply through an [`Entry`]'s reply target.
///
/// `Delivered` is the socket fast path: `try_send` was attempted and its
/// result is carried through unchanged. `InProcess` hands the message back
/// so the caller can decode the request id and fire the matching oneshot
/// via [`ConnectionRegistry::fire_in_process`], keeping that decode off the
/// socket path. `NoSlot` means no entry exists for the key.
#[derive(Debug)]
#[must_use]
pub enum ReplyRoute {
    Delivered(Result<(), async_channel::TrySendError<BusMessage>>),
    InProcess(BusMessage),
    NoSlot(BusMessage),
}

/// Rejection reasons for [`ConnectionRegistry::install_reply_slot`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplySlotError {
    /// No registry entry exists for the key: the in-process entry was
    /// never installed or has already been torn down.
    #[error("no registry entry for this key")]
    EntryNotFound,
    /// The entry is a socket target. Socket replies go through the
    /// per-peer queue; a reply slot on such an entry could never fire.
    #[error("registry entry is a socket target")]
    NotInProcess,
    /// A waiter is already registered under this request id. Replacing it
    /// would drop the first waiter's sender and could deliver its reply
    /// to the wrong caller, so the second install is rejected instead.
    #[error("a reply slot for this request id is already installed")]
    DuplicateRequest,
}

/// Cancel-safety handle for an installed in-process reply slot.
///
/// Dropping the guard removes the slot unless the reply already fired
/// ([`ConnectionRegistry::fire_in_process`] consumes the slot on delivery,
/// making the drop a no-op) or the whole entry was replaced (fenced by the
/// entry's [`InstanceToken`], mirroring `remove_if_token_matches`). A
/// caller that times out or is cancelled therefore never leaks its oneshot
/// sender.
///
/// Contract: a request id must not be reused under the same key while a
/// previous guard for that id is still live; the stale guard's drop would
/// remove the new slot. Callers mint monotonically increasing request ids,
/// which rules this out.
#[must_use = "dropping the guard removes the reply slot"]
#[derive(Debug)]
pub struct ReplySlotGuard<'a, K>
where
    K: Eq + Hash + Copy + Debug + 'static,
{
    registry: &'a ConnectionRegistry<K>,
    key: K,
    request: u64,
    token: InstanceToken,
}

impl<K> Drop for ReplySlotGuard<'_, K>
where
    K: Eq + Hash + Copy + Debug + 'static,
{
    fn drop(&mut self) {
        let mut entries = self.registry.entries.borrow_mut();
        let Some(entry) = entries.get_mut(&self.key) else {
            return;
        };
        if entry.token != self.token {
            return;
        }
        let ReplyTarget::InProcess { by_request } = &mut entry.reply else {
            return;
        };
        by_request.remove(&self.request);
    }
}

#[derive(Debug)]
struct Entry {
    reply: ReplyTarget,
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
        entries
            .get(&key)
            .and_then(|entry| entry.reply.as_socket())
            .map(f)
    }

    /// Route `msg` to the entry's reply target, handing it back when no
    /// live target accepts it.
    ///
    /// Lets `send_to_client` skip the unconditional `Frozen::clone()` the
    /// `with_sender` shape forced. Socket targets move `msg` straight into
    /// `try_send`; in-process targets hand it back via
    /// [`ReplyRoute::InProcess`] so the caller can resolve the request id
    /// and fire the oneshot without decoding on the socket path.
    ///
    /// Returns:
    /// - [`ReplyRoute::Delivered`]: socket target; carries the `try_send`
    ///   result (`Full` / `Closed` errors carry `msg` back per
    ///   `async_channel`'s contract).
    /// - [`ReplyRoute::InProcess`]: in-process target; `msg` handed back.
    /// - [`ReplyRoute::NoSlot`]: no entry for `key`; `msg` handed back.
    pub fn try_send_or_return(&self, key: K, msg: BusMessage) -> ReplyRoute {
        let entries = self.entries.borrow();
        let Some(entry) = entries.get(&key) else {
            return ReplyRoute::NoSlot(msg);
        };
        match &entry.reply {
            ReplyTarget::Socket(sender) => ReplyRoute::Delivered(sender.try_send(msg)),
            ReplyTarget::InProcess { .. } => ReplyRoute::InProcess(msg),
        }
    }

    /// Fire the in-process reply registered under `request` for `key`.
    ///
    /// Companion to [`ReplyRoute::InProcess`]; the socket path never reaches
    /// here. Removes the matching oneshot and delivers `msg` on it.
    ///
    /// # Errors
    ///
    /// Hands `msg` back when `key` has no in-process target or no waiter is
    /// registered for `request`, so the caller surfaces the same not-found
    /// outcome as a missing socket.
    pub fn fire_in_process(&self, key: K, request: u64, msg: BusMessage) -> Result<(), BusMessage> {
        let mut entries = self.entries.borrow_mut();
        let Some(entry) = entries.get_mut(&key) else {
            return Err(msg);
        };
        let ReplyTarget::InProcess { by_request } = &mut entry.reply else {
            return Err(msg);
        };
        match by_request.remove(&request) {
            Some(reply_tx) => reply_tx.send(msg),
            None => Err(msg),
        }
    }

    /// Register a oneshot waiter for the reply to `request` on `key`'s
    /// in-process entry.
    ///
    /// The returned receiver resolves when `send_to_client` routes a reply
    /// carrying this request id (via [`Self::fire_in_process`]), or with
    /// `Canceled` when the entry is torn down first. The guard removes the
    /// slot on drop; see [`ReplySlotGuard`] for the exact semantics.
    ///
    /// # Errors
    ///
    /// [`ReplySlotError::EntryNotFound`] when `key` has no entry,
    /// [`ReplySlotError::NotInProcess`] when the entry is a socket target,
    /// [`ReplySlotError::DuplicateRequest`] when a waiter is already
    /// registered under `request`.
    pub fn install_reply_slot(
        &self,
        key: K,
        request: u64,
    ) -> Result<(ReplySlotGuard<'_, K>, oneshot::Receiver<BusMessage>), ReplySlotError> {
        let mut entries = self.entries.borrow_mut();
        let Some(entry) = entries.get_mut(&key) else {
            return Err(ReplySlotError::EntryNotFound);
        };
        let token = entry.token;
        let ReplyTarget::InProcess { by_request } = &mut entry.reply else {
            return Err(ReplySlotError::NotInProcess);
        };
        let HmEntry::Vacant(slot) = by_request.entry(request) else {
            return Err(ReplySlotError::DuplicateRequest);
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        slot.insert(reply_tx);
        Ok((
            ReplySlotGuard {
                registry: self,
                key,
                request,
                token,
            },
            reply_rx,
        ))
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
                reply: ReplyTarget::Socket(sender),
                writer_handle: Some(writer_handle),
                reader_handle: Some(reader_handle),
                token,
                _conn_shutdown: conn_shutdown,
            },
        );
        Ok(token)
    }

    /// Register an in-process reply target for `key`.
    ///
    /// In-process peers have no socket and no reader / writer tasks:
    /// replies routed to `key` resolve request-keyed oneshots installed
    /// via [`Self::install_reply_slot`]. Teardown goes through
    /// [`Self::remove`] / [`Self::remove_if_token_matches`]; dropping the
    /// entry drops every pending sender, so all outstanding receivers
    /// resolve `Canceled`.
    ///
    /// Returns `None` when `key` is already registered (socket or
    /// in-process); the caller must tear the existing entry down first.
    pub fn insert_in_process(&self, key: K) -> Option<InstanceToken> {
        let mut entries = self.entries.borrow_mut();
        let HmEntry::Vacant(slot) = entries.entry(key) else {
            return None;
        };
        let token = self.mint_token();
        // Nothing ever waits on this shutdown (no reader to wake); it
        // exists only because every entry owns one.
        let (conn_shutdown, _) = super::Shutdown::new();
        slot.insert(Entry {
            reply: ReplyTarget::InProcess {
                by_request: HashMap::new(),
            },
            writer_handle: None,
            reader_handle: None,
            token,
            _conn_shutdown: conn_shutdown,
        });
        Some(token)
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
        // Dropping the Entry drops `_conn_shutdown`; see its rustdoc on
        // `Entry` for why that wakes the reader and closes the channel.
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
        if let Some(sender) = entry.reply.as_socket() {
            sender.close();
        }
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
        if let Some(sender) = entry.reply.as_socket() {
            sender.close();
        }
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
            if let Some(sender) = entry.reply.as_socket() {
                sender.close();
            }
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
/// bucket indirection. Storage is on the order of tens of KB per bus,
/// paid once at construction.
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
            .and_then(|entry| entry.reply.as_socket())
            .map(f)
    }

    /// Try to send `msg` to the replica socket registered under `key`, handing
    /// `msg` back as `Err` when no slot is present.
    ///
    /// Replicas only ever register socket reply targets, so unlike
    /// [`ConnectionRegistry::try_send_or_return`] (which returns a [`ReplyRoute`]
    /// also covering the in-process case) this stays a flat
    /// `Result<try-send outcome, BusMessage>`.
    ///
    /// # Errors
    ///
    /// `Err(msg)` when `key` has no registered slot. The inner `Err` carries a
    /// `try_send` failure (`Full` / `Closed`), which returns `msg` per
    /// `async_channel`'s contract.
    pub fn try_send_or_return(
        &self,
        key: u8,
        msg: BusMessage,
    ) -> Result<Result<(), async_channel::TrySendError<BusMessage>>, BusMessage> {
        let slots = self.slots.borrow();
        match slots[usize::from(key)]
            .as_ref()
            .and_then(|entry| entry.reply.as_socket())
        {
            Some(sender) => Ok(sender.try_send(msg)),
            None => Err(msg),
        }
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
            reply: ReplyTarget::Socket(sender),
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
        // Dropping the Entry drops `_conn_shutdown`; see its rustdoc on
        // `Entry` for why that wakes the reader and closes the channel.
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
        if let Some(sender) = entry.reply.as_socket() {
            sender.close();
        }
        if let Some(writer_handle) = entry.writer_handle.take() {
            let _ = compio::time::timeout(timeout, writer_handle).await;
        }
        drop(entry.reader_handle);
    }

    /// See [`ConnectionRegistry::close_peer_if_token_matches`].
    ///
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
            let Some(removed_entry) = slot.take_if(|e| e.token == token) else {
                return false;
            };
            self.len.set(self.len.get() - 1);
            removed_entry
        };
        if let Some(sender) = entry.reply.as_socket() {
            sender.close();
        }
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
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE};
    use server_common::Message;

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

    /// Fast-path: slot present, queue accepts. Verifies the consuming
    /// `try_send_or_return` shape replaces `with_sender(_, |s| s.try_send(msg.clone()))`
    /// without dropping behaviour.
    #[compio::test]
    async fn try_send_or_return_succeeds_when_slot_present() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        let writer = spawn_dummy_writer(rx.clone());
        let reader = spawn_dummy_reader(token);
        reg.insert(1u8, tx, writer, reader, dummy_conn_shutdown())
            .expect("insert ok");

        let outer = reg.try_send_or_return(1u8, make_bus_msg());
        assert!(matches!(outer, ReplyRoute::Delivered(Ok(()))));
        // Receiver drains the message; closing the writer's rx end via
        // close_peer is not necessary for this assertion.
        let received = rx.recv().await.expect("queue had message");
        assert_eq!(received.len(), HEADER_SIZE);
    }

    /// No slot for `key`: the message is handed back to the caller so the
    /// slow path (or no-op for clients) can decide what to do.
    #[compio::test]
    async fn try_send_or_return_returns_msg_when_slot_missing() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let msg = make_bus_msg();
        let original_len = msg.len();

        let outcome = reg.try_send_or_return(99u8, msg);
        let ReplyRoute::NoSlot(returned) = outcome else {
            panic!("missing slot should return msg");
        };
        assert_eq!(returned.len(), original_len);
    }

    /// Slot present but queue full: inner `Err(TrySendError::Full(msg))` so
    /// the caller can map to `SendError::Backpressure`.
    #[compio::test]
    async fn try_send_or_return_reports_full_when_queue_at_capacity() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        let (_shutdown, token) = Shutdown::new();
        // Capacity-1 queue, no draining receiver.
        let (tx, rx) = async_channel::bounded(1);
        // Spawn a no-op task that simply holds the receiver alive without
        // recv-ing, so the queue stays full.
        let writer = compio::runtime::spawn(async move {
            let _keep = rx;
            std::future::pending::<()>().await;
        });
        let reader = spawn_dummy_reader(token);
        reg.insert(1u8, tx, writer, reader, dummy_conn_shutdown())
            .expect("insert ok");

        // Saturate the queue.
        assert!(matches!(
            reg.try_send_or_return(1u8, make_bus_msg()),
            ReplyRoute::Delivered(Ok(()))
        ));
        // Second send: slot present, queue full.
        let outcome = reg.try_send_or_return(1u8, make_bus_msg());
        assert!(matches!(
            outcome,
            ReplyRoute::Delivered(Err(async_channel::TrySendError::Full(_)))
        ));
    }

    /// Mirror of `try_send_or_return_returns_msg_when_slot_missing` for the
    /// fixed-array `ReplicaRegistry`. Guards the slow-path forward in
    /// `send_to_replica` against silent payload loss when no slot exists.
    #[compio::test]
    async fn replica_try_send_or_return_returns_msg_when_slot_missing() {
        let reg = ReplicaRegistry::new();
        let msg = make_bus_msg();
        let original_len = msg.len();

        let outcome = reg.try_send_or_return(7u8, msg);
        let returned = outcome.expect_err("missing slot should return msg");
        assert_eq!(returned.len(), original_len);
    }

    /// End-to-end registry seam: install entry + slot, route, fire, await.
    #[compio::test]
    async fn in_process_slot_receives_fired_reply() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");

        let (guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");
        let msg = match reg.try_send_or_return(1u8, make_bus_msg()) {
            ReplyRoute::InProcess(msg) => msg,
            other => panic!("expected InProcess route, got {other:?}"),
        };
        reg.fire_in_process(1u8, 42, msg).expect("waiter present");

        let received = reply_rx.await.expect("reply delivered");
        assert_eq!(received.len(), HEADER_SIZE);
        drop(guard);
        assert!(reg.contains(1u8), "guard drop must not remove the entry");
    }

    #[compio::test]
    async fn insert_in_process_duplicate_key_returns_none() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        assert!(reg.insert_in_process(1u8).is_some());
        assert!(reg.insert_in_process(1u8).is_none());
        assert_eq!(reg.len(), 1);
    }

    #[compio::test]
    async fn install_reply_slot_rejects_missing_and_socket_entries() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        assert_eq!(
            reg.install_reply_slot(1u8, 1).unwrap_err(),
            ReplySlotError::EntryNotFound
        );

        let (_shutdown, token) = Shutdown::new();
        let (tx, rx) = async_channel::bounded(8);
        reg.insert(
            1u8,
            tx,
            spawn_dummy_writer(rx),
            spawn_dummy_reader(token),
            dummy_conn_shutdown(),
        )
        .expect("socket insert ok");
        assert_eq!(
            reg.install_reply_slot(1u8, 1).unwrap_err(),
            ReplySlotError::NotInProcess
        );
    }

    /// Second install under a live request id must be rejected, and the
    /// first waiter must stay wired (the reply still reaches it).
    #[compio::test]
    async fn install_reply_slot_rejects_duplicate_request() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");

        let (_guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("first install");
        assert_eq!(
            reg.install_reply_slot(1u8, 42).unwrap_err(),
            ReplySlotError::DuplicateRequest
        );

        reg.fire_in_process(1u8, 42, make_bus_msg())
            .expect("first waiter still registered");
        assert_eq!(reply_rx.await.expect("reply delivered").len(), HEADER_SIZE);
    }

    /// Reply for a request id nobody waits on: handed back, no panic, and
    /// the unrelated slot stays registered.
    #[compio::test]
    async fn fire_in_process_unknown_request_returns_msg() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");
        let (_guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");

        let msg = make_bus_msg();
        let original_len = msg.len();
        let returned = reg
            .fire_in_process(1u8, 7, msg)
            .expect_err("no waiter for request 7");
        assert_eq!(returned.len(), original_len);

        reg.fire_in_process(1u8, 42, make_bus_msg())
            .expect("slot 42 untouched");
        assert_eq!(reply_rx.await.expect("reply delivered").len(), HEADER_SIZE);
    }

    /// Timeout / cancellation path: dropping the guard removes the slot,
    /// cancels the receiver, and a later reply is shed as no-waiter.
    #[compio::test]
    async fn dropped_guard_removes_slot_and_cancels_receiver() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");

        let (guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");
        drop(guard);

        assert!(reply_rx.await.is_err(), "sender dropped with the slot");
        assert!(
            reg.fire_in_process(1u8, 42, make_bus_msg()).is_err(),
            "late reply must be handed back, not delivered"
        );
        let (_guard, _reply_rx) = reg
            .install_reply_slot(1u8, 42)
            .expect("request id reusable after guard drop");
    }

    /// After the reply fired the slot is gone; the guard's drop is a no-op
    /// and the request id becomes reusable.
    #[compio::test]
    async fn guard_drop_is_noop_after_reply_fired() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");

        let (guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");
        reg.fire_in_process(1u8, 42, make_bus_msg())
            .expect("waiter present");
        drop(guard);

        assert_eq!(reply_rx.await.expect("reply delivered").len(), HEADER_SIZE);
        let (_guard, _reply_rx) = reg
            .install_reply_slot(1u8, 42)
            .expect("request id reusable after fire + drop");
    }

    /// Entry teardown with a pending waiter: the receiver resolves
    /// `Canceled` instead of hanging, and the stale guard's later drop
    /// must not disturb a reinstalled entry's slot (token fence).
    #[compio::test]
    async fn entry_teardown_cancels_pending_receiver_and_fences_stale_guard() {
        let reg: ConnectionRegistry<u8> = ConnectionRegistry::new();
        reg.insert_in_process(1u8).expect("fresh key");
        let (stale_guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");

        assert!(reg.remove(1u8));
        assert!(reply_rx.await.is_err(), "teardown cancels pending waiter");

        reg.insert_in_process(1u8)
            .expect("reinstall after teardown");
        let (_guard, reply_rx) = reg.install_reply_slot(1u8, 42).expect("slot installs");
        drop(stale_guard);
        reg.fire_in_process(1u8, 42, make_bus_msg())
            .expect("stale guard must not evict the new slot");
        assert_eq!(reply_rx.await.expect("reply delivered").len(), HEADER_SIZE);
    }
}
