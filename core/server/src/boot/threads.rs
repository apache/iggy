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

//! Shard OS threads: thread entry, pin, join, and the shutdown plumbing.

use crate::boot::handoff::{BootstrapBarrier, MetadataHandoff};
use crate::boot::shard_main;
use crate::boot::topology::RosterCells;
use crate::server_error::{ServerError, ShardJoinFailure, ShardJoinFailureKind};
use crate::shard_allocator::{ShardAllocator, ShardInfo};
use compio::runtime::ResumeUnwind;
use configs::server::ServerConfig;
use configs::sharding::{
    INBOX_CAPACITY_MAX, RECONCILE_PERIODIC_INTERVAL_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX,
    SHUTDOWN_JOIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX,
};
use message_bus::{IggyMessageBus, ReplicaOwnerTable};
use metadata::AppliedFrontier;
use partitions::FatalCommit;
use server_common::executor::create_shard_executor;
use shard::metrics::ShardMetrics;
use shard::{Receiver as ShardReceiver, Sender, ShardFrame, TaggedSender};
use std::backtrace::Backtrace;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};
use std::{panic, thread};
use tracing::{error, info, warn};

/// Result of a multi-shard bootstrap.
///
/// Carries the cross-thread shutdown flag, one OS-thread `JoinHandle`
/// per shard, and the first panic `install_panic_hook` recorded. The
/// caller flips the flag via [`Self::install_ctrlc_handler`] and then
/// drains every shard via [`Self::join_all`], bounded by the shared
/// `ShutdownDeadline` (`system.sharding.shutdown_join_timeout`).
pub struct ShardHandles {
    pub(in crate::boot) shutdown_flag: Arc<AtomicBool>,
    pub(in crate::boot) shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)>,
    pub(in crate::boot) deadline: Arc<ShutdownDeadline>,
    pub(in crate::boot) first_panic: Arc<OnceLock<String>>,
}

impl ShardHandles {
    /// Install a SIGINT/Ctrl-C handler that flips the shutdown flag on
    /// the first signal. A second signal is logged but otherwise
    /// ignored so an in-flight WAL fsync or replica drain runs to
    /// completion.
    ///
    /// # Errors
    ///
    /// Returns the underlying `ctrlc::Error` if the handler cannot be
    /// installed (typically because another handler already owns the
    /// signal).
    pub fn install_ctrlc_handler(&self) -> Result<(), ctrlc::Error> {
        let flag = Arc::clone(&self.shutdown_flag);
        ctrlc::set_handler(move || {
            if flag.swap(true, Ordering::Relaxed) {
                // Second Ctrl-C: leave the shutdown machinery to drain.
                // Refusing to abort here keeps the WAL fsync / replica
                // drain from being interrupted mid-frame.
                warn!("second Ctrl-C ignored; server is already shutting down");
            } else {
                info!("Ctrl-C received; signalling server shutdown");
            }
        })
    }

    /// Drain every shard thread. This is the main thread's park for the
    /// server's whole lifetime, so shards are awaited WITHOUT any time
    /// bound while the server runs; the `shutdown_join_timeout` clock
    /// only starts once the cross-thread shutdown flag flips (Ctrl-C or
    /// a shard failure). Each shard's outcome is logged (`info` on clean
    /// exit, `error` on Err, panic, or wedge). If any shard failed,
    /// returns every failure together as
    /// [`ServerError::ShardJoinFailures`] so the operator sees the
    /// full set rather than just the first.
    ///
    /// A shard whose thread is still running when the post-shutdown
    /// deadline passes is abandoned (its `JoinHandle` dropped, the OS
    /// thread left to die with the process) and reported as
    /// [`ShardJoinFailureKind::Wedged`]: a wedged pump or listener must
    /// not block process exit forever. Shard 0 gets the peer-wait floor
    /// as grace on top, because its `PeerExitWait` is allowed to hold the
    /// metadata writer that long past a spent budget.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::ShardJoinFailures`] if any shard
    /// returned a `Result::Err`, panicked, or wedged past the deadline.
    /// The variant carries every per-shard failure in shard-id order so
    /// the caller does not need to read the trace log to discover
    /// late-failing shards. Returns [`ServerError::Panicked`] when every
    /// thread exited `Ok` but the panic hook recorded a panic: a task
    /// compio's `spawn` caught, which no thread result can carry.
    pub fn join_all(self) -> Result<(), ServerError> {
        let mut failures: Vec<ShardJoinFailure> = Vec::new();
        // Shards run thread-per-core with compio's blocking fallback pool
        // disabled, so an io_uring opcode the kernel lacks aborts every shard
        // with the same panic. Surface the actionable diagnostic once.
        let mut io_uring_diagnostic_shown = false;
        for (shard_id, handle) in self.shard_threads {
            // Shard 0's `PeerExitWait` is allowed to overrun the budget by
            // its floor, so its join has to tolerate the same overrun or a
            // shutdown that correctly held the writer for a slow peer reports
            // the shard that honoured the fence as wedged.
            let grace = if shard_id == 0 {
                self.deadline.peer_wait_floor
            } else {
                Duration::ZERO
            };
            let waited = self.deadline.budget.saturating_add(grace);
            let Some(joined) =
                join_until_shutdown_deadline(handle, &self.shutdown_flag, &self.deadline, grace)
            else {
                error!(
                    shard_id,
                    ?waited,
                    "shard thread still running at the shutdown join deadline; abandoning it"
                );
                failures.push(ShardJoinFailure {
                    shard_id,
                    kind: ShardJoinFailureKind::Wedged { waited },
                });
                continue;
            };
            match joined {
                Ok(Ok(())) => {
                    info!(shard_id, "shard thread exited cleanly");
                }
                Ok(Err(error)) => {
                    error!(shard_id, error = %error, "shard thread returned error");
                    failures.push(ShardJoinFailure {
                        shard_id,
                        kind: ShardJoinFailureKind::Error(Box::new(error)),
                    });
                }
                Err(panic_payload) => {
                    let message = panic_payload_to_string(&*panic_payload);
                    error!(shard_id, message = %message, "shard thread panicked");
                    if !io_uring_diagnostic_shown
                        && message
                            .contains(server_common::diagnostics::ASYNCIFY_POOL_DISABLED_PANIC_MSG)
                    {
                        server_common::diagnostics::print_incomplete_io_uring_ops_info();
                        io_uring_diagnostic_shown = true;
                    }
                    failures.push(ShardJoinFailure {
                        shard_id,
                        kind: ShardJoinFailureKind::Panic { message },
                    });
                }
            }
        }
        if !failures.is_empty() {
            return Err(ServerError::ShardJoinFailures { failures });
        }
        self.first_panic.get().map_or_else(
            || Ok(()),
            |description| {
                Err(ServerError::Panicked {
                    description: description.clone(),
                })
            },
        )
    }
}

/// Install the process-wide panic hook and return the slot it records
/// the first panic into.
///
/// The hook runs on the panicking thread before the unwind, so it sees
/// every panic on a shard: the thread body, whose unwind
/// [`run_shard_thread`] already turns into a join failure, and the tasks
/// compio's `spawn` catches, which nothing observes while the server
/// runs (a dead listener or connection task leaves every thread exiting
/// `Ok`). It logs the panic with its backtrace, records the first one for
/// [`ShardHandles::join_all`] to fail the exit on, and flips the shutdown
/// flag so every shard drains instead of the half-alive state one dead
/// task leaves behind. The previous hook still runs after it, so stderr
/// keeps the standard panic line.
pub(in crate::boot) fn install_panic_hook(shutdown_flag: Arc<AtomicBool>) -> Arc<OnceLock<String>> {
    let first_panic = Arc::new(OnceLock::new());
    let recorded = Arc::clone(&first_panic);
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let current = thread::current();
        let thread_name = current.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map_or_else(|| "<unknown>".to_string(), ToString::to_string);
        let message = panic_payload_to_string(info.payload());
        let backtrace = Backtrace::force_capture();
        error!(thread = thread_name, location = %location, backtrace = %backtrace, "{message}");
        let _ = recorded.set(format!(
            "thread '{thread_name}' panicked at {location}: {message}"
        ));
        shutdown_flag.store(true, Ordering::Relaxed);
        previous_hook(info);
    }));
    first_panic
}

/// Poll cadence for the bounded shard joins. Coarse enough to cost
/// nothing during a normal drain, fine enough that exit latency past
/// the last shard's return stays imperceptible.
const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Join `handle`, waiting indefinitely while the server runs. The
/// budget clock starts only when `shutdown_flag` is observed set (arming
/// the shared [`ShutdownDeadline`], so every shard join and shard 0's
/// peer wait drain under ONE budget); a running server parked here for
/// hours must never be mistaken for a wedged shard. `grace` extends this
/// caller's share of the budget by an overrun the joined thread is allowed
/// (shard 0's peer-wait floor). `None` means the thread was still running
/// at the post-shutdown deadline and the handle was dropped (the OS thread
/// keeps running detached; process exit reaps it). `JoinHandle` has no
/// timed join, so this polls `is_finished` at [`JOIN_POLL_INTERVAL`]; the
/// closing `join()` on a finished thread returns immediately.
fn join_until_shutdown_deadline(
    handle: thread::JoinHandle<Result<(), ServerError>>,
    shutdown_flag: &AtomicBool,
    deadline: &ShutdownDeadline,
    grace: Duration,
) -> Option<thread::Result<Result<(), ServerError>>> {
    while !handle.is_finished() {
        if shutdown_flag.load(Ordering::Relaxed) && deadline.remaining_with_grace(grace).is_zero() {
            return None;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
    Some(handle.join())
}

/// Best-effort extraction of the panic message from a
/// `Box<dyn Any + Send>` returned by `JoinHandle::join`. Tries the two
/// payload shapes the standard library guarantees (`&'static str` and
/// `String`) and falls back to a placeholder so the panic still surfaces
/// in the error chain.
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<panic payload not String/&str>".to_string()
}

/// Joins survivor shard threads after a partial-spawn failure, bounded
/// by the same `shutdown_join_timeout` budget as the normal exit path.
///
/// Polls every survivor's `is_finished` in one loop instead of spawning
/// per-survivor joiner threads: the likely OS state on this path is
/// `pthread_create` EAGAIN (the parent spawn just failed with it), so
/// nothing here may create threads, and polling drains all survivors in
/// parallel anyway. A survivor still running at the deadline is
/// abandoned with an error log so the failed bootstrap can surface its
/// spawn error instead of hanging on a wedged shard.
pub(in crate::boot) fn join_partial_shard_survivors(
    shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)>,
    deadline: &ShutdownDeadline,
) {
    let mut remaining = shard_threads;
    loop {
        let mut still_running = Vec::with_capacity(remaining.len());
        for (shard_id, survivor) in remaining {
            if survivor.is_finished() {
                let _ = survivor.join();
                info!(shard_id, "survivor shard thread drained");
            } else {
                still_running.push((shard_id, survivor));
            }
        }
        remaining = still_running;
        if remaining.is_empty() || deadline.remaining().is_zero() {
            break;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
    for (shard_id, _survivor) in remaining {
        error!(
            shard_id,
            waited = ?deadline.budget,
            "survivor shard thread still running at the shutdown join deadline; abandoning it"
        );
    }
}

/// Flips the cross-thread shutdown flag on `Drop` unless disarmed.
///
/// A shard thread that exits via an error `?` or a panic unwind would
/// otherwise leave sibling shards parked forever on `bus.token().wait()`:
/// their watchdogs never observe the flag and the bus has no
/// `Drop`-triggered shutdown. Arming this for the whole thread body makes
/// every non-clean exit drive sibling-shard teardown. Disarmed only on a
/// clean `Ok(())`.
struct ShutdownOnDrop {
    flag: Arc<AtomicBool>,
    armed: bool,
}

impl ShutdownOnDrop {
    const fn new(flag: Arc<AtomicBool>) -> Self {
        Self { flag, armed: true }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Relaxed);
        }
    }
}

/// The single post-shutdown budget, shared by the main thread's shard
/// joins and shard 0's peer wait.
///
/// Both waits are bounded by `system.sharding.shutdown_join_timeout` and
/// they NEST: shard 0 cannot start waiting for its peers until its own
/// drain returned, which is already inside the join budget. Arming one
/// instant on first use, whichever wait gets there first, keeps the two
/// inside one deadline instead of stacking two full budgets, so a
/// correct shutdown cannot report shard 0 as wedged.
///
/// The two waits enforce different things, so the budget is shared but not
/// fungible: the join bounds LIVENESS (process exit must not hang on a
/// wedged shard) while the peer wait enforces SAFETY (the metadata writer
/// must outlive every reader). `peer_wait_floor` is what keeps an exhausted
/// join budget from cancelling the safety fence, and the joins in turn
/// tolerate that floor as grace (see [`ShardHandles::join_all`]).
pub(in crate::boot) struct ShutdownDeadline {
    armed: OnceLock<Instant>,
    budget: Duration,
    peer_wait_floor: Duration,
}

impl ShutdownDeadline {
    pub(in crate::boot) const fn new(budget: Duration, peer_wait_floor: Duration) -> Self {
        Self {
            armed: OnceLock::new(),
            budget,
            peer_wait_floor,
        }
    }

    /// Time left in the shared budget plus `grace`, arming it on the first
    /// call. Callers must only reach this once the shutdown flag is set: a
    /// running server would otherwise start the clock.
    fn remaining_with_grace(&self, grace: Duration) -> Duration {
        self.armed
            .get_or_init(|| Instant::now() + self.budget)
            .checked_add(grace)
            .map_or(Duration::MAX, |limit| {
                limit.saturating_duration_since(Instant::now())
            })
    }

    fn remaining(&self) -> Duration {
        self.remaining_with_grace(Duration::ZERO)
    }

    /// How long shard 0 may block on its peers: what is left of the shared
    /// budget, floored at a peer's whole exit (one poll interval to observe the
    /// shutdown flag plus one drain budget) so a join that already spent the
    /// budget cannot release the metadata writer while a peer still reads
    /// through it. Only [`PeerExitWait::drop`]'s panic arm skips the fence.
    fn remaining_for_peer_wait(&self) -> Duration {
        self.remaining().max(self.peer_wait_floor)
    }
}

/// Peer shards still running: each [`PeerExitGuard`] counts one out,
/// [`PeerExitWait`] blocks shard 0 until the count is zero.
///
/// Shard 0 owns the metadata state machine's only write handle and every
/// peer reads through handles that stop working the moment it drops, so
/// the shard that owns the writer must outlive every reader on every exit
/// but one: [`PeerExitWait::drop`] skips the wait while shard 0 is
/// panicking, because parking an unwinding thread on a `Condvar` inside
/// `runtime.block_on` would stall the `io_uring` driver, and a panic in
/// shard 0's own pump can be exactly what the peers are blocked on. A
/// peer mid-read then panics too; the first panic is already recorded and
/// the process is going down either way.
pub(in crate::boot) struct PeerExitCountdown {
    running: Mutex<usize>,
    all_exited: Condvar,
}

impl PeerExitCountdown {
    pub(in crate::boot) const fn new(peers: usize) -> Self {
        Self {
            running: Mutex::new(peers),
            all_exited: Condvar::new(),
        }
    }

    /// Block until every peer has counted itself out or `timeout` elapses.
    /// `Err` carries the number of peers still running at the deadline.
    fn wait(&self, timeout: Duration) -> Result<(), usize> {
        let (guard, _) = self
            .all_exited
            .wait_timeout_while(
                self.running.lock().unwrap_or_else(PoisonError::into_inner),
                timeout,
                |running| *running > 0,
            )
            .unwrap_or_else(PoisonError::into_inner);
        let running = *guard;
        drop(guard);
        if running == 0 { Ok(()) } else { Err(running) }
    }

    fn peer_exited(&self) {
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        *running = running.saturating_sub(1);
        if *running == 0 {
            self.all_exited.notify_all();
        }
    }

    /// Count out peers that never spawned. The countdown is sized before the
    /// spawn loop, so a failed `thread::Builder::spawn` leaves peers whose
    /// [`PeerExitGuard`] will never exist: without this shard 0 waits out its
    /// whole join budget for threads that were never there.
    pub(in crate::boot) fn peers_never_spawned(&self, count: usize) {
        for _ in 0..count {
            self.peer_exited();
        }
    }
}

/// Counts one peer shard out of the [`PeerExitCountdown`] on drop.
///
/// Held by `run_shard_thread` from before the runtime exists, so it drops
/// after the runtime and its tasks are gone: past that point nothing on
/// the peer's thread can still read shard 0's metadata. Drop runs on every
/// exit path, the error `?` returns and panic unwinds included.
struct PeerExitGuard {
    countdown: Arc<PeerExitCountdown>,
}

impl PeerExitGuard {
    const fn new(countdown: Arc<PeerExitCountdown>) -> Self {
        Self { countdown }
    }
}

impl Drop for PeerExitGuard {
    fn drop(&mut self) {
        self.countdown.peer_exited();
    }
}

/// Shard 0's side of the [`PeerExitCountdown`]: blocks on drop until every
/// peer has exited, so whatever is declared before it outlives every
/// peer's reads.
///
/// Flips the shutdown flag before waiting: a peer parked on its bus token
/// only starts its drain once the flag is set, and the thread-level
/// `ShutdownOnDrop` flips it only after `block_on` returns, which is after
/// this wait. Bounded by [`ShutdownDeadline::remaining_for_peer_wait`] --
/// what is left of the budget [`ShardHandles::join_all`] shares, but never
/// less than one drain -- with the same abandon-and-log policy, so a wedged
/// peer cannot hold shard 0 indefinitely while a spent join budget still
/// cannot cut the wait to nothing.
pub(in crate::boot) struct PeerExitWait {
    countdown: Arc<PeerExitCountdown>,
    shutdown_flag: Arc<AtomicBool>,
    deadline: Arc<ShutdownDeadline>,
}

impl PeerExitWait {
    pub(in crate::boot) const fn new(
        countdown: Arc<PeerExitCountdown>,
        shutdown_flag: Arc<AtomicBool>,
        deadline: Arc<ShutdownDeadline>,
    ) -> Self {
        Self {
            countdown,
            shutdown_flag,
            deadline,
        }
    }
}

impl Drop for PeerExitWait {
    fn drop(&mut self) {
        // The panic is the fault to report and `ShutdownOnDrop` still
        // flips the flag for the peers; blocking an unwinding thread here
        // would only delay it.
        if thread::panicking() {
            return;
        }
        self.shutdown_flag.store(true, Ordering::Relaxed);
        let remaining = self.deadline.remaining_for_peer_wait();
        if let Err(peers_running) = self.countdown.wait(remaining) {
            warn!(
                peers_running,
                waited = ?remaining,
                budget = ?self.deadline.budget,
                "peer shards still running at the shutdown join deadline; \
                 releasing shard 0 anyway"
            );
        }
    }
}

/// Resolve the operator's `cpu_allocation` into concrete shard
/// assignments plus the checked `u16` shard count.
///
/// Shard ids index `ReplicaOwnerTable` slots as `u16`. `OWNER_NONE`
/// (`u16::MAX`) is reserved as the empty-slot sentinel, so a server
/// configured with `u16::MAX` shards would mint a shard id that
/// collides with the sentinel and an owner-table lookup could never
/// tell that shard apart from an unowned slot. Reject at boot so the
/// invariant is held by the type system, not by hoping the operator
/// never configures 65535 cores worth of shards.
pub(in crate::boot) fn resolve_shard_assignments(
    sharding: &configs::sharding::ShardingConfig,
) -> Result<(Vec<ShardInfo>, u16), ServerError> {
    let allocator = ShardAllocator::new(&sharding.cpu_allocation, sharding.pin_cores)
        .map_err(ServerError::ShardAllocator)?;
    let assignments = allocator
        .to_shard_assignments()
        .map_err(ServerError::ShardAllocator)?;
    if assignments.is_empty() {
        return Err(ServerError::ShardsCountZero);
    }
    match u16::try_from(assignments.len()) {
        Ok(count) if count < message_bus::OWNER_NONE => Ok((assignments, count)),
        _ => Err(ServerError::ShardsCountOverflow {
            count: assignments.len(),
        }),
    }
}

/// Re-validate the runtime sharding knobs that the per-shard runtime
/// consumes directly: the two inbox capacities, the three shutdown
/// durations and their ordering, and the reconcile tick. Mirrors
/// `ShardingConfig::validate` for exactly those, so a caller that built the
/// config without running it (e.g. tests, embedded usage) cannot OOM at
/// boot, starve a core, or wedge process exit with an out-of-range value.
/// `cpu_allocation` is not re-checked here - [`resolve_shard_assignments`]
/// resolves it through the allocator and rejects it there.
pub(in crate::boot) fn validate_sharding_runtime_knobs(
    sharding: &configs::sharding::ShardingConfig,
) -> Result<(), ServerError> {
    let inbox_capacity = sharding.inbox_capacity;
    if inbox_capacity == 0 || inbox_capacity > INBOX_CAPACITY_MAX {
        return Err(ServerError::InvalidInboxCapacity {
            value: inbox_capacity,
            max: INBOX_CAPACITY_MAX,
        });
    }
    let reply_inbox_capacity = sharding.reply_inbox_capacity;
    if reply_inbox_capacity == 0 || reply_inbox_capacity > INBOX_CAPACITY_MAX {
        return Err(ServerError::InvalidReplyInboxCapacity {
            value: reply_inbox_capacity,
            max: INBOX_CAPACITY_MAX,
        });
    }
    let drain_timeout = sharding.shutdown_drain_timeout.get_duration();
    if drain_timeout.is_zero() || drain_timeout > SHUTDOWN_DRAIN_TIMEOUT_MAX {
        return Err(ServerError::InvalidShutdownDrainTimeout {
            value: drain_timeout,
            max: SHUTDOWN_DRAIN_TIMEOUT_MAX,
        });
    }
    let poll_interval = sharding.shutdown_poll_interval.get_duration();
    if poll_interval.is_zero() || poll_interval > SHUTDOWN_POLL_INTERVAL_MAX {
        return Err(ServerError::InvalidShutdownPollInterval {
            value: poll_interval,
            max: SHUTDOWN_POLL_INTERVAL_MAX,
        });
    }
    // Ordering: a poll cadence coarser than the drain budget makes the
    // cross-thread shutdown flag effectively unobservable during teardown.
    if poll_interval > drain_timeout {
        return Err(ServerError::ShutdownPollExceedsDrain {
            poll: poll_interval,
            drain: drain_timeout,
        });
    }
    let join_timeout = sharding.shutdown_join_timeout.get_duration();
    if join_timeout > SHUTDOWN_JOIN_TIMEOUT_MAX {
        return Err(ServerError::InvalidShutdownJoinTimeout {
            value: join_timeout,
            max: SHUTDOWN_JOIN_TIMEOUT_MAX,
        });
    }
    // A join budget under the drain budget abandons shards mid-drain,
    // interrupting the WAL fsync / replica drain, and now also cuts shard
    // 0's peer wait short of the writer's last reader.
    if join_timeout < drain_timeout {
        return Err(ServerError::ShutdownJoinBelowDrain {
            join: join_timeout,
            drain: drain_timeout,
        });
    }
    // Zero feeds `run_reconciler`'s sleep inside an unconditional loop, so the
    // tick arm is ready every iteration and `reconcile_once` runs back to back,
    // starving the pump on that core.
    let reconcile_interval = sharding.reconcile_periodic_interval.get_duration();
    if reconcile_interval.is_zero() || reconcile_interval > RECONCILE_PERIODIC_INTERVAL_MAX {
        return Err(ServerError::InvalidReconcilePeriodicInterval {
            value: reconcile_interval,
            max: RECONCILE_PERIODIC_INTERVAL_MAX,
        });
    }
    Ok(())
}

/// Per-shard OS thread entry. Pins CPU + memory, builds the compio
/// runtime, and `block_on`s `shard_main`.
#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
pub(in crate::boot) fn run_shard_thread(
    shard_id: u16,
    total_shards: u16,
    replica_id: Option<u8>,
    assignment: ShardInfo,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    reply_inbox: ShardReceiver<ShardFrame>,
    config: Arc<ServerConfig>,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    roster_cells: RosterCells,
    metadata_applied_frontier: Arc<AppliedFrontier>,
    shard_metrics_all: Vec<ShardMetrics>,
    peer_exit: Arc<PeerExitCountdown>,
    shutdown_deadline: Arc<ShutdownDeadline>,
) -> Result<(), ServerError> {
    // Armed for the whole thread body: a post-spawn error `?` or a panic
    // unwind here must flip `shutdown_flag` so sibling watchdogs drive
    // their bus shutdown instead of parking forever on `bus.token().wait()`.
    let mut shutdown_guard = ShutdownOnDrop::new(Arc::clone(&shutdown_flag));
    // Declared before the runtime so it drops after it: a peer counts
    // itself out only once no task of its runtime can read shard 0's
    // metadata any more.
    let _peer_exit_guard = (shard_id != 0).then(|| PeerExitGuard::new(Arc::clone(&peer_exit)));

    assignment
        .bind_cpu()
        .map_err(|source| ServerError::CpuAffinityFailed { shard_id, source })?;
    assignment
        .bind_memory()
        .map_err(|source| ServerError::MemoryAffinityFailed { shard_id, source })?;

    // `enrich_runtime_create_error` folds the io_uring remediation (raise
    // `ulimit -l`, unblock seccomp, kernel-flag floor) into the error, so the
    // guidance survives into the shard-join failure report instead of only
    // stderr. Multi-shard boxes exhaust RLIMIT_MEMLOCK on per-shard rings
    // before the bootstrap runtime does, so this path needs it most.
    let runtime = create_shard_executor().map_err(|source| {
        let source = server_common::diagnostics::enrich_runtime_create_error(source);
        ServerError::ShardRuntimeCreateFailed { shard_id, source }
    })?;

    let result = runtime.block_on(async move {
        // `shard_main`'s future grows past clippy's `large_futures` cap
        // (it ferries the metadata handoff, bus, builders, and inflight
        // I/O in one state machine). Heap-pin it so the top-level
        // `block_on` future stays small; one allocation per startup buys
        // the stack budget back.
        Box::pin(shard_main(
            shard_id,
            total_shards,
            replica_id,
            senders,
            inbox,
            reply_inbox,
            &config,
            shutdown_flag,
            metadata_handoff,
            barrier,
            owner_table,
            roster_cells,
            metadata_applied_frontier,
            shard_metrics_all,
            peer_exit,
            shutdown_deadline,
        ))
        .await
    });

    if result.is_ok() {
        shutdown_guard.disarm();
    }
    result
}

/// Await the message pump's completion before the shard returns: its
/// post-loop work includes the final flush of every committed journal to
/// segment storage, and returning first drops the compio runtime, which
/// cancels that flush at its next await point.
///
/// `Err` means the pump was already dead (a panic, or an exit outside the
/// stop protocol), so its final flush never ran and the shard must not
/// report a clean exit. The verdict is the inner `JoinError`; the timeout
/// wrapper alone cannot see it, and a shard that swallows it prints
/// "exited cleanly" over a corpse.
pub(in crate::boot) async fn await_pump_drain(
    pump_handle: Option<compio::runtime::JoinHandle<Option<FatalCommit>>>,
    config: &ServerConfig,
    shard_id: u16,
) -> Result<(), ServerError> {
    let Some(pump_handle) = pump_handle else {
        return Ok(());
    };
    let drain_budget = config.system.sharding.shutdown_drain_timeout.get_duration();
    let Ok(join_result) = compio::time::timeout(drain_budget, pump_handle).await else {
        error!(
            shard = shard_id,
            timeout = ?drain_budget,
            "message pump did not drain within the shutdown budget; \
             committed journal tail may not have flushed"
        );
        return Err(ServerError::ShardPumpDrainTimedOut {
            shard_id,
            timeout: drain_budget,
        });
    };
    // `JoinError` renders a panic as the bare "Task has panicked" and the
    // type is not re-exported, so the payload -- the only part with
    // diagnostic value -- is lifted by re-raising into an immediate catch.
    // The panic hook already ran when the task died; `resume_unwind` does
    // not run it again, so nothing is printed twice and the message finally
    // reaches the tracing sink too.
    let reason = match panic::catch_unwind(panic::AssertUnwindSafe(|| join_result.resume_unwind()))
    {
        Ok(Some(None)) => return Ok(()),
        // The pump drained and flushed; it just has nothing left to serve.
        // Fail the shard so the process exits non-zero: a node that stopped
        // because it could not persist a cluster-committed op must not look
        // to an orchestrator like a clean shutdown.
        Ok(Some(Some(fault))) => {
            error!(
                shard = shard_id,
                namespace_raw = fault.namespace_raw,
                op = fault.op,
                operation = ?fault.operation,
                "message pump stopped on a partition commit fault; \
                 the server is shutting down"
            );
            return Err(ServerError::ShardFatal {
                shard_id,
                namespace_raw: fault.namespace_raw,
                op: fault.op,
            });
        }
        Ok(None) => "task was cancelled".to_string(),
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map(|message| (*message).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .map_or_else(
                || "task panicked".to_string(),
                |message| format!("task panicked: {message}"),
            ),
    };
    error!(
        shard = shard_id,
        "message pump died instead of draining ({reason}); \
         committed journal tail may not have flushed"
    );
    Err(ServerError::ShardPumpDied { shard_id, reason })
}

/// Spawn a per-shard polling task that watches the cross-thread shutdown
/// flag and triggers this shard's bus shutdown on transition. The flag
/// is the only Send signal we have; the bus' shutdown machinery is
/// `!Send` (`Rc<Cell<bool>>` + per-shard `async_channel`), so it must be
/// triggered from within the runtime that owns the bus.
///
/// The caller owns the returned handle and must await it on the exit paths
/// where shutdown is in progress (flag set or bus token triggered):
/// dropping it there cancels the watchdog mid-`bus.shutdown()`, truncating
/// in-flight `ClientForwardFailed` replies (terminal per `SendError` docs).
/// It cannot go through `bus.track_background` instead: the watchdog itself
/// drives `bus.shutdown()`, and the bg-drain loop in `shutdown()` would
/// re-enter awaiting the watchdog's own pending shutdown call
/// (self-deadlock). The await is bounded: once the token fires the loop
/// stands down within one poll interval, and the shutdown call itself is
/// capped by `drain_timeout`.
#[allow(clippy::needless_pass_by_value)]
pub(in crate::boot) fn spawn_shutdown_watchdog(
    bus: Rc<IggyMessageBus>,
    shutdown_flag: Arc<AtomicBool>,
    drain_timeout: Duration,
    poll_interval: Duration,
) -> compio::runtime::JoinHandle<()> {
    let bus_for_task = Rc::clone(&bus);
    let bus_token = bus.token();
    compio::runtime::spawn(async move {
        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                break;
            }
            if bus_token.is_triggered() {
                // Bus shutdown was driven from elsewhere (e.g. internal
                // failure path). Watchdog has nothing left to do.
                return;
            }
            compio::time::sleep(poll_interval).await;
        }
        let _ = bus_for_task.shutdown(drain_timeout).await;
    })
}

/// Stop senders of the background loops `shard_main` spawns, fired together
/// on `shard_main`'s bind-failure and normal-shutdown exits.
pub(in crate::boot) struct StopSignals {
    pub(in crate::boot) pump: Sender<()>,
    pub(in crate::boot) reconciler: Sender<()>,
    pub(in crate::boot) heartbeat: Option<Sender<()>>,
    pub(in crate::boot) pat_cleaner: Option<Sender<()>>,
    pub(in crate::boot) segment_cleaner: Option<Sender<()>>,
}

impl StopSignals {
    /// Best-effort: a loop that already exited has dropped its receiver.
    pub(in crate::boot) fn fire(&self) {
        let _ = self.pump.try_send(());
        let _ = self.reconciler.try_send(());
        for stop in [&self.heartbeat, &self.pat_cleaner, &self.segment_cleaner]
            .into_iter()
            .flatten()
        {
            let _ = stop.try_send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_on_drop_armed_flips_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        drop(ShutdownOnDrop::new(Arc::clone(&flag)));
        assert!(
            flag.load(Ordering::Relaxed),
            "an armed guard must flip the flag on drop (covers the error `?` \
             and panic-unwind exit paths of run_shard_thread)"
        );
    }

    #[test]
    fn shutdown_on_drop_disarmed_leaves_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut guard = ShutdownOnDrop::new(Arc::clone(&flag));
        guard.disarm();
        drop(guard);
        assert!(
            !flag.load(Ordering::Relaxed),
            "a disarmed guard must not flip the flag (clean `Ok(())` exit)"
        );
    }

    #[test]
    fn peer_exit_countdown_releases_the_waiter_once_every_peer_is_out() {
        let countdown = Arc::new(PeerExitCountdown::new(2));
        let guards: [PeerExitGuard; 2] =
            std::array::from_fn(|_| PeerExitGuard::new(Arc::clone(&countdown)));
        assert_eq!(
            countdown.wait(Duration::from_millis(10)),
            Err(2),
            "two live peers must hold the waiter past a short deadline"
        );
        let peers: Vec<_> = guards
            .into_iter()
            .map(|guard| {
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(20));
                    drop(guard);
                })
            })
            .collect();
        assert_eq!(
            countdown.wait(Duration::from_secs(30)),
            Ok(()),
            "the last guard drop must release the waiter"
        );
        for peer in peers {
            peer.join()
                .expect("peer thread dropped its guard without panicking");
        }
    }

    #[test]
    fn peer_exit_guard_counts_out_during_unwind() {
        let countdown = Arc::new(PeerExitCountdown::new(1));
        let guard = PeerExitGuard::new(Arc::clone(&countdown));
        // `resume_unwind` skips the panic hook, so the unwind is silent.
        let unwound = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _guard = guard;
            panic::resume_unwind(Box::new("peer shard body panicked"));
        }));
        assert!(unwound.is_err());
        assert_eq!(
            countdown.wait(Duration::ZERO),
            Ok(()),
            "a guard dropped by a panic unwind must still count its peer out"
        );
    }

    /// Stops a parked stand-in thread once the test's binding goes out of
    /// scope. The joiner under test drops the `JoinHandle`, so the thread
    /// cannot be joined back; flagging it keeps it from parking for the life
    /// of the test binary.
    struct ThreadStopper(Arc<AtomicBool>);

    impl Drop for ThreadStopper {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// A shard thread that never returns on its own: the stand-in for a
    /// wedged pump that the deadline tests need.
    fn spawn_wedged_shard() -> (thread::JoinHandle<Result<(), ServerError>>, ThreadStopper) {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = thread::spawn({
            let stop = Arc::clone(&stop);
            move || -> Result<(), ServerError> {
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(JOIN_POLL_INTERVAL);
                }
                Ok(())
            }
        });
        (handle, ThreadStopper(stop))
    }

    #[test]
    fn peer_exit_wait_flips_the_flag_before_waiting() {
        // Stands in for a peer parked on its bus token: it exits only once
        // the shutdown flag is set, so a waiter that set the flag after
        // waiting would sit out the whole budget.
        let countdown = Arc::new(PeerExitCountdown::new(1));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let peer = thread::spawn({
            let guard = PeerExitGuard::new(Arc::clone(&countdown));
            let shutdown_flag = Arc::clone(&shutdown_flag);
            move || {
                while !shutdown_flag.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                }
                drop(guard);
            }
        });
        let started = Instant::now();
        drop(PeerExitWait::new(
            countdown,
            Arc::clone(&shutdown_flag),
            Arc::new(ShutdownDeadline::new(
                Duration::from_secs(30),
                Duration::from_secs(10),
            )),
        ));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the waiter must not sit out its budget on a peer that waits for the flag"
        );
        assert!(shutdown_flag.load(Ordering::Relaxed));
        peer.join()
            .expect("peer thread dropped its guard without panicking");
    }

    #[test]
    fn peer_exit_wait_gives_up_at_the_deadline() {
        let countdown = Arc::new(PeerExitCountdown::new(1));
        let _wedged_peer = PeerExitGuard::new(Arc::clone(&countdown));
        // Degenerate budget == floor: the floor cannot extend the wait, so
        // the abandon is bounded by the budget alone.
        let timeout = Duration::from_millis(50);
        let started = Instant::now();
        drop(PeerExitWait::new(
            countdown,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ShutdownDeadline::new(timeout, timeout)),
        ));
        let waited = started.elapsed();
        assert!(
            waited >= timeout && waited < Duration::from_secs(5),
            "a wedged peer must be abandoned at the deadline, waited {waited:?}"
        );
    }

    /// Regression: the two waits nest (shard 0 cannot start waiting for its
    /// peers until its own drain returned, already inside the join budget).
    /// With a budget each, a clean shutdown reported shard 0 as wedged and
    /// exited non-zero. What is left of the shared budget is what the peer
    /// wait gets, whenever that clears the floor.
    #[test]
    fn peer_wait_inherits_the_shared_budget_above_its_floor() {
        const BUDGET: Duration = Duration::from_secs(30);
        const FLOOR: Duration = Duration::from_secs(10);
        let deadline = ShutdownDeadline::new(BUDGET, FLOOR);
        let inherited = deadline.remaining_for_peer_wait();
        assert!(
            inherited > FLOOR && inherited <= BUDGET,
            "an unspent budget must be inherited, not replaced by the floor: {inherited:?}"
        );
    }

    /// The other half of the same nesting: the join budget bounds LIVENESS
    /// (process exit must not hang on a wedged shard) while this wait enforces
    /// SAFETY (shard 0 owns the metadata write handle every peer reads
    /// through). Funding the fence out of the join budget let an exit-latency
    /// timeout cancel it: the drain spent the budget, `wait(ZERO)` returned
    /// without blocking, and shard 0 dropped the writer under a live reader.
    #[test]
    fn peer_wait_honours_a_live_peer_past_a_spent_join_budget() {
        const BUDGET: Duration = Duration::from_millis(20);
        const FLOOR: Duration = Duration::from_millis(200);
        let deadline = Arc::new(ShutdownDeadline::new(BUDGET, FLOOR));
        let shutdown_flag = AtomicBool::new(true);
        // Stands in for the slow drain that arms and then spends the shared
        // budget before shard 0 can reach its peer wait.
        let (wedged_shard, _stopper) = spawn_wedged_shard();
        assert!(
            join_until_shutdown_deadline(wedged_shard, &shutdown_flag, &deadline, Duration::ZERO)
                .is_none(),
            "the join must spend the budget it armed"
        );
        assert!(deadline.remaining().is_zero(), "the budget must be spent");

        let countdown = Arc::new(PeerExitCountdown::new(1));
        let _live_peer = PeerExitGuard::new(Arc::clone(&countdown));
        let started = Instant::now();
        drop(PeerExitWait::new(
            countdown,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&deadline),
        ));
        let waited = started.elapsed();
        assert!(
            waited >= FLOOR,
            "a spent join budget must not release the metadata writer while a peer still reads \
             through it, waited {waited:?}"
        );
    }

    #[test]
    fn peer_exit_wait_ignores_peers_that_never_spawned() {
        // A failed spawn leaves peers with no guard to count them out; the
        // one that did spawn must still be the only thing the waiter waits on.
        let countdown = Arc::new(PeerExitCountdown::new(3));
        countdown.peers_never_spawned(2);
        let spawned = PeerExitGuard::new(Arc::clone(&countdown));
        assert_eq!(
            countdown.wait(Duration::from_millis(10)),
            Err(1),
            "the peer that did spawn must still hold the waiter"
        );
        drop(spawned);
        assert_eq!(countdown.wait(Duration::ZERO), Ok(()));
    }

    #[test]
    fn peer_exit_wait_with_no_peers_returns_at_once() {
        let started = Instant::now();
        drop(PeerExitWait::new(
            Arc::new(PeerExitCountdown::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(ShutdownDeadline::new(
                Duration::from_secs(30),
                Duration::from_secs(10),
            )),
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a single-shard server has nobody to wait for"
        );
    }

    #[test]
    fn peer_exit_wait_never_blocks_an_unwinding_thread() {
        let countdown = Arc::new(PeerExitCountdown::new(1));
        let _still_running = PeerExitGuard::new(Arc::clone(&countdown));
        let wait = PeerExitWait::new(
            countdown,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ShutdownDeadline::new(
                Duration::from_secs(30),
                Duration::from_secs(10),
            )),
        );
        let started = Instant::now();
        let unwound = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _wait = wait;
            panic::resume_unwind(Box::new("shard 0 body panicked"));
        }));
        assert!(unwound.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a waiter dropped during unwind must return without waiting"
        );
    }

    #[compio::test]
    async fn pump_drain_timeout_is_not_reported_as_clean() {
        let mut config = ServerConfig::default();
        let timeout = Duration::from_millis(1);
        Arc::get_mut(&mut config.system)
            .expect("a fresh ServerConfig owns its system config")
            .sharding
            .shutdown_drain_timeout = iggy_common::IggyDuration::new(timeout);
        let pump = compio::runtime::spawn(std::future::pending::<Option<FatalCommit>>());

        let error = await_pump_drain(Some(pump), &config, 7)
            .await
            .expect_err("a live pump past the drain budget is not a clean exit");
        assert!(matches!(
            error,
            ServerError::ShardPumpDrainTimedOut {
                shard_id: 7,
                timeout: actual,
            } if actual == timeout
        ));
    }

    #[compio::test]
    async fn pump_stopped_by_a_commit_fault_is_not_reported_as_clean() {
        // The pump drained and flushed, so the join succeeds. Reporting that
        // as a clean exit would hand an orchestrator exit code 0 for a node
        // that stopped because it could not persist a cluster-committed op.
        let config = ServerConfig::default();
        let fault = FatalCommit {
            namespace_raw: 42,
            op: 7,
            operation: iggy_binary_protocol::Operation::SendMessages,
        };
        let pump = compio::runtime::spawn(async move { Some(fault) });

        let error = await_pump_drain(Some(pump), &config, 3)
            .await
            .expect_err("a pump that stopped on a commit fault is not a clean exit");
        assert!(matches!(
            error,
            ServerError::ShardFatal {
                shard_id: 3,
                namespace_raw: 42,
                op: 7,
            }
        ));
    }

    /// Regression: the shutdown-join deadline must arm at SHUTDOWN, not
    /// at boot. The original bound measured from `join_all` entry, so any
    /// healthy server outliving `shutdown_join_timeout` (30s default) was
    /// abandoned as "wedged" and the process exited - every BDD run died
    /// at t+30s while the test container was still compiling.
    #[test]
    fn join_waits_unbounded_while_the_server_runs() {
        let shutdown_flag = AtomicBool::new(false);
        // Thread outlives a deliberately tiny join budget; with the flag
        // clear the budget must never even arm.
        let handle = thread::spawn(|| -> Result<(), ServerError> {
            thread::sleep(Duration::from_millis(300));
            Ok(())
        });
        let deadline = ShutdownDeadline::new(Duration::from_millis(20), Duration::from_millis(20));
        let joined =
            join_until_shutdown_deadline(handle, &shutdown_flag, &deadline, Duration::ZERO);
        assert!(
            matches!(joined, Some(Ok(Ok(())))),
            "a running server must be awaited indefinitely, not abandoned as wedged"
        );
        assert!(
            deadline.armed.get().is_none(),
            "the join deadline must not arm before the shutdown flag flips"
        );
    }

    #[test]
    fn join_all_fails_the_exit_on_a_panic_no_thread_surfaced() {
        // A listener or connection task panic leaves every shard thread
        // exiting Ok; only the hook's record can keep that from reading
        // as a clean shutdown to an orchestrator.
        let first_panic = Arc::new(OnceLock::new());
        first_panic
            .set("thread 'shard-0' panicked at listener.rs:1:1: boom".to_string())
            .expect("a fresh slot accepts the first record");
        let handle = thread::spawn(|| -> Result<(), ServerError> { Ok(()) });
        let handles = ShardHandles {
            shutdown_flag: Arc::new(AtomicBool::new(true)),
            shard_threads: vec![(0, handle)],
            deadline: Arc::new(ShutdownDeadline::new(
                Duration::from_secs(1),
                Duration::from_millis(500),
            )),
            first_panic,
        };
        let error = handles
            .join_all()
            .expect_err("a recorded panic must fail the exit even when every thread exited Ok");
        assert!(
            matches!(&error, ServerError::Panicked { description } if description.contains("boom")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn panic_hook_records_the_panic_and_flips_the_shutdown_flag() {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let first_panic = install_panic_hook(Arc::clone(&shutdown_flag));
        let joined = thread::Builder::new()
            .name("shard-7".to_string())
            .spawn(|| panic!("injected task panic"))
            .expect("spawn")
            .join();
        assert!(joined.is_err(), "the thread must have panicked");
        assert!(
            shutdown_flag.load(Ordering::Relaxed),
            "a panic anywhere must drive the whole server down"
        );
        let description = first_panic.get().expect("the hook records the first panic");
        assert!(
            description.starts_with("thread 'shard-7' panicked at ")
                && description.ends_with(": injected task panic"),
            "unexpected record: {description}"
        );
    }

    #[test]
    fn join_abandons_a_wedged_shard_after_the_shutdown_deadline() {
        let shutdown_flag = AtomicBool::new(true);
        let (handle, _stopper) = spawn_wedged_shard();
        let deadline = ShutdownDeadline::new(Duration::from_millis(100), Duration::from_millis(50));
        let joined =
            join_until_shutdown_deadline(handle, &shutdown_flag, &deadline, Duration::ZERO);
        assert!(
            joined.is_none(),
            "a shard still running past the post-shutdown budget must be abandoned"
        );
        assert!(
            deadline.armed.get().is_some(),
            "the deadline arms once the flag is set"
        );
    }
}
