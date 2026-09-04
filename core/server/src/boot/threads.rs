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
    INBOX_CAPACITY_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX,
};
use message_bus::{IggyMessageBus, ReplicaOwnerTable};
use partitions::FatalCommit;
use server_common::executor::create_shard_executor;
use shard::metrics::ShardMetrics;
use shard::{Receiver as ShardReceiver, Sender, ShardFrame, TaggedSender};
use std::backtrace::Backtrace;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use std::{panic, thread};
use tracing::{error, info, warn};

/// Result of a multi-shard bootstrap.
///
/// Carries the cross-thread shutdown flag, one OS-thread `JoinHandle`
/// per shard, and the first panic `install_panic_hook` recorded. The
/// caller flips the flag via [`Self::install_ctrlc_handler`] and then
/// drains every shard via [`Self::join_all`], bounded by `join_timeout`
/// (`system.sharding.shutdown_join_timeout`).
pub struct ShardHandles {
    pub(in crate::boot) shutdown_flag: Arc<AtomicBool>,
    pub(in crate::boot) shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)>,
    pub(in crate::boot) join_timeout: Duration,
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
    /// not block process exit forever.
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
        // Armed on the first poll that observes the shutdown flag, shared
        // across all shards: one budget covers the whole drain, not one
        // budget per shard.
        let mut deadline: Option<Instant> = None;
        // Shards run thread-per-core with compio's blocking fallback pool
        // disabled, so an io_uring opcode the kernel lacks aborts every shard
        // with the same panic. Surface the actionable diagnostic once.
        let mut io_uring_diagnostic_shown = false;
        for (shard_id, handle) in self.shard_threads {
            let Some(joined) = join_until_shutdown_deadline(
                handle,
                &self.shutdown_flag,
                self.join_timeout,
                &mut deadline,
            ) else {
                error!(
                    shard_id,
                    waited = ?self.join_timeout,
                    "shard thread still running at the shutdown join deadline; abandoning it"
                );
                failures.push(ShardJoinFailure {
                    shard_id,
                    kind: ShardJoinFailureKind::Wedged {
                        waited: self.join_timeout,
                    },
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
/// `join_timeout` clock starts only when `shutdown_flag` is observed set
/// (arming the caller-shared `deadline` once, so all shards drain under
/// ONE budget); a running server parked here for hours must never be
/// mistaken for a wedged shard. `None` means the thread was still
/// running at the post-shutdown deadline and the handle was dropped
/// (the OS thread keeps running detached; process exit reaps it).
/// `JoinHandle` has no timed join, so this polls `is_finished` at
/// [`JOIN_POLL_INTERVAL`]; the closing `join()` on a finished thread
/// returns immediately.
fn join_until_shutdown_deadline(
    handle: thread::JoinHandle<Result<(), ServerError>>,
    shutdown_flag: &AtomicBool,
    join_timeout: Duration,
    deadline: &mut Option<Instant>,
) -> Option<thread::Result<Result<(), ServerError>>> {
    while !handle.is_finished() {
        if deadline.is_none() && shutdown_flag.load(Ordering::Relaxed) {
            *deadline = Some(Instant::now() + join_timeout);
        }
        if let Some(deadline) = deadline
            && Instant::now() >= *deadline
        {
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
    join_timeout: Duration,
) {
    let deadline = Instant::now() + join_timeout;
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
        if remaining.is_empty() || Instant::now() >= deadline {
            break;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
    for (shard_id, _survivor) in remaining {
        error!(
            shard_id,
            waited = ?join_timeout,
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
/// consumes directly. Mirrors `ShardingConfig::validate` so a caller
/// that built the config without running it (e.g. tests, embedded
/// usage) cannot OOM at boot or wedge process exit with an out-of-range
/// value.
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
    shard_metrics_all: Vec<ShardMetrics>,
    synthetic_counter: crate::external_auth::SyntheticUserIdCounter,
) -> Result<(), ServerError> {
    // Armed for the whole thread body: a post-spawn error `?` or a panic
    // unwind here must flip `shutdown_flag` so sibling watchdogs drive
    // their bus shutdown instead of parking forever on `bus.token().wait()`.
    let mut shutdown_guard = ShutdownOnDrop::new(Arc::clone(&shutdown_flag));

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
            shard_metrics_all,
            synthetic_counter,
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
        let mut deadline = None;
        let joined = join_until_shutdown_deadline(
            handle,
            &shutdown_flag,
            Duration::from_millis(20),
            &mut deadline,
        );
        assert!(
            matches!(joined, Some(Ok(Ok(())))),
            "a running server must be awaited indefinitely, not abandoned as wedged"
        );
        assert!(
            deadline.is_none(),
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
            join_timeout: Duration::from_secs(1),
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
        // Never finishes: stands in for a wedged pump. The thread leaks
        // into the test process, which exits right after.
        let handle = thread::spawn(|| -> Result<(), ServerError> {
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        });
        let mut deadline = None;
        let joined = join_until_shutdown_deadline(
            handle,
            &shutdown_flag,
            Duration::from_millis(100),
            &mut deadline,
        );
        assert!(
            joined.is_none(),
            "a shard still running past the post-shutdown budget must be abandoned"
        );
        assert!(deadline.is_some(), "the deadline arms once the flag is set");
    }
}
