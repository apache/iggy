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

//! Deterministic single-threaded cooperative executor.
//!
//! Holds many `!Send` futures (shard message pumps) on one thread and
//! preempts them only at their own await points. Whenever more than one task
//! is ready, the next task to poll is picked uniformly at random from a
//! PRNG seeded by the simulation seed, so the interleaving of shard tasks is
//! a pure, replayable function of that seed. Time is virtual: timers fire
//! only through [`DetExecutor::advance_time`], never from the wall clock.
//!
//! Re-entrancy is settled by ownership rather than runtime checks: waking a
//! task (including self-wakes and crossfire's inline wake-on-`try_send`)
//! only appends to a shared queue, while `spawn`/`abort`/`run_until_stalled`
//! need `&mut DetExecutor`, which task code can never hold. The simulator is
//! the only spawner.

pub mod time;

use futures::task::ArcWake;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub use time::{SimInstant, TimerHandle};

/// Salt for deriving the executor's PRNG stream from the simulation seed.
///
/// Sibling of the workload fault salt (`0x5A1A_F0E5_FACE_0001`): independent
/// streams keep scheduling draws from perturbing network or workload traces.
pub const EXECUTOR_SEED_SALT: u64 = 0x5A1A_F0E5_FACE_0002;

/// A type-erased, `!Send` task future: what the executor stores and what
/// [`PendingSpawns`] stages.
type BoxedTask = Pin<Box<dyn Future<Output = ()>>>;

/// Handle to a spawned task. Generational: a `TaskId` outliving its task is
/// harmless: wakes and aborts against a stale generation are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskId {
    index: u32,
    generation: u32,
}

/// Why [`DetExecutor::run_until_stalled`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// No task is ready: every live task waits on a wake (channel or timer).
    Quiescent { polls: u32 },
    /// The poll budget ran out with tasks still ready. In the simulator this
    /// is always a bug (livelock); callers should panic with the seed.
    BudgetExhausted { polls: u32 },
}

pub struct DetExecutor {
    /// Task slab; slots are recycled through `free` with bumped generations.
    /// Index-addressed everywhere.
    tasks: Vec<Slot>,
    free: Vec<u32>,
    /// Ready queue in insertion order; the next victim is a seeded uniform
    /// pick, so the schedule is a pure function of (seed, wake history).
    ready: Vec<TaskId>,
    rng: Xoshiro256Plus,
    wake: Arc<WakeQueue>,
    timer: TimerHandle,
    /// Futures staged by task code through `MessageBus::spawn` (the sim's
    /// spawn seam), drained into real tasks at the top of each
    /// `run_until_stalled` turn. `Rc`-shared so a bus can enqueue without
    /// `&mut self`, which task code can never hold.
    spawns: Rc<PendingSpawns>,
    /// Task ids of detached spawns keyed by owning replica, so a crash can
    /// abort a replica's in-flight dispatch tasks alongside its pump.
    spawned: BTreeMap<u8, Vec<TaskId>>,
    live: usize,
    poll_seq: u64,
    trace_hash: u64,
}

impl DetExecutor {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            tasks: Vec::new(),
            free: Vec::new(),
            ready: Vec::new(),
            rng: Xoshiro256Plus::seed_from_u64(seed ^ EXECUTOR_SEED_SALT),
            wake: Arc::new(WakeQueue {
                woken: Mutex::new(Vec::new()),
            }),
            timer: TimerHandle::new(),
            spawns: Rc::new(PendingSpawns::default()),
            spawned: BTreeMap::new(),
            live: 0,
            poll_seq: 0,
            trace_hash: FNV_OFFSET,
        }
    }

    /// Spawn a task. It starts ready and gets polled on the next
    /// [`run_until_stalled`](Self::run_until_stalled).
    ///
    /// Futures are deliberately not `Send`: shard tasks hold `Rc` state.
    ///
    /// # Panics
    /// Panics if more than `u32::MAX` slots would be needed.
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_boxed(Box::pin(future), None)
    }

    /// Spawn an already-boxed future. Shared by [`Self::spawn`] and the
    /// pending-spawn drain, which carries the future type-erased.
    fn spawn_boxed(&mut self, future: BoxedTask, owner: Option<u8>) -> TaskId {
        let (index, generation) = if let Some(index) = self.free.pop() {
            let Slot::Vacant { next_generation } = self.tasks[index as usize] else {
                unreachable!("free list entries are always vacant");
            };
            (index, next_generation)
        } else {
            let index = u32::try_from(self.tasks.len()).expect("task slab exceeds u32 slots");
            self.tasks.push(Slot::Vacant { next_generation: 0 });
            (index, 0)
        };
        let id = TaskId { index, generation };
        let waker = futures::task::waker(Arc::new(TaskWaker {
            id,
            queue: Arc::clone(&self.wake),
        }));
        self.tasks[index as usize] = Slot::Occupied(TaskState {
            generation,
            queued: true,
            waker,
            future: Some(future),
            owner,
        });
        self.ready.push(id);
        self.live += 1;
        id
    }

    /// Abort a task: its future is dropped immediately, running destructors
    /// (channel recv futures cancel their registered wakers, sleep futures
    /// cancel their timers). Returns `false` if the task already finished.
    ///
    /// Models a hard crash: no graceful drain runs.
    pub fn abort(&mut self, id: TaskId) -> bool {
        match self.tasks.get_mut(id.index as usize) {
            Some(Slot::Occupied(state)) if state.generation == id.generation => {
                self.vacate(id);
                true
            }
            _ => false,
        }
    }

    /// Handle to the pending-spawn queue, cloned into each sim bus so
    /// `MessageBus::spawn` can enqueue a detached task without `&mut self`.
    #[must_use]
    pub fn spawner(&self) -> Rc<PendingSpawns> {
        Rc::clone(&self.spawns)
    }

    /// Abort every detached task a replica's bus spawned, tearing them down
    /// with the pump on a crash instead of leaving orphaned dispatch tasks
    /// running against a dead replica. Stale ids (already finished) are
    /// ignored by [`Self::abort`].
    pub fn abort_replica_spawned(&mut self, replica: u8) {
        if let Some(ids) = self.spawned.remove(&replica) {
            for id in ids {
                self.abort(id);
            }
        }
    }

    /// Turn futures staged via [`Self::spawner`] into tasks. Called at the
    /// top of each `run_until_stalled` turn, so a spawn raised inside a poll
    /// becomes ready on the next turn, exactly like a wake. FIFO drain plus
    /// deterministic slot assignment keep the schedule a pure function of
    /// the seed; each spawn folds a marker into the trace hash.
    fn drain_pending_spawns(&mut self) {
        let staged: Vec<(u8, BoxedTask)> = std::mem::take(&mut *self.spawns.inbox.borrow_mut());
        for (replica, future) in staged {
            self.trace_hash = fnv1a_fold(self.trace_hash, SPAWN_TRACE_MARKER);
            self.trace_hash = fnv1a_fold(self.trace_hash, u64::from(replica));
            let id = self.spawn_boxed(future, Some(replica));
            self.spawned.entry(replica).or_default().push(id);
        }
    }

    /// Poll ready tasks (seeded pick) until none is ready or the budget runs
    /// out. Wakes raised during polling (including from `try_send` into a
    /// channel a task waits on) feed back into the same loop.
    ///
    /// # Panics
    /// Panics if the internal wake mutex is poisoned.
    pub fn run_until_stalled(&mut self, poll_budget: u32) -> RunOutcome {
        let mut polls = 0u32;
        loop {
            self.drain_pending_spawns();
            self.drain_wake_queue();
            if self.ready.is_empty() {
                return RunOutcome::Quiescent { polls };
            }
            if polls == poll_budget {
                return RunOutcome::BudgetExhausted { polls };
            }
            let pick = self.rng.random_range(0..self.ready.len());
            // `remove` (not `swap_remove`): it preserves the order of the
            // remaining ready ids, so the seeded pick -> task mapping stays a
            // pure function of the seed. `ready` holds only concurrently-ready
            // tasks (bounded by live pumps + in-flight dispatch tasks), so the
            // O(R) shift is cheap.
            let id = self.ready.remove(pick);
            if self.poll_task(id) {
                polls += 1;
            }
        }
    }

    /// Advance virtual time, firing due timers in `(deadline, seq)` order.
    /// Fired timers wake their tasks; call
    /// [`run_until_stalled`](Self::run_until_stalled) afterwards to poll them.
    pub fn advance_time(&mut self, delta: Duration) {
        for seq in self.timer.advance(delta) {
            // Timer fires are schedule events: fold them so two runs that
            // diverge only in timer order still produce different hashes.
            self.trace_hash = fnv1a_fold(self.trace_hash, TIMER_TRACE_MARKER);
            self.trace_hash = fnv1a_fold(self.trace_hash, seq);
        }
    }

    /// Cloneable handle to the virtual clock, for wiring into the sim bus.
    #[must_use]
    pub fn timer(&self) -> TimerHandle {
        self.timer.clone()
    }

    /// Current virtual time.
    #[must_use]
    pub fn now(&self) -> SimInstant {
        self.timer.now()
    }

    /// Number of tasks that have been spawned and not yet finished/aborted.
    #[must_use]
    pub const fn live_tasks(&self) -> usize {
        self.live
    }

    /// Rolling FNV-1a hash of the schedule so far: every poll's
    /// `(poll_seq, index, generation)` plus every timer fire. Same seed and
    /// inputs must yield the same hash; determinism tests assert on it.
    /// Explicit algorithm (not `DefaultHasher`) so the value is stable across
    /// Rust releases.
    ///
    /// Replay caveat: the hash *encoding* is pinned here, but the *schedule*
    /// it encodes is the sequence of seeded `rand` draws (this executor's pick
    /// plus the network / workload / entry-shard draws elsewhere), all through
    /// `rand`'s `random_range` reduction, fixed only within the locked
    /// `Cargo.lock`. A `rand` / `rand_xoshiro` bump could shift every seed's
    /// schedule, so replay holds within a build, not across a dependency bump.
    /// Cross-version replay would mean replacing every crate `random_range`
    /// with a version-stable reduction, deferred until recorded seeds must
    /// survive upgrades.
    /// deferred until recorded seeds must survive upgrades.
    #[must_use]
    pub const fn schedule_hash(&self) -> u64 {
        self.trace_hash
    }

    /// Move freshly woken task ids into the ready queue, deduplicating via
    /// the per-slot `queued` flag and dropping wakes for dead generations.
    fn drain_wake_queue(&mut self) {
        let woken = {
            let mut queue = self.wake.woken.lock().expect("wake mutex poisoned");
            std::mem::take(&mut *queue)
        };
        for id in woken {
            if let Some(Slot::Occupied(state)) = self.tasks.get_mut(id.index as usize)
                && state.generation == id.generation
                && !state.queued
            {
                state.queued = true;
                self.ready.push(id);
            }
        }
    }

    /// Poll one task to its next await point. Returns `false` for a stale id
    /// (task finished or aborted after being enqueued).
    fn poll_task(&mut self, id: TaskId) -> bool {
        let Some(Slot::Occupied(state)) = self.tasks.get_mut(id.index as usize) else {
            return false;
        };
        if state.generation != id.generation {
            return false;
        }
        // Clear `queued` before polling so a wake raised inside the poll
        // re-enqueues the task instead of getting deduplicated away.
        state.queued = false;
        let waker = state.waker.clone();
        let mut future = state.future.take().expect("future present outside of poll");

        self.trace_hash = fnv1a_fold(self.trace_hash, self.poll_seq);
        self.trace_hash = fnv1a_fold(self.trace_hash, u64::from(id.index));
        self.trace_hash = fnv1a_fold(self.trace_hash, u64::from(id.generation));
        self.poll_seq += 1;

        // The slot keeps `future: None` while the poll runs. Nothing can
        // observe that state: abort/spawn need `&mut self`, unreachable from
        // task code, and wakes only touch the wake queue.
        let mut cx = Context::from_waker(&waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(()) => self.vacate(id),
            Poll::Pending => {
                let Some(Slot::Occupied(state)) = self.tasks.get_mut(id.index as usize) else {
                    unreachable!("slot vanished during poll");
                };
                state.future = Some(future);
            }
        }
        true
    }

    fn vacate(&mut self, id: TaskId) {
        let owner = match &self.tasks[id.index as usize] {
            Slot::Occupied(state) => state.owner,
            Slot::Vacant { .. } => None,
        };
        self.tasks[id.index as usize] = Slot::Vacant {
            next_generation: id.generation.wrapping_add(1),
        };
        self.free.push(id.index);
        self.live -= 1;
        // Drop the finished id from its replica's tracking vec so `spawned`
        // stays bounded to live tasks. On a crash teardown the entry is already
        // gone (`abort_replica_spawned` takes the whole vec first), so this is
        // a no-op; on normal completion we prune here. Order is irrelevant:
        // `spawned` is never folded into the trace, only walked wholesale on
        // crash.
        if let Some(replica) = owner
            && let Some(ids) = self.spawned.get_mut(&replica)
            && let Some(pos) = ids.iter().position(|&task| task == id)
        {
            ids.swap_remove(pos);
        }
    }
}

impl std::fmt::Debug for DetExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DetExecutor")
            .field("live", &self.live)
            .field("ready", &self.ready.len())
            .field("poll_seq", &self.poll_seq)
            .field("trace_hash", &self.trace_hash)
            .finish_non_exhaustive()
    }
}

enum Slot {
    Vacant { next_generation: u32 },
    Occupied(TaskState),
}

struct TaskState {
    generation: u32,
    /// True while the task sits in `ready`; dedups repeated wakes.
    queued: bool,
    /// Built once at spawn; stable identity for `Waker::will_wake`.
    waker: Waker,
    /// Taken out during poll so no executor borrow spans user code.
    future: Option<Pin<Box<dyn Future<Output = ()>>>>,
    /// Owning replica for a bus-spawned detached task (`None` for a direct
    /// [`DetExecutor::spawn`]). Lets [`DetExecutor::vacate`] prune the id from
    /// `spawned` on completion, keeping the per-replica vec bounded to live
    /// tasks rather than every off-pump task the replica ever spawned.
    owner: Option<u8>,
}

struct WakeQueue {
    woken: Mutex<Vec<TaskId>>,
}

/// Futures staged by task code for the executor to turn into tasks.
///
/// Staged via the sim bus's `MessageBus::spawn`. Interior-mutable and
/// `Rc`-shared: task code enqueues here because it can never hold
/// `&mut DetExecutor`; the executor drains it each `run_until_stalled`
/// turn. The erased futures are `!Send`, which is why the sim bus that
/// holds this queue is `!Send` too.
#[derive(Default)]
pub struct PendingSpawns {
    inbox: RefCell<Vec<(u8, BoxedTask)>>,
}

impl PendingSpawns {
    /// Stage a detached future from `replica`'s bus. Drained FIFO.
    pub fn push(&self, replica: u8, future: BoxedTask) {
        self.inbox.borrow_mut().push((replica, future));
    }
}

impl std::fmt::Debug for PendingSpawns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSpawns")
            .field("staged", &self.inbox.borrow().len())
            .finish()
    }
}

/// `ArcWake` (not a hand-rolled `RawWaker` over `Rc`): crossfire stores the
/// `Waker` inside channel internals, so it must genuinely satisfy the
/// `Send + Sync` contract of `std::task::Waker`.
struct TaskWaker {
    id: TaskId,
    queue: Arc<WakeQueue>,
}

impl ArcWake for TaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .queue
            .woken
            .lock()
            .expect("wake mutex poisoned")
            .push(arc_self.id);
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
/// Distinguishes timer-fire events from poll events in the trace.
const TIMER_TRACE_MARKER: u64 = 0x7131_3E52_F19E_u64;
/// Distinguishes spawn events from poll and timer events in the trace.
const SPAWN_TRACE_MARKER: u64 = 0x5350_4157_4E5E_u64;

fn fnv1a_fold(mut hash: u64, value: u64) -> u64 {
    for byte in value.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    /// Completes on the second poll; self-wakes on the first.
    struct YieldOnce {
        yielded: bool,
    }

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn yield_once() -> YieldOnce {
        YieldOnce { yielded: false }
    }

    #[test]
    fn spawn_and_complete() {
        let mut executor = DetExecutor::new(1);
        let done: Rc<Cell<u32>> = Rc::new(Cell::new(0));
        for _ in 0..8 {
            let done = Rc::clone(&done);
            executor.spawn(async move {
                yield_once().await;
                done.set(done.get() + 1);
            });
        }
        assert_eq!(executor.live_tasks(), 8);
        let outcome = executor.run_until_stalled(1_000);
        assert!(matches!(outcome, RunOutcome::Quiescent { .. }));
        assert_eq!(done.get(), 8);
        assert_eq!(executor.live_tasks(), 0);
    }

    fn schedule_hash_for_seed(seed: u64) -> u64 {
        let mut executor = DetExecutor::new(seed);
        for _ in 0..16 {
            executor.spawn(async {
                for _ in 0..8 {
                    yield_once().await;
                }
            });
        }
        assert!(matches!(
            executor.run_until_stalled(10_000),
            RunOutcome::Quiescent { .. }
        ));
        executor.schedule_hash()
    }

    #[test]
    fn schedule_is_a_pure_function_of_the_seed() {
        // Same seed replays an identical schedule; a different seed diverges.
        // 16 tasks x 8 yields, so a cross-seed collision would need identical
        // pick sequences over ~128 draws.
        assert_eq!(
            schedule_hash_for_seed(0xDEAD_BEEF),
            schedule_hash_for_seed(0xDEAD_BEEF)
        );
        assert_ne!(
            schedule_hash_for_seed(0xDEAD_BEEF),
            schedule_hash_for_seed(0xCAFE_BABE)
        );
    }

    #[test]
    fn wake_other_task_during_poll() {
        let mut executor = DetExecutor::new(11);
        let (sender, receiver) = shard::channel::<u32>(4);
        let got = Rc::new(Cell::new(0u32));
        let got_clone = Rc::clone(&got);
        executor.spawn(async move {
            let value = receiver.recv().await.expect("sender must not drop first");
            got_clone.set(value);
        });
        // Park the receiver task first.
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(executor.live_tasks(), 1);
        // Sender task wakes the parked receiver mid-run.
        executor.spawn(async move {
            sender.try_send(42).expect("channel has capacity");
        });
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(got.get(), 42);
        assert_eq!(executor.live_tasks(), 0);
    }

    /// The M2 de-risk test: a crossfire `try_send` from plain (non-task) code
    /// must wake a receiver task parked under this executor.
    #[test]
    fn crossfire_try_send_wakes_parked_recv_task() {
        let mut executor = DetExecutor::new(13);
        let (sender, receiver) = shard::channel::<u64>(4);
        let seen: Rc<Cell<u64>> = Rc::new(Cell::new(0));
        let seen_clone = Rc::clone(&seen);
        executor.spawn(async move {
            while let Ok(value) = receiver.recv().await {
                seen_clone.set(seen_clone.get() + value);
            }
        });
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));

        sender.try_send(40).expect("capacity");
        sender.try_send(2).expect("capacity");
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(seen.get(), 42);

        // Dropping the sender ends the recv loop; the task must finish.
        drop(sender);
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(executor.live_tasks(), 0);
    }

    #[test]
    fn timer_wakes_sleeping_task_on_advance() {
        let mut executor = DetExecutor::new(17);
        let timer = executor.timer();
        let fired = Rc::new(Cell::new(false));
        let fired_clone = Rc::clone(&fired);
        executor.spawn(async move {
            timer.sleep(Duration::from_millis(10)).await;
            fired_clone.set(true);
        });
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert!(!fired.get());

        executor.advance_time(Duration::from_millis(9));
        executor.run_until_stalled(100);
        assert!(!fired.get(), "timer fired before its deadline");

        executor.advance_time(Duration::from_millis(1));
        executor.run_until_stalled(100);
        assert!(fired.get());
        assert_eq!(executor.live_tasks(), 0);
    }

    #[test]
    fn abort_runs_destructors_and_ignores_stale_wake() {
        struct SetOnDrop(Rc<Cell<bool>>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let mut executor = DetExecutor::new(19);
        let (sender, receiver) = shard::channel::<u32>(1);
        let dropped = Rc::new(Cell::new(false));
        let guard = SetOnDrop(Rc::clone(&dropped));
        let id = executor.spawn(async move {
            let _guard = guard;
            // Parks forever; only abort can end this task.
            let _ = receiver.recv().await;
        });
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert!(executor.abort(id));
        assert!(
            dropped.get(),
            "abort must drop the future and run Drop impls"
        );
        assert!(!executor.abort(id), "second abort must observe a dead task");

        // A send after abort raises a stale wake (crossfire may still hold the
        // waker); the dead generation must swallow it without repolling.
        let _ = sender.try_send(1);
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { polls: 0 }
        ));
        assert_eq!(executor.live_tasks(), 0);
    }

    #[test]
    fn budget_exhaustion_on_spin_task() {
        let mut executor = DetExecutor::new(23);
        executor.spawn(async {
            loop {
                yield_once().await;
            }
        });
        assert_eq!(
            executor.run_until_stalled(64),
            RunOutcome::BudgetExhausted { polls: 64 }
        );
    }

    #[test]
    fn slot_reuse_bumps_generation() {
        let mut executor = DetExecutor::new(29);
        let first = executor.spawn(async {});
        executor.run_until_stalled(10);
        let second = executor.spawn(async {});
        assert_ne!(first, second, "recycled slot must carry a new generation");
        assert!(
            !executor.abort(first),
            "stale id must not abort the new task"
        );
        assert!(executor.abort(second));
    }

    #[test]
    fn pending_spawn_from_task_runs_next_turn() {
        let mut executor = DetExecutor::new(3);
        let spawner = executor.spawner();
        let ran = Rc::new(Cell::new(false));
        let ran_clone = Rc::clone(&ran);
        executor.spawn(async move {
            yield_once().await;
            spawner.push(
                0,
                Box::pin(async move {
                    ran_clone.set(true);
                }),
            );
        });
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert!(ran.get(), "task-spawned future must run before quiescence");
        assert_eq!(executor.live_tasks(), 0);
    }

    fn schedule_hash_with_spawns(seed: u64) -> u64 {
        let mut executor = DetExecutor::new(seed);
        let spawner = executor.spawner();
        for _ in 0..8 {
            let spawner = Rc::clone(&spawner);
            executor.spawn(async move {
                for i in 0..4u8 {
                    yield_once().await;
                    spawner.push(i % 3, Box::pin(yield_once()));
                }
            });
        }
        assert!(matches!(
            executor.run_until_stalled(100_000),
            RunOutcome::Quiescent { .. }
        ));
        executor.schedule_hash()
    }

    #[test]
    fn spawns_keep_schedule_deterministic() {
        assert_eq!(
            schedule_hash_with_spawns(0xABCD),
            schedule_hash_with_spawns(0xABCD)
        );
        assert_ne!(
            schedule_hash_with_spawns(0xABCD),
            schedule_hash_with_spawns(0x1234)
        );
    }

    #[test]
    fn abort_replica_spawned_tears_down_detached_tasks() {
        let mut executor = DetExecutor::new(5);
        let spawner = executor.spawner();
        let (_tx, receiver) = shard::channel::<u32>(1);
        // Detached task tagged to replica 2 that parks forever on recv.
        spawner.push(
            2,
            Box::pin(async move {
                let _ = receiver.recv().await;
            }),
        );
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(executor.live_tasks(), 1, "detached task should be parked");
        executor.abort_replica_spawned(2);
        assert_eq!(
            executor.live_tasks(),
            0,
            "crash aborts the replica's spawned tasks"
        );
        // A replica with no tracked spawns is a no-op.
        executor.abort_replica_spawned(2);
    }

    #[test]
    fn completed_spawn_is_pruned_from_replica_tracking() {
        let mut executor = DetExecutor::new(31);
        let spawner = executor.spawner();
        // A detached task tagged to replica 4 that completes immediately.
        spawner.push(4, Box::pin(async {}));
        assert!(matches!(
            executor.run_until_stalled(100),
            RunOutcome::Quiescent { .. }
        ));
        assert_eq!(executor.live_tasks(), 0);
        // The finished id must not linger in `spawned`: a normally-completing
        // off-pump task previously leaked its TaskId here for the life of the
        // sim, and a late crash then did O(total-spawns-ever) abort work.
        assert!(
            executor.spawned.get(&4).is_none_or(Vec::is_empty),
            "completed detached spawn must be pruned from replica tracking"
        );
    }
}
