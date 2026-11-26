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

//! How much of its shard's time the long-term rewrites take.
//!
//! A rewrite runs on the shard that serves the sends to its partition, and
//! copies and sums once more every byte that the producers sent. Flat out,
//! it holds the shard a stride at a time for tens of milliseconds a
//! segment, and a send that arrives meanwhile waits behind its strides. So
//! while sends wait on the shard, its rewrites together keep to a share of
//! the time: each counts the time it works, and once their work is more
//! than their share of the time that passed, the one that finds it sleeps
//! until it is not. With no send waiting they run flat out.
//!
//! Only the time between two waits counts as work, so a rewrite that waits
//! for a store elsewhere, to read the blobs or to take the segment, does
//! not use up the share. A store on the node itself does that I/O with the
//! node's own cores, so a wait for it is work. A rewrite always works a
//! stride before it sleeps, so it always gets on.

use std::cell::Cell;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;

#[cfg(test)]
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use server_common::yield_to_reactor;

use crate::placed_read::{REWRITE_PACED_YIELD_BYTES, REWRITE_YIELD_BYTES};

/// The share of the time that the rewrites of a shard work while sends
/// wait on it, in thousandths.
///
/// Redpanda's reconciler, which rewrites the batches of its cloud topics
/// the same way, runs in a scheduling group of 150 shares against 1000 for
/// produce: 150 of every 1150 parts of a core that both want.
pub(crate) const REWRITE_SHARE_PERMILLE: u64 = 130;

/// The time whose share the rewrites may bank, and then work at once when
/// sends start to wait.
///
/// Its share is about one stride of a rewrite's work, so that a send that
/// arrives at rewrites that have not worked for a while waits for one
/// stride and not for a run of them. A rewrite that sleeps looks again
/// this often at whether sends still wait.
pub(crate) const REWRITE_SHARE_WINDOW: Duration = Duration::from_millis(2);

/// What the rewrites read of their shard to keep to their share.
pub trait RewriteClock {
    /// The shard's monotonic clock, in microseconds.
    fn now_micros(&self) -> u64;

    /// Wait `duration` on the shard's timer.
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;

    /// Whether sends wait on the shard that the rewrites would hold up.
    fn sends_waiting(&self) -> bool;
}

/// The share of a shard's time that its rewrites take together.
pub struct RewriteBudget {
    clock: Rc<dyn RewriteClock>,
    /// Whether the store that the rewrites read and write is on this node.
    store_on_node: bool,
    /// Nanoseconds of work the rewrites may do before one sleeps. Below
    /// zero, work they did past their share.
    credit: Cell<i64>,
    /// When the credit was last counted.
    counted_at: Cell<u64>,
}

impl RewriteBudget {
    /// The budget of rewrites that keep to their share by `clock`, from now
    /// on, and that use a store on this node when `store_on_node`.
    #[must_use]
    pub fn new(clock: Rc<dyn RewriteClock>, store_on_node: bool) -> Self {
        let now = clock.now_micros();
        Self {
            clock,
            store_on_node,
            credit: Cell::new(banked()),
            counted_at: Cell::new(now),
        }
    }

    /// Whether the rewrites keep to their share now.
    fn holds_back(&self) -> bool {
        self.clock.sends_waiting()
    }

    /// Count `worked` microseconds that a rewrite worked, and the time that
    /// passed. Returns how long the rewrite sleeps to keep to the share,
    /// `None` when it need not.
    fn charge(&self, worked: u64) -> Option<Duration> {
        let now = self.clock.now_micros();
        let passed = now.saturating_sub(self.counted_at.replace(now));
        if !self.holds_back() {
            self.credit.set(banked());
            return None;
        }
        let earned = nanos(passed.saturating_mul(REWRITE_SHARE_PERMILLE));
        let credit = self
            .credit
            .get()
            .saturating_add(earned)
            .saturating_sub(nanos(worked.saturating_mul(1000)))
            .min(banked());
        self.credit.set(credit);
        // The sleep earns the credit back at the next count.
        (credit < 0)
            .then(|| Duration::from_micros(credit.unsigned_abs().div_ceil(REWRITE_SHARE_PERMILLE)))
    }

    /// Sleep `rest`, a window at a time, and stop once sends no longer
    /// wait: a send answered or a partition past its backlog limit does not
    /// tell the rewrites, so a sleeper looks for itself.
    async fn sleep_off(&self, mut rest: Duration) {
        while !rest.is_zero() && self.holds_back() {
            let window = rest.min(REWRITE_SHARE_WINDOW);
            self.clock.sleep(window).await;
            rest = rest.saturating_sub(window);
        }
    }
}

impl fmt::Debug for RewriteBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RewriteBudget")
            .field("store_on_node", &self.store_on_node)
            .field("credit", &self.credit.get())
            .field("counted_at", &self.counted_at.get())
            .finish_non_exhaustive()
    }
}

/// What one rewrite worked since it last counted it against its budget.
///
/// Dropped, it counts the work it has not counted yet, without a sleep: the
/// next rewrite to give way sleeps it off.
pub(crate) struct RewriteShare {
    /// `None` for a rewrite that keeps to no share.
    budget: Option<Rc<RewriteBudget>>,
    /// Microseconds the rewrite worked since it last gave way.
    worked: u64,
    /// When the rewrite last went on after a wait.
    resumed_at: u64,
    /// Bytes the rewrite took since it last gave way.
    since_yield: usize,
}

impl RewriteShare {
    /// A rewrite that keeps to no share, and only gives way between its
    /// strides.
    pub(crate) const fn unpaced() -> Self {
        Self {
            budget: None,
            worked: 0,
            resumed_at: 0,
            since_yield: 0,
        }
    }

    /// A rewrite that keeps to the share of `budget`, from now on.
    pub(crate) fn paced(budget: Rc<RewriteBudget>) -> Self {
        let now = budget.clock.now_micros();
        Self {
            budget: Some(budget),
            worked: 0,
            resumed_at: now,
            since_yield: 0,
        }
    }

    /// The budget the rewrite keeps to, for work it goes on with later.
    pub(crate) fn budget(&self) -> Option<Rc<RewriteBudget>> {
        self.budget.clone()
    }

    /// Bytes the rewrite may take before it gives way next. Gives way first
    /// when it took a whole stride.
    ///
    /// The stride is read again after the yield: a send that arrived in the
    /// meantime shortens it.
    pub(crate) async fn room(&mut self) -> usize {
        if self.since_yield >= self.stride() {
            self.give_way().await;
        }
        self.stride().saturating_sub(self.since_yield)
    }

    /// The rewrite took `len` bytes of its stride.
    pub(crate) const fn took(&mut self, len: usize) {
        self.since_yield = self.since_yield.saturating_add(len);
    }

    /// Bytes the rewrite copies and sums between two calls of
    /// [`Self::give_way`]: fewer while it keeps to its share.
    fn stride(&self) -> usize {
        if self
            .budget
            .as_ref()
            .is_some_and(|budget| budget.holds_back())
        {
            REWRITE_PACED_YIELD_BYTES
        } else {
            REWRITE_YIELD_BYTES
        }
    }

    /// The rewrite stops to wait for something other than its share: the
    /// time since it went on was work.
    fn pause(&mut self) {
        let Some(budget) = &self.budget else {
            return;
        };
        let now = budget.clock.now_micros();
        self.worked = self
            .worked
            .saturating_add(now.saturating_sub(self.resumed_at));
        self.resumed_at = now;
    }

    /// The rewrite goes on after a wait.
    fn resume(&mut self) {
        if let Some(budget) = &self.budget {
            self.resumed_at = budget.clock.now_micros();
        }
    }

    /// Wait for `store`, a read or a put. The wait is work when the store is
    /// on this node. Otherwise only the polls of `store` are: they look up
    /// the cache and put bytes together on the shard.
    pub(crate) async fn wait_for_store<F: Future>(&mut self, store: F) -> F::Output {
        let Some(clock) = self
            .budget
            .as_ref()
            .filter(|budget| !budget.store_on_node)
            .map(|budget| Rc::clone(&budget.clock))
        else {
            return store.await;
        };
        self.pause();
        let mut store = pin!(store);
        let mut polled = 0_u64;
        let output = poll_fn(|context| {
            let start = clock.now_micros();
            let poll = store.as_mut().poll(context);
            polled = polled.saturating_add(clock.now_micros().saturating_sub(start));
            poll
        })
        .await;
        self.worked = self.worked.saturating_add(polled);
        self.resume();
        output
    }

    /// Between two strides of work: let the shard's other tasks run, or,
    /// while sends wait, sleep for as long as the rewrites worked past
    /// their share.
    pub(crate) async fn give_way(&mut self) {
        self.since_yield = 0;
        let Some(budget) = self.budget.clone() else {
            yield_to_reactor().await;
            return;
        };
        self.pause();
        match budget.charge(std::mem::take(&mut self.worked)) {
            Some(rest) => budget.sleep_off(rest).await,
            None => yield_to_reactor().await,
        }
        self.resume();
    }
}

impl Drop for RewriteShare {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.clone() {
            self.pause();
            let _ = budget.charge(std::mem::take(&mut self.worked));
        }
    }
}

/// The most credit the rewrites hold, in nanoseconds.
fn banked() -> i64 {
    let window = u64::try_from(REWRITE_SHARE_WINDOW.as_micros()).unwrap_or(u64::MAX);
    nanos(window.saturating_mul(REWRITE_SHARE_PERMILLE))
}

fn nanos(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A clock that moves only when a test moves it or a rewrite sleeps.
#[cfg(test)]
#[derive(Default)]
pub struct ManualClock {
    pub now: Cell<u64>,
    /// Microseconds the rewrites slept, all told.
    pub slept: Cell<u64>,
    pub sends_waiting: Cell<bool>,
    /// When sends stop waiting, `None` for never.
    pub sends_stop_at: Cell<Option<u64>>,
}

#[cfg(test)]
impl ManualClock {
    pub fn pass(&self, micros: u64) {
        self.now.set(self.now.get() + micros);
    }

    /// A wait of `micros` that passes in another task, so that none of it
    /// passes while the waiter is polled.
    pub fn waited(self: &Rc<Self>, micros: u64) -> LocalBoxFuture<'static, ()> {
        let clock = Rc::clone(self);
        let (passed, waited) = futures::channel::oneshot::channel();
        compio::runtime::spawn(async move {
            clock.pass(micros);
            let _ = passed.send(());
        })
        .detach();
        async move {
            let _ = waited.await;
        }
        .boxed_local()
    }
}

#[cfg(test)]
impl RewriteClock for ManualClock {
    fn now_micros(&self) -> u64 {
        self.now.get()
    }

    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        let slept = u64::try_from(duration.as_micros()).unwrap();
        self.pass(slept);
        self.slept.set(self.slept.get() + slept);
        async {}.boxed_local()
    }

    fn sends_waiting(&self) -> bool {
        self.sends_waiting.get()
            && self
                .sends_stop_at
                .get()
                .is_none_or(|stop_at| self.now.get() < stop_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(clock: &Rc<ManualClock>, store_on_node: bool) -> Rc<RewriteBudget> {
        Rc::new(RewriteBudget::new(
            Rc::clone(clock) as Rc<dyn RewriteClock>,
            store_on_node,
        ))
    }

    /// A rewrite of `strides` strides of `stride` microseconds of work
    /// each, which waits `fetch` microseconds for its store before every
    /// fourth.
    async fn rewrite(
        budget: Rc<RewriteBudget>,
        clock: &Rc<ManualClock>,
        strides: u64,
        stride: u64,
        fetch: u64,
    ) {
        let mut share = RewriteShare::paced(budget);
        for done in 0..strides {
            if done % 4 == 0 {
                share.wait_for_store(clock.waited(fetch)).await;
            }
            clock.pass(stride);
            share.give_way().await;
        }
    }

    fn share_of(clock: &ManualClock, worked: u64) -> u64 {
        worked * 1000 / clock.now.get()
    }

    fn assert_near_the_share(share: u64) {
        assert!(
            (REWRITE_SHARE_PERMILLE - 2..=REWRITE_SHARE_PERMILLE + 5).contains(&share),
            "worked {share} per mille of the time"
        );
    }

    #[compio::test]
    async fn given_sends_waiting_when_rewriting_should_work_its_share_of_the_time_and_finish() {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);

        rewrite(budget(&clock, false), &clock, 400, 250, 0).await;

        assert_near_the_share(share_of(&clock, 400 * 250));
    }

    #[compio::test]
    async fn given_no_sends_waiting_when_rewriting_should_never_sleep() {
        let clock = Rc::new(ManualClock::default());

        rewrite(budget(&clock, false), &clock, 400, 250, 100).await;

        assert_eq!(clock.slept.get(), 0);
        assert_eq!(clock.now.get(), 400 * 250 + 100 * 100);
    }

    /// A wait for a store elsewhere is not work: it passes for the share as
    /// any other time does, so the rewrite sleeps less. A store on the node
    /// reads with its cores, so a wait for it is work.
    #[compio::test]
    async fn given_waits_for_the_store_when_rewriting_under_sends_should_count_them_as_work_only_on_the_node()
     {
        let elsewhere = Rc::new(ManualClock::default());
        let on_node = Rc::new(ManualClock::default());
        let working = Rc::new(ManualClock::default());
        for clock in [&elsewhere, &on_node, &working] {
            clock.sends_waiting.set(true);
        }

        rewrite(budget(&elsewhere, false), &elsewhere, 400, 250, 5_000).await;
        rewrite(budget(&on_node, true), &on_node, 400, 250, 5_000).await;
        rewrite(budget(&working, false), &working, 400, 250, 0).await;

        assert!(
            elsewhere.slept.get() < working.slept.get(),
            "slept {} with waits for the store, {} without",
            elsewhere.slept.get(),
            working.slept.get()
        );
        assert_near_the_share(share_of(&on_node, 400 * 250 + 100 * 5_000));
    }

    /// A stride far longer than the banked share still ends, and the
    /// rewrite sleeps it off before the next.
    #[compio::test]
    async fn given_strides_longer_than_the_window_when_rewriting_under_sends_should_keep_to_the_share()
     {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);
        let stride = 5 * u64::try_from(REWRITE_SHARE_WINDOW.as_micros()).unwrap();

        rewrite(budget(&clock, false), &clock, 20, stride, 0).await;

        assert_near_the_share(share_of(&clock, 20 * stride));
    }

    /// Two rewrites of one shard keep to one share between them, not to
    /// one each. Each goes on only once the other gave way, as on the
    /// shard's one thread.
    #[compio::test]
    async fn given_two_rewrites_of_one_shard_when_rewriting_under_sends_should_share_one_share() {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);
        let budget = budget(&clock, false);
        let mut first = RewriteShare::paced(Rc::clone(&budget));
        let mut second = RewriteShare::paced(budget);

        for _ in 0..200 {
            first.resume();
            clock.pass(250);
            first.give_way().await;
            second.resume();
            clock.pass(250);
            second.give_way().await;
        }

        assert_near_the_share(share_of(&clock, 400 * 250));
    }

    /// Once sends stop waiting, the rewrite goes on at full speed at once,
    /// whatever it owed.
    #[compio::test]
    async fn given_sends_that_stop_waiting_when_rewriting_should_stop_sleeping() {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);
        let mut share = RewriteShare::paced(budget(&clock, false));
        for _ in 0..100 {
            clock.pass(250);
            share.give_way().await;
        }
        let slept = clock.slept.get();
        assert!(slept > 0);

        clock.sends_waiting.set(false);
        for _ in 0..100 {
            clock.pass(250);
            share.give_way().await;
        }

        assert_eq!(clock.slept.get(), slept);
    }

    /// A rewrite that owes a long sleep wakes within a window of the moment
    /// sends stop waiting, as when the last one is answered or a partition
    /// goes past its backlog limit, and not at the end of the sleep.
    #[compio::test]
    async fn given_sends_that_stop_waiting_while_a_rewrite_sleeps_should_wake_it() {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);
        let mut share = RewriteShare::paced(budget(&clock, false));
        clock.pass(1_000_000);
        clock.sends_stop_at.set(Some(clock.now.get() + 5_000));

        share.give_way().await;

        let window = u64::try_from(REWRITE_SHARE_WINDOW.as_micros()).unwrap();
        assert!(
            (5_000..5_000 + window).contains(&clock.slept.get()),
            "slept {}",
            clock.slept.get()
        );
    }

    /// The polls of a wait for a store elsewhere look up the cache and put
    /// bytes together on the shard, so their time is work. The time between
    /// them is not.
    #[compio::test]
    async fn given_a_wait_for_a_store_elsewhere_when_its_polls_work_should_charge_only_the_polls() {
        let polling = Rc::new(ManualClock::default());
        let waiting = Rc::new(ManualClock::default());
        for clock in [&polling, &waiting] {
            clock.sends_waiting.set(true);
        }
        let mut polled = RewriteShare::paced(budget(&polling, false));
        let mut waited = RewriteShare::paced(budget(&waiting, false));

        polled
            .wait_for_store(async {
                polling.pass(10_000);
                polling.waited(1_000).await;
            })
            .await;
        polled.give_way().await;
        waited.wait_for_store(waiting.waited(11_000)).await;
        waited.give_way().await;

        assert_eq!(waiting.slept.get(), 0);
        assert!(
            polling.slept.get() > 60_000,
            "slept {}",
            polling.slept.get()
        );
    }

    /// Work that no yield followed, as a put to a store on the node that
    /// ends a rewrite, is counted once the rewrite ends. The next rewrite
    /// sleeps it off.
    #[compio::test]
    async fn given_work_after_the_last_yield_when_a_rewrite_ends_should_charge_it_to_the_next() {
        let clock = Rc::new(ManualClock::default());
        clock.sends_waiting.set(true);
        let budget = budget(&clock, true);
        let mut share = RewriteShare::paced(Rc::clone(&budget));
        share.wait_for_store(clock.waited(100_000)).await;
        drop(share);

        RewriteShare::paced(budget).give_way().await;

        assert!(clock.slept.get() > 100_000, "slept {}", clock.slept.get());
    }

    /// A send that arrives while the rewrite gives way shortens the stride
    /// it goes on with.
    #[compio::test]
    async fn given_a_send_that_arrives_during_a_yield_when_taking_room_should_read_the_stride_again()
     {
        let clock = Rc::new(ManualClock::default());
        let mut share = RewriteShare::paced(budget(&clock, false));
        share.took(REWRITE_YIELD_BYTES);
        let arriving = Rc::clone(&clock);
        compio::runtime::spawn(async move { arriving.sends_waiting.set(true) }).detach();

        let room = share.room().await;

        assert_eq!(room, REWRITE_PACED_YIELD_BYTES);
    }
}
