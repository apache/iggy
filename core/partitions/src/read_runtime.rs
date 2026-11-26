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

//! The clock, the timer and the task spawner of the reads that wait on the
//! object store.

use std::future::Future;
use std::pin::pin;
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures::future::{self, Either, LocalBoxFuture};
use message_bus::MessageBus;

/// The clock, the timer and the task spawner that reads of the object store
/// run on.
///
/// A shard gives its bus, so that a read keeps to the clock that set the
/// deadline of its poll, and the simulator runs the read in virtual time.
pub trait ReadRuntime {
    /// Monotonic time, on the timeline of the poll deadlines.
    fn now(&self) -> Instant;

    /// A timer of `duration` that counts from this call, not from its first
    /// poll.
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'_, ()>;

    /// Run `future` as a detached task.
    fn spawn(&self, future: LocalBoxFuture<'static, ()>);
}

impl dyn ReadRuntime + '_ {
    /// The output of `future`, or `None` when `duration` passes first.
    #[allow(clippy::future_not_send)]
    pub async fn timeout<T>(
        &self,
        duration: Duration,
        future: impl Future<Output = T>,
    ) -> Option<T> {
        match future::select(pin!(future), self.sleep(duration)).await {
            Either::Left((output, _)) => Some(output),
            Either::Right(((), _)) => None,
        }
    }
}

impl<B: MessageBus> ReadRuntime for B {
    fn now(&self) -> Instant {
        self.monotonic_now()
    }

    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'_, ()> {
        MessageBus::sleep(self, duration).boxed_local()
    }

    fn spawn(&self, future: LocalBoxFuture<'static, ()>) {
        MessageBus::spawn(self, future);
    }
}

/// The compio runtime and the system clock, for a store that no shard gave a
/// runtime.
pub struct CompioRuntime;

impl ReadRuntime for CompioRuntime {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'_, ()> {
        // `compio::time::sleep` reads the clock at its first poll.
        compio::time::sleep_until(Instant::now() + duration).boxed_local()
    }

    fn spawn(&self, future: LocalBoxFuture<'static, ()>) {
        compio::runtime::spawn(future).detach();
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::time::Duration;

    use super::{CompioRuntime, ReadRuntime};

    #[compio::test]
    async fn given_a_ready_future_when_timing_it_out_should_return_its_output() {
        let runtime: &dyn ReadRuntime = &CompioRuntime;

        let output = runtime.timeout(Duration::ZERO, future::ready(7_u8)).await;

        assert_eq!(output, Some(7));
    }

    #[compio::test]
    async fn given_a_future_that_never_ends_when_its_time_passes_should_return_none() {
        let runtime: &dyn ReadRuntime = &CompioRuntime;

        let output = runtime
            .timeout(Duration::from_millis(1), future::pending::<u8>())
            .await;

        assert_eq!(output, None);
    }
}
