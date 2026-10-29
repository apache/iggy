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

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use pyo3_async_runtimes::tokio::get_runtime;

/// Runs an SDK future in the coroutine of an `async fn` pymethod, which pyo3 polls on the thread
/// of the asyncio event loop. A poll that completes hands its value to Python in the same call,
/// and a cancel drops the future, so a cancel-safe SDK future loses nothing.
///
/// Each poll enters the Tokio runtime, because the SDK spawns tasks and arms timers. The waker of
/// the coroutine takes the GIL, so it runs on the blocking pool instead of the Tokio worker that
/// wakes it.
pub(crate) async fn on_event_loop<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let waker = Waker::from(Arc::new(BlockingWaker(cx.waker().clone())));
        let _runtime = get_runtime().enter();
        future.as_mut().poll(&mut Context::from_waker(&waker))
    })
    .await
}

struct BlockingWaker(Waker);

impl Wake for BlockingWaker {
    fn wake(self: Arc<Self>) {
        get_runtime().spawn_blocking(move || self.0.wake_by_ref());
    }
}
