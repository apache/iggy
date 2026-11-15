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

use std::any::TypeId;
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::generic::{self, ContextExt, Runtime};
use pyo3_async_runtimes::tokio::{get_current_locals, get_runtime, scope};
use tokio::task::{JoinError, JoinHandle};

/// How long Python waits at exit for the threads inside the exit gate. A thread holds its pass
/// while it hands a value to Python or runs a consumer callback, so it usually leaves within
/// milliseconds of taking the GIL. The bound keeps a blocked callback from stalling the exit, and
/// Python then finalizes as it would without the gate.
const EXIT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

const INTERPRETER_EXITING: &str = "the Python interpreter is exiting";

/// Every thread that Python did not start enters Python through this gate. Python 3.11 to 3.13
/// end such a thread with `pthread_exit` when it takes the GIL after finalization starts, which
/// aborts the process because the unwind crosses Rust frames. Python 3.14 hangs the thread
/// instead. Python runs `atexit` handlers before finalization starts, so an `atexit` handler
/// closes the gate and waits for the threads inside.
static EXIT_GATE: ExitGate = ExitGate::new();

/// Registers the `atexit` handler that closes the exit gate.
pub(crate) fn register_exit_gate(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let handler = wrap_pyfunction!(close_exit_gate, module)?;
    module
        .py()
        .import("atexit")?
        .call_method1("register", (handler,))?;
    Ok(())
}

/// Converts an SDK future to an asyncio future, as `pyo3_async_runtimes::tokio::future_into_py`
/// does, and hands its result to Python through the exit gate. A unit result resolves to `None`.
/// Behind the closed gate the call fails at once.
pub(crate) fn future_into_py<F, T>(py: Python<'_>, future: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    start_call()?;
    generic::future_into_py::<GatedTokio, _, _>(py, async move { future.await.map(Resolved) })
}

/// Runs an SDK future in the coroutine of an `async fn` pymethod, which pyo3 polls on the thread
/// of the asyncio event loop. A poll that completes hands its value to Python in the same call,
/// and a cancel drops the future, so a cancel-safe SDK future loses nothing. Behind the closed
/// exit gate the call fails at once.
///
/// Each poll enters the Tokio runtime, because the SDK spawns tasks and arms timers. The waker of
/// the coroutine takes the GIL, so it runs on the blocking pool instead of the Tokio worker that
/// wakes it, and through the exit gate.
pub(crate) async fn on_event_loop<T>(future: impl Future<Output = PyResult<T>>) -> PyResult<T> {
    start_call()?;
    let mut future = pin!(future);
    poll_fn(|cx| {
        let waker = Waker::from(Arc::new(BlockingWaker(cx.waker().clone())));
        let _runtime = get_runtime().enter();
        future.as_mut().poll(&mut Context::from_waker(&waker))
    })
    .await
}

/// Takes the GIL on a thread that Python did not start, unless the interpreter is exiting.
pub(crate) fn attach_unless_exiting<R>(
    f: impl for<'py> FnOnce(Python<'py>) -> PyResult<R>,
) -> PyResult<R> {
    EXIT_GATE
        .run(|| Python::attach(f))
        .unwrap_or_else(|| Err(PyRuntimeError::new_err(INTERPRETER_EXITING)))
}

/// Polls a future that can take the GIL on a thread that Python did not start, inside the exit
/// gate. The future of `into_future` does so when Python drops its awaitable unfinished. A poll
/// behind the closed gate fails the future instead.
pub(crate) async fn gated<T>(future: impl Future<Output = PyResult<T>>) -> PyResult<T> {
    let mut future = pin!(future);
    poll_fn(|cx| {
        EXIT_GATE
            .run(|| future.as_mut().poll(cx))
            .unwrap_or_else(|| Poll::Ready(Err(PyRuntimeError::new_err(INTERPRETER_EXITING))))
    })
    .await
}

/// Whether the exit gate is closed, so that no thread of the SDK can call into Python.
pub(crate) fn exit_gate_closed() -> bool {
    EXIT_GATE.lock().closed_by.is_some()
}

/// Admits a call that a Python thread starts. The result of a call behind the closed gate would
/// never reach Python, so its awaitable would never complete.
fn start_call() -> PyResult<()> {
    if EXIT_GATE.reopen() {
        Ok(())
    } else {
        Err(PyRuntimeError::new_err(INTERPRETER_EXITING))
    }
}

#[pyfunction]
fn close_exit_gate(py: Python<'_>) {
    // The threads inside wait for the GIL that this thread holds.
    py.detach(|| EXIT_GATE.close(EXIT_DRAIN_TIMEOUT));
}

/// The result of an awaitable. A unit result becomes `None`, as the result of a Python function
/// without a return value, where pyo3 converts `()` to an empty tuple.
struct Resolved<T>(T);

impl<'py, T> IntoPyObject<'py> for Resolved<T>
where
    T: IntoPyObject<'py> + 'static,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            return Ok(py.None().into_bound(py));
        }
        self.0.into_bound_py_any(py)
    }
}

/// The Tokio runtime of `pyo3_async_runtimes::tokio`, handing results to Python through the exit
/// gate.
struct GatedTokio;

impl Runtime for GatedTokio {
    type JoinError = JoinError;
    type JoinHandle = JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        get_runtime().spawn(fut)
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        get_runtime().spawn_blocking(move || {
            EXIT_GATE.run(f);
        })
    }
}

impl ContextExt for GatedTokio {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        // The future of `scope` names `R`, which this signature does not bound by 'static, while
        // the block is 'static through what it captures.
        Box::pin(async move { scope(locals, fut).await })
    }

    fn get_task_locals() -> Option<TaskLocals> {
        // The task-local that `scope` sets is private to pyo3_async_runtimes. Its reader falls
        // back to the running loop, as every caller of this method does on `None`.
        Python::attach(|py| get_current_locals(py).ok())
    }
}

struct BlockingWaker(Waker);

impl Wake for BlockingWaker {
    fn wake(self: Arc<Self>) {
        get_runtime().spawn_blocking(move || EXIT_GATE.run(|| self.0.wake_by_ref()));
    }
}

/// Counts the threads inside, so that `close` can wait for them.
struct ExitGate {
    state: Mutex<GateState>,
    emptied: Condvar,
}

struct GateState {
    inside: usize,
    /// The thread that closed the gate, while the gate is closed.
    closed_by: Option<ThreadId>,
}

impl ExitGate {
    const fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                inside: 0,
                closed_by: None,
            }),
            emptied: Condvar::new(),
        }
    }

    /// Runs `f` inside the gate, or returns `None` if the gate is closed.
    fn run<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
        let _pass = self.enter()?;
        Some(f())
    }

    /// Closes the gate and waits until the threads inside leave, at most for `timeout`.
    fn close(&self, timeout: Duration) {
        let current = thread::current().id();
        let mut state = self.lock();
        state.closed_by = Some(current);
        let _state = self
            .emptied
            .wait_timeout_while(state, timeout, |state| state.inside > 0)
            .unwrap_or_else(PoisonError::into_inner);
    }

    /// Opens the gate again if the current thread closed it, and returns whether the gate is
    /// open. Python runs the remaining `atexit` handlers on that thread, and a result that one of
    /// them awaits must reach it. Python then finalizes as it would without the gate. Calls from
    /// other threads, such as daemon threads, leave the gate closed.
    fn reopen(&self) -> bool {
        let mut state = self.lock();
        if state
            .closed_by
            .is_some_and(|closer| closer == thread::current().id())
        {
            state.closed_by = None;
        }
        state.closed_by.is_none()
    }

    fn enter(&self) -> Option<Pass<'_>> {
        let mut state = self.lock();
        if state.closed_by.is_some() {
            return None;
        }
        state.inside += 1;
        Some(Pass(self))
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        // No critical section can panic halfway, so the state of a poisoned lock is consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Held by a thread inside the gate.
struct Pass<'a>(&'a ExitGate);

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.inside -= 1;
        if state.inside == 0 {
            self.0.emptied.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    use super::*;

    const LONG_WAIT: Duration = Duration::from_secs(10);
    const SHORT_WAIT: Duration = Duration::from_millis(50);

    #[test]
    fn closed_gate_refuses_entry() {
        let gate = ExitGate::new();
        assert_eq!(gate.run(|| "inside"), Some("inside"));

        gate.close(SHORT_WAIT);
        assert_eq!(gate.run(|| "inside"), None);
    }

    #[test]
    fn close_returns_after_the_threads_inside_leave() {
        let gate = &ExitGate::new();
        let left = &AtomicBool::new(false);
        let (entered, wait_entered) = mpsc::channel();
        thread::scope(|scope| {
            scope.spawn(move || {
                gate.run(|| {
                    entered.send(()).unwrap();
                    // Gives `close` time to start waiting.
                    thread::sleep(SHORT_WAIT);
                    left.store(true, Ordering::SeqCst);
                })
            });
            wait_entered.recv().unwrap();
            gate.close(LONG_WAIT);
            assert!(left.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn close_stops_waiting_after_the_timeout() {
        let gate = &ExitGate::new();
        let (entered, wait_entered) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        thread::scope(|scope| {
            scope.spawn(move || {
                gate.run(|| {
                    entered.send(()).unwrap();
                    wait_release.recv().unwrap();
                })
            });
            wait_entered.recv().unwrap();
            gate.close(SHORT_WAIT);
            assert_eq!(gate.lock().inside, 1);
            release.send(()).unwrap();
        });
    }

    #[test]
    fn only_the_closing_thread_reopens_the_gate() {
        let gate = &ExitGate::new();
        assert!(gate.reopen());
        gate.close(SHORT_WAIT);

        let reopened_by_another_thread =
            thread::scope(|scope| scope.spawn(|| gate.reopen()).join().unwrap());
        assert!(!reopened_by_another_thread);
        assert_eq!(gate.run(|| "inside"), None);

        assert!(gate.reopen());
        assert_eq!(gate.run(|| "inside"), Some("inside"));
    }
}
