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

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use iggy::prelude::{IggyError, PolledMessages as RustPolledMessages};
use tokio::task::{AbortHandle, JoinHandle};

use crate::{RUNTIME, ffi};

const ENDED: &str = "the poll already ended";

type PollTask = JoinHandle<Result<RustPolledMessages, IggyError>>;

/// The C++ handle of a poll that runs on the runtime. Dropping the task drops the request,
/// which cancels a parked deferred poll on the server.
pub struct PendingPoll {
    task: Mutex<Option<PollTask>>,
    // Aborts without the lock, so a cancel does not wait for a running wait to time out.
    abort: AbortHandle,
}

impl PendingPoll {
    pub(crate) fn new(task: PollTask) -> Self {
        Self {
            abort: task.abort_handle(),
            task: Mutex::new(Some(task)),
        }
    }

    fn task(&self) -> MutexGuard<'_, Option<PollTask>> {
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn wait(
        &self,
        timeout_micros: u64,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<ffi::PollOutcome, String> {
        *error_info = ffi::ErrorInfo::default();
        let mut task = self.task();
        let handle = task
            .as_mut()
            .ok_or_else(|| format!("Could not wait for the poll: {ENDED}"))?;
        let joined = RUNTIME.block_on(async {
            tokio::time::timeout(Duration::from_micros(timeout_micros), handle).await
        });
        let Ok(joined) = joined else {
            return Ok(ffi::PollOutcome {
                ready: false,
                batch: RustPolledMessages::empty().into(),
            });
        };
        task.take();
        match joined {
            Ok(Ok(batch)) => Ok(ffi::PollOutcome {
                ready: true,
                batch: batch.into(),
            }),
            Ok(Err(error)) => {
                error_info.record(&error);
                Err(format!("Could not poll messages: {error}"))
            }
            Err(error) if error.is_cancelled() => {
                Err("Could not poll messages: the poll was cancelled".to_owned())
            }
            Err(error) => Err(format!("Could not poll messages: {error}")),
        }
    }

    pub fn cancel(&self) {
        self.abort.abort();
        if let Some(handle) = self.task().take() {
            // The task drops the request when it is aborted, so this returns after the drop.
            let _cancelled = RUNTIME.block_on(handle);
        }
    }
}

impl Drop for PendingPoll {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    #[test]
    fn given_a_parked_poll_when_cancelled_should_end_it_and_refuse_a_later_wait() {
        let task = RUNTIME.spawn(pending::<Result<RustPolledMessages, IggyError>>());
        let poll = PendingPoll::new(task);
        let mut error_info = ffi::ErrorInfo::default();

        let outcome = poll
            .wait(1_000, &mut error_info)
            .expect("a running poll times out");
        assert!(!outcome.ready, "a parked poll must not be ready");

        poll.cancel();
        poll.cancel();
        let refused = poll.wait(1_000, &mut error_info).err();
        assert_eq!(
            refused.as_deref(),
            Some("Could not wait for the poll: the poll already ended")
        );
    }

    #[test]
    fn given_a_finished_poll_when_waited_should_return_its_batch_once() {
        let task = RUNTIME.spawn(async { Ok(RustPolledMessages::empty()) });
        let poll = PendingPoll::new(task);
        let mut error_info = ffi::ErrorInfo::default();

        let outcome = poll
            .wait(5_000_000, &mut error_info)
            .expect("a finished poll returns its batch");
        assert!(outcome.ready, "a finished poll must be ready");
        assert!(outcome.batch.messages.is_empty());
        assert!(poll.wait(1_000, &mut error_info).is_err());
    }

    #[test]
    fn given_a_failed_poll_when_waited_should_record_its_error() {
        let task = RUNTIME.spawn(async { Err(IggyError::InvalidConfiguration) });
        let poll = PendingPoll::new(task);
        let mut error_info = ffi::ErrorInfo::default();

        assert!(poll.wait(5_000_000, &mut error_info).is_err());
        assert_eq!(error_info.kind, "invalid_configuration");
    }
}
