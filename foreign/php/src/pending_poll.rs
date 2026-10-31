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

use std::time::Duration;

use ext_php_rs::{exception::PhpResult, php_class, php_impl, types::Zval};
use iggy::prelude::{IggyError, PolledMessages as RustPolledMessages};
use tokio::task::JoinHandle;

use crate::client::optional_unsigned;
use crate::error::to_php_exception;
use crate::receive_message::PolledMessages;
use crate::runtime::runtime;

const ENDED: &str = "the poll already ended";

pub(crate) type PollTask = JoinHandle<Result<RustPolledMessages, IggyError>>;

/// A poll that runs in the background, from Client::startPollMessages(). Wait for its batch in
/// steps, or cancel it. Destroying it cancels the poll.
#[php_class]
#[php(name = "Iggy\\PendingPoll")]
pub struct PendingPoll {
    task: Option<PollTask>,
}

impl From<PollTask> for PendingPoll {
    fn from(task: PollTask) -> Self {
        Self { task: Some(task) }
    }
}

#[php_impl]
impl PendingPoll {
    /// The batch of the poll. Without `timeout_micros`, the call blocks until the poll ends.
    /// With it, the call returns null while the poll still runs. A deferred poll that found no
    /// message in its max wait returns an empty batch. A failed poll throws, and so does a call
    /// after the batch was returned or the poll was cancelled.
    pub fn wait(&mut self, timeout_micros: Option<&Zval>) -> PhpResult<Option<PolledMessages>> {
        let timeout = optional_unsigned::<u64>(timeout_micros, "timeout_micros")?;
        let task = self.task.as_mut().ok_or_else(|| to_php_exception(ENDED))?;
        // The timer needs the runtime context, so it is built inside `block_on`.
        let joined = runtime().block_on(async {
            match timeout {
                Some(micros) => tokio::time::timeout(Duration::from_micros(micros), task)
                    .await
                    .ok(),
                None => Some(task.await),
            }
        });
        let Some(joined) = joined else {
            return Ok(None);
        };
        self.task = None;
        match joined {
            Ok(polled) => polled
                .map(|batch| Some(PolledMessages::from(batch)))
                .map_err(to_php_exception),
            Err(error) => Err(to_php_exception(format!("the poll failed: {error}"))),
        }
    }

    /// Drops the request. A parked deferred poll is then cancelled on the server, and one with
    /// CommitPolicy::onRead() that the server had not answered stores nothing. It returns after
    /// the request was dropped and does nothing after the poll ended.
    pub fn cancel(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            // An aborted task drops the request before the join ends.
            let _cancelled = runtime().block_on(task);
        }
    }
}

impl Drop for PendingPoll {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
