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

use std::sync::Arc;

use ext_php_rs::{exception::PhpResult, php_class, php_impl, types::ZendCallable};
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit as RustAutoCommit, AutoCommitWhen as RustAutoCommitWhen,
    IggyConsumer as RustIggyConsumer, IggyDuration,
};
use tokio::sync::Mutex;

use crate::error::to_php_exception;
use crate::receive_message::ReceiveMessage;
use crate::runtime::runtime;

/// A PHP class representing the Iggy consumer.
#[php_class]
#[php(name = "Iggy\\Consumer")]
pub struct IggyConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
    pub(crate) name: String,
    pub(crate) stream: String,
    pub(crate) topic: String,
}

#[php_impl]
impl IggyConsumer {
    /// Get the last consumed offset or null if no offset has been consumed yet.
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.with_consumer(|inner| inner.get_last_consumed_offset(partition_id))
    }

    /// Get the last stored offset or null if no offset has been stored yet.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.with_consumer(|inner| inner.get_last_stored_offset(partition_id))
    }

    /// Gets the name of the consumer group.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Gets the current partition id or 0 if no messages have been polled yet.
    pub fn partition_id(&self) -> u32 {
        self.with_consumer(RustIggyConsumer::partition_id)
    }

    /// Gets the stream identifier this consumer is configured for.
    pub fn stream(&self) -> String {
        self.stream.clone()
    }

    /// Gets the topic identifier this consumer is configured for.
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    /// Stores the provided offset for the provided partition id.
    ///
    /// If partition_id is null, at least one message must have been polled first.
    pub fn store_offset(&self, offset: u64, partition_id: Option<u32>) -> PhpResult {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let inner = inner.lock().await;
            let partition_id = Self::partition_id_or_current_after_poll(&inner, partition_id)?;
            inner
                .store_offset(offset, partition_id)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Deletes the stored offset for the provided partition id.
    ///
    /// If partition_id is null, at least one message must have been polled first.
    pub fn delete_offset(&self, partition_id: Option<u32>) -> PhpResult {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let inner = inner.lock().await;
            let partition_id = Self::partition_id_or_current_after_poll(&inner, partition_id)?;
            inner
                .delete_offset(partition_id)
                .await
                .map_err(to_php_exception)
        })
    }

    /// Consumes messages with a PHP callback.
    ///
    /// The callback is called as callback(ReceiveMessage $message). A finite limit is required.
    ///
    /// With AutoCommit::when(), offsets may already be queued for commit before the
    /// PHP callback runs. Use AutoCommit::disabled() and call storeOffset() after a
    /// successful callback when at-least-once callback processing is required.
    pub fn consume_messages(&self, callback: ZendCallable, limit: u32) -> PhpResult<u32> {
        // TODO: Add an iterator-style API like Python's iter_messages() so callers
        // can compose consumption with generators or fibers instead of callbacks only.
        let mut consumed = 0;

        while consumed < limit {
            let Some(message) = self.next_message()? else {
                break;
            };

            callback
                .try_call(vec![&message])
                .map_err(to_php_exception)?;
            consumed += 1;
        }

        Ok(consumed)
    }
}

impl IggyConsumer {
    fn with_consumer<T>(&self, f: impl FnOnce(&RustIggyConsumer) -> T) -> T {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let inner = inner.lock().await;
            f(&inner)
        })
    }

    fn next_message(&self) -> PhpResult<Option<ReceiveMessage>> {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            let mut inner = inner.lock().await;

            match inner.next().await {
                Some(Ok(message)) => Ok(Some(ReceiveMessage {
                    inner: message.message,
                    partition_id: message.partition_id,
                })),
                Some(Err(err)) => Err(to_php_exception(err)),
                None => Ok(None),
            }
        })
    }

    fn partition_id_or_current_after_poll(
        inner: &RustIggyConsumer,
        partition_id: Option<u32>,
    ) -> PhpResult<Option<u32>> {
        if let Some(partition_id) = partition_id {
            return Ok(Some(partition_id));
        }

        let current_partition_id = inner.partition_id();
        if inner
            .get_last_consumed_offset(current_partition_id)
            .is_none()
        {
            return Err(
                "'partition_id' is required until at least one message has been polled".into(),
            );
        }

        Ok(Some(current_partition_id))
    }
}

#[php_class]
#[php(name = "Iggy\\AutoCommit")]
#[derive(Clone, Copy)]
pub struct AutoCommit {
    pub(crate) inner: RustAutoCommit,
}

#[php_impl]
impl AutoCommit {
    pub fn disabled() -> Self {
        Self {
            inner: RustAutoCommit::Disabled,
        }
    }

    pub fn interval(interval_micros: u64) -> Self {
        Self {
            inner: RustAutoCommit::Interval(IggyDuration::from(interval_micros)),
        }
    }

    pub fn interval_or_when(interval_micros: u64, when: &AutoCommitWhen) -> Self {
        Self {
            inner: RustAutoCommit::IntervalOrWhen(IggyDuration::from(interval_micros), when.inner),
        }
    }

    pub fn when(when: &AutoCommitWhen) -> Self {
        Self {
            inner: RustAutoCommit::When(when.inner),
        }
    }
}

impl From<&AutoCommit> for RustAutoCommit {
    fn from(value: &AutoCommit) -> Self {
        value.inner
    }
}

#[php_class]
#[php(name = "Iggy\\AutoCommitWhen")]
#[derive(Clone, Copy)]
pub struct AutoCommitWhen {
    pub(crate) inner: RustAutoCommitWhen,
}

#[php_impl]
impl AutoCommitWhen {
    pub fn polling_messages() -> Self {
        Self {
            inner: RustAutoCommitWhen::PollingMessages,
        }
    }

    pub fn consuming_all_messages() -> Self {
        Self {
            inner: RustAutoCommitWhen::ConsumingAllMessages,
        }
    }

    pub fn consuming_each_message() -> Self {
        Self {
            inner: RustAutoCommitWhen::ConsumingEachMessage,
        }
    }

    pub fn consuming_every_nth_message(n: u32) -> Self {
        Self {
            inner: RustAutoCommitWhen::ConsumingEveryNthMessage(n),
        }
    }
}

impl From<&AutoCommitWhen> for RustAutoCommitWhen {
    fn from(value: &AutoCommitWhen) -> Self {
        value.inner
    }
}
