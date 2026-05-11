/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{sync::Arc, time::Duration};

use ext_php_rs::{
    exception::{PhpException, PhpResult},
    php_class, php_impl,
    types::ZendCallable,
};
use iggy::prelude::{
    AutoCommit as RustAutoCommit, AutoCommitAfter as RustAutoCommitAfter,
    AutoCommitWhen as RustAutoCommitWhen, IggyConsumer as RustIggyConsumer, IggyDuration,
};
use tokio::sync::Mutex;

use crate::iterator::ReceiveMessageIterator;

/// A PHP class representing the Iggy consumer.
#[php_class]
pub struct IggyConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[php_impl]
impl IggyConsumer {
    /// Get the last consumed offset or null if no offset has been consumed yet.
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_consumed_offset(partition_id)
    }

    /// Get the last stored offset or null if no offset has been stored yet.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.inner
            .blocking_lock()
            .get_last_stored_offset(partition_id)
    }

    /// Gets the name of the consumer group.
    pub fn name(&self) -> String {
        self.inner.blocking_lock().name().to_string()
    }

    /// Gets the current partition id or 0 if no messages have been polled yet.
    pub fn partition_id(&self) -> u32 {
        self.inner.blocking_lock().partition_id()
    }

    /// Gets the stream identifier this consumer is configured for.
    pub fn stream(&self) -> String {
        self.inner.blocking_lock().stream().to_string()
    }

    /// Gets the topic identifier this consumer is configured for.
    pub fn topic(&self) -> String {
        self.inner.blocking_lock().topic().to_string()
    }

    /// Stores the provided offset for the provided partition id.
    ///
    /// If partition_id is null, the current partition id is used.
    pub fn store_offset(&self, offset: u64, partition_id: Option<u32>) -> PhpResult {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .lock()
                .await
                .store_offset(offset, partition_id)
                .await
                .map_err(|err| PhpException::default(err.to_string()))
        })
    }

    /// Deletes the stored offset for the provided partition id.
    ///
    /// If partition_id is null, the current partition id is used.
    pub fn delete_offset(&self, partition_id: Option<u32>) -> PhpResult {
        let inner = self.inner.clone();

        runtime().block_on(async move {
            inner
                .lock()
                .await
                .delete_offset(partition_id)
                .await
                .map_err(|err| PhpException::default(err.to_string()))
        })
    }

    /// Returns an iterator whose next() method blocks until a message is available.
    pub fn iter_messages(&self) -> ReceiveMessageIterator {
        ReceiveMessageIterator {
            inner: self.inner.clone(),
        }
    }

    /// Consumes messages with a PHP callback.
    ///
    /// The callback is called as callback(ReceiveMessage $message). If limit is null,
    /// this method runs until the consumer stream ends or an error occurs.
    pub fn consume_messages(&self, callback: ZendCallable, limit: Option<u32>) -> PhpResult<u32> {
        let mut consumed = 0;
        let max_messages = limit.unwrap_or(u32::MAX);

        while consumed < max_messages {
            let Some(message) = self.iter_messages().next()? else {
                break;
            };

            callback
                .try_call(vec![&message])
                .map_err(|err| PhpException::default(err.to_string()))?;
            consumed += 1;
        }

        Ok(consumed)
    }
}

#[php_class]
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
            inner: RustAutoCommit::Interval(iggy_duration_from_micros(interval_micros)),
        }
    }

    pub fn interval_or_when(interval_micros: u64, when: &AutoCommitWhen) -> Self {
        Self {
            inner: RustAutoCommit::IntervalOrWhen(
                iggy_duration_from_micros(interval_micros),
                when.inner,
            ),
        }
    }

    pub fn interval_or_after(interval_micros: u64, after: &AutoCommitAfter) -> Self {
        Self {
            inner: RustAutoCommit::IntervalOrAfter(
                iggy_duration_from_micros(interval_micros),
                after.inner,
            ),
        }
    }

    pub fn when(when: &AutoCommitWhen) -> Self {
        Self {
            inner: RustAutoCommit::When(when.inner),
        }
    }

    pub fn after(after: &AutoCommitAfter) -> Self {
        Self {
            inner: RustAutoCommit::After(after.inner),
        }
    }
}

impl From<&AutoCommit> for RustAutoCommit {
    fn from(value: &AutoCommit) -> Self {
        value.inner
    }
}

#[php_class]
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

#[php_class]
#[derive(Clone, Copy)]
pub struct AutoCommitAfter {
    pub(crate) inner: RustAutoCommitAfter,
}

#[php_impl]
impl AutoCommitAfter {
    pub fn consuming_all_messages() -> Self {
        Self {
            inner: RustAutoCommitAfter::ConsumingAllMessages,
        }
    }

    pub fn consuming_each_message() -> Self {
        Self {
            inner: RustAutoCommitAfter::ConsumingEachMessage,
        }
    }

    pub fn consuming_every_nth_message(n: u32) -> Self {
        Self {
            inner: RustAutoCommitAfter::ConsumingEveryNthMessage(n),
        }
    }
}

impl From<&AutoCommitAfter> for RustAutoCommitAfter {
    fn from(value: &AutoCommitAfter) -> Self {
        value.inner
    }
}

fn iggy_duration_from_micros(micros: u64) -> IggyDuration {
    IggyDuration::new(Duration::from_micros(micros))
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to initialize Tokio runtime")
    })
}
