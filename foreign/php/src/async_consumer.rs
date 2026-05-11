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

use std::sync::Arc;

use ext_php_rs::{php_class, php_impl, types::ZendCallable};
use futures::StreamExt;
use iggy::prelude::IggyConsumer as RustIggyConsumer;
use php_tokio::EventLoop;
use tokio::sync::Mutex;

use crate::receive_message::ReceiveMessage;

type AsyncPhpResult<T = ()> = Result<T, String>;

#[php_class]
pub struct IggyAsyncConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[php_impl]
impl IggyAsyncConsumer {
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
    pub fn store_offset(&self, offset: u64, partition_id: Option<u32>) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .lock()
                .await
                .store_offset(offset, partition_id)
                .await
                .map_err(to_async_exception)
        })
    }

    /// Deletes the stored offset for the provided partition id.
    ///
    /// If partition_id is null, the current partition id is used.
    pub fn delete_offset(&self, partition_id: Option<u32>) -> AsyncPhpResult {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            inner
                .lock()
                .await
                .delete_offset(partition_id)
                .await
                .map_err(to_async_exception)
        })
    }

    /// Returns an iterator whose next() method suspends until a message is available.
    pub fn iter_messages(&self) -> IggyAsyncReceiveMessageIterator {
        IggyAsyncReceiveMessageIterator {
            inner: self.inner.clone(),
        }
    }

    /// Consumes messages with a PHP callback.
    ///
    /// The callback is called as callback(ReceiveMessage $message). If limit is null,
    /// this method runs until the consumer stream ends or an error occurs.
    pub fn consume_messages(
        &self,
        callback: ZendCallable,
        limit: Option<u32>,
    ) -> AsyncPhpResult<u32> {
        let mut consumed = 0;
        let max_messages = limit.unwrap_or(u32::MAX);
        let iterator = self.iter_messages();

        while consumed < max_messages {
            let Some(message) = iterator.next()? else {
                break;
            };

            callback
                .try_call(vec![&message])
                .map_err(|err| err.to_string())?;
            consumed += 1;
        }

        Ok(consumed)
    }
}

#[php_class]
pub struct IggyAsyncReceiveMessageIterator {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
}

#[php_impl]
impl IggyAsyncReceiveMessageIterator {
    pub fn next(&self) -> AsyncPhpResult<Option<ReceiveMessage>> {
        let inner = self.inner.clone();

        EventLoop::suspend_on(async move {
            let mut inner = inner.lock().await;

            match inner.next().await {
                Some(Ok(message)) => Ok(Some(ReceiveMessage {
                    inner: message.message,
                    partition_id: message.partition_id,
                })),
                Some(Err(err)) => Err(err.to_string()),
                None => Ok(None),
            }
        })
    }
}

fn to_async_exception(error: impl std::fmt::Display) -> String {
    error.to_string()
}
