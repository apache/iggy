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
use std::time::Duration;

use ext_php_rs::{
    exception::PhpResult,
    php_class, php_impl,
    types::{ZendCallable, Zval},
};
use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use iggy::prelude::{
    IggyConsumer as RustIggyConsumer, IggyConsumerState, IggyError, ReadError, ReceivedMessage,
};
use tokio::sync::{Mutex, MutexGuard, mpsc, oneshot};
use tokio::time::timeout;

use crate::client::optional_unsigned;
use crate::error::to_php_exception;
use crate::message_iterator::MessageIterator;
use crate::receive_message::{Checkpoint, PollingStrategy, ReceiveMessage};
use crate::runtime::runtime;

/// The handler of a consumeMessages() run waits for the callback of each message before it takes
/// the next one, so one message at most is on its way to the callback.
const DELIVERIES_IN_FLIGHT: usize = 1;

/// A PHP class representing the Iggy consumer.
#[php_class]
#[php(name = "Iggy\\Consumer")]
pub struct IggyConsumer {
    pub(crate) inner: Arc<Mutex<RustIggyConsumer>>,
    /// Reads and commits without `inner`, which a consumeMessages() run holds while its callback
    /// runs.
    pub(crate) state: IggyConsumerState,
    pub(crate) name: String,
    pub(crate) stream: String,
    pub(crate) topic: String,
}

#[php_impl]
impl IggyConsumer {
    /// Get the last consumed offset or null if no offset has been consumed yet.
    pub fn get_last_consumed_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_consumed_offset(partition_id)
    }

    /// Get the last stored offset or null if no offset has been stored yet.
    pub fn get_last_stored_offset(&self, partition_id: u32) -> Option<u64> {
        self.state.get_last_stored_offset(partition_id)
    }

    /// Gets the name of the consumer group.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Gets the current partition id or 0 if no messages have been polled yet.
    pub fn partition_id(&self) -> u32 {
        self.state.partition_id()
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
    ///
    /// It always sends, also an offset below the stored one. The offset is committed under the
    /// partition context of the latest message consumed from that partition. After the
    /// partition was recreated or changed owner, that context can be newer than the message's,
    /// and the offset then lands on the newer partition. Use commit($message->checkpoint()) to
    /// commit under the context that delivered the message.
    ///
    /// When nothing was consumed from that partition, the store uses the context of its route
    /// and can fail once with HistoryUnavailable (87), or ConsumerGroupPartitionNotOwned
    /// (5009), after another client deleted and recreated the partition. The failed route is
    /// dropped, and the next call routes again.
    pub fn store_offset(&self, offset: u64, partition_id: Option<u32>) -> PhpResult {
        let partition_id = self.partition_id_or_current_after_poll(partition_id)?;
        runtime()
            .block_on(self.state.store_offset(offset, partition_id))
            .map_err(to_php_exception)
    }

    /// Stores the offset of a checkpoint that this consumer delivered, which covers every
    /// earlier message of its partition.
    ///
    /// Pass $message->checkpoint() to commit a message after later polls or a rebalance. A
    /// checkpoint of an older generation of its partition, after seek() or a new assignment, or
    /// at or below the last commit of its generation, stores nothing. One from a deleted or
    /// recreated partition fails with HistoryUnavailable (87), and one from a previous owner
    /// fails with ConsumerGroupPartitionNotOwned (5009). A checkpoint that this consumer did not
    /// deliver, and every commit under CommitPolicy::onRead(), fail with InvalidCommand (3).
    pub fn commit(&self, checkpoint: &Checkpoint) -> PhpResult {
        runtime()
            .block_on(self.state.commit(checkpoint.inner))
            .map_err(to_php_exception)
    }

    /// Counts a checkpoint that this consumer delivered under CommitPolicy::auto() and stores
    /// nothing now: the policy stores the latest counted position of its partition. With
    /// CommitAfter::Marked only marks count. It works also in a consumeMessages() callback.
    /// Without CommitPolicy::auto(), and for a checkpoint that this consumer did not deliver,
    /// it throws InvalidCommand (3).
    pub fn mark(&self, checkpoint: &Checkpoint) -> PhpResult {
        self.state.mark(checkpoint.inner).map_err(to_php_exception)
    }

    /// Sets the next read position of a partition and drops the rest of its current batch. Under
    /// CommitPolicy::onRead(), a seek to an offset above 0 first stores the offset before it, so
    /// a restart resumes at the new position, and a failed store fails the seek with its
    /// exception and moves nothing. Every other seek stores nothing.
    ///
    /// It also resumes a partition that stopped, for example after HistoryUnavailable (87), or
    /// after a message that does not decode: seek to the `skip` of its ReadException. A
    /// partition that the consumer does not read, and a continuation of another partition, fail
    /// with InvalidCommand (3). Under CommitPolicy::onRead(), a read of the partition with
    /// an unknown outcome fails with PartitionLaneBlocked (14009): abandon its operation first.
    pub fn seek(&self, partition_id: u32, start: &PollingStrategy) -> PhpResult {
        let mut consumer = lock_consumer(&self.inner, "seek()")?;
        runtime()
            .block_on(consumer.seek(partition_id, start.into()))
            .map_err(to_php_exception)
    }

    /// Deletes the stored offset for the provided partition id.
    ///
    /// If partition_id is null, at least one message must have been polled first.
    ///
    /// The delete uses the context of its route. It can fail once with HistoryUnavailable
    /// (87), or ConsumerGroupPartitionNotOwned (5009), after another client deleted and
    /// recreated the partition. The failed route is dropped, and the next call routes again.
    pub fn delete_offset(&self, partition_id: Option<u32>) -> PhpResult {
        let partition_id = self.partition_id_or_current_after_poll(partition_id)?;
        runtime()
            .block_on(self.state.delete_offset(partition_id))
            .map_err(to_php_exception)
    }

    /// Consumes messages with a PHP callback, and returns how many it consumed.
    ///
    /// The callback is called as callback(ReceiveMessage $message). A finite limit is required.
    /// The run ends after `limit` messages, after shutdown(), or with an exception.
    ///
    /// Under CommitPolicy::auto(), a message counts before its callback with
    /// CommitAfter::Delivery, and after its callback returns with CommitAfter::Processing. An
    /// exception of the callback ends the run, and consumeMessages() throws it. Its message
    /// then stays uncounted under CommitAfter::Processing, so the next run reads it again.
    ///
    /// The consumer retries a failed read by itself and does not throw it. An error that ends
    /// the run throws Iggy\Exception\ReadException: HistoryUnavailable (87), which stops its
    /// partition until seek(), and under CommitPolicy::onRead() an unknown outcome, which
    /// stops its partition until its operation is abandoned and the partition is sought. A
    /// blocked lane, a shut down client and any error while the client cannot reconnect by
    /// itself also end the run.
    ///
    /// With `idle_timeout_micros`, a run that gets no message for that long ends and returns
    /// the count of the messages it consumed, so a single-threaded worker regains control on an
    /// idle topic. Null waits without a limit.
    ///
    /// The callback can call commit(), mark(), storeOffset(), deleteOffset() and the getters. seek(),
    /// shutdown(), iterMessages() and consumeMessages() throw in the callback, because the run
    /// holds the consumer.
    pub fn consume_messages(
        &self,
        callback: ZendCallable,
        limit: u32,
        idle_timeout_micros: Option<&Zval>,
    ) -> PhpResult<u32> {
        let idle_timeout = optional_unsigned::<u64>(idle_timeout_micros, "idle_timeout_micros")?
            .map(Duration::from_micros);
        let mut consumer = lock_consumer(&self.inner, "consumeMessages()")?;
        if limit == 0 {
            return Ok(0);
        }
        let (deliveries, mut delivered) = mpsc::channel(DELIVERIES_IN_FLIGHT);
        let handoff = Handoff { deliveries };
        let (stop, stopped) = oneshot::channel();
        let mut stop = Some(stop);
        let mut run = consumer.consume_messages(&handoff, stopped);
        let mut consumed = 0;

        loop {
            let waited = runtime().block_on(async {
                match idle_timeout.filter(|_| stop.is_some()) {
                    Some(idle_timeout) => {
                        timeout(idle_timeout, next_delivery(&mut delivered, &mut run))
                            .await
                            .ok()
                    }
                    None => Some(next_delivery(&mut delivered, &mut run).await),
                }
            });
            let Some(delivery) = waited else {
                // A message that the run took before the stop still reaches the callback.
                if let Some(stop) = stop.take() {
                    let _ = stop.send(());
                }
                continue;
            };
            let Some(Delivery { message, answer }) = delivery? else {
                return Ok(consumed);
            };
            let message = ReceiveMessage::from(message);
            // Outside `block_on`, because a binding call in the callback blocks on the runtime
            // again, and a nested `block_on` panics.
            let called = callback.try_call(vec![&message]);
            consumed += 1;
            if called.is_ok() && consumed < limit {
                let _ = answer.send(Ok(()));
                continue;
            }
            // The biased select of the run sees this signal before it takes another message, so
            // the run ends right after it counts this one.
            if let Some(stop) = stop.take() {
                let _ = stop.send(());
            }
            let _ = answer.send(called.as_ref().map(|_| ()).map_err(|_| IggyError::Error));
            runtime().block_on(run).map_err(to_php_exception)?;
            return called.map(|_| consumed).map_err(to_php_exception);
        }
    }

    /// Returns an iterator over messages for use with foreach.
    ///
    /// With `idle_timeout_micros`, the iteration ends when no message arrives for that long,
    /// and a later foreach over the same iterator reads on. Null waits without a limit.
    ///
    /// Under CommitPolicy::auto(), a message counts when the iterator yields it with
    /// CommitAfter::Delivery, and when the iterator moves past it with CommitAfter::Processing. A
    /// failed read throws Iggy\Exception\ReadException. The consumer stays usable and retries by
    /// itself. HistoryUnavailable (87) stops its partition until seek().
    pub fn iter_messages(&self, idle_timeout_micros: Option<&Zval>) -> PhpResult<MessageIterator> {
        let idle_timeout = optional_unsigned::<u64>(idle_timeout_micros, "idle_timeout_micros")?
            .map(Duration::from_micros);
        Ok(MessageIterator::new(self.inner.clone(), idle_timeout))
    }

    /// Stops the consumer after its queued offset stores.
    ///
    /// Under CommitPolicy::auto(), it stores the latest counted position of each partition. With
    /// CommitPolicy::onRead(), it moves the offset of each
    /// partition back to the last delivered message, so the next run reads the prefetched
    /// messages again. A group member then leaves its group. Afterwards consumeMessages()
    /// returns at once and iterMessages() ends, and a second call does nothing. Destroying the
    /// consumer without shutdown() sends only the queued stores and does not leave the group.
    ///
    /// The first failed store of the OnRead rewind is thrown. Otherwise a partition with an
    /// unknown outcome throws PartitionLaneBlocked (14009): abandon its operation before a new
    /// consumer reads the partition.
    pub fn shutdown(&self) -> PhpResult {
        let mut consumer = lock_consumer(&self.inner, "shutdown()")?;
        runtime()
            .block_on(consumer.shutdown())
            .map_err(to_php_exception)
    }
}

impl IggyConsumer {
    fn partition_id_or_current_after_poll(
        &self,
        partition_id: Option<u32>,
    ) -> PhpResult<Option<u32>> {
        if let Some(partition_id) = partition_id {
            return Ok(Some(partition_id));
        }

        let current_partition_id = self.state.partition_id();
        if self
            .state
            .get_last_consumed_offset(current_partition_id)
            .is_none()
        {
            return Err(to_php_exception(
                "'partition_id' is required until at least one message has been polled",
            ));
        }

        Ok(Some(current_partition_id))
    }
}

/// The offset that a consumer stored on a partition.
#[php_class]
#[php(name = "Iggy\\ConsumerOffsetInfo")]
pub struct ConsumerOffsetInfo {
    pub(crate) partition_id: u32,
    pub(crate) current_offset: u64,
    pub(crate) stored_offset: u64,
}

#[php_impl]
impl ConsumerOffsetInfo {
    /// The partition of the offset.
    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// The offset of the last message of the partition when the offset was read.
    #[php(getter, change_case = "snake_case")]
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// The stored offset.
    #[php(getter, change_case = "snake_case")]
    pub fn stored_offset(&self) -> u64 {
        self.stored_offset
    }
}

/// Locks the consumer for a call that needs it alone.
///
/// A consumeMessages() run holds the consumer while its callback runs on this thread, so a call
/// from the callback that waited for the lock would wait forever.
pub(crate) fn lock_consumer<'a>(
    consumer: &'a Mutex<RustIggyConsumer>,
    call: &str,
) -> PhpResult<MutexGuard<'a, RustIggyConsumer>> {
    consumer
        .try_lock()
        .map_err(|_| to_php_exception(format!("{call} cannot run while consumeMessages() runs")))
}

/// A message of a consumeMessages() run on its way to the callback, with the answer that its
/// handler waits for.
struct Delivery {
    message: ReceivedMessage,
    answer: oneshot::Sender<Result<(), IggyError>>,
}

/// The handler of a consumeMessages() run. Only the PHP thread can call the callback, so the
/// handler hands each message over and returns the answer for its callback.
struct Handoff {
    deliveries: mpsc::Sender<Delivery>,
}

impl MessageConsumer for Handoff {
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
        let (answer, answered) = oneshot::channel();
        self.deliveries
            .send(Delivery { message, answer })
            .await
            .map_err(|_| IggyError::Error)?;
        answered.await.unwrap_or(Err(IggyError::Error))
    }
}

/// Polls the run until its handler hands over a message, or until the run ends.
async fn next_delivery(
    delivered: &mut mpsc::Receiver<Delivery>,
    run: &mut (impl Future<Output = Result<(), ReadError>> + Unpin),
) -> PhpResult<Option<Delivery>> {
    tokio::select! {
        Some(delivery) = delivered.recv() => Ok(Some(delivery)),
        result = run => result.map(|()| None).map_err(to_php_exception),
    }
}
