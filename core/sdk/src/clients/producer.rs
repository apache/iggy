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
use super::ORDERING;
use crate::client_wrappers::ClientWrapper;
use crate::clients::MAX_BATCH_LENGTH;
use crate::clients::producer_config::DirectConfig;
use crate::clients::producer_dispatcher::ProducerDispatcher;
use crate::runtime::{Interval, Runtime};
use bytes::Bytes;
use futures_util::StreamExt;
use iggy_binary_protocol::{Client, MessageClient, StreamClient, TopicClient};
use iggy_common::locking::{IggySharedMut, IggySharedMutFn};
use iggy_common::{
    CompressionAlgorithm, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyExpiry, IggyMessage, IggyTimestamp, MaxTopicSize, Partitioner, Partitioning,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tracing::{error, info, trace, warn};

#[maybe_async::maybe_async]
pub trait Dispatcher: Send + Sync + 'static {
    async fn send(&self, stream: Arc<Identifier>, topic: Arc<Identifier>, msgs: Vec<IggyMessage>, partitioning: Option<Arc<Partitioning>>) -> Result<(), iggy_common::IggyError>;
    async fn shutdown(&mut self);
}

pub struct BackgroundDispatcher {
    dispatcher: ProducerDispatcher,
}

#[maybe_async::async_impl]
impl Dispatcher for BackgroundDispatcher {
    async fn send(&self, stream: Arc<Identifier>, topic: Arc<Identifier>, msgs: Vec<IggyMessage>, partitioning: Option<Arc<Partitioning>>) -> Result<(), iggy_common::IggyError> {
        self.dispatcher.dispatch(msgs, stream, topic, partitioning).await
    }

    async fn shutdown(&mut self) {
        self.dispatcher.shutdown().await
    }
}

pub struct DirectDispatcher<R: Runtime> {
    core: Arc<ProducerCore<R>>,
    config: DirectConfig,
}

impl<R: Runtime> DirectDispatcher<R> {
    pub fn new(core: Arc<ProducerCore<R>>, config: DirectConfig) -> Self {
        Self { core, config }
    }
}

#[maybe_async::maybe_async]
impl<R: Runtime + 'static> Dispatcher for DirectDispatcher<R> {
    async fn send(&self, stream: Arc<Identifier>, topic: Arc<Identifier>, mut msgs: Vec<IggyMessage>, partitioning: Option<Arc<Partitioning>>) -> Result<(), iggy_common::IggyError> {
        if msgs.is_empty() {
            return Ok(());
        }

        // Handle linger time
        let linger_time_micros = self.config.linger_time.as_micros();
        if linger_time_micros > 0 {
            self.core.wait_before_sending(
                linger_time_micros,
                self.core.last_sent_at.load(ORDERING)
            ).await;
        }

        // Handle batching
        let max_batch = if self.config.batch_length == 0 {
            MAX_BATCH_LENGTH
        } else {
            self.config.batch_length as usize
        };

        while !msgs.is_empty() {
            let batch_size = max_batch.min(msgs.len());

            // Split off the batch from the front of the vector
            let chunk: Vec<IggyMessage> = msgs.drain(0..batch_size).collect();

            if let Err(err) = self.core.send_raw(&stream, &topic, chunk, partitioning.clone()).await {
                // Return remaining messages as failed
                return Err(self.core.make_failed_error(err, msgs));
            }

            self.core.last_sent_at.store(IggyTimestamp::now().into(), ORDERING);
            // No need to update index since we're draining from the front
        }

        Ok(())
    }

    async fn shutdown(&mut self) {}
}


#[cfg(test)]
use mockall::automock;

#[maybe_async::maybe_async]
#[cfg_attr(test, automock)]
pub trait ProducerCoreBackend: Send + Sync + 'static {
    async fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError>;
}

pub struct ProducerCore<R: Runtime> {
    initialized: AtomicBool,
    can_send: Arc<AtomicBool>,
    client: Arc<IggySharedMut<ClientWrapper>>,
    stream_id: Arc<Identifier>,
    stream_name: String,
    topic_id: Arc<Identifier>,
    topic_name: String,
    partitioning: Option<Arc<Partitioning>>,
    encryptor: Option<Arc<EncryptorKind>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
    default_partitioning: Arc<Partitioning>,
    last_sent_at: Arc<AtomicU64>,
    send_retries_count: Option<u32>,
    send_retries_interval: Option<IggyDuration>,
    rt: Arc<R>,
}

impl<R: Runtime> ProducerCore<R> {
    #[maybe_async::maybe_async]
    pub async fn init(&self) -> Result<(), IggyError> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        info!("Initializing producer for stream: {stream_id} and topic: {topic_id}...");
        self.subscribe_events().await;
        let client = self.client.clone();
        let client = client.read().await;
        if client.get_stream(&stream_id).await?.is_none() {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(IggyError::StreamNameNotFound(self.stream_name.clone()));
            }

            let (name, id) = match stream_id.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream_id.get_u32_value()?),
                ),
                IdKind::String => (self.stream_id.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name, id).await?;
        }

        if client.get_topic(&stream_id, &topic_id).await?.is_none() {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_name.clone(),
                    self.stream_name.clone(),
                ));
            }

            let (name, id) = match self.topic_id.kind {
                IdKind::Numeric => (
                    self.topic_name.to_owned(),
                    Some(self.topic_id.get_u32_value()?),
                ),
                IdKind::String => (self.topic_id.get_string_value()?, None),
            };
            info!("Creating topic: {name} for stream: {}", self.stream_name);
            client
                .create_topic(
                    &self.stream_id,
                    &self.topic_name,
                    self.topic_partitions_count,
                    CompressionAlgorithm::None,
                    self.topic_replication_factor,
                    id,
                    self.topic_message_expiry,
                    self.topic_max_size,
                )
                .await?;
        }

        let _ = self
            .initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        info!("Producer has been initialized for stream: {stream_id} and topic: {topic_id}.");
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let can_send = self.can_send.clone();

        #[cfg(feature = "async")]
        self.rt.spawn(Box::new(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        can_send.store(false, ORDERING);
                        warn!("Client has been shutdown");
                    }
                    DiagnosticEvent::Connected => {
                        can_send.store(false, ORDERING);
                        trace!("Connected to the server");
                    }
                    DiagnosticEvent::Disconnected => {
                        can_send.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        can_send.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        can_send.store(false, ORDERING);
                    }
                }
            }
        }));

        #[cfg(feature = "sync")]
        // TODO change async-broadcast to sync impl
        self.rt.spawn(Box::new(move || {
            // while let Some(event) = receiver.next() {
            //     trace!("Received diagnostic event: {event}");
            //     match event {
            //         DiagnosticEvent::Shutdown => {
            //             can_send.store(false, ORDERING);
            //             warn!("Client has been shutdown");
            //         }
            //         DiagnosticEvent::Connected => {
            //             can_send.store(false, ORDERING);
            //             trace!("Connected to the server");
            //         }
            //         DiagnosticEvent::Disconnected => {
            //             can_send.store(false, ORDERING);
            //             warn!("Disconnected from the server");
            //         }
            //         DiagnosticEvent::SignedIn => {
            //             can_send.store(true, ORDERING);
            //         }
            //         DiagnosticEvent::SignedOut => {
            //             can_send.store(false, ORDERING);
            //         }
            //     }
            // }
        }));
    }

    #[maybe_async::maybe_async]
    async fn try_send_messages(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        let client = self.client.read().await;
        let Some(max_retries) = self.send_retries_count else {
            return client
                .send_messages(stream, topic, partitioning, messages)
                .await;
        };

        if max_retries == 0 {
            return client
                .send_messages(stream, topic, partitioning, messages)
                .await;
        }

        let mut timer = if let Some(interval) = self.send_retries_interval {
            let mut timer = self.rt.interval(interval);
            timer.tick().await;
            Some(timer)
        } else {
            None
        };

        self.wait_until_connected(max_retries, stream, topic, &mut timer)
            .await?;
        self.send_with_retries(
            max_retries,
            stream,
            topic,
            partitioning,
            messages,
            &mut timer,
        )
        .await
    }

    #[maybe_async::maybe_async]
    async fn wait_until_connected(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        timer: &mut Option<R::Interval>,
    ) -> Result<(), IggyError> {
        let mut retries = 0;
        while !self.can_send.load(ORDERING) {
            retries += 1;
            if retries > max_retries {
                error!(
                    "Failed to send messages to topic: {topic}, stream: {stream} \
                     after {max_retries} retries. Client is disconnected."
                );
                return Err(IggyError::CannotSendMessagesDueToClientDisconnection);
            }

            error!(
                "Trying to send messages to topic: {topic}, stream: {stream} \
                 but the client is disconnected. Retrying {retries}/{max_retries}..."
            );

            if let Some(timer) = timer.as_mut() {
                trace!(
                    "Waiting for the next retry to send messages to topic: {topic}, \
                     stream: {stream} for disconnected client..."
                );
                timer.tick().await;
            }
        }
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn send_with_retries(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
        timer: &mut Option<R::Interval>,
    ) -> Result<(), IggyError> {
        let client = self.client.read().await;
        let mut retries = 0;
        loop {
            match client
                .send_messages(stream, topic, partitioning, messages)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => {
                    retries += 1;
                    if retries > max_retries {
                        error!(
                            "Failed to send messages to topic: {topic}, stream: {stream} \
                             after {max_retries} retries. {error}."
                        );
                        return Err(error);
                    }

                    error!(
                        "Failed to send messages to topic: {topic}, stream: {stream}. \
                         {error} Retrying {retries}/{max_retries}..."
                    );

                    if let Some(t) = timer.as_mut() {
                        trace!(
                            "Waiting for the next retry to send messages to topic: {topic}, \
                             stream: {stream}..."
                        );
                        t.tick().await;
                    }
                }
            }
        }
    }

    fn encrypt_messages(&self, messages: &mut [IggyMessage]) -> Result<(), IggyError> {
        if let Some(encryptor) = &self.encryptor {
            for message in messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;
            }
        }
        Ok(())
    }

    fn get_partitioning(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        messages: &[IggyMessage],
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<Arc<Partitioning>, IggyError> {
        if let Some(partitioner) = &self.partitioner {
            trace!("Calculating partition id using custom partitioner.");
            let partition_id = partitioner.calculate_partition_id(stream, topic, messages)?;
            Ok(Arc::new(Partitioning::partition_id(partition_id)))
        } else {
            trace!("Using the provided partitioning.");
            Ok(partitioning.unwrap_or_else(|| {
                self.partitioning
                    .clone()
                    .unwrap_or_else(|| self.default_partitioning.clone())
            }))
        }
    }

    #[maybe_async::maybe_async]
    async fn wait_before_sending(&self, interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before sending messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!(
            "Waiting for {remaining} microseconds before sending messages... {interval} - {elapsed} = {remaining}"
        );
        self.rt.sleep(IggyDuration::new(Duration::from_micros(remaining))).await;
    }

    fn make_failed_error(&self, cause: IggyError, failed: Vec<IggyMessage>) -> IggyError {
        IggyError::ProducerSendFailed {
            cause: Box::new(cause),
            failed: Arc::new(failed),
            stream_name: self.stream_name.clone(),
            topic_name: self.topic_name.clone(),
        }
    }

    /// Raw send method without batching or linger time - for use by dispatchers
    #[maybe_async::maybe_async]
    pub async fn send_raw(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        mut msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if msgs.is_empty() {
            return Ok(());
        }

        if let Err(err) = self.encrypt_messages(&mut msgs) {
            return Err(self.make_failed_error(err, msgs));
        }

        let part = match self.get_partitioning(stream, topic, &msgs, partitioning.clone()) {
            Ok(p) => p,
            Err(err) => {
                return Err(self.make_failed_error(err, msgs));
            }
        };

        self.try_send_messages(stream, topic, &part, &mut msgs)
            .await
            .map_err(|err| self.make_failed_error(err, msgs))
    }
}

#[maybe_async::maybe_async]
impl<R: Runtime + 'static> ProducerCoreBackend for ProducerCore<R> {
    async fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        // Simple pass-through to send_raw for backward compatibility
        self.send_raw(stream, topic, msgs, partitioning).await
    }
}

pub struct IggyProducer<R: Runtime, D: Dispatcher> {
    core: Arc<ProducerCore<R>>,
    dispatcher: D,
}

impl<R: Runtime + 'static, D: Dispatcher> IggyProducer<R, D> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<ClientWrapper>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        partitioning: Option<Partitioning>,
        encryptor: Option<Arc<EncryptorKind>>,
        partitioner: Option<Arc<dyn Partitioner>>,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        topic_partitions_count: u32,
        topic_replication_factor: Option<u8>,
        topic_message_expiry: IggyExpiry,
        topic_max_size: MaxTopicSize,
        send_retries_count: Option<u32>,
        send_retries_interval: Option<IggyDuration>,
        rt: Arc<R>,
        dispatcher: D,
    ) -> Self {
        let core = Arc::new(ProducerCore {
            initialized: AtomicBool::new(false),
            client: Arc::new(client),
            can_send: Arc::new(AtomicBool::new(true)),
            stream_id: Arc::new(stream),
            stream_name,
            topic_id: Arc::new(topic),
            topic_name,
            partitioning: partitioning.map(Arc::new),
            encryptor,
            partitioner,
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            topic_partitions_count,
            topic_replication_factor,
            topic_message_expiry,
            topic_max_size,
            default_partitioning: Arc::new(Partitioning::balanced()),
            last_sent_at: Arc::new(AtomicU64::new(0)),
            send_retries_count,
            send_retries_interval,
            rt,
        });

        Self { core, dispatcher }
    }

    pub fn stream(&self) -> &Identifier {
        &self.core.stream_id
    }

    pub fn topic(&self) -> &Identifier {
        &self.core.topic_id
    }

    /// Initializes the producer by subscribing to diagnostic events, creating the stream and topic if they do not exist etc.
    ///
    /// Note: This method must be invoked before producing messages.
    #[maybe_async::maybe_async]
    pub async fn init(&self) -> Result<(), IggyError> {
        self.core.init().await
    }

    #[maybe_async::maybe_async]
    pub async fn send(&self, messages: Vec<IggyMessage>) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        self.dispatcher.send(stream_id, topic_id, messages, None).await
    }

    #[maybe_async::maybe_async]
    pub async fn send_one(&self, message: IggyMessage) -> Result<(), IggyError> {
        self.send(vec![message]).await
    }

    #[maybe_async::maybe_async]
    pub async fn send_with_partitioning(
        &self,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        self.dispatcher.send(stream_id, topic_id, messages, partitioning).await
    }

    #[maybe_async::maybe_async]
    pub async fn send_to(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        self.dispatcher.send(stream, topic, messages, partitioning).await
    }

    #[maybe_async::maybe_async]
    pub async fn shutdown(&mut self) {
        self.dispatcher.shutdown().await
    }
}
