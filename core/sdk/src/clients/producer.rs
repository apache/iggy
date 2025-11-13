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
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::MAX_BATCH_LENGTH;
use crate::clients::producer_builder::SendMode;
use crate::clients::producer_config::DirectConfig;
use crate::clients::producer_dispatcher::ProducerDispatcher;
use bytes::Bytes;
use futures_util::StreamExt;
use iggy_binary_protocol::{Client, MessageClient, StreamClient, TopicClient};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    CompressionAlgorithm, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyExpiry, IggyMessage, IggyTimestamp, MaxTopicSize, Partitioner, Partitioning,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::time::{Interval, sleep};
use tracing::{error, info, trace, warn};

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait ProducerCoreBackend: Send + Sync + 'static {
    fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
}

pub struct ProducerCore {
    initialized: AtomicBool,
    can_send: Arc<AtomicBool>,
    client: Arc<IggyRwLock<ClientWrapper>>,
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
    direct_config: Option<DirectConfig>,
}

impl ProducerCore {
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

            let (name, _id) = match stream_id.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream_id.get_u32_value()?),
                ),
                IdKind::String => (self.stream_id.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name).await?;
        }

        if client.get_topic(&stream_id, &topic_id).await?.is_none() {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_name.clone(),
                    self.stream_name.clone(),
                ));
            }

            let (name, _id) = match self.topic_id.kind {
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

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let can_send = self.can_send.clone();

        tokio::spawn(async move {
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
        });
    }

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
            let mut timer = tokio::time::interval(interval.get_duration());
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

    async fn wait_until_connected(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        timer: &mut Option<Interval>,
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

    async fn send_with_retries(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
        timer: &mut Option<Interval>,
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

    async fn wait_before_sending(interval: u64, last_sent_at: u64) {
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
        sleep(Duration::from_micros(remaining)).await;
    }

    fn make_failed_error(&self, cause: IggyError, failed: Vec<IggyMessage>) -> IggyError {
        IggyError::ProducerSendFailed {
            cause: Box::new(cause),
            failed: Arc::new(failed),
            stream_name: self.stream_name.clone(),
            topic_name: self.topic_name.clone(),
        }
    }
}

impl ProducerCoreBackend for ProducerCore {
    async fn send_internal(
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

        match &self.direct_config {
            Some(cfg) => {
                let linger_time_micros = cfg.linger_time.as_micros();
                if linger_time_micros > 0 {
                    Self::wait_before_sending(linger_time_micros, self.last_sent_at.load(ORDERING))
                        .await;
                }

                let max = if cfg.batch_length == 0 {
                    MAX_BATCH_LENGTH
                } else {
                    cfg.batch_length as usize
                };
                let mut index = 0;
                while index < msgs.len() {
                    let end = (index + max).min(msgs.len());
                    let chunk = &mut msgs[index..end];

                    if let Err(err) = self.try_send_messages(stream, topic, &part, chunk).await {
                        let failed_tail = msgs.split_off(index);
                        return Err(self.make_failed_error(err, failed_tail));
                    }
                    self.last_sent_at
                        .store(IggyTimestamp::now().into(), ORDERING);
                    index = end;
                }
            }
            // background send on
            _ => {
                self.try_send_messages(stream, topic, &part, &mut msgs)
                    .await
                    .map_err(|err| self.make_failed_error(err, msgs))?;
                self.last_sent_at
                    .store(IggyTimestamp::now().into(), ORDERING);
            }
        }

        Ok(())
    }
}

unsafe impl Send for IggyProducer {}
unsafe impl Sync for IggyProducer {}

pub struct IggyProducer {
    core: Arc<ProducerCore>,
    dispatcher: Option<ProducerDispatcher>,
}

impl IggyProducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggyRwLock<ClientWrapper>,
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
        mode: SendMode,
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
            direct_config: match mode {
                SendMode::Direct(ref cfg) => Some(cfg.clone()),
                _ => None,
            },
        });
        let dispatcher = match mode {
            SendMode::Background(cfg) => Some(ProducerDispatcher::new(core.clone(), cfg)),
            _ => None,
        };

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
    pub async fn init(&self) -> Result<(), IggyError> {
        self.core.init().await
    }

    pub async fn send(&self, messages: Vec<IggyMessage>) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        match &self.dispatcher {
            Some(disp) => disp.dispatch(messages, stream_id, topic_id, None).await,
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, None)
                    .await
            }
        }
    }

    pub async fn send_one(&self, message: IggyMessage) -> Result<(), IggyError> {
        self.send(vec![message]).await
    }

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

        match &self.dispatcher {
            Some(disp) => {
                disp.dispatch(messages, stream_id, topic_id, partitioning)
                    .await
            }
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, partitioning)
                    .await
            }
        }
    }

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

        match &self.dispatcher {
            Some(disp) => disp.dispatch(messages, stream, topic, partitioning).await,
            None => {
                self.core
                    .send_internal(&stream, &topic, messages, partitioning)
                    .await
            }
        }
    }

    pub async fn shutdown(self) {
        if let Some(disp) = self.dispatcher {
            disp.shutdown().await;
        }
    }
}
