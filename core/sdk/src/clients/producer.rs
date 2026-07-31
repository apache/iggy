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

use super::ORDERING;
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::MAX_BATCH_LENGTH;
use crate::clients::producer_builder::SendMode;
use crate::clients::producer_config::DirectConfig;
use crate::clients::producer_dispatcher::ProducerDispatcher;
use bytes::Bytes;
use futures_util::StreamExt;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{Client, MessageClient, StreamClient, TopicClient};
use iggy_common::{
    CompressionAlgorithm, DiagnosticEvent, EncryptorKind, IdKind, Identifier, IggyDuration,
    IggyError, IggyExpiry, IggyMessage, IggyTimestamp, MaxTopicSize, Partitioner, Partitioning,
    SendMessagesConfirmationResponse, SendMessagesResponse,
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
    /// Sends `msgs`, returning the confirmations of every chunk the send was
    /// split into, concatenated in chunk order.
    fn send_internal(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> impl Future<Output = Result<SendMessagesResponse, IggyError>> + Send;
}

/// Reply for a send that produced no confirmation: nothing was sent, or the
/// send is still queued on a background dispatcher.
pub(crate) fn no_confirmations() -> SendMessagesResponse {
    SendMessagesResponse {
        confirmations: Vec::new(),
    }
}

/// True when `error` can only have been raised after the server committed the
/// batch. Resending then turns one durable write into as many copies as the
/// retry budget allows, on a plane that keeps no reply cache to collapse them.
///
/// Both kinds are raised while decoding the HTTP reply body, which is reached
/// only once the status check has accepted a 2xx, so the batch landed and just
/// its confirmation is unreadable. The binary path degrades an unreadable body
/// to an empty confirmation list and never raises either kind.
///
/// Membership is scoped to the send path and must stay conservative. An error
/// meaning the request never arrived, or that the server rejected it before
/// committing, has to keep retrying.
fn implies_committed_send(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::InvalidBytesResponse | IggyError::InvalidJsonResponse
    )
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
    ) -> Result<SendMessagesResponse, IggyError> {
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

        self.wait_until_connected(max_retries, stream, topic)
            .await?;
        self.send_with_retries(&client, max_retries, stream, topic, partitioning, messages)
            .await
    }

    async fn wait_until_connected(
        &self,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
    ) -> Result<(), IggyError> {
        let mut retries = 0;
        let mut timer: Option<Interval> = None;

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

            if let Some(interval) = self.send_retries_interval {
                let timer =
                    timer.get_or_insert_with(|| tokio::time::interval(interval.get_duration()));
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
        client: &ClientWrapper,
        max_retries: u32,
        stream: &Identifier,
        topic: &Identifier,
        partitioning: &Arc<Partitioning>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        let mut retries = 0;
        let mut timer: Option<Interval> = None;

        loop {
            match client
                .send_messages(stream, topic, partitioning, messages)
                .await
            {
                // Only the attempt that finally succeeds yields a confirmation;
                // failed attempts have none to report.
                Ok(confirmation) => return Ok(confirmation),
                Err(error) => {
                    if implies_committed_send(&error) {
                        error!(
                            "Not retrying a send to topic: {topic}, stream: {stream}: the batch \
                             committed and only its confirmation could not be read. {error}."
                        );
                        return Err(error);
                    }

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

                    if let Some(interval) = self.send_retries_interval {
                        let timer = timer
                            .get_or_insert_with(|| tokio::time::interval(interval.get_duration()));
                        trace!(
                            "Waiting for the next retry to send messages to topic: {topic}, \
                             stream: {stream}..."
                        );
                        timer.tick().await;
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

                if let Some(ref user_headers) = message.user_headers {
                    let encrypted_headers = encryptor.encrypt(user_headers)?;
                    message.header.user_headers_length = encrypted_headers.len() as u32;
                    message.user_headers = Some(Bytes::from(encrypted_headers));
                }
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

    fn make_failed_error(
        &self,
        cause: IggyError,
        failed: Vec<IggyMessage>,
        committed: Vec<SendMessagesConfirmationResponse>,
    ) -> IggyError {
        IggyError::ProducerSendFailed {
            cause: Box::new(cause),
            failed: Arc::new(failed),
            committed: Arc::new(committed),
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
    ) -> Result<SendMessagesResponse, IggyError> {
        if msgs.is_empty() {
            return Ok(no_confirmations());
        }

        if let Err(err) = self.encrypt_messages(&mut msgs) {
            return Err(self.make_failed_error(err, msgs, Vec::new()));
        }

        let part = match self.get_partitioning(stream, topic, &msgs, partitioning.clone()) {
            Ok(p) => p,
            Err(err) => {
                return Err(self.make_failed_error(err, msgs, Vec::new()));
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
                let mut confirmations = Vec::with_capacity(msgs.len().div_ceil(max));
                while index < msgs.len() {
                    let end = (index + max).min(msgs.len());
                    let chunk = &mut msgs[index..end];

                    match self.try_send_messages(stream, topic, &part, chunk).await {
                        Ok(response) => confirmations.extend(response.confirmations),
                        Err(err) => {
                            let failed_tail = msgs.split_off(index);
                            return Err(self.make_failed_error(err, failed_tail, confirmations));
                        }
                    }
                    self.last_sent_at
                        .store(IggyTimestamp::now().into(), ORDERING);
                    index = end;
                }
                Ok(SendMessagesResponse { confirmations })
            }
            // background send on
            _ => {
                let response = self
                    .try_send_messages(stream, topic, &part, &mut msgs)
                    .await
                    .map_err(|err| self.make_failed_error(err, msgs, Vec::new()))?;
                self.last_sent_at
                    .store(IggyTimestamp::now().into(), ORDERING);
                Ok(response)
            }
        }
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

    /// Sends `messages` and returns the commit confirmations of every chunk the
    /// send was split into, concatenated in chunk order. A retried chunk
    /// contributes only the confirmation of the attempt that finally succeeded.
    ///
    /// Delivery is at-least-once. An earlier retry may already have committed
    /// the same messages at a lower offset, so `base_offset` never implies
    /// uniqueness.
    ///
    /// A batch is confirmed once it is committed in memory, not once it is
    /// fsynced. A crash-restart can stamp a later batch with an offset a client
    /// has already recorded.
    ///
    /// The confirmation list is empty whenever the server sends no confirmation
    /// payload, which the legacy server never does, and for a `background`
    /// producer, which hands the messages to a dispatcher and returns before the
    /// send happens. Branch on `confirmations.is_empty()` instead of indexing.
    pub async fn send(
        &self,
        messages: Vec<IggyMessage>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream_id, topic_id, None)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, None)
                    .await
            }
        }
    }

    /// See [`IggyProducer::send`] for the confirmation semantics.
    pub async fn send_one(&self, message: IggyMessage) -> Result<SendMessagesResponse, IggyError> {
        self.send(vec![message]).await
    }

    /// See [`IggyProducer::send`] for the confirmation semantics.
    pub async fn send_with_partitioning(
        &self,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        let stream_id = self.core.stream_id.clone();
        let topic_id = self.core.topic_id.clone();

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream_id, topic_id, partitioning)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream_id, &topic_id, messages, partitioning)
                    .await
            }
        }
    }

    /// See [`IggyProducer::send`] for the confirmation semantics.
    pub async fn send_to(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        messages: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(no_confirmations());
        }

        match &self.dispatcher {
            Some(disp) => disp
                .dispatch(messages, stream, topic, partitioning)
                .await
                .map(|()| no_confirmations()),
            None => {
                self.core
                    .send_internal(&stream, &topic, messages, partitioning)
                    .await
            }
        }
    }

    /// Flushes buffered messages in `background` mode before returning. A
    /// `direct`-mode producer has nothing to flush. Dropping the producer
    /// instead of calling this silently discards unflushed `background`
    /// messages.
    pub async fn shutdown(self) {
        if let Some(dispatcher) = self.dispatcher {
            dispatcher.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::implies_committed_send;
    use iggy_common::IggyError;

    #[test]
    fn test_unreadable_confirmation_of_a_committed_batch_stops_retrying() {
        assert!(implies_committed_send(&IggyError::InvalidBytesResponse));
        assert!(implies_committed_send(&IggyError::InvalidJsonResponse));
    }

    #[test]
    fn test_errors_reachable_before_a_commit_keep_retrying() {
        for error in [
            IggyError::Disconnected,
            IggyError::EmptyResponse,
            IggyError::Unauthenticated,
            IggyError::Unauthorized,
            IggyError::CannotSendMessagesDueToClientDisconnection,
            IggyError::HttpResponseError(500, String::new()),
            IggyError::ResourceNotFound(String::new()),
        ] {
            assert!(
                !implies_committed_send(&error),
                "{error} does not prove a commit, so it must stay retryable"
            );
        }
    }
}
