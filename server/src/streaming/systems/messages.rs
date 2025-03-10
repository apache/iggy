use crate::streaming::cache::memory_tracker::CacheMemoryTracker;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::systems::COMPONENT;
use bytes::Bytes;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::consumer::Consumer;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::Message;
use iggy::messages::send_messages::Partitioning;
use iggy::models::messages::{PolledMessage, PolledMessages};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use iggy::{error::IggyError, identifier::Identifier};
use tracing::{error, trace};

impl System {
    pub async fn poll_messages(
        &self,
        session: &Session,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        args: PollingArgs,
    ) -> Result<PolledMessages, IggyError> {
        self.ensure_authenticated(session)?;
        if args.count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
        self.permissioner
            .poll_messages(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to poll messages for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;

        if !topic.has_partitions() {
            return Err(IggyError::NoPartitions(topic.topic_id, topic.stream_id));
        }

        // There might be no partition assigned, if it's the consumer group member without any partitions.
        let Some((polling_consumer, partition_id)) = topic
            .resolve_consumer_with_partition_id(consumer, session.client_id, partition_id, true)
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer: {consumer}, client ID: {}, partition ID: {:?}", session.client_id, partition_id))? else {
            return Ok(PolledMessages {
                messages: vec![],
                partition_id: 0,
                current_offset: 0,
            })
        };

        let mut polled_messages = topic
            .get_messages(polling_consumer, partition_id, args.strategy, args.count)
            .await?;

        if polled_messages.messages.is_empty() {
            return Ok(polled_messages);
        }

        let offset = polled_messages.messages.last().unwrap().offset;
        if args.auto_commit {
            trace!("Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}", offset, consumer, stream_id, topic_id, partition_id);
            topic
                .store_consumer_offset_internal(polling_consumer, offset, partition_id)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset internal, polling consumer: {}, offset: {}, partition ID: {}", polling_consumer, offset, partition_id)) ?;
        }

        if self.encryptor.is_none() {
            return Ok(polled_messages);
        }

        let encryptor = self.encryptor.as_ref().unwrap();
        let mut decrypted_messages = Vec::with_capacity(polled_messages.messages.len());
        for message in polled_messages.messages.iter() {
            let payload = encryptor.decrypt(&message.payload);
            match payload {
                Ok(payload) => {
                    decrypted_messages.push(PolledMessage {
                        id: message.id,
                        state: message.state,
                        offset: message.offset,
                        timestamp: message.timestamp,
                        checksum: message.checksum,
                        length: IggyByteSize::from(payload.len() as u64),
                        payload: Bytes::from(payload),
                        headers: message.headers.clone(),
                    });
                }
                Err(error) => {
                    error!("Cannot decrypt the message. Error: {}", error);
                    return Err(IggyError::CannotDecryptData);
                }
            }
        }
        polled_messages.messages = decrypted_messages;
        Ok(polled_messages)
    }

    pub async fn append_messages(
        &self,
        session: &Session,
        stream_id: Identifier,
        topic_id: Identifier,
        partitioning: Partitioning,
        messages: Vec<Message>,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, &stream_id, &topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
        self.permissioner.append_messages(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ))?;

        let mut batch_size_bytes = IggyByteSize::default();
        let mut messages = messages;
        if let Some(encryptor) = &self.encryptor {
            for message in messages.iter_mut() {
                let payload = encryptor.encrypt(&message.payload);
                match payload {
                    Ok(payload) => {
                        message.payload = Bytes::from(payload);
                        message.length = message.payload.len() as u32;
                        batch_size_bytes += message.get_size_bytes();
                    }
                    Err(error) => {
                        error!("Cannot encrypt the message. Error: {}", error);
                        return Err(IggyError::CannotEncryptData);
                    }
                }
            }
        } else {
            batch_size_bytes = messages
                .iter()
                .map(|msg| msg.get_size_bytes())
                .sum::<IggyByteSize>();
        }

        if let Some(memory_tracker) = CacheMemoryTracker::get_instance() {
            if !memory_tracker.will_fit_into_cache(batch_size_bytes) {
                self.clean_cache(batch_size_bytes).await;
            }
        }
        let messages_count = messages.len() as u64;
        topic
            .append_messages(batch_size_bytes, partitioning, messages, confirmation)
            .await?;
        self.metrics.increment_messages(messages_count);
        Ok(())
    }

    pub async fn flush_unsaved_buffer(
        &self,
        session: &Session,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, &stream_id, &topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
        // Reuse those permissions as if you can append messages you can flush them
        self.permissioner.append_messages(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - permission denied to append messages for user {} on stream ID: {}, topic ID: {}",
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id
        ))?;
        topic.flush_unsaved_buffer(partition_id, fsync).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PollingArgs {
    pub strategy: PollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

impl PollingArgs {
    pub fn new(strategy: PollingStrategy, count: u32, auto_commit: bool) -> Self {
        Self {
            strategy,
            count,
            auto_commit,
        }
    }
}
