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

use crate::{Identifier, RUNTIME, StreamDetails, TopicDetails, ffi};
use bytes::Bytes;
use iggy::prelude::{
    Client as RustClient, CompressionAlgorithm as RustCompressionAlgorithm, Consumer,
    IggyClient as RustIggyClient, IggyClientBuilder as RustIggyClientBuilder, IggyDuration,
    IggyError as RustIggyError, IggyExpiry as RustIggyExpiry, IggyMessage as RustMessage,
    IggyTimestamp, MaxTopicSize as RustMaxTopicSize, MessageClient, Partitioning,
    PolledMessages as RustPolledMessages, PollingKind, PollingStrategy as RustPollingStrategy,
    StreamClient, TopicClient, UserClient,
};
use std::result::Result;
use std::str::FromStr;
use std::sync::Arc;

pub(crate) struct Client {
    pub(crate) inner: Arc<RustIggyClient>,
}

pub(crate) fn new_connection(connection_string: String) -> Result<*mut Client, String> {
    let connection_str = connection_string.as_str();
    let client = if connection_str.is_empty() {
        RustIggyClientBuilder::new()
            .with_tcp()
            .build()
            .map_err(|error| format!("Could not build default connection: {error}"))?
    } else if connection_str.starts_with("iggy://") || connection_str.starts_with("iggy+") {
        RustIggyClient::from_connection_string(connection_str).map_err(|error| {
            format!(
                "Could not parse connection string '{}': {error}",
                connection_str
            )
        })?
    } else {
        RustIggyClientBuilder::new()
            .with_tcp()
            .with_server_address(connection_string.clone())
            .build()
            .map_err(|error| {
                format!(
                    "Could not build connection for address '{}': {error}",
                    connection_str
                )
            })?
    };

    Ok(Box::into_raw(Box::new(Client {
        inner: Arc::new(client),
    })))
}

impl Client {
    pub(crate) fn connect(&self) -> Result<(), String> {
        RUNTIME
            .block_on(self.inner.connect())
            .map_err(|error| format!("Could not connect: {error}"))?;
        Ok(())
    }

    pub(crate) fn login_user(&self, username: String, password: String) -> Result<(), String> {
        RUNTIME
            .block_on(self.inner.login_user(&username, &password))
            .map_err(|error| format!("Could not login user '{}': {error}", username))?;
        Ok(())
    }

    pub(crate) fn create_stream(&self, stream_name: String) -> Result<(), String> {
        RUNTIME
            .block_on(self.inner.create_stream(&stream_name))
            .map_err(|error| format!("Could not create stream '{}': {error}", stream_name))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_topic(
        &self,
        stream_id: &Identifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: String,
        replication_factor: u8,
        expiry_type: String,
        expiry_value: u32,
        max_topic_size: String,
    ) -> Result<(), String> {
        let compression_algorithm = RustCompressionAlgorithm::from_str(
            compression_algorithm.as_str(),
        )
        .map_err(|error| {
            format!(
                "Could not parse compression algorithm '{}': {error}",
                compression_algorithm
            )
        })?;
        let replication_factor = if replication_factor == 0 {
            None
        } else {
            Some(replication_factor)
        };
        let expiry = match expiry_type.as_str() {
            "server_default" => RustIggyExpiry::ServerDefault,
            "never_expire" => RustIggyExpiry::NeverExpire,
            "duration" => RustIggyExpiry::ExpireDuration(IggyDuration::from(expiry_value as u64)),
            _ => {
                return Err(format!(
                    "Invalid expiry type '{}'. Expected server_default, never_expire, or duration.",
                    expiry_type
                ));
            }
        };
        let max_topic_size =
            RustMaxTopicSize::from_str(max_topic_size.as_str()).map_err(|error| {
                format!(
                    "Could not parse max topic size '{}': {error}",
                    max_topic_size
                )
            })?;

        RUNTIME
            .block_on(self.inner.create_topic(
                stream_id.as_rust(),
                name.as_str(),
                partitions_count,
                compression_algorithm,
                replication_factor,
                expiry,
                max_topic_size,
            ))
            .map_err(|error| format!("Could not create topic '{}': {error}", name))?;
        Ok(())
    }

    pub(crate) fn send_messages(
        &self,
        messages: Vec<ffi::Message>,
        stream_id: &Identifier,
        topic: &Identifier,
        partitioning: u32,
    ) -> Result<(), String> {
        let mut rust_messages: Vec<RustMessage> = messages
            .into_iter()
            .map(RustMessage::try_from)
            .collect::<std::result::Result<_, _>>()
            .map_err(|error| format!("Could not build message payloads: {error}"))?;

        let partitioning = Partitioning::partition_id(partitioning);
        RUNTIME
            .block_on(self.inner.send_messages(
                stream_id.as_rust(),
                topic.as_rust(),
                &partitioning,
                rust_messages.as_mut(),
            ))
            .map_err(|error| format!("Could not send messages: {error}"))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic: &Identifier,
        partition_id: u32,
        polling_strategy_kind: String,
        polling_strategy_value: u64,
        count: u32,
        auto_commit: bool,
    ) -> Result<ffi::PolledMessages, String> {
        let consumer = Consumer::default();
        let kind = PollingKind::from_str(polling_strategy_kind.as_str()).map_err(|error| {
            format!(
                "Invalid polling strategy kind '{}': {error}",
                polling_strategy_kind
            )
        })?;
        let polling_strategy = match kind {
            PollingKind::Offset => RustPollingStrategy::offset(polling_strategy_value),
            PollingKind::Timestamp => {
                RustPollingStrategy::timestamp(IggyTimestamp::from(polling_strategy_value))
            }
            PollingKind::First => RustPollingStrategy::first(),
            PollingKind::Last => RustPollingStrategy::last(),
            PollingKind::Next => RustPollingStrategy::next(),
        };

        let polled_messages = RUNTIME
            .block_on(self.inner.poll_messages(
                stream_id.as_rust(),
                topic.as_rust(),
                Some(partition_id),
                &consumer,
                &polling_strategy,
                count,
                auto_commit,
            ))
            .map_err(|error| format!("Could not poll messages: {error}"))?;

        Ok(ffi::PolledMessages::from(&polled_messages))
    }

    pub(crate) fn get_stream(&self, stream_id: &Identifier) -> Result<*mut StreamDetails, String> {
        let stream = RUNTIME
            .block_on(self.inner.get_stream(stream_id.as_rust()))
            .map_err(|error| format!("Could not fetch stream: {error}"))?;
        match stream {
            Some(details) => Ok(Box::into_raw(Box::new(StreamDetails::new(details)))),
            None => Err(format!(
                "Stream not found for identifier '{}'.",
                stream_id.get_value()
            )),
        }
    }

    pub(crate) fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<*mut TopicDetails, String> {
        let topic = RUNTIME
            .block_on(
                self.inner
                    .get_topic(stream_id.as_rust(), topic_id.as_rust()),
            )
            .map_err(|error| format!("Could not fetch topic: {error}"))?;
        match topic {
            Some(details) => Ok(Box::into_raw(Box::new(TopicDetails::new(details)))),
            None => Err(format!(
                "Topic not found for stream '{}' and topic '{}'.",
                stream_id.get_value(),
                topic_id.get_value()
            )),
        }
    }
}

pub(crate) unsafe fn delete_connection(client: *mut Client) {
    if client.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(client));
    }
}

impl From<&RustMessage> for ffi::Message {
    fn from(message: &RustMessage) -> Self {
        let id_high = (message.header.id >> 64) as u64;
        let id_low = message.header.id as u64;
        let user_headers = message
            .user_headers
            .as_ref()
            .map(|headers| headers.to_vec())
            .unwrap_or_default();

        ffi::Message {
            checksum: message.header.checksum,
            id_high,
            id_low,
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            user_headers_length: message.header.user_headers_length,
            payload_length: message.header.payload_length,
            payload: message.payload.to_vec(),
            user_headers,
        }
    }
}

impl From<&RustPolledMessages> for ffi::PolledMessages {
    fn from(polled_messages: &RustPolledMessages) -> Self {
        let messages = polled_messages
            .messages
            .iter()
            .map(ffi::Message::from)
            .collect();

        ffi::PolledMessages {
            partition_id: polled_messages.partition_id,
            current_offset: polled_messages.current_offset,
            count: polled_messages.count,
            messages,
        }
    }
}

impl TryFrom<ffi::Message> for RustMessage {
    type Error = RustIggyError;

    fn try_from(message: ffi::Message) -> Result<Self, RustIggyError> {
        let payload = Bytes::from(message.payload);
        let builder = RustMessage::builder().payload(payload);
        builder.build()
    }
}
