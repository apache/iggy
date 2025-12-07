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

//! Prelude module for the Iggy SDK.
//!
//! This module re-exports the most common types, traits, and functions
//! from the Iggy SDK to make them easier to import and use.
//!
//! # Examples
//!
//! ```
//! use iggy::prelude::*;
//! ```

pub use iggy_binary_protocol::{
    Client, ClusterClient, ConsumerGroupClient, ConsumerOffsetClient, MessageClient,
    PartitionClient, PersonalAccessTokenClient, SegmentClient, StreamClient, SystemClient,
    TopicClient, UserClient,
};
pub use iggy_common::{
    Aes256GcmEncryptor, Args, ArgsOptional, AutoLogin, BytesSerializable, CacheMetrics,
    CacheMetricsKey, ClientError, ClientInfoDetails, ClusterMetadata, ClusterNode, ClusterNodeRole,
    ClusterNodeStatus, CompressionAlgorithm, Consumer, ConsumerGroupDetails, ConsumerKind,
    EncryptorKind, FlushUnsavedBuffer, GlobalPermissions, HeaderKey, HeaderValue, HttpClientConfig,
    HttpClientConfigBuilder, IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE, IGGY_MESSAGE_HEADER_SIZE,
    IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_ID_OFFSET_RANGE,
    IGGY_MESSAGE_OFFSET_OFFSET_RANGE, IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE,
    IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE, INDEX_SIZE,
    IdKind, Identifier, IdentityInfo, IggyByteSize, IggyDuration, IggyError, IggyExpiry,
    IggyIndexView, IggyMessage, IggyMessageHeader, IggyMessageHeaderView, IggyMessageView,
    IggyMessageViewIterator, IggyTimestamp, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE, MaxTopicSize,
    Partition, Partitioner, Partitioning, Permissions, PersonalAccessTokenExpiry, PollMessages,
    PolledMessages, PollingKind, PollingStrategy, QuicClientConfig, QuicClientConfigBuilder,
    QuicClientReconnectionConfig, SEC_IN_MICRO, SendMessages, Sizeable, SnapshotCompression, Stats,
    Stream, StreamDetails, StreamPermissions, SystemSnapshotType, TcpClientConfig,
    TcpClientConfigBuilder, TcpClientReconnectionConfig, Topic, TopicDetails, TopicPermissions,
    TransportEndpoints, TransportProtocol, UserId, UserStatus, Validatable, WebSocketClientConfig,
    WebSocketClientConfigBuilder, WebSocketClientReconnectionConfig, defaults,
    defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USER_ID, DEFAULT_ROOT_USERNAME},
    locking,
};

pub use crate::{
    client_provider,
    client_provider::ClientProviderConfig,
    client_wrappers::{client_wrapper::ClientWrapper, connection_info::ConnectionInfo},
    clients::{
        client::IggyClient,
        client_builder::IggyClientBuilder,
        consumer::{AutoCommit, AutoCommitAfter, AutoCommitWhen, IggyConsumer, ReceivedMessage},
        consumer_builder::IggyConsumerBuilder,
        producer::IggyProducer,
        producer_builder::IggyProducerBuilder,
        producer_config::{BackgroundConfig, DirectConfig},
    },
    consumer_ext::IggyConsumerMessageExt,
    stream_builder::{
        IggyConsumerConfig, IggyProducerConfig, IggyStream, IggyStreamConfig, IggyStreamConsumer,
        IggyStreamProducer,
    },
    tcp::tcp_client::TcpClient,
    websocket::websocket_client::WebSocketClient,
};
