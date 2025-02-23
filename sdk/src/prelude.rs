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

pub use crate::bytes_serializable::BytesSerializable;
pub use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    StreamClient, SystemClient, TopicClient, UserClient,
};
pub use crate::client_provider;
pub use crate::client_provider::ClientProviderConfig;
pub use crate::clients::builder::IggyClientBuilder;
pub use crate::clients::client::IggyClient;
pub use crate::clients::consumer::{
    AutoCommit, AutoCommitAfter, AutoCommitWhen, IggyConsumer, ReceivedMessage,
};
pub use crate::clients::producer::{IggyProducer, IggyProducerBuilder};
pub use crate::compression::compression_algorithm::CompressionAlgorithm;
pub use crate::consumer::{Consumer, ConsumerKind};
pub use crate::consumer_ext::IggyConsumerMessageExt;
pub use crate::error::IggyError;
pub use crate::identifier::Identifier;
pub use crate::messages::{
    FlushUnsavedBuffer, Partitioning, PollMessages, PolledMessages, PollingKind, PollingStrategy,
    SendMessages,
};
pub use crate::models::consumer_group::ConsumerGroupDetails;
pub use crate::models::messaging::{
    HeaderKey, HeaderValue, IggyIndexView, IggyMessage, IggyMessageHeader, IggyMessageHeaderView,
    IggyMessageView, IggyMessageViewIterator, INDEX_SIZE, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE,
};
pub use crate::models::messaging::{
    IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE, IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_HEADER_SIZE, IGGY_MESSAGE_ID_OFFSET_RANGE, IGGY_MESSAGE_OFFSET_OFFSET_RANGE,
    IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE, IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
};
pub use crate::models::partition::Partition;
pub use crate::models::permissions::{
    GlobalPermissions, Permissions, StreamPermissions, TopicPermissions,
};
pub use crate::models::stats::Stats;
pub use crate::models::stream::Stream;
pub use crate::models::topic::Topic;
pub use crate::models::user_status::UserStatus;
pub use crate::snapshot::{SnapshotCompression, SystemSnapshotType};
pub use crate::stream_builder::IggyConsumerConfig;
pub use crate::stream_builder::IggyStreamConsumer;
pub use crate::stream_builder::{IggyProducerConfig, IggyStreamProducer};
pub use crate::stream_builder::{IggyStream, IggyStreamConfig};
pub use crate::users::defaults::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_USER_ID,
};
pub use crate::utils::byte_size::IggyByteSize;
pub use crate::utils::duration::IggyDuration;
pub use crate::utils::expiry::IggyExpiry;
pub use crate::utils::sizeable::Sizeable;
pub use crate::utils::timestamp::IggyTimestamp;
pub use crate::utils::topic_size::MaxTopicSize;
pub use crate::validatable::Validatable;
