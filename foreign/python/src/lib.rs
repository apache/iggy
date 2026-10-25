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

pub mod client;
mod config;
mod consumer;
mod durability;
mod duration;
mod identifier;
mod metadata_changes;
mod options;
mod partitioning;
mod permissions;
mod producer;
mod receive_message;
mod resource_options;
mod send_message;
mod stats;
mod stream;
mod topic;
mod user;
mod user_headers;

use client::{IggyClient, OptionError, PendingOperationError, PyIggyError};
use config::{
    AutoLogin, HttpConfig, PendingOperationsConfig, QuicConfig, QuicReconnectionConfig, TcpConfig,
    TcpReconnectionConfig, WebSocketConfig, WebSocketFramingConfig, WebSocketReconnectionConfig,
};
use consumer::{
    AutoCommit, AutoCommitAfter, AutoCommitWhen, Consumer, ConsumerGroup, ConsumerGroupDetails,
    ConsumerGroupMember, IggyConsumer, ReceiveMessageIterator,
};
use metadata_changes::{
    MetadataChangeCursor, MetadataChangeGroup, MetadataChangeKind, MetadataChangePhase,
    MetadataChangeRecord, MetadataChangeStart, MetadataChangeStatus, MetadataChanges,
    MetadataConsumerGroupCreated, MetadataConsumerGroupDeleted, MetadataConsumerGroupItem,
    MetadataCurrentItem, MetadataCurrentPageToken, MetadataMemberItem, MetadataMemberJoined,
    MetadataMemberLeft, MetadataMemberLeftReason, MetadataPartitionHandedOver,
    MetadataPartitionsChanged, MetadataPartitionsItem, MetadataPermissionsUpdatedReason,
    MetadataSession, MetadataSessionClosed, MetadataSessionClosedReason, MetadataStreamChange,
    MetadataStreamDeleted, MetadataTokenChange, MetadataTokenDeleted, MetadataTokenDeletedReason,
    MetadataTopicChange, MetadataTopicDeleted, MetadataTopicItem, MetadataUnknownChange,
    MetadataUnknownItem, MetadataUserChange, MetadataUserIdentity, MetadataUserPermissionsUpdated,
};
use options::OptionSpec;
use partitioning::Partitioning;
use permissions::{GlobalPermissions, Permissions, StreamPermissions, TopicPermissions};
use producer::{
    BackgroundProducerConfig, BackpressureMode, DirectProducerConfig, IggyProducer,
    ProducerSendError, ProducerSharding,
};
use pyo3::prelude::*;
use receive_message::{ConsumerPosition, PartitionContext, PollingStrategy, ReceiveMessage};
use resource_options::{
    OptionKey, PartitionResizePolicy, StreamOptions, TopicOptions, UserOptions,
    encryption_key_fingerprint,
};
use send_message::{SendMessage, SendMessagesConfirmation, SendMessagesResponse};
use stats::{CacheMetrics, CacheMetricsKey, Stats};
use stream::{Stream, StreamDetails};
use topic::{IggyExpiry, MaxTopicSize, Partition, Topic, TopicDetails};
use user::{IdentityInfo, TokenInfo, UserInfo, UserInfoDetails, UserStatus};
use user_headers::{HeaderKey, HeaderValue, UserHeaders};

/// Python client for Apache Iggy, the persistent message streaming platform.
#[pymodule]
fn apache_iggy(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    durability::Durability::register(py, m)?;
    m.add_class::<SendMessage>()?;
    m.add_class::<SendMessagesResponse>()?;
    m.add_class::<SendMessagesConfirmation>()?;
    m.add_class::<ReceiveMessage>()?;
    m.add_class::<PartitionContext>()?;
    m.add_class::<ConsumerPosition>()?;
    m.add_class::<IggyClient>()?;
    m.add_class::<PyIggyError>()?;
    m.add_class::<PendingOperationError>()?;
    m.add_class::<OptionError>()?;
    m.add_class::<IggyProducer>()?;
    m.add_class::<ProducerSendError>()?;
    m.add_class::<DirectProducerConfig>()?;
    m.add_class::<BackgroundProducerConfig>()?;
    m.add_class::<ProducerSharding>()?;
    m.add_class::<BackpressureMode>()?;
    m.add_class::<AutoLogin>()?;
    m.add_class::<PendingOperationsConfig>()?;
    m.add_class::<TcpConfig>()?;
    m.add_class::<TcpReconnectionConfig>()?;
    m.add_class::<QuicConfig>()?;
    m.add_class::<QuicReconnectionConfig>()?;
    m.add_class::<HttpConfig>()?;
    m.add_class::<WebSocketConfig>()?;
    m.add_class::<WebSocketReconnectionConfig>()?;
    m.add_class::<WebSocketFramingConfig>()?;
    m.add_class::<StreamDetails>()?;
    m.add_class::<Stream>()?;
    m.add_class::<Stats>()?;
    m.add_class::<CacheMetrics>()?;
    m.add_class::<CacheMetricsKey>()?;
    m.add_class::<Topic>()?;
    m.add_class::<TopicDetails>()?;
    m.add_class::<IggyExpiry>()?;
    m.add_class::<MaxTopicSize>()?;
    m.add_class::<OptionSpec>()?;
    m.add_class::<OptionKey>()?;
    m.add_class::<StreamOptions>()?;
    m.add_class::<TopicOptions>()?;
    m.add_class::<UserOptions>()?;
    m.add_class::<PartitionResizePolicy>()?;
    m.add_function(wrap_pyfunction!(encryption_key_fingerprint, m)?)?;
    m.add_class::<Partitioning>()?;
    m.add_class::<Partition>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<ConsumerGroup>()?;
    m.add_class::<ConsumerGroupDetails>()?;
    m.add_class::<ConsumerGroupMember>()?;
    m.add_class::<PollingStrategy>()?;
    m.add_class::<IggyConsumer>()?;
    m.add_class::<AutoCommit>()?;
    m.add_class::<AutoCommitAfter>()?;
    m.add_class::<AutoCommitWhen>()?;
    m.add_class::<ReceiveMessageIterator>()?;
    m.add_class::<IdentityInfo>()?;
    m.add_class::<TokenInfo>()?;
    m.add_class::<UserStatus>()?;
    m.add_class::<UserInfo>()?;
    m.add_class::<UserInfoDetails>()?;
    m.add_class::<UserHeaders>()?;
    m.add_class::<HeaderKey>()?;
    m.add_class::<HeaderValue>()?;
    m.add_class::<Permissions>()?;
    m.add_class::<GlobalPermissions>()?;
    m.add_class::<StreamPermissions>()?;
    m.add_class::<TopicPermissions>()?;
    m.add_class::<MetadataChanges>()?;
    m.add_class::<MetadataChangeRecord>()?;
    m.add_class::<MetadataCurrentItem>()?;
    m.add_class::<MetadataChangeCursor>()?;
    m.add_class::<MetadataCurrentPageToken>()?;
    m.add_class::<MetadataChangeStart>()?;
    m.add_class::<MetadataChangeGroup>()?;
    m.add_class::<MetadataChangeKind>()?;
    m.add_class::<MetadataChangeStatus>()?;
    m.add_class::<MetadataChangePhase>()?;
    m.add_class::<MetadataMemberLeftReason>()?;
    m.add_class::<MetadataTokenDeletedReason>()?;
    m.add_class::<MetadataSessionClosedReason>()?;
    m.add_class::<MetadataPermissionsUpdatedReason>()?;
    m.add_class::<MetadataStreamChange>()?;
    m.add_class::<MetadataStreamDeleted>()?;
    m.add_class::<MetadataTopicChange>()?;
    m.add_class::<MetadataTopicItem>()?;
    m.add_class::<MetadataTopicDeleted>()?;
    m.add_class::<MetadataPartitionsChanged>()?;
    m.add_class::<MetadataPartitionsItem>()?;
    m.add_class::<MetadataConsumerGroupCreated>()?;
    m.add_class::<MetadataConsumerGroupDeleted>()?;
    m.add_class::<MetadataConsumerGroupItem>()?;
    m.add_class::<MetadataMemberJoined>()?;
    m.add_class::<MetadataMemberLeft>()?;
    m.add_class::<MetadataPartitionHandedOver>()?;
    m.add_class::<MetadataMemberItem>()?;
    m.add_class::<MetadataUserChange>()?;
    m.add_class::<MetadataUserIdentity>()?;
    m.add_class::<MetadataUserPermissionsUpdated>()?;
    m.add_class::<MetadataTokenChange>()?;
    m.add_class::<MetadataTokenDeleted>()?;
    m.add_class::<MetadataSession>()?;
    m.add_class::<MetadataSessionClosed>()?;
    m.add_class::<MetadataUnknownChange>()?;
    m.add_class::<MetadataUnknownItem>()?;
    Ok(())
}
