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
pub mod consumer;
pub mod durability;
pub mod error;
pub mod identifier;
pub mod message_iterator;
pub mod metadata_changes;
pub mod options;
pub mod receive_message;
pub mod runtime;
pub mod send_message;
pub mod sensitive_parameters;
pub mod stream;
pub mod topic;

use ext_php_rs::prelude::*;

use crate::client::{IggyClient, PendingOperationsConfig};
use crate::consumer::{AutoCommit, AutoCommitWhen, IggyConsumer};
use crate::error::{
    AuthenticationException, ConnectionException, IggyException, NotFoundException,
    PendingOperationException, TransientException,
};
use crate::message_iterator::MessageIterator;
use crate::metadata_changes::{
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
use crate::options::{OptionSpec, OptionsScope, StreamOptionKey, TopicOptionKey};
use crate::receive_message::{ConsumerPosition, PartitionContext, PollingStrategy, ReceiveMessage};
use crate::send_message::{SendMessage, SendMessagesConfirmation, SendMessagesResponse};
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .request_startup_function(sensitive_parameters::mark_sensitive_parameters)
        .enumeration::<durability::Durability>()
        .enumeration::<StreamOptionKey>()
        .enumeration::<TopicOptionKey>()
        .enumeration::<OptionsScope>()
        .enumeration::<MetadataChangeGroup>()
        .enumeration::<MetadataChangeKind>()
        .enumeration::<MetadataChangeStatus>()
        .enumeration::<MetadataChangePhase>()
        .enumeration::<MetadataMemberLeftReason>()
        .enumeration::<MetadataTokenDeletedReason>()
        .enumeration::<MetadataSessionClosedReason>()
        .enumeration::<MetadataPermissionsUpdatedReason>()
        // Parent classes must be registered before subclasses because ext-php-rs resolves
        // the parent ClassEntry during child registration.
        .class::<IggyException>()
        .class::<ConnectionException>()
        .class::<AuthenticationException>()
        .class::<NotFoundException>()
        .class::<TransientException>()
        .class::<PendingOperationException>()
        .class::<PendingOperationsConfig>()
        .class::<IggyClient>()
        .class::<IggyConsumer>()
        .class::<MessageIterator>()
        .class::<AutoCommit>()
        .class::<AutoCommitWhen>()
        .class::<PartitionContext>()
        .class::<ConsumerPosition>()
        .class::<PollingStrategy>()
        .class::<ReceiveMessage>()
        .class::<SendMessage>()
        .class::<SendMessagesConfirmation>()
        .class::<SendMessagesResponse>()
        .class::<StreamDetails>()
        .class::<TopicDetails>()
        .class::<OptionSpec>()
        .class::<MetadataChanges>()
        .class::<MetadataChangeRecord>()
        .class::<MetadataCurrentItem>()
        .class::<MetadataChangeCursor>()
        .class::<MetadataCurrentPageToken>()
        .class::<MetadataChangeStart>()
        .class::<MetadataStreamChange>()
        .class::<MetadataStreamDeleted>()
        .class::<MetadataTopicChange>()
        .class::<MetadataTopicItem>()
        .class::<MetadataTopicDeleted>()
        .class::<MetadataPartitionsChanged>()
        .class::<MetadataPartitionsItem>()
        .class::<MetadataConsumerGroupCreated>()
        .class::<MetadataConsumerGroupDeleted>()
        .class::<MetadataConsumerGroupItem>()
        .class::<MetadataMemberJoined>()
        .class::<MetadataMemberLeft>()
        .class::<MetadataPartitionHandedOver>()
        .class::<MetadataMemberItem>()
        .class::<MetadataUserChange>()
        .class::<MetadataUserIdentity>()
        .class::<MetadataUserPermissionsUpdated>()
        .class::<MetadataTokenChange>()
        .class::<MetadataTokenDeleted>()
        .class::<MetadataSession>()
        .class::<MetadataSessionClosed>()
        .class::<MetadataUnknownChange>()
        .class::<MetadataUnknownItem>()
}
