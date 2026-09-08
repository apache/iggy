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

use crate::{Consumer, ConsumerOffsetInfo, Identifier, IggyError};
use async_trait::async_trait;

/// This trait defines the methods to interact with the consumer offset module.
#[async_trait]
pub trait ConsumerOffsetClient {
    /// Store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    /// A new key at the per-partition limit returns [`IggyError::TooManyConsumerOffsets`] (3024).
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError>;
    /// Get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    /// An absent offset key returns `Ok(None)` when its stream, topic, and consumer group resolve.
    /// A local auto-commit cursor can be visible before its durable store commits.
    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError>;
    /// Delete the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    /// A missing durable key returns [`IggyError::ConsumerOffsetNotFound`] (3021).
    /// Missing numeric and named groups return codes 5000 and 5003 respectively.
    /// A replica unable to admit the request returns [`IggyError::TransientNotAccepted`].
    /// Deletion does not allocate a new key and is allowed at the offset limit.
    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError>;
}
