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

use async_trait::async_trait;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize, Topic, TopicDetails,
};

/// This trait defines the methods to interact with the topic module.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait TopicClient {
    /// Get the info about a specific topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError>;
    /// Get the info about all the topics.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError>;
    /// Create a new topic.
    ///
    /// Authentication is required, and the permission to manage the topics.
    #[allow(clippy::too_many_arguments)]
    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError>;
    /// Update a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError>;
    /// Delete a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Purge a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
}
