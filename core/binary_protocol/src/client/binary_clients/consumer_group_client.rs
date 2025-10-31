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
use iggy_common::{ConsumerGroup, ConsumerGroupDetails, Identifier, IggyError};

/// This trait defines the methods to interact with the consumer group module.
#[async_trait]
pub trait ConsumerGroupClient {
    /// Get the info about a specific consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError>;
    /// Get the info about all the consumer groups for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError>;
    /// Create a new consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError>;
    /// Delete a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Join a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Leave a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
}
