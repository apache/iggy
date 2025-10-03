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
use iggy_common::{Consumer, ConsumerOffsetInfo, Identifier, IggyError};

/// This trait defines the methods to interact with the consumer offset module.
#[async_trait]
pub trait ConsumerOffsetClient {
    /// Store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<usize>,
        offset: u64,
    ) -> Result<(), IggyError>;
    /// Get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<usize>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError>;
    /// Delete the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<usize>,
    ) -> Result<(), IggyError>;
}
