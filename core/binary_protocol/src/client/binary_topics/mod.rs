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

use crate::utils::auth::fail_if_not_authenticated;
use crate::utils::mapper;
use crate::{BinaryClient, TopicClient};
use iggy_common::create_topic::CreateTopic;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::get_topic::GetTopic;
use iggy_common::get_topics::GetTopics;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize, Topic, TopicDetails,
};

#[async_trait::async_trait]
impl<B: BinaryClient> TopicClient for B {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_topic(response).map(Some)
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetTopics {
                stream_id: stream_id.clone(),
            })
            .await?;
        mapper::map_topics(response)
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateTopic {
                stream_id: stream_id.clone(),
                name: name.to_string(),
                partitions_count,
                compression_algorithm,
                replication_factor,
                message_expiry,
                max_topic_size,
            })
            .await?;
        mapper::map_topic(response)
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            name: name.to_string(),
            compression_algorithm,
            replication_factor,
            message_expiry,
            max_topic_size,
        })
        .await?;
        Ok(())
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&PurgeTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
        })
        .await?;
        Ok(())
    }
}
