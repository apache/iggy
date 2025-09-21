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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use async_trait::async_trait;
use iggy_binary_protocol::TopicClient;
use iggy_common::create_topic::CreateTopic;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{Topic, TopicDetails};

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        let response = self
            .get(&get_details_path(
                &stream_id.as_cow_str(),
                &topic_id.as_cow_str(),
            ))
            .await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let topic = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(topic))
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        let response = self.get(&get_path(&stream_id.as_cow_str())).await?;
        let topics = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(topics)
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
        let response = self
            .post(
                &get_path(&stream_id.as_cow_str()),
                &CreateTopic {
                    stream_id: stream_id.clone(),
                    name: name.to_string(),
                    partitions_count,
                    compression_algorithm,
                    replication_factor,
                    message_expiry,
                    max_topic_size,
                },
            )
            .await?;
        let topic = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(topic)
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
        self.put(
            &get_details_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &UpdateTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: name.to_string(),
                compression_algorithm,
                replication_factor,
                message_expiry,
                max_topic_size,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.delete(&get_details_path(
            &stream_id.as_cow_str(),
            &topic_id.as_cow_str(),
        ))
        .await?;
        Ok(())
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.delete(&format!(
            "{}/purge",
            &get_details_path(&stream_id.as_cow_str(), &topic_id.as_cow_str(),)
        ))
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str) -> String {
    format!("streams/{stream_id}/topics")
}

fn get_details_path(stream_id: &str, topic_id: &str) -> String {
    format!("{}/{topic_id}", get_path(stream_id))
}
