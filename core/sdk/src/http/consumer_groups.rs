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
use crate::prelude::IggyError;
use async_trait::async_trait;
use iggy_binary_protocol::ConsumerGroupClient;
use iggy_common::Identifier;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::{ConsumerGroup, ConsumerGroupDetails};

#[async_trait]
impl ConsumerGroupClient for HttpClient {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                group_id
            ))
            .await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let consumer_group = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(consumer_group))
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        let response = self
            .get(&get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()))
            .await?;
        let consumer_groups = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(consumer_groups)
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        let response = self
            .post(
                &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                &CreateConsumerGroup {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    name: name.to_string(),
                },
            )
            .await?;
        let consumer_group = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(consumer_group)
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let path = format!(
            "{}/{}",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &group_id.as_cow_str()
        );
        self.delete(&path).await?;
        Ok(())
    }

    async fn join_consumer_group(
        &self,
        _: &Identifier,
        _: &Identifier,
        _: &Identifier,
    ) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    async fn leave_consumer_group(
        &self,
        _: &Identifier,
        _: &Identifier,
        _: &Identifier,
    ) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-groups")
}
