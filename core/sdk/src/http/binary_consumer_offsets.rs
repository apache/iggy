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
use crate::prelude::Identifier;
use crate::prelude::IggyError;
use async_trait::async_trait;
use iggy_binary_protocol::ConsumerOffsetClient;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{Consumer, ConsumerOffsetInfo};

#[async_trait]
impl ConsumerOffsetClient for HttpClient {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.put(
            &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &StoreConsumerOffset {
                consumer: consumer.clone(),
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partition_id,
                offset,
            },
        )
        .await?;
        Ok(())
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let response = self
            .get_with_query(
                &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                &GetConsumerOffset {
                    consumer: consumer.clone(),
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    partition_id,
                },
            )
            .await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let offset = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(offset))
    }

    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        let partition_id = partition_id
            .map(|id| format!("?partition_id={id}"))
            .unwrap_or_default();
        let path = format!(
            "{}/{}{partition_id}",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            consumer.id
        );
        self.delete(&path).await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-offsets")
}
