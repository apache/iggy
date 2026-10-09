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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::http::path::encode_segment;
use crate::prelude::Identifier;
use crate::prelude::IggyError;
use async_trait::async_trait;
use iggy_common::ConsumerOffsetClient;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
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
        let path = format!(
            "{}/{}",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            encode_segment(&consumer.id.as_cow_str())
        );
        self.delete_with_query(
            &path,
            &DeleteConsumerOffset {
                consumer_kind: consumer.kind,
                partition_id,
            },
        )
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    let encoded_stream = encode_segment(stream_id);
    let encoded_topic = encode_segment(topic_id);
    format!("streams/{encoded_stream}/topics/{encoded_topic}/consumer-offsets")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{MessageClient, PollingStrategy};

    /// Nothing listens on this port, so a call that reached the network would fail with a
    /// transport error instead.
    const UNREACHABLE_API: &str = "http://127.0.0.1:1";

    #[tokio::test]
    async fn given_external_group_when_polling_over_http_should_refuse_before_sending() {
        let client = HttpClient::new(UNREACHABLE_API).unwrap();
        let group = Consumer::external_group(Identifier::numeric(1).unwrap());
        let stream = Identifier::numeric(1).unwrap();
        let topic = Identifier::numeric(1).unwrap();

        assert!(matches!(
            client
                .poll_messages(
                    &stream,
                    &topic,
                    Some(0),
                    &group,
                    &PollingStrategy::next(),
                    1,
                    false
                )
                .await,
            Err(IggyError::FeatureUnavailable)
        ));
    }

    #[test]
    fn given_plain_ids_when_building_path_should_join_segments() {
        let path = get_path("1", "orders");

        assert_eq!(path, "streams/1/topics/orders/consumer-offsets");
    }

    #[test]
    fn given_reserved_characters_when_building_path_should_percent_encode() {
        let path = get_path("my stream", "my/topic");

        assert_eq!(
            path,
            "streams/my%20stream/topics/my%2Ftopic/consumer-offsets"
        );
    }
}
