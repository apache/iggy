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
use crate::prelude::Identifier;
use crate::prelude::IggyError;
use async_trait::async_trait;
use iggy_common::ConsumerOffsetClient;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{Consumer, ConsumerKind, ConsumerOffsetInfo};

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
        refuse_external_group(consumer)?;
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
        refuse_external_group(consumer)?;
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
        refuse_external_group(consumer)?;
        let partition_id = partition_id
            .map(|id| format!("?partition_id={id}"))
            .unwrap_or_default();

        let path = format!(
            "{}/{}",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            consumer.id
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
    format!("streams/{stream_id}/topics/{topic_id}/consumer-offsets")
}

/// The REST API cannot name a consumer kind, so an external group would land on a plain
/// consumer's offset.
pub(crate) fn refuse_external_group(consumer: &Consumer) -> Result<(), IggyError> {
    if consumer.kind == ConsumerKind::ExternalGroup {
        return Err(IggyError::FeatureUnavailable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{MessageClient, PollingStrategy};

    /// Nothing listens on this port, so a call that reached the network would fail with a
    /// transport error instead.
    const UNREACHABLE_API: &str = "http://127.0.0.1:1";

    #[tokio::test]
    async fn given_external_group_when_calling_over_http_should_refuse_before_sending() {
        let client = HttpClient::new(UNREACHABLE_API).unwrap();
        let group = Consumer::external_group(Identifier::numeric(1).unwrap());
        let stream = Identifier::numeric(1).unwrap();
        let topic = Identifier::numeric(1).unwrap();

        assert!(matches!(
            client
                .store_consumer_offset(&group, &stream, &topic, Some(0), 5)
                .await,
            Err(IggyError::FeatureUnavailable)
        ));
        assert!(matches!(
            client
                .get_consumer_offset(&group, &stream, &topic, Some(0))
                .await,
            Err(IggyError::FeatureUnavailable)
        ));
        assert!(matches!(
            client
                .delete_consumer_offset(&group, &stream, &topic, Some(0))
                .await,
            Err(IggyError::FeatureUnavailable)
        ));
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
    fn given_plain_consumer_when_guarded_should_pass() {
        let consumer = Consumer::new(Identifier::numeric(1).unwrap());

        assert!(refuse_external_group(&consumer).is_ok());
    }
}
