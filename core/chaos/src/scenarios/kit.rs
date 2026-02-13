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

use super::NamespacePrefixes;
use crate::safe_name::SafeResourceName;
use iggy::prelude::*;
use std::str::FromStr;

pub struct TopicDefaults {
    pub partitions: u32,
    pub compression: CompressionAlgorithm,
    pub expiry: IggyExpiry,
    pub max_size: MaxTopicSize,
}

impl Default for TopicDefaults {
    fn default() -> Self {
        Self {
            partitions: 3,
            compression: CompressionAlgorithm::None,
            expiry: IggyExpiry::NeverExpire,
            max_size: MaxTopicSize::Unlimited,
        }
    }
}

/// Convenience wrapper for the repeated create-stream / create-topic /
/// create-consumer-group / send-messages boilerplate in scenario `setup()`.
pub struct Setup<'a> {
    client: &'a IggyClient,
    prefixes: &'a NamespacePrefixes,
}

impl<'a> Setup<'a> {
    pub fn new(client: &'a IggyClient, prefixes: &'a NamespacePrefixes) -> Self {
        Self { client, prefixes }
    }

    pub async fn create_stream(&self, suffix: &str) -> Result<SafeResourceName, IggyError> {
        let name = SafeResourceName::new(&self.prefixes.stable, suffix);
        self.client.create_stream(&name).await?;
        Ok(name)
    }

    pub async fn create_topic(
        &self,
        stream: &SafeResourceName,
        suffix: &str,
        defaults: &TopicDefaults,
    ) -> Result<SafeResourceName, IggyError> {
        let stream_id = Identifier::from_str_value(stream).unwrap();
        let name = SafeResourceName::new(&self.prefixes.base, suffix);
        self.client
            .create_topic(
                &stream_id,
                &name,
                defaults.partitions,
                defaults.compression,
                None,
                defaults.expiry,
                defaults.max_size,
            )
            .await?;
        Ok(name)
    }

    pub async fn create_consumer_group(
        &self,
        stream: &SafeResourceName,
        topic: &SafeResourceName,
        suffix: &str,
    ) -> Result<SafeResourceName, IggyError> {
        let stream_id = Identifier::from_str_value(stream).unwrap();
        let topic_id = Identifier::from_str_value(topic).unwrap();
        let name = SafeResourceName::new(&self.prefixes.base, suffix);
        self.client
            .create_consumer_group(&stream_id, &topic_id, &name)
            .await?;
        Ok(name)
    }

    pub async fn send_str(
        &self,
        stream: &SafeResourceName,
        topic: &SafeResourceName,
        partition: u32,
        payload: &str,
    ) -> Result<(), IggyError> {
        let stream_id = Identifier::from_str_value(stream).unwrap();
        let topic_id = Identifier::from_str_value(topic).unwrap();
        let msg = IggyMessage::from_str(payload).unwrap();
        let mut messages = vec![msg];
        self.client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
    }
}
