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

use crate::configs::kafka::KafkaConfig;
use crate::kafka::protocol::types::{CreateTopicResult, CreateTopicsRequest, CreateTopicsResponse};
use crate::kafka::protocol::{NONE, TOPIC_ALREADY_EXISTS};
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedTopic;
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use std::rc::Rc;
use tracing::warn;

pub async fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig, body: &[u8]) -> Vec<u8> {
    let mut pos = 0;
    let req = match CreateTopicsRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return error_response(),
    };

    // Ensure default stream exists
    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => return error_response(),
    };

    if shard.resolve_stream(&stream_ident).is_err()
        && let Err(e) = shard.create_stream(config.default_stream.clone()).await
    {
        warn!(
            "Failed to create default stream '{}': {e}",
            config.default_stream
        );
        return error_response();
    }

    let stream = match shard.resolve_stream(&stream_ident) {
        Ok(s) => s,
        Err(_) => return error_response(),
    };

    let mut results = Vec::with_capacity(req.topics.len());
    for topic_req in &req.topics {
        let topic_ident = match Identifier::from_str_value(&topic_req.name) {
            Ok(id) => id,
            Err(_) => {
                results.push(CreateTopicResult {
                    name: topic_req.name.clone(),
                    error_code: -1,
                });
                continue;
            }
        };

        // Check if topic already exists
        if shard.resolve_topic(&stream_ident, &topic_ident).is_ok() {
            results.push(CreateTopicResult {
                name: topic_req.name.clone(),
                error_code: TOPIC_ALREADY_EXISTS,
            });
            continue;
        }

        let partitions = if topic_req.num_partitions > 0 {
            topic_req.num_partitions as u32
        } else {
            config.num_partitions
        };
        match shard
            .create_topic(
                stream,
                topic_req.name.clone(),
                IggyExpiry::NeverExpire,
                CompressionAlgorithm::None,
                MaxTopicSize::Unlimited,
                None,
            )
            .await
        {
            Ok(topic_id) => {
                // Create additional partitions (create_topic creates 0 by default)
                let resolved_topic = ResolvedTopic {
                    stream_id: stream.0,
                    topic_id,
                };
                if let Err(e) = shard.create_partitions(resolved_topic, partitions).await {
                    warn!(
                        "Failed to create partitions for topic '{}': {e}",
                        topic_req.name
                    );
                }
                results.push(CreateTopicResult {
                    name: topic_req.name.clone(),
                    error_code: NONE,
                });
            }
            Err(e) => {
                warn!("Failed to create topic '{}': {e}", topic_req.name);
                results.push(CreateTopicResult {
                    name: topic_req.name.clone(),
                    error_code: -1,
                });
            }
        }
    }

    CreateTopicsResponse { topics: results }.encode()
}

fn error_response() -> Vec<u8> {
    CreateTopicsResponse { topics: vec![] }.encode()
}
