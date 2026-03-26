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
use crate::kafka::protocol::NONE;
use crate::kafka::protocol::types::{
    Broker, MetadataRequest, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedTopic;
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use std::rc::Rc;
use tracing::debug;

pub async fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig, body: &[u8]) -> Vec<u8> {
    let mut pos = 0;
    let req = match MetadataRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return error_response(),
    };

    let bound_addr = shard.kafka_bound_address.get().unwrap_or_else(|| {
        config
            .address
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9092".parse().unwrap())
    });

    let brokers = vec![Broker {
        node_id: 0,
        host: bound_addr.ip().to_string(),
        port: bound_addr.port() as i32,
        rack: None,
    }];

    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => {
            return MetadataResponse {
                brokers,
                controller_id: 0,
                topics: vec![],
            }
            .encode();
        }
    };

    if config.auto_create_topics
        && let Some(requested) = &req.topics
    {
        ensure_stream_exists(shard, &stream_ident, config).await;
        for name in requested {
            ensure_topic_exists(shard, &stream_ident, name, config.num_partitions).await;
        }
    }

    let stream = match shard.resolve_stream(&stream_ident) {
        Ok(s) => s,
        Err(_) => {
            return MetadataResponse {
                brokers,
                controller_id: 0,
                topics: vec![],
            }
            .encode();
        }
    };

    let topics = shard.metadata.with_metadata(|m| {
        let Some(stream_meta) = m.streams.get(stream.0) else {
            return vec![];
        };

        let should_include = |name: &str| match &req.topics {
            None => true,
            Some(names) => names.iter().any(|n| n == name),
        };

        stream_meta
            .topics
            .iter()
            .filter(|(_, t)| should_include(&t.name))
            .map(|(_, t)| {
                let partitions = t
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(i, _)| PartitionMetadata {
                        error_code: NONE,
                        partition_index: i as i32,
                        leader: 0,
                        replicas: vec![0],
                        isr: vec![0],
                    })
                    .collect();
                TopicMetadata {
                    error_code: NONE,
                    name: t.name.to_string(),
                    is_internal: false,
                    partitions,
                }
            })
            .collect()
    });

    MetadataResponse {
        brokers,
        controller_id: 0,
        topics,
    }
    .encode()
}

async fn ensure_stream_exists(
    shard: &Rc<IggyShard>,
    stream_ident: &Identifier,
    config: &KafkaConfig,
) {
    if shard.resolve_stream(stream_ident).is_ok() {
        return;
    }
    debug!("Auto-creating default stream '{}'", config.default_stream);
    let _ = shard.create_stream(config.default_stream.clone()).await;
}

async fn ensure_topic_exists(
    shard: &Rc<IggyShard>,
    stream_ident: &Identifier,
    topic_name: &str,
    num_partitions: u32,
) {
    let topic_ident = match Identifier::from_str_value(topic_name) {
        Ok(id) => id,
        Err(_) => return,
    };
    if shard.resolve_topic(stream_ident, &topic_ident).is_ok() {
        return;
    }
    let stream = match shard.resolve_stream(stream_ident) {
        Ok(s) => s,
        Err(_) => return,
    };
    debug!("Auto-creating topic '{topic_name}'");
    if let Ok(topic_id) = shard
        .create_topic(
            stream,
            topic_name.to_string(),
            IggyExpiry::NeverExpire,
            CompressionAlgorithm::None,
            MaxTopicSize::Unlimited,
            None,
        )
        .await
    {
        let resolved = ResolvedTopic {
            stream_id: stream.0,
            topic_id,
        };
        let _ = shard.create_partitions(resolved, num_partitions).await;
    }
}

fn error_response() -> Vec<u8> {
    MetadataResponse {
        brokers: vec![],
        controller_id: -1,
        topics: vec![],
    }
    .encode()
}
