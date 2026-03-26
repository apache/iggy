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
use crate::kafka::protocol::record_batch;
use crate::kafka::protocol::types::{
    ProducePartitionResponse, ProduceRequest, ProduceResponse, ProduceTopicResponse,
};
use crate::kafka::protocol::{NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedPartition;
use bytes::Bytes;
use iggy_common::{Identifier, IggyMessage, IggyMessagesBatchMut, Sizeable};
use std::rc::Rc;
use tracing::warn;

pub async fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig, body: &[u8]) -> Vec<u8> {
    let mut pos = 0;
    let req = match ProduceRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return error_response(),
    };

    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => return error_response(),
    };

    let mut responses = Vec::with_capacity(req.topic_data.len());

    for topic_data in &req.topic_data {
        let topic_ident = match Identifier::from_str_value(&topic_data.name) {
            Ok(id) => id,
            Err(_) => {
                responses.push(topic_error(&topic_data.name, &topic_data.partition_data));
                continue;
            }
        };

        let resolved = match shard.resolve_topic(&stream_ident, &topic_ident) {
            Ok(r) => r,
            Err(_) => {
                responses.push(topic_error(&topic_data.name, &topic_data.partition_data));
                continue;
            }
        };

        let mut partition_responses = Vec::with_capacity(topic_data.partition_data.len());

        for partition_data in &topic_data.partition_data {
            let records_bytes = match &partition_data.records {
                Some(data) if !data.is_empty() => data,
                _ => {
                    partition_responses.push(ProducePartitionResponse {
                        index: partition_data.index,
                        error_code: NONE,
                        base_offset: -1,
                        log_append_time_ms: -1,
                    });
                    continue;
                }
            };

            let (_base_timestamp, records) = match record_batch::decode_record_batch(records_bytes)
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Failed to decode record batch for {}/{}: {e}",
                        topic_data.name, partition_data.index
                    );
                    partition_responses.push(ProducePartitionResponse {
                        index: partition_data.index,
                        error_code: -1,
                        base_offset: -1,
                        log_append_time_ms: -1,
                    });
                    continue;
                }
            };

            let mut messages = Vec::with_capacity(records.len());
            let mut total_size = 0u32;
            for record in records {
                let payload = match record.value {
                    Some(v) if !v.is_empty() => Bytes::from(v),
                    _ => Bytes::from_static(b"\0"), // Iggy requires non-empty payload
                };
                match IggyMessage::builder().payload(payload).build() {
                    Ok(msg) => {
                        total_size += msg.get_size_bytes().as_bytes_u32();
                        messages.push(msg);
                    }
                    Err(e) => {
                        warn!("Failed to build IggyMessage: {e}");
                    }
                }
            }

            if messages.is_empty() {
                partition_responses.push(ProducePartitionResponse {
                    index: partition_data.index,
                    error_code: NONE,
                    base_offset: -1,
                    log_append_time_ms: -1,
                });
                continue;
            }

            let batch = IggyMessagesBatchMut::from_messages(&messages, total_size);
            let partition = ResolvedPartition {
                stream_id: resolved.stream_id,
                topic_id: resolved.topic_id,
                partition_id: partition_data.index as usize,
            };

            match shard.append_messages(partition, batch).await {
                Ok(()) => {
                    partition_responses.push(ProducePartitionResponse {
                        index: partition_data.index,
                        error_code: NONE,
                        base_offset: -1,
                        log_append_time_ms: -1,
                    });
                }
                Err(e) => {
                    warn!(
                        "append_messages failed for {}/{}: {e}",
                        topic_data.name, partition_data.index
                    );
                    partition_responses.push(ProducePartitionResponse {
                        index: partition_data.index,
                        error_code: -1,
                        base_offset: -1,
                        log_append_time_ms: -1,
                    });
                }
            }
        }

        responses.push(ProduceTopicResponse {
            name: topic_data.name.clone(),
            partition_responses,
        });
    }

    ProduceResponse {
        responses,
        throttle_time_ms: 0,
    }
    .encode()
}

fn topic_error(
    name: &str,
    partitions: &[crate::kafka::protocol::types::ProducePartitionData],
) -> ProduceTopicResponse {
    ProduceTopicResponse {
        name: name.to_string(),
        partition_responses: partitions
            .iter()
            .map(|p| ProducePartitionResponse {
                index: p.index,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                base_offset: -1,
                log_append_time_ms: -1,
            })
            .collect(),
    }
}

fn error_response() -> Vec<u8> {
    ProduceResponse {
        responses: vec![],
        throttle_time_ms: 0,
    }
    .encode()
}
