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
use crate::kafka::protocol::types::{
    ListOffsetsPartitionResponse, ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use crate::kafka::protocol::{NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::shard::IggyShard;
use iggy_common::Identifier;
use std::rc::Rc;

const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

pub fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig, body: &[u8]) -> Vec<u8> {
    let mut pos = 0;
    let req = match ListOffsetsRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return ListOffsetsResponse { topics: vec![] }.encode(),
    };

    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => return ListOffsetsResponse { topics: vec![] }.encode(),
    };

    let mut topics = Vec::with_capacity(req.topics.len());

    for req_topic in &req.topics {
        let topic_ident = match Identifier::from_str_value(&req_topic.name) {
            Ok(id) => id,
            Err(_) => {
                topics.push(topic_error(&req_topic.name, &req_topic.partitions));
                continue;
            }
        };

        let resolved = match shard.resolve_topic(&stream_ident, &topic_ident) {
            Ok(r) => r,
            Err(_) => {
                topics.push(topic_error(&req_topic.name, &req_topic.partitions));
                continue;
            }
        };

        let mut partitions = Vec::with_capacity(req_topic.partitions.len());
        for rp in &req_topic.partitions {
            let offset = match rp.timestamp {
                EARLIEST => 0i64,
                LATEST => {
                    let stats = shard.metadata.get_partition_stats(
                        &iggy_common::sharding::IggyNamespace::new(
                            resolved.stream_id,
                            resolved.topic_id,
                            rp.partition_index as usize,
                        ),
                    );
                    match stats {
                        Some(s) => s.current_offset() as i64 + 1,
                        None => 0,
                    }
                }
                _ => -1, // timestamp-based lookup not implemented in Phase 1
            };

            partitions.push(ListOffsetsPartitionResponse {
                partition_index: rp.partition_index,
                error_code: NONE,
                timestamp: rp.timestamp,
                offset,
            });
        }

        topics.push(ListOffsetsTopicResponse {
            name: req_topic.name.clone(),
            partitions,
        });
    }

    ListOffsetsResponse { topics }.encode()
}

fn topic_error(
    name: &str,
    partitions: &[crate::kafka::protocol::types::ListOffsetsPartition],
) -> ListOffsetsTopicResponse {
    ListOffsetsTopicResponse {
        name: name.to_string(),
        partitions: partitions
            .iter()
            .map(|p| ListOffsetsPartitionResponse {
                partition_index: p.partition_index,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                timestamp: -1,
                offset: -1,
            })
            .collect(),
    }
}
