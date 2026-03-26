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
use crate::kafka::protocol::types::{DeleteTopicResult, DeleteTopicsRequest, DeleteTopicsResponse};
use crate::kafka::protocol::{NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::shard::IggyShard;
use iggy_common::Identifier;
use std::rc::Rc;
use tracing::warn;

pub async fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig, body: &[u8]) -> Vec<u8> {
    let mut pos = 0;
    let req = match DeleteTopicsRequest::decode(body, &mut pos) {
        Ok(r) => r,
        Err(_) => return error_response(),
    };

    let stream_ident = match Identifier::from_str_value(&config.default_stream) {
        Ok(id) => id,
        Err(_) => return error_response(),
    };

    let mut results = Vec::with_capacity(req.topic_names.len());
    for name in &req.topic_names {
        let topic_ident = match Identifier::from_str_value(name) {
            Ok(id) => id,
            Err(_) => {
                results.push(DeleteTopicResult {
                    name: name.clone(),
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                });
                continue;
            }
        };

        match shard.resolve_topic(&stream_ident, &topic_ident) {
            Ok(resolved) => match shard.delete_topic(resolved).await {
                Ok(_) => {
                    results.push(DeleteTopicResult {
                        name: name.clone(),
                        error_code: NONE,
                    });
                }
                Err(e) => {
                    warn!("Failed to delete topic '{name}': {e}");
                    results.push(DeleteTopicResult {
                        name: name.clone(),
                        error_code: -1,
                    });
                }
            },
            Err(_) => {
                results.push(DeleteTopicResult {
                    name: name.clone(),
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                });
            }
        }
    }

    DeleteTopicsResponse { topics: results }.encode()
}

fn error_response() -> Vec<u8> {
    DeleteTopicsResponse { topics: vec![] }.encode()
}
