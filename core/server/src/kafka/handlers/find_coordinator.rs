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
use crate::kafka::protocol::types::FindCoordinatorResponse;
use crate::shard::IggyShard;
use std::rc::Rc;

pub fn handle(shard: &Rc<IggyShard>, config: &KafkaConfig) -> Vec<u8> {
    let bound_addr = shard.kafka_bound_address.get().unwrap_or_else(|| {
        config
            .address
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9092".parse().unwrap())
    });

    FindCoordinatorResponse {
        error_code: 0,
        node_id: 0,
        host: bound_addr.ip().to_string(),
        port: bound_addr.port() as i32,
    }
    .encode()
}
