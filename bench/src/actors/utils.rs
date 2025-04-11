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

use bytes::Bytes;
use iggy::prelude::IggyMessage;
use std::time::Duration;

pub fn put_timestamp_in_first_message(message: &mut IggyMessage) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    let mut payload_vec = message.payload.to_vec();
    if payload_vec.len() < 8 {
        let mut new_payload = vec![0u8; 8];
        new_payload.extend_from_slice(&payload_vec);
        payload_vec = new_payload;
    }
    let timestamp_bytes = now.to_le_bytes();
    payload_vec[0..8].copy_from_slice(&timestamp_bytes);
    message.payload = Bytes::from(payload_vec);
}

pub fn calculate_latency_from_first_message(message: &IggyMessage) -> Duration {
    let send_timestamp = u64::from_le_bytes(message.payload[0..8].try_into().unwrap());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    Duration::from_micros(now - send_timestamp)
}
