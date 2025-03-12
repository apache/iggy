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
use iggy::messages::send_messages;

pub mod consumer_offsets;
pub mod messages;
pub mod partition;
pub mod persistence;
pub mod segments;
pub mod storage;

pub const COMPONENT: &str = "STREAMING_PARTITIONS";

#[allow(dead_code)]
fn create_messages() -> Vec<send_messages::Message> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(2, "message 3.2"),
        create_message(1, "message 1.2"),
        create_message(3, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> send_messages::Message {
    let payload = Bytes::from(payload.to_string());
    send_messages::Message::new(Some(id), payload, None)
}
