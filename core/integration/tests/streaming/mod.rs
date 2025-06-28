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

mod common;
mod consumer_offset;
mod get_by_offset;
mod get_by_timestamp;
mod messages;
mod partition;
mod segment;
mod shard;
mod snapshot;
mod stream;
mod topic;
mod topic_messages;

fn create_messages() -> Vec<IggyMessage> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(4, "message 3.2"),
        create_message(5, "message 1.2"),
        create_message(6, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> IggyMessage {
    let payload = Bytes::from(payload.to_string());
    IggyMessage::builder()
        .id(id)
        .payload(payload)
        .build()
        .expect("Failed to create message with valid payload and headers")
}
