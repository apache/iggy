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

pub mod codec;
pub mod record_batch;
pub mod types;

// Kafka API keys
pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const FIND_COORDINATOR: i16 = 10;
pub const API_VERSIONS: i16 = 18;
pub const CREATE_TOPICS: i16 = 19;
pub const DELETE_TOPICS: i16 = 20;

// Kafka error codes used in responses
pub const NONE: i16 = 0;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[derive(Debug)]
pub enum KafkaCodecError {
    BufferUnderflow,
    InvalidString,
}

impl std::fmt::Display for KafkaCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BufferUnderflow => write!(f, "buffer underflow"),
            Self::InvalidString => write!(f, "invalid UTF-8 string"),
        }
    }
}

impl std::error::Error for KafkaCodecError {}
