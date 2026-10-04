// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaProtocolError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(String),
    #[error("buffer underflow: needed {needed} bytes, remaining {remaining}")]
    BufferUnderflow { needed: usize, remaining: usize },
    #[error("invalid frame length: {0}")]
    InvalidFrameLength(i32),
    #[error("request exceeds max frame size ({max_bytes} bytes): {actual_bytes} bytes")]
    FrameTooLarge {
        max_bytes: usize,
        actual_bytes: usize,
    },
    /// Metadata's per-topic `name` is nullable on the wire (v10+ allows topic-id-only lookups),
    /// but this stub only echoes names back, so a null name has no response to build.
    #[error("null topic name in request")]
    NullTopicName,
    /// Wraps a `kafka_protocol` decode/encode failure (`anyhow::Error`, no stable variant
    /// taxonomy on the wire-format crate's side).
    #[error("malformed request: {0}")]
    Malformed(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, KafkaProtocolError>;
