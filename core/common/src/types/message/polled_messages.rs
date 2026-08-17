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

use crate::{IggyMessage, IggyMessageHeader, error::IggyError};
use bytes::Bytes;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchMessageHeader};
use serde::{Deserialize, Serialize};
use tracing::error;

/// The wrapper on top of the collection of messages that are polled from the partition.
/// It consists of the following fields:
/// - `partition_id`: the identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `count`: the count of messages.
/// - `messages`: the collection of messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    /// The identifier of the partition. If it's '0', then there's no partition assigned to the consumer group member.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The count of messages.
    pub count: u32,
    /// The collection of messages.
    pub messages: Vec<IggyMessage>,
}

impl PolledMessages {
    pub fn empty() -> Self {
        Self {
            partition_id: 0,
            current_offset: 0,
            count: 0,
            messages: Vec::new(),
        }
    }
}

impl PolledMessages {
    /// Decode a `PollMessages` response body: the 16-byte prefix followed by
    /// the served batch records (`[256B batch header][frames]`, deltas
    /// resolved against the stamped bases).
    ///
    /// # Errors
    /// [`IggyError::InvalidNumberEncoding`] on a short prefix;
    /// [`IggyError::InvalidMessagePayloadLength`] on a malformed record.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let partition_id = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let current_offset = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let messages = messages_from_batches(bytes.slice(16..), count)?;

        Ok(Self {
            partition_id,
            current_offset,
            count,
            messages,
        })
    }
}

/// Walk the served batch records, resolving each frame's deltas to absolute
/// values. Payload and user-header `Bytes` are zero-copy slices of the
/// response buffer.
fn messages_from_batches(buffer: Bytes, count: u32) -> Result<Vec<IggyMessage>, IggyError> {
    let mut messages = Vec::with_capacity(count as usize);
    let mut position = 0usize;
    while position < buffer.len() {
        let batch = BatchHeader::decode(&buffer[position..]).map_err(|decode_error| {
            error!("Failed to decode polled batch header: {decode_error}");
            IggyError::InvalidMessagePayloadLength
        })?;
        let batch_end = position
            .checked_add(batch.total_size())
            .filter(|end| *end <= buffer.len())
            .ok_or(IggyError::InvalidMessagePayloadLength)?;
        let mut cursor = position + BATCH_HEADER_SIZE;
        while cursor < batch_end {
            let frame =
                BatchMessageHeader::decode(&buffer[cursor..batch_end]).map_err(|decode_error| {
                    error!("Failed to decode polled message frame: {decode_error}");
                    IggyError::InvalidMessagePayloadLength
                })?;
            let payload_start = cursor + iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE;
            let payload_end = payload_start + frame.payload_length as usize;
            let user_headers_end = payload_end + frame.user_headers_length as usize;
            if user_headers_end > batch_end {
                return Err(IggyError::InvalidMessagePayloadLength);
            }

            let header = IggyMessageHeader {
                checksum: frame.checksum,
                id: frame.id,
                offset: batch.base_offset + u64::from(frame.offset_delta),
                // Broker append time is stamped once per batch; the
                // per-message delta applies to `origin_timestamp` only.
                timestamp: batch.base_timestamp,
                origin_timestamp: batch.origin_timestamp + u64::from(frame.timestamp_delta),
                user_headers_length: frame.user_headers_length,
                payload_length: frame.payload_length,
                reserved: 0,
            };
            let payload = buffer.slice(payload_start..payload_end);
            let user_headers = if frame.user_headers_length > 0 {
                Some(buffer.slice(payload_end..user_headers_end))
            } else {
                None
            };
            messages.push(IggyMessage {
                header,
                payload,
                user_headers,
            });
            cursor = user_headers_end;
        }
        position = batch_end;
    }

    Ok(messages)
}
