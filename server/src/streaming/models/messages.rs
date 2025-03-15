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

use crate::streaming::local_sizeable::LocalSizeable;
use crate::streaming::local_sizeable::RealSize;
use crate::streaming::models::COMPONENT;
use bytes::{BufMut, Bytes, BytesMut};
use error_set::ErrContext;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::PolledMessage;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::checksum;
use iggy::utils::sizeable::Sizeable;
use iggy::{messages::send_messages::Message, models::messages::MessageState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

// It's the same as PolledMessages from Iggy models, but with the Arc<Message> instead of Message.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages: Vec<Arc<PolledMessage>>,
}

#[derive(Debug)]
pub struct RetainedMessage {
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub checksum: u32,
    pub message_state: MessageState,
    pub headers: Option<Bytes>,
    pub payload: Bytes,
}

impl RetainedMessage {
    pub fn to_polled_message(&self) -> Result<PolledMessage, IggyError> {
        let headers = self.headers.clone().map(HashMap::from_bytes).transpose()?;
        let message = PolledMessage {
            offset: self.offset,
            state: self.message_state,
            timestamp: self.timestamp,
            id: self.id,
            checksum: self.checksum,
            headers,
            length: IggyByteSize::from(self.payload.len() as u64),
            payload: self.payload.clone(),
        };
        Ok(message)
    }
}

impl RetainedMessage {
    pub fn new(offset: u64, timestamp: u64, message: Message) -> Self {
        RetainedMessage {
            offset,
            timestamp,
            checksum: checksum::calculate(&message.payload),
            message_state: MessageState::Available,
            id: message.id,
            payload: message.payload,
            headers: message.headers.map(|h| h.to_bytes()),
        }
    }

    pub fn extend(&self, bytes: &mut BytesMut) {
        let length = self.get_size_bytes();
        let id = self.id;
        let offset = self.offset;
        let timestamp = self.timestamp;
        let payload = self.payload.clone();
        let checksum = self.checksum;
        let message_state = self.message_state;
        let headers = &self.headers;

        bytes.put_u32_le(length.as_bytes_u64() as u32);
        bytes.put_u64_le(offset);
        bytes.put_u8(message_state.as_code());
        bytes.put_u64_le(timestamp);
        bytes.put_u128_le(id);
        bytes.put_u32_le(checksum);
        if let Some(headers) = headers {
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(headers.len() as u32);
            bytes.put_slice(headers);
        } else {
            bytes.put_u32_le(0u32);
        }
        bytes.put_slice(&payload);
    }

    pub fn try_from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let offset = u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse message offset")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let message_state = MessageState::from_code(bytes[8]).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to parse message state")
        })?;
        let timestamp = u64::from_le_bytes(
            bytes[9..17]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse message timestamp")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let id = u128::from_le_bytes(
            bytes[17..33]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse message id")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let checksum = u32::from_le_bytes(
            bytes[33..37]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse message checksum")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let headers_length = u32::from_le_bytes(
            bytes[37..41]
                .try_into()
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to parse message headers_length")
                })
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let headers = if headers_length > 0 {
            Some(bytes.slice(41..41 + headers_length as usize))
        } else {
            None
        };
        let position = 41 + headers_length as usize;
        let payload = bytes.slice(position..);

        Ok(RetainedMessage {
            id,
            offset,
            timestamp,
            checksum,
            message_state,
            headers,
            payload,
        })
    }
}

impl Sizeable for RetainedMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let headers_len = self.headers.as_ref().map(|h| 4 + h.len()).unwrap_or(4);
        let size = 16 + 8 + 8 + 4 + 1 + headers_len + self.payload.len();
        IggyByteSize::from(size as u64)
    }
}

impl RealSize for RetainedMessage {
    fn real_size(&self) -> IggyByteSize {
        let mut total_size = 0;

        total_size += mem::size_of::<u128>(); // id
        total_size += mem::size_of::<u64>(); // offset
        total_size += mem::size_of::<u64>(); // timestamp
        total_size += mem::size_of::<u32>(); // checksum
        total_size += mem::size_of::<MessageState>(); // message_state

        total_size += mem::size_of::<Option<Bytes>>(); // headers
        if let Some(headers) = &self.headers {
            total_size += headers.len(); // headers length
            total_size += mem::size_of::<Bytes>() * 2; // Bytes overhead
        }

        total_size += self.payload.len(); // payload length
        total_size += mem::size_of::<Bytes>() * 2; // Bytes overhead

        IggyByteSize::from(total_size as u64)
    }
}

impl RealSize for Arc<RetainedMessage> {
    fn real_size(&self) -> IggyByteSize {
        let arc_overhead = mem::size_of::<usize>() as u64 * 2;
        self.deref().real_size() + IggyByteSize::from(arc_overhead)
    }
}

impl<T> LocalSizeable for T
where
    T: Deref<Target = RetainedMessage>,
{
    fn get_size_bytes(&self) -> IggyByteSize {
        let headers_len = self.headers.as_ref().map(|h| 4 + h.len()).unwrap_or(4);
        let size = 16 + 8 + 8 + 4 + 1 + headers_len + self.payload.len();
        IggyByteSize::from(size as u64)
    }
}
