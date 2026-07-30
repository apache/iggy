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

use crate::codec::{WireDecode, WireEncode, capped_capacity, read_u32_le, read_u64_le};
use crate::error::WireError;
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// Size of one confirmation entry:
/// `stream_id(4) + topic_id(4) + partition_id(4) + base_offset(8)`.
const CONFIRMATION_SIZE: usize = 20;

/// Commit confirmation for one partition written by a `SendMessages` request.
///
/// Wire format:
/// ```text
/// [stream_id:4][topic_id:4][partition_id:4][base_offset:8]
/// ```
///
/// `base_offset` is the offset assigned to the first message of the batch in
/// that partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessagesConfirmationResponse {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub base_offset: u64,
}

impl WireEncode for SendMessagesConfirmationResponse {
    fn encoded_size(&self) -> usize {
        CONFIRMATION_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.stream_id);
        buf.put_u32_le(self.topic_id);
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.base_offset);
    }
}

impl WireDecode for SendMessagesConfirmationResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let stream_id = read_u32_le(buf, 0)?;
        let topic_id = read_u32_le(buf, 4)?;
        let partition_id = read_u32_le(buf, 8)?;
        let base_offset = read_u64_le(buf, 12)?;
        Ok((
            Self {
                stream_id,
                topic_id,
                partition_id,
                base_offset,
            },
            CONFIRMATION_SIZE,
        ))
    }
}

/// `SendMessages` response.
///
/// Wire format:
/// ```text
/// [confirmations_count:4][SendMessagesConfirmationResponse]*
/// ```
///
/// `confirmations_count == 0` means the batch committed but no offsets are
/// available. The server currently reports a single partition per request;
/// the list decodes any count so a later multi-partition send needs no wire
/// change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessagesResponse {
    pub confirmations: Vec<SendMessagesConfirmationResponse>,
}

impl WireEncode for SendMessagesResponse {
    fn encoded_size(&self) -> usize {
        4 + self.confirmations.len() * CONFIRMATION_SIZE
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.confirmations.len() as u32);
        for confirmation in &self.confirmations {
            confirmation.encode(buf);
        }
    }
}

impl WireDecode for SendMessagesResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let confirmations_count = read_u32_le(buf, 0)?;
        let remaining = buf.len().saturating_sub(4);
        let mut confirmations = Vec::with_capacity(capped_capacity(
            confirmations_count as usize,
            remaining,
            CONFIRMATION_SIZE,
        ));
        let mut offset = 4;
        for _ in 0..confirmations_count {
            let (confirmation, consumed) =
                SendMessagesConfirmationResponse::decode(&buf[offset..])?;
            offset += consumed;
            confirmations.push(confirmation);
        }
        // The payload is the whole frame body, never a prefix of a larger
        // value, so leftover bytes mean a shape this build cannot read.
        if offset != buf.len() {
            return Err(WireError::Validation(Cow::Owned(format!(
                "send_messages response has {} trailing bytes",
                buf.len() - offset
            ))));
        }
        Ok((Self { confirmations }, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confirmation(partition_id: u32) -> SendMessagesConfirmationResponse {
        SendMessagesConfirmationResponse {
            stream_id: 1,
            topic_id: 2,
            partition_id,
            base_offset: 42,
        }
    }

    #[test]
    fn roundtrip_single_confirmation() {
        let response = SendMessagesResponse {
            confirmations: vec![confirmation(3)],
        };
        let bytes = response.to_bytes();
        assert_eq!(bytes.len(), 4 + CONFIRMATION_SIZE);
        let (decoded, consumed) = SendMessagesResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, response);
    }

    #[test]
    fn roundtrip_multiple_confirmations() {
        let response = SendMessagesResponse {
            confirmations: vec![confirmation(0), confirmation(1), confirmation(2)],
        };
        let bytes = response.to_bytes();
        let (decoded, consumed) = SendMessagesResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, response);
    }

    #[test]
    fn roundtrip_zero_confirmations() {
        let response = SendMessagesResponse {
            confirmations: vec![],
        };
        let bytes = response.to_bytes();
        assert_eq!(&bytes[..], &[0x00, 0x00, 0x00, 0x00]);
        let (decoded, consumed) = SendMessagesResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(decoded, response);
    }

    #[test]
    fn wire_compat_confirmation_layout() {
        let response = SendMessagesResponse {
            confirmations: vec![SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 2,
                partition_id: 3,
                base_offset: 4,
            }],
        };
        assert_eq!(
            &response.to_bytes()[..],
            &[
                0x01, 0x00, 0x00, 0x00, // count
                0x01, 0x00, 0x00, 0x00, // stream_id
                0x02, 0x00, 0x00, 0x00, // topic_id
                0x03, 0x00, 0x00, 0x00, // partition_id
                0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset
            ]
        );
    }

    #[test]
    fn confirmation_truncated_returns_error() {
        let bytes = confirmation(1).to_bytes();
        for i in 0..bytes.len() {
            assert!(
                SendMessagesConfirmationResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn truncated_returns_error() {
        let response = SendMessagesResponse {
            confirmations: vec![confirmation(0), confirmation(1)],
        };
        let bytes = response.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                SendMessagesResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let response = SendMessagesResponse {
            confirmations: vec![confirmation(1)],
        };
        let mut bytes = response.to_bytes().to_vec();
        bytes.push(0xFF);
        assert!(matches!(
            SendMessagesResponse::decode(&bytes),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn trailing_bytes_after_zero_confirmations_rejected() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(matches!(
            SendMessagesResponse::decode(&bytes),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn empty_input_returns_error() {
        assert!(matches!(
            SendMessagesResponse::decode(&[]),
            Err(WireError::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn bogus_confirmations_count_does_not_oom() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(u32::MAX);
        assert!(SendMessagesResponse::decode(&buf).is_err());
    }
}
