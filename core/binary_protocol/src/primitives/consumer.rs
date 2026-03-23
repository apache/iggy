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

use crate::WireError;
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode, read_u8};
use bytes::{BufMut, BytesMut};

const KIND_CONSUMER: u8 = 1;
const KIND_CONSUMER_GROUP: u8 = 2;

/// Wire consumer type. Identifies either a single consumer or a consumer group.
///
/// Wire format: `[kind:1][identifier:variable]`
/// - kind=1: consumer
/// - kind=2: consumer group
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConsumer {
    pub kind: u8,
    pub id: WireIdentifier,
}

impl WireConsumer {
    #[must_use]
    pub const fn consumer(id: WireIdentifier) -> Self {
        Self {
            kind: KIND_CONSUMER,
            id,
        }
    }

    #[must_use]
    pub const fn consumer_group(id: WireIdentifier) -> Self {
        Self {
            kind: KIND_CONSUMER_GROUP,
            id,
        }
    }
}

impl WireEncode for WireConsumer {
    fn encoded_size(&self) -> usize {
        1 + self.id.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.kind);
        self.id.encode(buf);
    }
}

impl WireDecode for WireConsumer {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let kind = read_u8(buf, 0)?;
        if kind != KIND_CONSUMER && kind != KIND_CONSUMER_GROUP {
            return Err(WireError::UnknownDiscriminant {
                type_name: "WireConsumer",
                value: kind,
                offset: 0,
            });
        }
        let (id, id_consumed) = WireIdentifier::decode(&buf[1..])?;
        Ok((Self { kind, id }, 1 + id_consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_consumer() {
        let c = WireConsumer::consumer(WireIdentifier::numeric(42));
        let bytes = c.to_bytes();
        assert_eq!(bytes.len(), 1 + 6);
        let (decoded, consumed) = WireConsumer::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, c);
    }

    #[test]
    fn roundtrip_consumer_group() {
        let c = WireConsumer::consumer_group(WireIdentifier::numeric(7));
        let bytes = c.to_bytes();
        let (decoded, consumed) = WireConsumer::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, c);
        assert_eq!(decoded.kind, KIND_CONSUMER_GROUP);
    }

    #[test]
    fn roundtrip_with_string_identifier() {
        let c = WireConsumer::consumer(WireIdentifier::named("my-consumer").unwrap());
        let bytes = c.to_bytes();
        let (decoded, consumed) = WireConsumer::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, c);
    }

    #[test]
    fn unknown_kind_rejected() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(0xFF);
        WireIdentifier::numeric(1).encode(&mut bytes);
        assert!(WireConsumer::decode(&bytes).is_err());
    }

    #[test]
    fn truncated_buffer() {
        let c = WireConsumer::consumer(WireIdentifier::numeric(1));
        let bytes = c.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WireConsumer::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
