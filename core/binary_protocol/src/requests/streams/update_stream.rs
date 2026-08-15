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
use crate::codec::{WireDecode, WireEncode};
use crate::primitives::identifier::WireName;
use crate::primitives::options::WireOptions;
use bytes::BytesMut;

/// `UpdateStream` request.
///
/// Wire format: `[identifier][name_len:1][name:N][options TLV to end]`
///
/// The options block mirrors `CreateStream`'s, so a stream setting added to
/// the catalog is updatable without another layout change. Keys absent from
/// the block are left alone rather than reset: an update patches the stored
/// map, so a client built before a key existed cannot erase it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStreamRequest {
    pub stream_id: WireIdentifier,
    pub name: WireName,
    pub options: WireOptions,
}

impl WireEncode for UpdateStreamRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.name.encoded_size() + self.options.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.name.encode(buf);
        self.options.encode(buf);
    }
}

impl WireDecode for UpdateStreamRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (name, consumed) = WireName::decode(&buf[pos..])?;
        pos += consumed;
        let options = WireOptions::from_slice(&buf[pos..])?;
        Ok((
            Self {
                stream_id,
                name,
                options,
            },
            buf.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    fn sample_options() -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(&[(2, b"future_key", 2, b"value")], &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }

    #[test]
    fn roundtrip() {
        let req = UpdateStreamRequest {
            stream_id: WireIdentifier::named("old-name").unwrap(),
            name: WireName::new("new-name").unwrap(),
            options: sample_options(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateStreamRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_fixed_fields_return_error() {
        let req = UpdateStreamRequest {
            stream_id: WireIdentifier::numeric(1),
            name: WireName::new("test").unwrap(),
            options: WireOptions::empty(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdateStreamRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn truncated_options_return_error() {
        // One pair, so any strict-interior truncation is invalid. Cutting at
        // the block boundary is a legitimate update carrying no options.
        let req = UpdateStreamRequest {
            stream_id: WireIdentifier::numeric(1),
            name: WireName::new("test").unwrap(),
            options: sample_options(),
        };
        let bytes = req.to_bytes();
        let fixed_end = bytes.len() - req.options.encoded_size();
        for i in fixed_end + 1..bytes.len() {
            assert!(
                UpdateStreamRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
