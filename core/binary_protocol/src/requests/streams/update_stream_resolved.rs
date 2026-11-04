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
use crate::requests::system::update_resource::{
    decode_optional_name, decode_resolved_blocks, encode_optional_name, encode_resolved_blocks,
    optional_name_size, resolved_blocks_size,
};
use bytes::BytesMut;

/// An `UpdateResource` on a stream after prepare: the rename extracted, the
/// set keys canonical in `explicit_options`, the reset keys resolved to their
/// defaults in `derived_options`.
///
/// Wire format:
/// ```text
/// [stream_id][has_name:u8][name?][explicit_len:u32][explicit][derived_len:u32][derived]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStreamResolvedRequest {
    pub stream_id: WireIdentifier,
    pub name: Option<WireName>,
    pub explicit_options: WireOptions,
    pub derived_options: WireOptions,
}

impl WireEncode for UpdateStreamResolvedRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + optional_name_size(self.name.as_ref())
            + resolved_blocks_size(&self.explicit_options, &self.derived_options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        encode_optional_name(self.name.as_ref(), buf);
        encode_resolved_blocks(&self.explicit_options, &self.derived_options, buf);
    }
}

impl WireDecode for UpdateStreamResolvedRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut position) = WireIdentifier::decode(buf)?;
        let (name, consumed) = decode_optional_name(buf, position)?;
        position += consumed;
        let (explicit_options, derived_options) = decode_resolved_blocks(buf, position)?;
        Ok((
            Self {
                stream_id,
                name,
                explicit_options,
                derived_options,
            },
            buf.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    #[test]
    fn given_resolved_patch_when_roundtripping_should_preserve_columns_and_provenance() {
        for name in [None, Some(WireName::new("renamed").unwrap())] {
            let request = UpdateStreamResolvedRequest {
                stream_id: WireIdentifier::named("events").unwrap(),
                name,
                explicit_options: options(b"future_key", b"value"),
                derived_options: WireOptions::empty(),
            };
            let bytes = request.to_bytes();
            assert_eq!(bytes.len(), request.encoded_size());
            assert_eq!(
                UpdateStreamResolvedRequest::decode_from(&bytes).unwrap(),
                request
            );
            for length in 0..bytes.len() {
                assert!(UpdateStreamResolvedRequest::decode(&bytes[..length]).is_err());
            }
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(UpdateStreamResolvedRequest::decode(&trailing).is_err());
        }
        let mut bad_presence = WireIdentifier::numeric(1).to_bytes().to_vec();
        bad_presence.push(2);
        assert!(UpdateStreamResolvedRequest::decode(&bad_presence).is_err());
    }

    fn options(key: &[u8], value: &[u8]) -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(&[(2, key, 2, value)], &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }
}
