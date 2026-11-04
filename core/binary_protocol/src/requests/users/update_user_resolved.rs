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
use crate::primitives::identifier::WireName;
use crate::primitives::options::WireOptions;
use crate::requests::system::update_resource::{
    decode_optional_name, decode_resolved_blocks, encode_optional_name, encode_resolved_blocks,
    optional_name_size, resolved_blocks_size,
};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// An `UpdateResource` on a user after prepare: the username and status
/// extracted, the set keys canonical in `explicit_options`, the reset keys
/// resolved to their defaults in `derived_options`.
///
/// Wire format:
/// ```text
/// [user_id][has_username:u8][username?][has_status:u8][status:u8?]
/// [explicit_len:u32][explicit][derived_len:u32][derived]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserResolvedRequest {
    pub user_id: WireIdentifier,
    pub username: Option<WireName>,
    pub status: Option<u8>,
    pub explicit_options: WireOptions,
    pub derived_options: WireOptions,
}

impl WireEncode for UpdateUserResolvedRequest {
    fn encoded_size(&self) -> usize {
        self.user_id.encoded_size()
            + optional_name_size(self.username.as_ref())
            + 1
            + usize::from(self.status.is_some())
            + resolved_blocks_size(&self.explicit_options, &self.derived_options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user_id.encode(buf);
        encode_optional_name(self.username.as_ref(), buf);
        match self.status {
            Some(status) => {
                buf.put_u8(1);
                buf.put_u8(status);
            }
            None => buf.put_u8(0),
        }
        encode_resolved_blocks(&self.explicit_options, &self.derived_options, buf);
    }
}

impl WireDecode for UpdateUserResolvedRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (user_id, mut position) = WireIdentifier::decode(buf)?;
        let (username, consumed) = decode_optional_name(buf, position)?;
        position += consumed;
        let status = match read_u8(buf, position)? {
            0 => None,
            1 => {
                position += 1;
                Some(read_u8(buf, position)?)
            }
            _ => {
                return Err(WireError::Validation(Cow::Borrowed(
                    "presence byte must be 0 or 1",
                )));
            }
        };
        position += 1;
        let (explicit_options, derived_options) = decode_resolved_blocks(buf, position)?;
        Ok((
            Self {
                user_id,
                username,
                status,
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

    #[test]
    fn given_resolved_patch_when_roundtripping_should_preserve_columns_and_provenance() {
        for (username, status) in [
            (None, None),
            (Some(WireName::new("alice2").unwrap()), None),
            (None, Some(2)),
            (Some(WireName::new("alice2").unwrap()), Some(1)),
        ] {
            let request = UpdateUserResolvedRequest {
                user_id: WireIdentifier::named("alice").unwrap(),
                username,
                status,
                explicit_options: WireOptions::empty(),
                derived_options: WireOptions::empty(),
            };
            let bytes = request.to_bytes();
            assert_eq!(bytes.len(), request.encoded_size());
            assert_eq!(
                UpdateUserResolvedRequest::decode_from(&bytes).unwrap(),
                request
            );
            for length in 0..bytes.len() {
                assert!(UpdateUserResolvedRequest::decode(&bytes[..length]).is_err());
            }
        }
        let mut bad_status = WireIdentifier::numeric(1).to_bytes().to_vec();
        bad_status.extend_from_slice(&[0, 3]);
        assert!(UpdateUserResolvedRequest::decode(&bad_status).is_err());
    }
}
