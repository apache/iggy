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

use bytes::{BufMut, BytesMut};

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u64_le};
use crate::requests::system::SessionIdentity;

/// Retirement evidence covers only the partition set at `namespace_revision`.
/// Ordered metadata apply must recheck that revision before reclaiming the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizeSessionRequest {
    pub identity: SessionIdentity,
    pub namespace_revision: u64,
}

impl WireEncode for FinalizeSessionRequest {
    fn encoded_size(&self) -> usize {
        self.identity.encoded_size() + size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.identity.encode(buf);
        buf.put_u64_le(self.namespace_revision);
    }
}

impl WireDecode for FinalizeSessionRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing session finalization bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (identity, consumed) = SessionIdentity::decode(buf)?;
        let namespace_revision = read_u64_le(buf, consumed)?;
        Ok((
            Self {
                identity,
                namespace_revision,
            },
            consumed + size_of::<u64>(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_finalization_evidence_when_decoding_should_require_the_namespace_revision() {
        let request = FinalizeSessionRequest {
            identity: SessionIdentity {
                client_id: u128::MAX,
                session: 7,
                metadata_watermark: 101,
            },
            namespace_revision: 2,
        };
        let bytes = request.to_bytes();
        assert_eq!(
            FinalizeSessionRequest::decode_from(&bytes).unwrap(),
            request
        );
        for end in 0..bytes.len() {
            assert!(
                FinalizeSessionRequest::decode_from(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(FinalizeSessionRequest::decode_from(&trailing).is_err());
    }
}
