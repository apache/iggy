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

use bytes::BytesMut;

use crate::requests::system::SessionIdentity;
use crate::{WireDecode, WireEncode, WireError};

pub const MAX_SESSIONS_PER_RETIREMENT: usize = 128;

/// Server-originated partition barrier. Every identity is already ended in
/// committed metadata. Admission excludes pending writes for the whole batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireSessionsRequest {
    pub identities: Vec<SessionIdentity>,
}

impl WireEncode for RetireSessionsRequest {
    fn encoded_size(&self) -> usize {
        self.identities.len() * SessionIdentity::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        for identity in &self.identities {
            identity.encode(buf);
        }
    }
}

impl WireDecode for RetireSessionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (chunks, remainder) = buf.as_chunks::<{ SessionIdentity::ENCODED_SIZE }>();
        if chunks.is_empty() || chunks.len() > MAX_SESSIONS_PER_RETIREMENT || !remainder.is_empty()
        {
            return Err(WireError::Validation(
                "invalid session retirement batch length".into(),
            ));
        }
        let identities = chunks
            .iter()
            .map(|bytes| {
                let identity = SessionIdentity::decode_from(bytes)?;
                if identity.client_id == 0
                    || identity.session == 0
                    || identity.metadata_watermark < identity.session
                {
                    return Err(WireError::Validation(
                        "invalid session retirement identity".into(),
                    ));
                }
                Ok(identity)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((Self { identities }, buf.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_batch_roundtrips_and_rejects_invalid_boundaries() {
        let identity = SessionIdentity {
            client_id: 1,
            session: 2,
            metadata_watermark: 3,
        };
        for count in [1, MAX_SESSIONS_PER_RETIREMENT] {
            let request = RetireSessionsRequest {
                identities: vec![identity; count],
            };
            let bytes = request.to_bytes();
            assert_eq!(RetireSessionsRequest::decode_from(&bytes).unwrap(), request);
            assert!(RetireSessionsRequest::decode_from(&bytes[..bytes.len() - 1]).is_err());
        }
        assert!(RetireSessionsRequest::decode_from(&[]).is_err());
        assert!(
            RetireSessionsRequest::decode_from(
                &RetireSessionsRequest {
                    identities: vec![identity; MAX_SESSIONS_PER_RETIREMENT + 1],
                }
                .to_bytes()
            )
            .is_err()
        );
        assert!(
            RetireSessionsRequest::decode_from(
                &RetireSessionsRequest {
                    identities: vec![
                        identity,
                        SessionIdentity {
                            session: 0,
                            ..identity
                        }
                    ],
                }
                .to_bytes()
            )
            .is_err()
        );
    }
}
