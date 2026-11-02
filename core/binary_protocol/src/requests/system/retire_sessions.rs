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

use crate::codec::read_u64_le;
use crate::requests::system::SessionIdentity;
use crate::{WireDecode, WireEncode, WireError};

pub const MAX_SESSIONS_PER_RETIREMENT: usize = 128;

/// Server-originated partition barrier. Every identity is already ended in
/// committed metadata. Admission excludes pending writes for the whole batch.
///
/// A nonzero `paid_through` marks the last batch of a retirement debt payment:
/// every session the metadata op `paid_through` had finalized is retired once
/// it commits. It rides as a trailing `u64`, absent when zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireSessionsRequest {
    pub identities: Vec<SessionIdentity>,
    pub paid_through: u64,
}

impl WireEncode for RetireSessionsRequest {
    fn encoded_size(&self) -> usize {
        self.identities.len() * SessionIdentity::ENCODED_SIZE
            + if self.paid_through == 0 {
                0
            } else {
                size_of::<u64>()
            }
    }

    fn encode(&self, buf: &mut BytesMut) {
        for identity in &self.identities {
            identity.encode(buf);
        }
        if self.paid_through != 0 {
            buf.put_u64_le(self.paid_through);
        }
    }
}

impl WireDecode for RetireSessionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (chunks, remainder) = buf.as_chunks::<{ SessionIdentity::ENCODED_SIZE }>();
        let paid_through = match remainder.len() {
            0 => 0,
            8 => read_u64_le(remainder, 0)?,
            _ => {
                return Err(WireError::Validation(
                    "invalid session retirement batch length".into(),
                ));
            }
        };
        if (paid_through == 0 && (chunks.is_empty() || !remainder.is_empty()))
            || chunks.len() > MAX_SESSIONS_PER_RETIREMENT
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
        Ok((
            Self {
                identities,
                paid_through,
            },
            buf.len(),
        ))
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
                paid_through: 0,
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
                    paid_through: 0,
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
                    paid_through: 0,
                }
                .to_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn given_a_debt_payment_when_decoding_should_carry_paid_through_even_without_identities() {
        let identity = SessionIdentity {
            client_id: 1,
            session: 2,
            metadata_watermark: 3,
        };
        for identities in [Vec::new(), vec![identity; MAX_SESSIONS_PER_RETIREMENT]] {
            let request = RetireSessionsRequest {
                identities,
                paid_through: 17,
            };
            let bytes = request.to_bytes();
            assert_eq!(bytes.len(), request.encoded_size());
            assert_eq!(RetireSessionsRequest::decode_from(&bytes).unwrap(), request);
            assert!(RetireSessionsRequest::decode_from(&bytes[..bytes.len() - 1]).is_err());
        }
        assert!(RetireSessionsRequest::decode_from(&0_u64.to_le_bytes()).is_err());
    }
}
