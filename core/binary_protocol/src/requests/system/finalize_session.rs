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
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::requests::system::SessionIdentity;

/// A partition incarnation that could not prove the session retired. Its next
/// primary retires the session before serving session-bound requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RetirementDebt {
    pub namespace: u64,
    pub created_revision: u64,
}

impl RetirementDebt {
    pub const ENCODED_SIZE: usize = 2 * size_of::<u64>();
}

/// Retirement evidence covers only the partition set at `namespace_revision`.
/// Ordered metadata apply must recheck that revision before reclaiming the session.
///
/// `debt` rides as a trailing section that is absent when empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeSessionRequest {
    pub identity: SessionIdentity,
    pub namespace_revision: u64,
    pub debt: Vec<RetirementDebt>,
}

impl WireEncode for FinalizeSessionRequest {
    fn encoded_size(&self) -> usize {
        let debt = if self.debt.is_empty() {
            0
        } else {
            size_of::<u32>() + self.debt.len() * RetirementDebt::ENCODED_SIZE
        };
        self.identity.encoded_size() + size_of::<u64>() + debt
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.identity.encode(buf);
        buf.put_u64_le(self.namespace_revision);
        if self.debt.is_empty() {
            return;
        }
        buf.put_u32_le(u32::try_from(self.debt.len()).expect("debt list fits a message"));
        for debt in &self.debt {
            buf.put_u64_le(debt.namespace);
            buf.put_u64_le(debt.created_revision);
        }
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
        let (identity, mut consumed) = SessionIdentity::decode(buf)?;
        let namespace_revision = read_u64_le(buf, consumed)?;
        consumed += size_of::<u64>();
        let mut debt = Vec::new();
        if consumed < buf.len() {
            let count = read_u32_le(buf, consumed)? as usize;
            consumed += size_of::<u32>();
            if count == 0 || buf.len() - consumed < count * RetirementDebt::ENCODED_SIZE {
                return Err(WireError::Validation(
                    "invalid session finalization debt section".into(),
                ));
            }
            debt.reserve_exact(count);
            for _ in 0..count {
                debt.push(RetirementDebt {
                    namespace: read_u64_le(buf, consumed)?,
                    created_revision: read_u64_le(buf, consumed + size_of::<u64>())?,
                });
                consumed += RetirementDebt::ENCODED_SIZE;
            }
        }
        Ok((
            Self {
                identity,
                namespace_revision,
                debt,
            },
            consumed,
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
            debt: Vec::new(),
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

    #[test]
    fn given_retirement_debt_when_decoding_should_round_trip_and_refuse_a_damaged_section() {
        let identity = SessionIdentity {
            client_id: 3,
            session: 7,
            metadata_watermark: 9,
        };
        let without = FinalizeSessionRequest {
            identity,
            namespace_revision: 4,
            debt: Vec::new(),
        };
        let request = FinalizeSessionRequest {
            debt: vec![
                RetirementDebt {
                    namespace: 11,
                    created_revision: 2,
                },
                RetirementDebt {
                    namespace: 12,
                    created_revision: 3,
                },
            ],
            ..without
        };
        let bytes = request.to_bytes();
        assert_eq!(bytes.len(), request.encoded_size());
        assert_eq!(
            bytes.len(),
            without.to_bytes().len() + 4 + 2 * RetirementDebt::ENCODED_SIZE
        );
        assert_eq!(
            FinalizeSessionRequest::decode_from(&bytes).unwrap(),
            request
        );
        for end in without.to_bytes().len() + 1..bytes.len() {
            assert!(
                FinalizeSessionRequest::decode_from(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut empty_section = without.to_bytes().to_vec();
        empty_section.extend_from_slice(&0_u32.to_le_bytes());
        assert!(FinalizeSessionRequest::decode_from(&empty_section).is_err());
    }
}
