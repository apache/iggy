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
use crate::codec::{WireDecode, WireEncode, read_u64_le, read_u128_le};
use crate::requests::users::login_register::{BIND_SECRET_BYTES, BindSecret};
use crate::version::ClientVersionInfo;
use bytes::{BufMut, BytesMut};
use secrecy::ExposeSecret;

/// Registered identity and the minimum metadata frontier required by a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionIdentity {
    pub client_id: u128,
    pub session: u64,
    pub metadata_watermark: u64,
}

impl WireEncode for SessionIdentity {
    fn encoded_size(&self) -> usize {
        32
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
        buf.put_u64_le(self.metadata_watermark);
    }
}

impl WireDecode for SessionIdentity {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((
            Self {
                client_id: read_u128_le(buf, 0)?,
                session: read_u64_le(buf, 16)?,
                metadata_watermark: read_u64_le(buf, 24)?,
            },
            32,
        ))
    }
}

/// Authenticate a connection without another Register. Session zero resolves
/// a lost registration reply using the retained client secret.
#[derive(Debug, Clone)]
pub struct BindSessionRequest {
    pub version_info: ClientVersionInfo,
    pub identity: SessionIdentity,
    pub bind_secret: BindSecret,
}

impl WireEncode for BindSessionRequest {
    fn encoded_size(&self) -> usize {
        self.version_info.encoded_size() + self.identity.encoded_size() + BIND_SECRET_BYTES
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.version_info.encode(buf);
        self.identity.encode(buf);
        buf.put_slice(self.bind_secret.expose_secret());
    }
}

impl WireDecode for BindSessionRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (version_info, prefix_len) = ClientVersionInfo::decode(buf)?;
        let (identity, identity_len) = SessionIdentity::decode(&buf[prefix_len..])?;
        let secret_pos = prefix_len + identity_len;
        let (bind_secret, _) = BindSecret::decode(&buf[secret_pos..])?;
        Ok((
            Self {
                version_info,
                identity,
                bind_secret,
            },
            secret_pos + BIND_SECRET_BYTES,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_reject_truncation() {
        let request = SessionIdentity {
            client_id: u128::MAX,
            session: 7,
            metadata_watermark: 11,
        };
        let bytes = request.to_bytes();
        assert_eq!(
            SessionIdentity::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
        for end in 0..bytes.len() {
            assert!(
                SessionIdentity::decode(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
    }

    #[test]
    fn bind_credential_roundtrips_and_rejects_every_truncated_frame() {
        let request = BindSessionRequest {
            version_info: ClientVersionInfo {
                protocol_version: crate::IGGY_PROTOCOL_VERSION,
                sdk_name: crate::WireName::new("test").unwrap(),
                sdk_version: crate::WireName::new("0.0.1").unwrap(),
            },
            identity: SessionIdentity {
                client_id: u128::MAX,
                session: 7,
                metadata_watermark: 11,
            },
            bind_secret: BindSecret::new(Box::new([0x5a; BIND_SECRET_BYTES])),
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = BindSessionRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.identity, request.identity);
        assert_eq!(decoded.version_info, request.version_info);
        assert_eq!(
            decoded.bind_secret.expose_secret(),
            request.bind_secret.expose_secret()
        );
        for end in 0..bytes.len() {
            assert!(
                BindSessionRequest::decode(&bytes[..end]).is_err(),
                "accepted truncated binding at {end}"
            );
        }
    }
}
