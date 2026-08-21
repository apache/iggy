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
use crate::codec::{WireDecode, WireEncode, read_u8};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// Resource whose option catalog is being described.
pub const OPTIONS_SCOPE_TOPIC: u8 = 1;
pub const OPTIONS_SCOPE_STREAM: u8 = 2;
pub const OPTIONS_SCOPE_USER: u8 = 3;

/// `DescribeOptions` request. Wire format: `[scope:u8]`
///
/// Because unknown option keys are rejected at create, a client cannot probe
/// support by trying; this read-only command serves the catalog instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeOptionsRequest {
    pub scope: u8,
}

impl WireEncode for DescribeOptionsRequest {
    fn encoded_size(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.scope);
    }
}

impl WireDecode for DescribeOptionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let scope = read_u8(buf, 0)?;
        if !(OPTIONS_SCOPE_TOPIC..=OPTIONS_SCOPE_USER).contains(&scope) {
            return Err(WireError::Validation(Cow::Owned(format!(
                "unknown options scope: {scope}"
            ))));
        }
        Ok((Self { scope }, 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for scope in [
            OPTIONS_SCOPE_TOPIC,
            OPTIONS_SCOPE_STREAM,
            OPTIONS_SCOPE_USER,
        ] {
            let req = DescribeOptionsRequest { scope };
            let bytes = req.to_bytes();
            let (decoded, consumed) = DescribeOptionsRequest::decode(&bytes).unwrap();
            assert_eq!(consumed, bytes.len());
            assert_eq!(decoded, req);
        }
    }

    #[test]
    fn unknown_scope_rejected() {
        assert!(DescribeOptionsRequest::decode(&[0]).is_err());
        assert!(DescribeOptionsRequest::decode(&[4]).is_err());
    }

    #[test]
    fn empty_buffer_rejected() {
        assert!(DescribeOptionsRequest::decode(&[]).is_err());
    }
}
