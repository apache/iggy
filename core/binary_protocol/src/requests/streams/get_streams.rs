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
use crate::codec::{WireDecode, WireEncode};
use crate::primitives::page::{WirePageQuery, decode_trailing_page};
use bytes::BytesMut;

/// `GetStreams` request: one page of the streams.
///
/// Wire format: `[page]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStreamsRequest {
    pub page: WirePageQuery,
}

impl WireEncode for GetStreamsRequest {
    fn encoded_size(&self) -> usize {
        self.page.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.page.encode(buf);
    }
}

impl WireDecode for GetStreamsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (page, consumed) = decode_trailing_page(buf, 0)?;
        Ok((Self { page }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;
    use crate::primitives::page::{WirePageCursor, WirePageOrder};

    #[test]
    fn given_a_page_when_round_tripping_should_keep_every_field() {
        let request = GetStreamsRequest {
            page: WirePageQuery {
                order: WirePageOrder::Created,
                descending: true,
                limit: 20,
                cursor: Some(WirePageCursor::Created {
                    created_at: 9,
                    id: 4,
                }),
                name: Some(WireName::new("ord").unwrap()),
            },
        };
        let bytes = request.to_bytes();
        assert_eq!(bytes.len(), request.encoded_size());
        assert_eq!(
            GetStreamsRequest::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
    }

    #[test]
    fn given_a_first_page_when_encoding_should_pin_the_bytes() {
        let request = GetStreamsRequest {
            page: WirePageQuery::default(),
        };
        assert_eq!(&request.to_bytes()[..], &[0, 0, 100, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn given_no_page_or_bytes_after_it_when_decoding_should_refuse() {
        assert!(GetStreamsRequest::decode(&[]).is_err());
        let mut bytes = GetStreamsRequest {
            page: WirePageQuery::default(),
        }
        .to_bytes()
        .to_vec();
        bytes.push(0);
        assert!(GetStreamsRequest::decode(&bytes).is_err());
    }
}
