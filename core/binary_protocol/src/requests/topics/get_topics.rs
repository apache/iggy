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

use crate::codec::{WireDecode, WireEncode};
use crate::primitives::page::{WirePageQuery, decode_trailing_page};
use crate::{WireError, WireIdentifier};
use bytes::BytesMut;

/// `GetTopics` request: one page of the topics of a stream.
///
/// Wire format: `[stream_id:WireIdentifier][page]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicsRequest {
    pub stream_id: WireIdentifier,
    pub page: WirePageQuery,
}

impl WireEncode for GetTopicsRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.page.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.page.encode(buf);
    }
}

impl WireDecode for GetTopicsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, stream_size) = WireIdentifier::decode(buf)?;
        let (page, consumed) = decode_trailing_page(buf, stream_size)?;
        Ok((Self { stream_id, page }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::page::WirePageCursor;

    #[test]
    fn given_a_page_when_round_tripping_should_keep_every_field() {
        let request = GetTopicsRequest {
            stream_id: WireIdentifier::named("my-stream").unwrap(),
            page: WirePageQuery {
                limit: 3,
                cursor: Some(WirePageCursor::Id(7)),
                ..WirePageQuery::default()
            },
        };
        let bytes = request.to_bytes();
        assert_eq!(bytes.len(), request.encoded_size());
        assert_eq!(
            GetTopicsRequest::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
    }

    #[test]
    fn given_a_first_page_when_encoding_should_pin_the_bytes() {
        let request = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(42),
            page: WirePageQuery::default(),
        };
        let mut expected = WireIdentifier::numeric(42).to_bytes().to_vec();
        expected.extend_from_slice(&[0, 0, 100, 0, 0, 0, 0, 0]);
        assert_eq!(&request.to_bytes()[..], &expected[..]);
    }

    #[test]
    fn given_no_page_or_bytes_after_it_when_decoding_should_refuse() {
        let stream = WireIdentifier::numeric(42).to_bytes();
        assert!(GetTopicsRequest::decode(&stream).is_err());
        let mut bytes = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(42),
            page: WirePageQuery::default(),
        }
        .to_bytes()
        .to_vec();
        bytes.push(0);
        assert!(GetTopicsRequest::decode(&bytes).is_err());
    }
}
