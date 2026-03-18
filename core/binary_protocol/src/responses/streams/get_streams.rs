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
use crate::responses::streams::StreamResponse;
use bytes::BytesMut;

/// `GetStreams` response: sequential stream headers.
///
/// Wire format:
/// ```text
/// [StreamResponse]*
/// ```
///
/// Empty payload means zero streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStreamsResponse {
    pub streams: Vec<StreamResponse>,
}

impl WireEncode for GetStreamsResponse {
    fn encoded_size(&self) -> usize {
        self.streams.iter().map(WireEncode::encoded_size).sum()
    }

    fn encode(&self, buf: &mut BytesMut) {
        for stream in &self.streams {
            stream.encode(buf);
        }
    }
}

impl WireDecode for GetStreamsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut streams = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (stream, consumed) = StreamResponse::decode(&buf[pos..])?;
            pos += consumed;
            streams.push(stream);
        }
        Ok((Self { streams }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;

    #[test]
    fn roundtrip_empty() {
        let resp = GetStreamsResponse { streams: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetStreamsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_multiple() {
        let resp = GetStreamsResponse {
            streams: vec![
                StreamResponse {
                    id: 1,
                    created_at: 100,
                    topics_count: 2,
                    size_bytes: 512,
                    messages_count: 50,
                    name: WireName::new("s1").unwrap(),
                },
                StreamResponse {
                    id: 2,
                    created_at: 200,
                    topics_count: 0,
                    size_bytes: 0,
                    messages_count: 0,
                    name: WireName::new("stream-two").unwrap(),
                },
            ],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetStreamsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }
}
