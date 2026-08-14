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
use crate::codec::{WireDecode, WireEncode};
use crate::primitives::identifier::WireName;
use crate::primitives::options::WireOptions;
use bytes::BytesMut;

/// `UpdateTopic` request.
///
/// Wire format:
/// `[stream_id:WireIdentifier][topic_id:WireIdentifier][name_len:u8][name:N]
///  [options TLV to end]`
///
/// Identity and the new name are the only fixed fields; every SETTING rides the
/// options block, which mirrors `CreateTopic`'s and carries the same catalog.
/// A knob added there is updatable here without another layout change, and no
/// setting has two homes to disagree between.
///
/// Keys absent from the block are LEFT ALONE rather than reset to their
/// defaults. A client built before a key existed cannot send it, so treating
/// the block as the topic's complete option set would let an old client wipe a
/// newer knob just by updating the name -- the same forward-compatibility
/// argument that makes unknown keys survive a round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTopicRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub name: WireName,
    pub options: WireOptions,
}

impl WireEncode for UpdateTopicRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.name.encoded_size()
            + self.options.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.name.encode(buf);
        self.options.encode(buf);
    }
}

impl WireDecode for UpdateTopicRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, consumed) = WireIdentifier::decode(&buf[pos..])?;
        pos += consumed;
        let (name, name_consumed) = WireName::decode(&buf[pos..])?;
        pos += name_consumed;
        let options = WireOptions::from_slice(&buf[pos..])?;
        Ok((
            Self {
                stream_id,
                topic_id,
                name,
                options,
            },
            buf.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    fn sample_options() -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(&[(2, b"future_option", 9, &[3u8])], &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }

    fn sample_request() -> UpdateTopicRequest {
        UpdateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            name: WireName::new("updated-topic").unwrap(),
            options: sample_options(),
        }
    }

    #[test]
    fn roundtrip() {
        let req = sample_request();
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = UpdateTopicRequest {
            stream_id: WireIdentifier::named("stream-a").unwrap(),
            topic_id: WireIdentifier::named("topic-b").unwrap(),
            name: WireName::new("new-name").unwrap(),
            options: WireOptions::empty(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_options_return_error() {
        // One key-value pair, so any strict-interior truncation of the block
        // is structurally invalid. Cutting at the block boundary is NOT an
        // error: that is a legitimate update carrying no options.
        let req = sample_request();
        let bytes = req.to_bytes();
        let fixed_end = bytes.len() - req.options.encoded_size();
        for i in fixed_end + 1..bytes.len() {
            assert!(
                UpdateTopicRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn truncated_fixed_fields_return_error() {
        let req = UpdateTopicRequest {
            options: WireOptions::empty(),
            ..sample_request()
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdateTopicRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = sample_request();
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }
}
