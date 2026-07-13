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

use tracing::error;

use crate::{Error, Payload, Schema, StreamEncoder};

pub struct BsonStreamEncoder;

impl StreamEncoder for BsonStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Bson
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Bson(document) => document.to_vec().map_err(|error| {
                error!(error = %error, "Failed to encode BSON payload");
                Error::InvalidBsonPayload
            }),
            _ => Err(Error::InvalidPayloadType),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::StreamDecoder;
    use crate::decoders::bson::BsonStreamDecoder;

    use super::*;

    #[test]
    fn given_bson_document_should_encode_document_bytes() {
        let document = bson::doc! {
            "name": "iggy",
            "version": 1,
            "active": true
        };

        let encoded = BsonStreamEncoder
            .encode(Payload::Bson(document.clone()))
            .expect("BSON document should encode");
        let expected = bson::deserialize_from_slice::<bson::Document>(&encoded)
            .expect("encoded bytes should contain a BSON document");

        assert_eq!(expected, document);
    }

    #[test]
    fn given_bson_document_when_encoded_and_decoded_should_round_trip() {
        let document = bson::doc! {
            "name": "iggy",
            "tags": ["bson", "connector"],
            "nested": {
                "active": true
            }
        };
        let payload = Payload::Bson(document.clone());

        let encoded = BsonStreamEncoder
            .encode(payload.clone())
            .expect("BSON payload should encode");
        let decoded = BsonStreamDecoder
            .decode(encoded)
            .expect("encoded BSON payload should decode");

        match decoded {
            Payload::Bson(decoded) => assert_eq!(decoded, document),
            other => panic!("expected BSON document payload, got {other:?}"),
        }
    }

    #[test]
    fn given_non_bson_payload_should_reject_encoding() {
        let result = BsonStreamEncoder.encode(Payload::Raw(vec![1, 2, 3]));

        assert_eq!(result, Err(Error::InvalidPayloadType));
    }
}
