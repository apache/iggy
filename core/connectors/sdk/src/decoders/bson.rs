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

use crate::{Error, Payload, Schema, StreamDecoder};

pub struct BsonStreamDecoder;

impl StreamDecoder for BsonStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Bson
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        let document =
            bson::deserialize_from_slice::<bson::Document>(&payload).map_err(|error| {
                error!(error = %error, "Failed to decode BSON payload");
                Error::CannotDecode(self.schema())
            })?;
        Ok(Payload::Bson(document))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_valid_document_bytes_should_decode_bson_payload() {
        let document = bson::doc! {
            "name": "iggy",
            "version": 1,
            "nested": {
                "enabled": true
            }
        };
        let bytes = document.to_vec().expect("BSON document should serialize");

        let result = BsonStreamDecoder
            .decode(bytes)
            .expect("valid BSON document should decode");

        match result {
            Payload::Bson(decoded) => assert_eq!(decoded, document),
            other => panic!("expected BSON document payload, got {other:?}"),
        }
    }

    #[test]
    fn given_invalid_bytes_should_reject_bson_payload() {
        let result = BsonStreamDecoder.decode(b"not bson".to_vec());

        assert!(matches!(result, Err(Error::CannotDecode(Schema::Bson))));
    }
}
