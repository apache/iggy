/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#[cfg(test)]
use crate::{DecodedMessage, Payload, TopicMetadata};
#[cfg(test)]
use simd_json::OwnedValue;
#[cfg(test)]
use simd_json::prelude::{TypedScalarValue, ValueAsScalar};
#[cfg(test)]
use uuid;

/// Helper function to create a test message with the given JSON payload
#[cfg(test)]
pub fn create_test_message(json: &str) -> DecodedMessage {
    let mut payload = json.to_string().into_bytes();
    let value = simd_json::to_owned_value(&mut payload).unwrap();
    DecodedMessage {
        id: None,
        offset: None,
        checksum: None,
        timestamp: None,
        origin_timestamp: None,
        headers: None,
        payload: Payload::Json(value),
    }
}

/// Helper function to create a non-JSON message with raw bytes
#[cfg(test)]
pub fn create_raw_test_message(bytes: Vec<u8>) -> DecodedMessage {
    DecodedMessage {
        id: None,
        offset: None,
        checksum: None,
        timestamp: None,
        origin_timestamp: None,
        headers: None,
        payload: Payload::Raw(bytes),
    }
}

/// Helper function to create a topic metadata for testing
#[cfg(test)]
pub fn create_test_topic_metadata() -> TopicMetadata {
    TopicMetadata {
        stream: "test-stream".to_string(),
        topic: "test-topic".to_string(),
    }
}

/// Helper function to extract the JSON object from a message
#[cfg(test)]
pub fn extract_json_object(msg: &DecodedMessage) -> Option<&simd_json::owned::Object> {
    if let Payload::Json(OwnedValue::Object(map)) = &msg.payload {
        Some(map)
    } else {
        None
    }
}

/// Helper function to assert that a JSON value is a number
#[cfg(test)]
pub fn assert_is_number(value: &OwnedValue, field_name: &str) {
    if !value.is_number() {
        panic!("{} should be a number", field_name);
    }
}

/// Helper function to assert that a JSON value is a string
#[cfg(test)]
pub fn assert_is_string(value: &OwnedValue, field_name: &str) {
    if !value.is_str() {
        panic!("{} should be a string", field_name);
    }
}

/// Helper function to assert that a JSON value is a string and validates as a UUID
#[cfg(test)]
pub fn assert_is_uuid(value: &OwnedValue, field_name: &str) {
    if !value.is_str() {
        panic!("{} should be a string", field_name);
    }

    let string_value = value.as_str().unwrap();
    if uuid::Uuid::parse_str(string_value).is_err() {
        panic!("{} is not a valid UUID", field_name);
    }
}
