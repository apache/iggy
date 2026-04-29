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

use crate::{
    DecodedMessage, Error, Payload, TopicMetadata, transforms::unwrap_envelope::UnwrapEnvelope,
};
use simd_json::OwnedValue;
use tracing::warn;

impl UnwrapEnvelope {
    pub(crate) fn transform_json(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
        };

        let Some(inner) = map.remove(self.field.as_str()) else {
            warn!(
                "unwrap_envelope: field '{}' not found in payload, passing through unchanged",
                self.field
            );
            return Ok(Some(message));
        };

        message.payload = Payload::Json(inner);
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use crate::transforms::Transform;
    use crate::transforms::json::test_utils::{
        create_raw_test_message, create_test_message, create_test_topic_metadata,
        extract_json_object,
    };
    use crate::transforms::unwrap_envelope::{UnwrapEnvelope, UnwrapEnvelopeConfig};
    use crate::{DecodedMessage, Payload};
    use simd_json::OwnedValue;

    #[test]
    fn should_extract_data_field_from_database_record_envelope() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "data".to_string(),
        });
        let msg = create_test_message(
            r#"{
                "table_name": "tpch.lineitem",
                "operation_type": "SELECT",
                "timestamp": "2026-04-24T19:57:00Z",
                "data": {"id": 1, "l_orderkey": 123, "l_partkey": 456},
                "old_data": null
            }"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert!(json_obj.contains_key("id"));
        assert!(json_obj.contains_key("l_orderkey"));
        assert!(json_obj.contains_key("l_partkey"));
        assert!(!json_obj.contains_key("table_name"));
        assert!(!json_obj.contains_key("operation_type"));
    }

    #[test]
    fn should_handle_missing_field_gracefully() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "data".to_string(),
        });
        let msg = create_test_message(r#"{"table_name": "users", "operation_type": "INSERT"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert!(json_obj.contains_key("table_name"));
    }

    #[test]
    fn should_handle_scalar_inner_value() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "payload".to_string(),
        });
        let msg = create_test_message(r#"{"payload": "just a string", "meta": 1}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        match &result.payload {
            Payload::Json(OwnedValue::String(s)) => assert_eq!(s.as_str(), "just a string"),
            other => panic!("Expected Json(String), got {other:?}"),
        }
    }

    #[test]
    fn should_handle_null_inner_value() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "old_data".to_string(),
        });
        let msg = create_test_message(r#"{"data": {"id": 1}, "old_data": null}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        match &result.payload {
            Payload::Json(OwnedValue::Static(simd_json::StaticNode::Null)) => {}
            other => panic!("Expected Json(Null), got {other:?}"),
        }
    }

    #[test]
    fn should_pass_through_non_json_payload() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "data".to_string(),
        });
        let msg = create_raw_test_message(vec![1, 2, 3, 4]);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        if let Payload::Raw(bytes) = &result.payload {
            assert_eq!(*bytes, vec![1u8, 2, 3, 4]);
        } else {
            panic!("Expected Raw payload");
        }
    }

    #[test]
    fn should_pass_through_non_object_json() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "data".to_string(),
        });
        let msg = DecodedMessage {
            id: None,
            offset: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: Payload::Json(OwnedValue::Array(Box::new(vec![
                OwnedValue::Static(simd_json::StaticNode::I64(1)),
                OwnedValue::Static(simd_json::StaticNode::I64(2)),
            ]))),
        };
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        match &result.payload {
            Payload::Json(OwnedValue::Array(arr)) => assert_eq!(arr.len(), 2),
            other => panic!("Expected Json(Array), got {other:?}"),
        }
    }

    #[test]
    fn should_unwrap_nested_array_field() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "items".to_string(),
        });
        let msg = create_test_message(r#"{"items": [1, 2, 3], "count": 3}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        match &result.payload {
            Payload::Json(OwnedValue::Array(arr)) => assert_eq!(arr.len(), 3),
            other => panic!("Expected Json(Array), got {other:?}"),
        }
    }

    #[test]
    fn should_work_with_deeply_nested_data() {
        let transform = UnwrapEnvelope::new(UnwrapEnvelopeConfig {
            field: "data".to_string(),
        });
        let msg = create_test_message(
            r#"{
                "data": {"user": {"name": "John", "age": 30}, "active": true},
                "meta": "ignored"
            }"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert!(json_obj.contains_key("user"));
        assert!(json_obj.contains_key("active"));
    }
}
