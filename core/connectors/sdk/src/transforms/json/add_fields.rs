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

use chrono::Utc;
use simd_json::OwnedValue;

use crate::{
    DecodedMessage, Error, Payload, TopicMetadata,
    transforms::add_fields::{AddFields, ComputedValue, FieldValue},
};

impl AddFields {
    pub(crate) fn transform_json(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
        };

        for field in &self.fields {
            let new_val = match &field.value {
                FieldValue::Static(v) => v.clone(),
                FieldValue::Computed(c) => compute_value(c),
            };
            map.insert(field.key.clone(), new_val);
        }

        Ok(Some(message))
    }
}

fn compute_value(kind: &ComputedValue) -> OwnedValue {
    let now = Utc::now();
    match kind {
        ComputedValue::DateTime => now.to_rfc3339().into(),
        ComputedValue::TimestampNanos => now.timestamp_nanos_opt().unwrap().into(),
        ComputedValue::TimestampMicros => now.timestamp_micros().into(),
        ComputedValue::TimestampMillis => now.timestamp_millis().into(),
        ComputedValue::TimestampSeconds => now.timestamp().into(),
        ComputedValue::UuidV4 => uuid::Uuid::new_v4().to_string().into(),
        ComputedValue::UuidV7 => uuid::Uuid::now_v7().to_string().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::json::test_utils::{
        assert_is_number, assert_is_uuid, create_raw_test_message, create_test_message,
        create_test_topic_metadata, extract_json_object,
    };
    use crate::transforms::{
        Transform,
        add_fields::{AddFields, ComputedValue, Field, FieldValue},
    };
    use simd_json::OwnedValue;

    #[test]
    fn test_empty_fields() {
        let transform = AddFields { fields: vec![] };
        let msg = create_test_message(r#"{"existing": "field"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 1);
        assert_eq!(json_obj["existing"], "field");
    }

    #[test]
    fn test_add_static_field() {
        let transform = AddFields {
            fields: vec![Field {
                key: "new_field".to_string(),
                value: FieldValue::Static(OwnedValue::from("new_value")),
            }],
        };
        let msg = create_test_message(r#"{"existing": "field"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["existing"], "field");
        assert_eq!(json_obj["new_field"], "new_value");
    }

    #[test]
    fn test_add_multiple_static_fields() {
        let transform = AddFields {
            fields: vec![
                Field {
                    key: "string_field".to_string(),
                    value: FieldValue::Static(OwnedValue::from("string_value")),
                },
                Field {
                    key: "number_field".to_string(),
                    value: FieldValue::Static(OwnedValue::from(42)),
                },
                Field {
                    key: "boolean_field".to_string(),
                    value: FieldValue::Static(OwnedValue::from(true)),
                },
            ],
        };
        let msg = create_test_message(r#"{"existing": "field"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 4);
        assert_eq!(json_obj["existing"], "field");
        assert_eq!(json_obj["string_field"], "string_value");
        assert_eq!(json_obj["number_field"], 42);
        assert_eq!(json_obj["boolean_field"], true);
    }

    #[test]
    fn test_add_computed_fields() {
        let transform = AddFields {
            fields: vec![
                Field {
                    key: "timestamp_ms".to_string(),
                    value: FieldValue::Computed(ComputedValue::TimestampMillis),
                },
                Field {
                    key: "uuid".to_string(),
                    value: FieldValue::Computed(ComputedValue::UuidV4),
                },
            ],
        };
        let msg = create_test_message(r#"{"existing": "field"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["existing"], "field");
        assert_is_number(&json_obj["timestamp_ms"], "timestamp_ms");
        assert_is_uuid(&json_obj["uuid"], "uuid");
    }

    #[test]
    fn test_overwrite_existing_field() {
        let transform = AddFields {
            fields: vec![Field {
                key: "existing".to_string(),
                value: FieldValue::Static(OwnedValue::from("new_value")),
            }],
        };
        let msg = create_test_message(r#"{"existing": "field"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 1);
        assert_eq!(json_obj["existing"], "new_value");
    }

    #[test]
    fn test_non_json_payload() {
        let transform = AddFields {
            fields: vec![Field {
                key: "new_field".to_string(),
                value: FieldValue::Static(OwnedValue::from("new_value")),
            }],
        };
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
}
