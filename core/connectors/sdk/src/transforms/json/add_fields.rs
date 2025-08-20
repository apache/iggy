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

use super::compute_value;
use crate::{
    DecodedMessage, Error, Payload, TopicMetadata,
    transforms::{AddFields, FieldValue},
};
use simd_json::OwnedValue;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::json::test_utils::{
        assert_is_number, assert_is_uuid, create_raw_test_message, create_test_message,
        create_test_topic_metadata, extract_json_object,
    };
    use crate::transforms::{
        ComputedValue, FieldValue, Transform,
        add_fields::{AddFields, Field},
    };
    use simd_json::OwnedValue;

    #[test]
    fn should_return_message_unchanged_when_no_fields() {
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
    fn should_add_static_field_to_json_message() {
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
    fn should_add_multiple_static_fields_with_different_types() {
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
    fn should_add_computed_fields_with_dynamic_values() {
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
    fn should_overwrite_existing_field_when_same_key_specified() {
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
    fn should_pass_through_non_json_payload_unchanged() {
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
