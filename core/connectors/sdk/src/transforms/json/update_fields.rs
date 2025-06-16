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
    transforms::{FieldValue, UpdateFields, update_fields::UpdateCondition},
};
use simd_json::OwnedValue;

impl UpdateFields {
    pub(crate) fn transform_json(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
        };

        for field in &self.fields {
            let present = map.contains_key(&field.key);
            let pass = match &field.condition {
                None | Some(UpdateCondition::Always) => true,
                Some(UpdateCondition::KeyExists) => present,
                Some(UpdateCondition::KeyNotExists) => !present,
            };
            if pass {
                let val = match &field.value {
                    FieldValue::Static(v) => v.clone(),
                    FieldValue::Computed(c) => compute_value(c),
                };
                map.insert(field.key.clone(), val);
            }
        }

        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::json::test_utils::{
        assert_is_uuid, create_raw_test_message, create_test_message, create_test_topic_metadata,
        extract_json_object,
    };
    use crate::transforms::{
        ComputedValue, FieldValue, Transform,
        update_fields::{Field, UpdateCondition, UpdateFields, UpdateFieldsConfig},
    };
    use simd_json::OwnedValue;
    use simd_json::prelude::TypedScalarValue;

    #[test]
    fn should_always_update_field_when_no_condition() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![Field {
                key: "status".to_string(),
                value: FieldValue::Static(OwnedValue::from("updated")),
                condition: None,
            }],
        });
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "updated");
    }

    #[test]
    fn should_update_only_existing_keys_when_key_exists() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "status".to_string(),
                    value: FieldValue::Static(OwnedValue::from("updated")),
                    condition: Some(UpdateCondition::KeyExists),
                },
                Field {
                    key: "missing_field".to_string(),
                    value: FieldValue::Static(OwnedValue::from("should not be added")),
                    condition: Some(UpdateCondition::KeyExists),
                },
            ],
        });
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "updated");
        assert!(!json_obj.contains_key("missing_field"));
    }

    #[test]
    fn should_conditionally_add_fields_when_key_not_exists() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "status".to_string(),
                    value: FieldValue::Static(OwnedValue::from("should not update")),
                    condition: Some(UpdateCondition::KeyNotExists),
                },
                Field {
                    key: "created_at".to_string(),
                    value: FieldValue::Static(OwnedValue::from("2023-01-01")),
                    condition: Some(UpdateCondition::KeyNotExists),
                },
            ],
        });
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "pending"); // Should remain unchanged
        assert_eq!(json_obj["created_at"], "2023-01-01"); // Should be added
    }

    #[test]
    fn should_update_or_add_fields_when_always_condition() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "status".to_string(),
                    value: FieldValue::Static(OwnedValue::from("updated")),
                    condition: Some(UpdateCondition::Always),
                },
                Field {
                    key: "new_field".to_string(),
                    value: FieldValue::Static(OwnedValue::from("new_value")),
                    condition: Some(UpdateCondition::Always),
                },
            ],
        });
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "updated"); // Should be updated
        assert_eq!(json_obj["new_field"], "new_value"); // Should be added
    }

    #[test]
    fn should_update_fields_with_dynamically_computed_values() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "updated_at".to_string(),
                    value: FieldValue::Computed(ComputedValue::DateTime),
                    condition: None,
                },
                Field {
                    key: "request_id".to_string(),
                    value: FieldValue::Computed(ComputedValue::UuidV4),
                    condition: Some(UpdateCondition::KeyNotExists),
                },
            ],
        });
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 4);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "pending");
        assert_is_uuid(&json_obj["request_id"], "request_id");
        // Note: We can't easily test DateTime format here, but we know it's a string
        assert!(json_obj["updated_at"].is_str());
    }

    #[test]
    fn should_pass_through_non_json_payload_unchanged() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![Field {
                key: "new_field".to_string(),
                value: FieldValue::Static(OwnedValue::from("new_value")),
                condition: None,
            }],
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
}
