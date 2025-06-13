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

use regex::Regex;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;

use crate::{DecodedMessage, Error, Payload, TopicMetadata};

use super::{
    Transform, TransformType,
    common::{Compilable, FieldValue, ValuePattern, compute_value},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    key: String,
    value: FieldValue,
    #[serde(default)]
    condition: Option<UpdateCondition<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateFieldsConfig {
    fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateCondition<T = String> {
    KeyExists,
    KeyNotExists,
    Matches(ValuePattern<T>),
    DoesNotMatch(ValuePattern<T>),
}

impl<T: Compilable> UpdateCondition<T> {
    pub fn compile(self) -> Result<UpdateCondition<Regex>, Error> {
        use UpdateCondition::*;
        Ok(match self {
            Matches(v) => Matches(v.compile()?),
            DoesNotMatch(v) => DoesNotMatch(v.compile()?),
            KeyExists => KeyExists,
            KeyNotExists => KeyNotExists,
        })
    }
}

struct CompiledField {
    key: String,
    value: FieldValue,
    condition: Option<UpdateCondition<Regex>>,
}

pub struct UpdateFields {
    fields: Vec<CompiledField>,
}

impl UpdateFields {
    pub fn new(cfg: UpdateFieldsConfig) -> Result<Self, Error> {
        let mut out = Vec::with_capacity(cfg.fields.len());
        for f in cfg.fields {
            out.push(CompiledField {
                key: f.key,
                value: f.value,
                condition: match f.condition {
                    Some(c) => Some(c.compile()?),
                    None => None,
                },
            });
        }
        Ok(Self { fields: out })
    }
}

impl Transform for UpdateFields {
    fn r#type(&self) -> TransformType {
        TransformType::UpdateFields
    }

    fn transform(
        &self,
        _meta: &TopicMetadata,
        mut msg: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = msg.payload else {
            return Ok(Some(msg));
        };

        for f in &self.fields {
            let present = map.contains_key(&f.key);
            let pass = match &f.condition {
                None => true,
                Some(UpdateCondition::KeyExists) => present,
                Some(UpdateCondition::KeyNotExists) => !present,
                Some(UpdateCondition::Matches(pat)) => present && pat.matches(&map[&f.key]),
                Some(UpdateCondition::DoesNotMatch(p)) => !present || !p.matches(&map[&f.key]),
            };
            if pass {
                let val = match &f.value {
                    FieldValue::Static(v) => v.clone(),
                    FieldValue::Computed(c) => compute_value(c),
                };
                map.insert(f.key.clone(), val);
            }
        }

        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::common::{ComputedValue, FieldValue};
    use crate::transforms::test_utils::{
        assert_is_number, assert_is_uuid, create_raw_test_message, create_test_message,
        create_test_topic_metadata, extract_json_object,
    };
    use simd_json::OwnedValue;

    #[test]
    fn test_basic_update() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![Field {
                key: "status".to_string(),
                value: FieldValue::Static(OwnedValue::from("updated")),
                condition: None,
            }],
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "updated");
    }

    #[test]
    fn test_condition_key_exists() {
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
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "updated");
        assert!(!json_obj.contains_key("missing_field"));
    }

    #[test]
    fn test_condition_key_not_exists() {
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
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "pending"); // Should remain unchanged
        assert_eq!(json_obj["created_at"], "2023-01-01"); // Should be added
    }

    #[test]
    fn test_condition_matches() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "status".to_string(),
                    value: FieldValue::Static(OwnedValue::from("complete")),
                    condition: Some(UpdateCondition::Matches(ValuePattern::Equals(
                        OwnedValue::from("pending"),
                    ))),
                },
                Field {
                    key: "priority".to_string(),
                    value: FieldValue::Static(OwnedValue::from("high")),
                    condition: Some(UpdateCondition::Matches(ValuePattern::Equals(
                        OwnedValue::from("medium"),
                    ))),
                },
            ],
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending", "priority": "low"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "complete"); // Should be updated
        assert_eq!(json_obj["priority"], "low"); // Should remain unchanged
    }

    #[test]
    fn test_condition_does_not_match() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "status".to_string(),
                    value: FieldValue::Static(OwnedValue::from("needs attention")),
                    condition: Some(UpdateCondition::DoesNotMatch(ValuePattern::Equals(
                        OwnedValue::from("complete"),
                    ))),
                },
                Field {
                    key: "priority".to_string(),
                    value: FieldValue::Static(OwnedValue::from("high")),
                    condition: Some(UpdateCondition::DoesNotMatch(ValuePattern::Equals(
                        OwnedValue::from("low"),
                    ))),
                },
            ],
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending", "priority": "low"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "needs attention"); // Should be updated (not "complete")
        assert_eq!(json_obj["priority"], "low"); // Should remain unchanged (is "low")
    }

    #[test]
    fn test_computed_values() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![
                Field {
                    key: "updated_at".to_string(),
                    value: FieldValue::Computed(ComputedValue::TimestampMillis),
                    condition: None,
                },
                Field {
                    key: "trace_id".to_string(),
                    value: FieldValue::Computed(ComputedValue::UuidV4),
                    condition: None,
                },
            ],
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "status": "pending"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 4);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["status"], "pending");
        assert_is_number(&json_obj["updated_at"], "updated_at");
        assert_is_uuid(&json_obj["trace_id"], "trace_id");
    }

    #[test]
    fn test_non_json_payload() {
        let transform = UpdateFields::new(UpdateFieldsConfig {
            fields: vec![Field {
                key: "status".to_string(),
                value: FieldValue::Static(OwnedValue::from("updated")),
                condition: None,
            }],
        })
        .unwrap();
        let msg = create_raw_test_message(vec![1, 2, 3, 4]);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        if let Payload::Raw(bytes) = &result.payload {
            assert_eq!(bytes, &vec![1, 2, 3, 4]);
        } else {
            panic!("Expected Raw payload");
        }
    }
}
