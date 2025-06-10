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

use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::collections::HashSet;

use crate::{DecodedMessage, Error, Payload, TopicMetadata};

use super::{
    Transform, TransformType,
    common::{KeyPattern, ValuePattern},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteFieldsConfig {
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    patterns: Vec<DeletePattern>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeletePattern {
    #[serde(default)]
    key_pattern: Option<KeyPattern<String>>,
    #[serde(default)]
    value_pattern: Option<ValuePattern<String>>,
}

struct CompiledPattern {
    key_pattern: Option<KeyPattern<regex::Regex>>,
    value_pattern: Option<ValuePattern<regex::Regex>>,
}

pub struct DeleteFields {
    direct_keys: HashSet<String>,
    patterns: Vec<CompiledPattern>,
}

impl DeleteFields {
    pub fn new(cfg: DeleteFieldsConfig) -> Result<Self, Error> {
        let direct_keys = cfg.fields.into_iter().collect();

        let mut patterns = Vec::with_capacity(cfg.patterns.len());
        for p in cfg.patterns {
            patterns.push(CompiledPattern {
                key_pattern: p.key_pattern.map(|kp| kp.compile()).transpose()?,
                value_pattern: p.value_pattern.map(|vp| vp.compile()).transpose()?,
            });
        }

        Ok(Self {
            direct_keys,
            patterns,
        })
    }

    fn should_remove(&self, k: &str, v: &OwnedValue) -> bool {
        if self.direct_keys.contains(k) {
            return true;
        }

        self.patterns.iter().any(|pat| {
            let key_ok = pat.key_pattern.as_ref().map_or(true, |kp| kp.matches(k));
            let value_ok = pat.value_pattern.as_ref().map_or(true, |vp| vp.matches(v));
            key_ok && value_ok
        })
    }
}

impl Transform for DeleteFields {
    fn r#type(&self) -> TransformType {
        TransformType::DeleteFields
    }

    fn transform(
        &self,
        _meta: &TopicMetadata,
        mut msg: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = msg.payload else {
            return Ok(Some(msg));
        };

        map.retain(|k, v| !self.should_remove(k, v));
        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::common::ValuePattern;
    use crate::transforms::test_utils::{
        create_raw_test_message, create_test_message, create_test_topic_metadata,
        extract_json_object,
    };
    use simd_json::OwnedValue;

    #[test]
    fn test_delete_direct_fields() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec!["field1".to_string(), "field3".to_string()],
            patterns: vec![],
        })
        .unwrap();
        let msg = create_test_message(
            r#"{"field1": "value1", "field2": "value2", "field3": 42, "field4": true}"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["field2"], "value2");
        assert_eq!(json_obj["field4"], true);
        assert!(!json_obj.contains_key("field1"));
        assert!(!json_obj.contains_key("field3"));
    }

    #[test]
    fn test_delete_with_key_pattern() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec![],
            patterns: vec![DeletePattern {
                key_pattern: Some(KeyPattern::StartsWith("temp_".to_string())),
                value_pattern: None,
            }],
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1, 
            "temp_value": 100, 
            "temp_flag": true, 
            "permanent": "keep this"
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["permanent"], "keep this");
        assert!(!json_obj.contains_key("temp_value"));
        assert!(!json_obj.contains_key("temp_flag"));
    }

    #[test]
    fn test_delete_with_value_pattern() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec![],
            patterns: vec![DeletePattern {
                key_pattern: None,
                value_pattern: Some(ValuePattern::IsNull),
            }],
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1, 
            "name": "test", 
            "description": null, 
            "metadata": null
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["name"], "test");
        assert!(!json_obj.contains_key("description"));
        assert!(!json_obj.contains_key("metadata"));
    }

    #[test]
    fn test_delete_with_combined_patterns() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec![],
            patterns: vec![DeletePattern {
                key_pattern: Some(KeyPattern::Contains("flag".to_string())),
                value_pattern: Some(ValuePattern::Equals(OwnedValue::from(false))),
            }],
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1, 
            "flag_active": true, 
            "flag_debug": false, 
            "debug": false
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 3);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["flag_active"], true);
        assert_eq!(json_obj["debug"], false);
        assert!(!json_obj.contains_key("flag_debug")); // Should be deleted
    }

    #[test]
    fn test_non_json_payload() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec!["field1".to_string()],
            patterns: vec![],
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
