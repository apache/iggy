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
pub struct FilterFieldsConfig {
    #[serde(default)]
    keep_fields: Vec<String>,
    #[serde(default)]
    patterns: Vec<FilterPattern>,
    #[serde(default = "default_include")]
    include_matching: bool,
}

fn default_include() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterPattern {
    #[serde(default)]
    key_pattern: Option<KeyPattern<String>>,
    #[serde(default)]
    value_pattern: Option<ValuePattern<String>>,
}

struct CompiledPattern {
    key_pattern: Option<KeyPattern<regex::Regex>>,
    value_pattern: Option<ValuePattern<regex::Regex>>,
}

pub struct FilterFields {
    include_matching: bool,
    keep_set: HashSet<String>,
    patterns: Vec<CompiledPattern>,
}

impl FilterFields {
    pub fn new(cfg: FilterFieldsConfig) -> Result<Self, Error> {
        let keep_set = cfg.keep_fields.into_iter().collect();

        let mut patterns = Vec::with_capacity(cfg.patterns.len());
        for p in cfg.patterns {
            patterns.push(CompiledPattern {
                key_pattern: p.key_pattern.map(|kp| kp.compile()).transpose()?,
                value_pattern: p.value_pattern.map(|vp| vp.compile()).transpose()?,
            });
        }

        Ok(Self {
            include_matching: cfg.include_matching,
            keep_set,
            patterns,
        })
    }

    #[inline]
    fn matches_patterns(&self, k: &str, v: &OwnedValue) -> bool {
        self.patterns.iter().any(|pat| {
            let key_ok = pat.key_pattern.as_ref().map_or(true, |kp| kp.matches(k));
            let value_ok = pat.value_pattern.as_ref().map_or(true, |vp| vp.matches(v));
            key_ok && value_ok
        })
    }
}

impl Transform for FilterFields {
    fn r#type(&self) -> TransformType {
        TransformType::FilterFields
    }

    fn transform(
        &self,
        _meta: &TopicMetadata,
        mut msg: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.keep_set.is_empty() && self.patterns.is_empty() {
            return Ok(Some(msg)); // nothing to do
        }

        let Payload::Json(OwnedValue::Object(ref mut map)) = msg.payload else {
            return Ok(Some(msg));
        };

        let include = self.include_matching;
        map.retain(|k, v| {
            let explicit_keep = self.keep_set.contains(k);
            if explicit_keep {
                return true; // never drop an explicitly kept key
            }

            let matched = self.matches_patterns(k, v);
            include ^ !matched // xor gives us include / exclude in one line
        });

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

    #[test]
    fn test_keep_specific_fields() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec!["id".to_string(), "name".to_string()],
            patterns: vec![],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "name": "test",
            "description": "should be removed",
            "created_at": "2023-01-01"
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
        assert!(!json_obj.contains_key("created_at"));
    }

    #[test]
    fn test_filter_include_key_pattern() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![FilterPattern {
                key_pattern: Some(KeyPattern::StartsWith("meta_".to_string())),
                value_pattern: None,
            }],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "meta_created": "2023-01-01",
            "meta_updated": "2023-01-02",
            "content": "test content"
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["meta_created"], "2023-01-01");
        assert_eq!(json_obj["meta_updated"], "2023-01-02");
        assert!(!json_obj.contains_key("id"));
        assert!(!json_obj.contains_key("content"));
    }

    #[test]
    fn test_filter_exclude_key_pattern() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![FilterPattern {
                key_pattern: Some(KeyPattern::StartsWith("temp_".to_string())),
                value_pattern: None,
            }],
            include_matching: false,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "name": "test",
            "temp_value": 100,
            "temp_flag": true
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
        assert!(!json_obj.contains_key("temp_value"));
        assert!(!json_obj.contains_key("temp_flag"));
    }

    #[test]
    fn test_filter_with_value_pattern() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![FilterPattern {
                key_pattern: None,
                value_pattern: Some(ValuePattern::IsNumber),
            }],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "count": 42,
            "name": "test",
            "active": true
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["count"], 42);
        assert!(!json_obj.contains_key("name"));
        assert!(!json_obj.contains_key("active"));
    }

    #[test]
    fn test_filter_combined_patterns() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![FilterPattern {
                key_pattern: Some(KeyPattern::Contains("date".to_string())),
                value_pattern: Some(ValuePattern::IsString),
            }],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "created_date": "2023-01-01",
            "update_date": "2023-01-02",
            "date_count": 5,
            "name": "test"
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["created_date"], "2023-01-01");
        assert_eq!(json_obj["update_date"], "2023-01-02");
        assert!(!json_obj.contains_key("id"));
        assert!(!json_obj.contains_key("date_count"));
        assert!(!json_obj.contains_key("name"));
    }

    #[test]
    fn test_keep_and_pattern_together() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec!["id".to_string()],
            patterns: vec![FilterPattern {
                key_pattern: Some(KeyPattern::StartsWith("meta_".to_string())),
                value_pattern: None,
            }],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(
            r#"{
            "id": 1,
            "name": "test",
            "meta_created": "2023-01-01",
            "content": "test content"
        }"#,
        );
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["meta_created"], "2023-01-01");
        assert!(!json_obj.contains_key("name"));
        assert!(!json_obj.contains_key("content"));
    }

    #[test]
    fn test_empty_config() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "name": "test"}"#);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["name"], "test");
    }

    #[test]
    fn test_non_json_payload() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec!["id".to_string()],
            patterns: vec![],
            include_matching: true,
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
