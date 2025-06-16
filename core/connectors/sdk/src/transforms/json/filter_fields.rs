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
    DecodedMessage, Error, Payload, TopicMetadata, transforms::filter_fields::FilterFields,
};
use simd_json::OwnedValue;

impl FilterFields {
    pub(crate) fn transform_json(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.keep_set.is_empty() && self.patterns.is_empty() {
            return Ok(Some(message)); // nothing to do
        }

        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
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

        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::json::test_utils::{
        create_raw_test_message, create_test_message, create_test_topic_metadata,
        extract_json_object,
    };
    use crate::transforms::{
        Transform,
        filter_fields::{
            FilterFields, FilterFieldsConfig, FilterPattern, KeyPattern, ValuePattern,
        },
    };

    #[test]
    fn should_keep_only_specified_fields_when_keep_list_provided() {
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
            .transform_json(&create_test_topic_metadata(), msg)
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
    fn should_include_fields_matching_key_pattern() {
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
            .transform_json(&create_test_topic_metadata(), msg)
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
    fn should_exclude_fields_matching_pattern_when_include_false() {
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
            .transform_json(&create_test_topic_metadata(), msg)
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
    fn should_filter_fields_based_on_value_pattern_matching() {
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
            .transform_json(&create_test_topic_metadata(), msg)
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
    fn should_filter_using_combined_key_and_value_patterns() {
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
            "updated_date": "2023-01-02",
            "expired_date": null,
            "version": "1.0"
        }"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["created_date"], "2023-01-01");
        assert_eq!(json_obj["updated_date"], "2023-01-02");
        assert!(!json_obj.contains_key("id"));
        assert!(!json_obj.contains_key("expired_date")); // null, not a string
        assert!(!json_obj.contains_key("version"));
    }

    #[test]
    fn should_combine_keep_fields_list_with_pattern_filtering() {
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
            "description": "should be removed"
        }"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["id"], 1); // explicitly kept
        assert_eq!(json_obj["meta_created"], "2023-01-01"); // matched pattern
        assert!(!json_obj.contains_key("name"));
        assert!(!json_obj.contains_key("description"));
    }

    #[test]
    fn should_return_message_unchanged_when_no_filters() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec![],
            patterns: vec![],
            include_matching: true,
        })
        .unwrap();
        let msg = create_test_message(r#"{"id": 1, "name": "test"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2); // should keep everything
        assert_eq!(json_obj["id"], 1);
        assert_eq!(json_obj["name"], "test");
    }

    #[test]
    fn should_pass_through_non_json_payload_unchanged() {
        let transform = FilterFields::new(FilterFieldsConfig {
            keep_fields: vec!["field1".to_string()],
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
            assert_eq!(*bytes, vec![1u8, 2, 3, 4]);
        } else {
            panic!("Expected Raw payload");
        }
    }
}
