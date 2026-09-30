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

//! Value conversion utilities for connectors.
//!
//! Shared conversion and payload-transform functions used by the connector
//! ecosystem: `simd_json`/`serde_json` bridging, JSON key renaming.

use crate::Payload;

/// Convert `simd_json::OwnedValue` to `serde_json::Value` via direct structural mapping.
///
/// NaN/Infinity f64 values are mapped to `null` since JSON has no representation
/// for these IEEE 754 special values.
pub fn owned_value_to_serde_json(value: &simd_json::OwnedValue) -> serde_json::Value {
    match value {
        simd_json::OwnedValue::Static(s) => match s {
            simd_json::StaticNode::Null => serde_json::Value::Null,
            simd_json::StaticNode::Bool(b) => serde_json::Value::Bool(*b),
            simd_json::StaticNode::I64(n) => serde_json::Value::Number((*n).into()),
            simd_json::StaticNode::U64(n) => serde_json::Value::Number((*n).into()),
            simd_json::StaticNode::F64(n) => serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        },
        simd_json::OwnedValue::String(s) => serde_json::Value::String(s.to_string()),
        simd_json::OwnedValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(owned_value_to_serde_json).collect())
        }
        simd_json::OwnedValue::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.to_string(), owned_value_to_serde_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

/// Renames each key of a `Payload::Json` object via `rename`; other payloads pass through. Collisions keep the later entry while the map is Vec-backed, unspecified once hash-backed.
pub(crate) fn apply_field_mappings(
    payload: Payload,
    mut rename: impl FnMut(String) -> String,
) -> Payload {
    match payload {
        Payload::Json(json_value) => {
            if let simd_json::OwnedValue::Object(mut map) = json_value {
                let renamed: Vec<(String, simd_json::OwnedValue)> = map
                    .drain()
                    .map(|(key, value)| (rename(key), value))
                    .collect();
                for (key, value) in renamed {
                    map.insert(key, value);
                }
                Payload::Json(simd_json::OwnedValue::Object(map))
            } else {
                Payload::Json(json_value)
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use simd_json::{OwnedValue, StaticNode};

    use super::*;

    #[test]
    fn test_null() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Null)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_bool() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Bool(true))),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Bool(false))),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn test_i64() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::I64(-42))),
            serde_json::json!(-42)
        );
    }

    #[test]
    fn test_u64() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::U64(100))),
            serde_json::json!(100)
        );
    }

    #[test]
    fn test_f64_finite() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(1.5))),
            serde_json::json!(1.5)
        );
    }

    #[test]
    fn test_f64_nan_maps_to_null() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::NAN))),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_f64_infinity_maps_to_null() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::INFINITY))),
            serde_json::Value::Null
        );
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::NEG_INFINITY))),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_string() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::String("hello".into())),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_array() {
        let input = OwnedValue::Array(Box::new(vec![
            OwnedValue::Static(StaticNode::Null),
            OwnedValue::Static(StaticNode::Bool(true)),
        ]));
        assert_eq!(
            owned_value_to_serde_json(&input),
            serde_json::json!([null, true])
        );
    }

    #[test]
    fn test_object() {
        let mut obj = simd_json::owned::Object::new();
        obj.insert("k".into(), OwnedValue::Static(StaticNode::I64(1)));
        let input = OwnedValue::Object(Box::new(obj));
        assert_eq!(
            owned_value_to_serde_json(&input),
            serde_json::json!({"k": 1})
        );
    }

    #[test]
    fn test_nested_object() {
        let mut inner = simd_json::owned::Object::new();
        inner.insert("b".into(), OwnedValue::String("v".into()));
        let mut outer = simd_json::owned::Object::new();
        outer.insert("a".into(), OwnedValue::Object(Box::new(inner)));
        let input = OwnedValue::Object(Box::new(outer));
        assert_eq!(
            owned_value_to_serde_json(&input),
            serde_json::json!({"a": {"b": "v"}})
        );
    }

    #[test]
    fn apply_field_mappings_should_rename_mapped_key_and_keep_unmapped_key() {
        let mappings = HashMap::from([("old_field".to_string(), "new_field".to_string())]);

        let payload = Payload::Json(simd_json::json!({
            "old_field": "renamed",
            "unchanged_field": "stays_same"
        }));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.get("new_field").unwrap(), &OwnedValue::from("renamed"));
            assert!(!map.contains_key("old_field"));
            assert_eq!(
                map.get("unchanged_field").unwrap(),
                &OwnedValue::from("stays_same")
            );
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_swap_two_fields_without_losing_either_value() {
        let mappings = HashMap::from([
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "a".to_string()),
        ]);

        let payload = Payload::Json(simd_json::json!({
            "a": 1,
            "b": 2
        }));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.get("a").unwrap(), &OwnedValue::from(2));
            assert_eq!(map.get("b").unwrap(), &OwnedValue::from(1));
            assert_eq!(map.len(), 2);
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_rename_chained_fields_without_a_pre_existing_target() {
        let mappings = HashMap::from([
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "c".to_string()),
        ]);
        let payload = Payload::Json(simd_json::json!({"a": 1, "b": 2}));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.get("b").unwrap(), &OwnedValue::from(1));
            assert_eq!(map.get("c").unwrap(), &OwnedValue::from(2));
            assert_eq!(map.len(), 2);
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_only_rename_top_level_keys() {
        let mappings = HashMap::from([("a".to_string(), "renamed".to_string())]);
        let payload = Payload::Json(simd_json::json!({
            "a": {"a": "nested_value"}
        }));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert!(!map.contains_key("a"));
            if let OwnedValue::Object(nested) = map.get("renamed").unwrap() {
                assert_eq!(nested.get("a").unwrap(), &OwnedValue::from("nested_value"));
            } else {
                panic!("Expected nested JSON object");
            }
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_allow_self_mapping_as_a_no_op() {
        let mappings = HashMap::from([("a".to_string(), "a".to_string())]);
        let payload = Payload::Json(simd_json::json!({"a": 1}));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.get("a").unwrap(), &OwnedValue::from(1));
            assert_eq!(map.len(), 1);
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_keep_last_value_when_rename_collides() {
        let mappings = HashMap::from([("a".to_string(), "c".to_string())]);
        let payload = Payload::Json(simd_json::json!({"a": 1, "c": 2}));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.len(), 1);
            assert_eq!(map.get("c").unwrap(), &OwnedValue::from(2));
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_collapse_a_pre_existing_duplicate_key_to_its_last_value() {
        // Raw bytes, not `json!` (which dedupes at macro-build time), so the real parser's duplicate-key handling is exercised.
        let mut bytes = br#"{"a":1,"a":2,"b":3}"#.to_vec();
        let value: OwnedValue = simd_json::to_owned_value(&mut bytes).unwrap();
        if let OwnedValue::Object(map) = &value {
            assert_eq!(
                map.len(),
                3,
                "expected both raw \"a\" entries to survive parsing"
            );
        } else {
            panic!("Expected JSON object");
        }
        let payload = Payload::Json(value);

        let result = apply_field_mappings(payload, |key| key);

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("a").unwrap(), &OwnedValue::from(2));
            assert_eq!(map.get("b").unwrap(), &OwnedValue::from(3));
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_rename_and_preserve_every_value_above_the_32_key_vec_threshold()
    {
        // 40 keys forces simd_json's backing map past the 32-key Vec threshold.
        let mut json = String::from("{");
        for i in 0..39 {
            json.push_str(&format!(r#""field_{i}":{i},"#));
        }
        json.push_str(r#""field_39":39}"#);
        let mut bytes = json.into_bytes();
        let value: OwnedValue = simd_json::to_owned_value(&mut bytes).unwrap();
        if let OwnedValue::Object(map) = &value {
            assert!(map.is_map(), "expected the hashbrown backend above 32 keys");
        } else {
            panic!("Expected JSON object");
        }
        let payload = Payload::Json(value);

        let mappings: HashMap<String, String> = (0..5)
            .map(|i| (format!("field_{i}"), format!("renamed_{i}")))
            .collect();
        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.len(), 40);
            for i in 0..40 {
                let key = if i < 5 {
                    format!("renamed_{i}")
                } else {
                    format!("field_{i}")
                };
                assert_eq!(
                    map.get(key.as_str())
                        .unwrap_or_else(|| panic!("missing {key}")),
                    &OwnedValue::from(i as i64),
                    "wrong value for {key}"
                );
            }
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_ignore_rename_when_old_key_absent() {
        let mappings = HashMap::from([("missing".to_string(), "new_name".to_string())]);
        let payload = Payload::Json(simd_json::json!({"a": 1}));

        let result =
            apply_field_mappings(payload, |key| mappings.get(&key).cloned().unwrap_or(key));

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert!(!map.contains_key("new_name"));
            assert_eq!(map.get("a").unwrap(), &OwnedValue::from(1));
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_leave_populated_object_unchanged_when_renames_empty() {
        let payload = Payload::Json(simd_json::json!({"a": 1, "b": 2}));

        let result = apply_field_mappings(payload, |key| key);

        if let Payload::Json(OwnedValue::Object(map)) = result {
            assert_eq!(map.get("a").unwrap(), &OwnedValue::from(1));
            assert_eq!(map.get("b").unwrap(), &OwnedValue::from(2));
            assert_eq!(map.len(), 2);
        } else {
            panic!("Expected JSON object");
        }
    }

    #[test]
    fn apply_field_mappings_should_leave_non_object_json_unchanged() {
        let payload = Payload::Json(simd_json::json!([1, 2, 3]));

        let result = apply_field_mappings(payload, |key| key);

        if let Payload::Json(json_value) = result {
            assert_eq!(json_value, simd_json::json!([1, 2, 3]));
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn apply_field_mappings_should_leave_non_json_payload_unchanged() {
        let payload = Payload::Raw(vec![1, 2, 3]);

        let result = apply_field_mappings(payload, |key| key);

        if let Payload::Raw(bytes) = result {
            assert_eq!(bytes, vec![1, 2, 3]);
        } else {
            panic!("Expected Raw payload");
        }
    }
}
