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

//! Value conversion utilities for connector sinks.
//!
//! Provides shared conversion functions between serialization formats used by
//! the connector ecosystem (e.g., `simd_json` ↔ `serde_json`).

/// Convert `simd_json::OwnedValue` to `serde_json::Value` via direct structural mapping.
/// NaN/Infinity f64 values are mapped to `null`.
///
/// Prefer [`owned_value_into_serde_json`] when you own the value.
pub fn owned_value_to_serde_json(value: &simd_json::OwnedValue) -> serde_json::Value {
    match value {
        simd_json::OwnedValue::Static(s) => static_node_to_serde_json(*s),
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

/// Convert an owned `simd_json::OwnedValue` to `serde_json::Value` via direct structural mapping.
/// NaN/Infinity f64 values are mapped to `null`.
///
/// Strings and object keys move into the result instead of being copied.
pub fn owned_value_into_serde_json(value: simd_json::OwnedValue) -> serde_json::Value {
    match value {
        simd_json::OwnedValue::Static(s) => static_node_to_serde_json(s),
        simd_json::OwnedValue::String(s) => serde_json::Value::String(s),
        simd_json::OwnedValue::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(owned_value_into_serde_json).collect())
        }
        simd_json::OwnedValue::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .into_iter()
                .map(|(k, v)| (k, owned_value_into_serde_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

/// Converts a `StaticNode` leaf. Unlike `simd_json::OwnedValue`'s own `TryInto<serde_json::Value>`, NaN/Infinity map to `null` here instead of erroring.
fn static_node_to_serde_json(node: simd_json::StaticNode) -> serde_json::Value {
    match node {
        simd_json::StaticNode::Null => serde_json::Value::Null,
        simd_json::StaticNode::Bool(b) => serde_json::Value::Bool(b),
        simd_json::StaticNode::I64(n) => serde_json::Value::Number(n.into()),
        simd_json::StaticNode::U64(n) => serde_json::Value::Number(n.into()),
        simd_json::StaticNode::F64(n) => serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::{OwnedValue, StaticNode};

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
    fn test_into_matches_to_for_every_variant() {
        let mut populated_obj = simd_json::owned::Object::new();
        populated_obj.insert("k".into(), OwnedValue::String("v".into()));
        let cases = [
            OwnedValue::Static(StaticNode::Null),
            OwnedValue::Static(StaticNode::Bool(true)),
            OwnedValue::Static(StaticNode::Bool(false)),
            OwnedValue::Static(StaticNode::I64(-42)),
            OwnedValue::Static(StaticNode::U64(42)),
            OwnedValue::Static(StaticNode::F64(3.54)),
            OwnedValue::Static(StaticNode::F64(f64::NAN)),
            OwnedValue::Static(StaticNode::F64(f64::INFINITY)),
            OwnedValue::Static(StaticNode::F64(f64::NEG_INFINITY)),
            OwnedValue::String("hello".into()),
            OwnedValue::Array(Box::default()),
            OwnedValue::Object(Box::new(simd_json::owned::Object::new())),
            OwnedValue::Array(Box::new(vec![
                OwnedValue::String("x".into()),
                OwnedValue::Static(StaticNode::I64(1)),
            ])),
            OwnedValue::Object(Box::new(populated_obj)),
        ];
        for case in cases {
            assert_eq!(
                owned_value_into_serde_json(case.clone()),
                owned_value_to_serde_json(&case),
                "owned_value_into_serde_json must match owned_value_to_serde_json for {case:?}"
            );
        }
    }

    #[test]
    fn test_into_moves_string_leaf_and_object_key_without_reallocating() {
        let leaf = String::from("leaf-value-not-interned");
        let leaf_ptr = leaf.as_ptr();
        let key = String::from("object-key-not-interned");
        let key_ptr = key.as_ptr();

        let mut obj = simd_json::owned::Object::new();
        obj.insert(key, OwnedValue::String(leaf));
        let input = OwnedValue::Object(Box::new(obj));

        let serde_json::Value::Object(map) = owned_value_into_serde_json(input) else {
            panic!("expected an object");
        };
        let (result_key, result_value) = map.into_iter().next().expect("one entry");
        let serde_json::Value::String(result_leaf) = result_value else {
            panic!("expected a string value");
        };

        assert_eq!(
            result_key.as_ptr(),
            key_ptr,
            "object key must move into the result, not reallocate"
        );
        assert_eq!(
            result_leaf.as_ptr(),
            leaf_ptr,
            "string leaf must move into the result, not reallocate"
        );
    }

    #[test]
    fn test_into_nested_object_converts_recursively() {
        let mut inner = simd_json::owned::Object::new();
        inner.insert("b".into(), OwnedValue::String("v".into()));
        inner.insert(
            "arr".into(),
            OwnedValue::Array(Box::new(vec![OwnedValue::Static(StaticNode::I64(-1))])),
        );
        let mut outer = simd_json::owned::Object::new();
        outer.insert("a".into(), OwnedValue::Object(Box::new(inner)));
        let input = OwnedValue::Object(Box::new(outer));
        assert_eq!(
            owned_value_into_serde_json(input),
            serde_json::json!({"a": {"b": "v", "arr": [-1]}})
        );
    }
}
