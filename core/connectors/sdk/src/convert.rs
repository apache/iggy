/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Value conversion utilities for connector sinks.
//!
//! Provides shared conversion functions between serialization formats used by
//! the connector ecosystem (e.g., `simd_json` ↔ `serde_json`).

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
