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

pub mod client;
pub mod config;
pub mod get_connector;
pub mod get_connectors;
pub mod get_transforms;
pub mod restart_connector;
pub mod stats;

use serde_json::Value;

/// Reads one string field of a JSON object, empty when missing or not a string.
pub(crate) fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// Reads one boolean field of a JSON object, false when missing or not a bool.
pub(crate) fn bool_field(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or_default()
}

/// Renders one JSON value for a table cell: strings bare, anything else compact.
pub(crate) fn cell(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        rest => serde_json::to_string(rest).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn should_read_string_field() {
        let value = json!({ "key": "my-sink" });
        assert_eq!(str_field(&value, "key"), "my-sink");
        assert_eq!(str_field(&value, "missing"), "");
    }

    #[test]
    fn should_read_string_field_only_for_strings() {
        let value = json!({ "key": 42 });
        assert_eq!(str_field(&value, "key"), "");
    }

    #[test]
    fn should_read_bool_field() {
        let value = json!({ "enabled": true });
        assert!(bool_field(&value, "enabled"));
        assert!(!bool_field(&value, "missing"));
    }

    #[test]
    fn should_render_cells() {
        assert_eq!(cell(&json!("text")), "text");
        assert_eq!(cell(&Value::Null), "");
        assert_eq!(cell(&json!(3)), "3");
        assert_eq!(cell(&json!(true)), "true");
        assert_eq!(cell(&json!({ "a": 1 })), r#"{"a":1}"#);
    }
}
