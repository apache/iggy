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

//! Environment variable value parsing utilities.

use figment::value::Value as FigmentValue;

/// Parse an environment variable value into a FigmentValue with type inference.
pub(crate) fn parse_env_value(value: &str) -> FigmentValue {
    // Handle array syntax [a, b, c]
    if value.starts_with('[') && value.ends_with(']') {
        let inner = value.trim_start_matches('[').trim_end_matches(']');
        let elements: Vec<FigmentValue> = split_array_elements(inner)
            .into_iter()
            .map(|s| parse_env_value(strip_quotes(s)))
            .collect();
        return FigmentValue::from(elements);
    }

    // Boolean
    match value.to_lowercase().as_str() {
        "true" => return FigmentValue::from(true),
        "false" => return FigmentValue::from(false),
        _ => {}
    }

    // Try numeric types
    if let Ok(n) = value.parse::<u64>() {
        return FigmentValue::from(n);
    }
    if let Ok(n) = value.parse::<i64>() {
        return FigmentValue::from(n);
    }
    if let Ok(n) = value.parse::<f64>() {
        return FigmentValue::from(n);
    }

    // Default to string
    FigmentValue::from(value)
}

/// Parse an environment variable value into a JSON value with type inference.
pub fn parse_env_value_to_json(value: &str) -> serde_json::Value {
    if value.starts_with('[') && value.ends_with(']') {
        let inner = value.trim_start_matches('[').trim_end_matches(']');
        let values: Vec<serde_json::Value> = split_array_elements(inner)
            .into_iter()
            .map(|s| parse_env_value_to_json(strip_quotes(s)))
            .collect();
        return serde_json::Value::Array(values);
    }

    match value.to_lowercase().as_str() {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        _ => {}
    }

    if let Ok(n) = value.parse::<u64>() {
        return serde_json::Value::Number(n.into());
    }
    if let Ok(n) = value.parse::<i64>() {
        return serde_json::Value::Number(n.into());
    }
    if let Ok(n) = value.parse::<f64>()
        && let Some(num) = serde_json::Number::from_f64(n)
    {
        return serde_json::Value::Number(num);
    }

    serde_json::Value::String(value.to_owned())
}

pub(crate) fn split_array_elements(s: &str) -> Vec<&str> {
    let mut elements = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                elements.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        elements.push(s[start..].trim());
    }
    elements
}

pub(crate) fn strip_quotes(s: &str) -> &str {
    s.trim_matches('"')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_value_to_json_handles_booleans() {
        assert_eq!(
            parse_env_value_to_json("true"),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            parse_env_value_to_json("TRUE"),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            parse_env_value_to_json("false"),
            serde_json::Value::Bool(false)
        );
        assert_eq!(
            parse_env_value_to_json("False"),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn parse_env_value_to_json_handles_integers() {
        assert_eq!(parse_env_value_to_json("42"), serde_json::json!(42));
        assert_eq!(parse_env_value_to_json("0"), serde_json::json!(0));
        assert_eq!(parse_env_value_to_json("-123"), serde_json::json!(-123));
    }

    #[test]
    fn parse_env_value_to_json_handles_floats() {
        assert_eq!(parse_env_value_to_json("1.5"), serde_json::json!(1.5));
        assert_eq!(parse_env_value_to_json("-2.5"), serde_json::json!(-2.5));
    }

    #[test]
    fn parse_env_value_to_json_handles_strings() {
        assert_eq!(parse_env_value_to_json("hello"), serde_json::json!("hello"));
        assert_eq!(
            parse_env_value_to_json("127.0.0.1:8080"),
            serde_json::json!("127.0.0.1:8080")
        );
    }

    #[test]
    fn parse_env_value_to_json_handles_arrays() {
        assert_eq!(
            parse_env_value_to_json("[1, 2, 3]"),
            serde_json::json!([1, 2, 3])
        );
        assert_eq!(
            parse_env_value_to_json("[\"a\", \"b\"]"),
            serde_json::json!(["a", "b"])
        );
        assert_eq!(
            parse_env_value_to_json("[true, false]"),
            serde_json::json!([true, false])
        );
    }

    #[test]
    fn split_array_elements_handles_simple_arrays() {
        assert_eq!(split_array_elements("a, b, c"), vec!["a", "b", "c"]);
        assert_eq!(split_array_elements("1,2,3"), vec!["1", "2", "3"]);
    }

    #[test]
    fn split_array_elements_handles_quoted_strings() {
        assert_eq!(
            split_array_elements("\"hello, world\", \"foo\""),
            vec!["\"hello, world\"", "\"foo\""]
        );
    }

    #[test]
    fn strip_quotes_removes_surrounding_quotes() {
        assert_eq!(strip_quotes("\"hello\""), "hello");
        assert_eq!(strip_quotes("hello"), "hello");
        assert_eq!(strip_quotes("\"\""), "");
    }
}
