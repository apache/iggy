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

use crate::configs::runtime::ResponseConfig;
use crate::error::RuntimeError;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Extracts data from JSON responses using configured paths
pub struct ResponseExtractor {
    data_path: Option<String>,
    error_path: Option<String>,
}

impl ResponseExtractor {
    /// Creates a new ResponseExtractor with optional configuration
    pub fn new(config: &ResponseConfig) -> Self {
        Self {
            data_path: config.data_path.clone(),
            error_path: config.error_path.clone(),
        }
    }

    /// Extracts data from a JSON response body
    ///
    /// If data_path is configured, navigates to that path and deserializes.
    /// Otherwise, deserializes the entire response body directly.
    pub fn extract<T: DeserializeOwned>(&self, response_body: &str) -> Result<T, RuntimeError> {
        let json_value: Value = serde_json::from_str(response_body).map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to parse JSON response: {err}"))
        })?;

        if let Some(error_path) = &self.error_path
            && let Some(error_value) = self.navigate_path(&json_value, error_path)
            && !error_value.is_null()
        {
            let error_msg = match error_value {
                Value::String(s) => s.clone(),
                _ => error_value.to_string(),
            };
            return Err(RuntimeError::HttpRequestFailed(format!(
                "API returned error: {error_msg}"
            )));
        }

        let target_value = if let Some(data_path) = &self.data_path {
            self.navigate_path(&json_value, data_path).ok_or_else(|| {
                let structure = Self::summarize_structure(&json_value);
                RuntimeError::HttpRequestFailed(format!(
                    "Data path '{data_path}' not found in response. Response structure: {structure}"
                ))
            })?
        } else {
            &json_value
        };

        serde_json::from_value(target_value.clone()).map_err(|err| {
            RuntimeError::HttpRequestFailed(format!(
                "Failed to deserialize data: {err}. Value: {target_value}"
            ))
        })
    }

    /// Navigates through a JSON structure using dot-notation path
    ///
    /// Example: "data.config" navigates to json["data"]["config"]
    fn navigate_path<'a>(&self, json: &'a Value, path: &str) -> Option<&'a Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for part in parts {
            current = match current {
                Value::Object(map) => map.get(part)?,
                Value::Array(arr) => {
                    if let Ok(index) = part.parse::<usize>() {
                        arr.get(index)?
                    } else {
                        return None;
                    }
                }
                _ => return None,
            };
        }

        Some(current)
    }

    /// Creates a summary of JSON structure for error messages
    fn summarize_structure(json: &Value) -> String {
        match json {
            Value::Object(map) => {
                let keys: Vec<&String> = map.keys().collect();
                if keys.len() <= 5 {
                    format!(
                        "{{ {} }}",
                        keys.iter()
                            .map(|k| k.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    format!(
                        "{{ {} ... and {} more fields }}",
                        keys[..5]
                            .iter()
                            .map(|k| k.as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                        keys.len() - 5
                    )
                }
            }
            Value::Array(arr) => format!("[array of {} items]", arr.len()),
            Value::String(_) => "string".to_string(),
            Value::Number(_) => "number".to_string(),
            Value::Bool(_) => "bool".to_string(),
            Value::Null => "null".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_direct_extraction_no_path() {
        let extractor = ResponseExtractor::new(&ResponseConfig::default());
        let response = r#"{"name": "test", "value": 42}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "test".to_string(),
                value: 42
            }
        );
    }

    #[test]
    fn test_nested_extraction_with_path() {
        let config = ResponseConfig {
            data_path: Some("data.config".to_string()),
            error_path: None,
        };
        let extractor = ResponseExtractor::new(&config);
        let response = r#"{"status": "ok", "data": {"config": {"name": "test", "value": 42}}}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "test".to_string(),
                value: 42
            }
        );
    }

    #[test]
    fn test_wrapped_response() {
        let config = ResponseConfig {
            data_path: Some("result".to_string()),
            error_path: None,
        };
        let extractor = ResponseExtractor::new(&config);
        let response = r#"{"result": {"name": "wrapped", "value": 99}}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "wrapped".to_string(),
                value: 99
            }
        );
    }

    #[test]
    fn test_missing_path_error() {
        let config = ResponseConfig {
            data_path: Some("missing.path".to_string()),
            error_path: None,
        };
        let extractor = ResponseExtractor::new(&config);
        let response = r#"{"status": "ok", "data": {"config": {}}}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Data path 'missing.path' not found")
        );
    }

    #[test]
    fn test_error_path_detection() {
        let config = ResponseConfig {
            data_path: Some("data".to_string()),
            error_path: Some("error.message".to_string()),
        };
        let extractor = ResponseExtractor::new(&config);
        let response = r#"{"error": {"message": "Something went wrong"}, "data": null}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("API returned error: Something went wrong")
        );
    }

    #[test]
    fn test_error_path_null_is_ok() {
        let config = ResponseConfig {
            data_path: Some("data".to_string()),
            error_path: Some("error".to_string()),
        };
        let extractor = ResponseExtractor::new(&config);
        let response = r#"{"error": null, "data": {"name": "success", "value": 1}}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "success".to_string(),
                value: 1
            }
        );
    }

    #[test]
    fn test_deep_nested_path() {
        let config = ResponseConfig {
            data_path: Some("response.body.result.data".to_string()),
            error_path: None,
        };
        let extractor = ResponseExtractor::new(&config);
        let response =
            r#"{"response": {"body": {"result": {"data": {"name": "deep", "value": 123}}}}}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "deep".to_string(),
                value: 123
            }
        );
    }

    #[test]
    fn test_array_index_access() {
        let config = ResponseConfig {
            data_path: Some("items.0".to_string()),
            error_path: None,
        };
        let extractor = ResponseExtractor::new(&config);
        let response =
            r#"{"items": [{"name": "first", "value": 10}, {"name": "second", "value": 20}]}"#;

        let result: Result<TestData, _> = extractor.extract(response);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            TestData {
                name: "first".to_string(),
                value: 10
            }
        );
    }
}
