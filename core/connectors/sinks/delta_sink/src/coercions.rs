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

use deltalake::kernel::Schema as DeltaSchema;
use deltalake::kernel::{DataType, PrimitiveType};

use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
enum CoercionNode {
    Coercion(Coercion),
    Tree(CoercionTree),
    ArrayTree(CoercionTree),
    ArrayPrimitive(Coercion),
}

#[derive(Debug, Clone, PartialEq)]
enum Coercion {
    ToString,
    ToTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoercionTree {
    root: HashMap<String, CoercionNode>,
}

/// Returns a [`CoercionTree`] so the schema can be walked efficiently level by level when performing conversions.
pub(crate) fn create_coercion_tree(schema: &DeltaSchema) -> CoercionTree {
    let mut root = HashMap::new();

    for field in schema.fields() {
        if let Some(node) = build_coercion_node(field.data_type()) {
            root.insert(field.name().to_string(), node);
        }
    }

    CoercionTree { root }
}

fn build_coercion_node(data_type: &DataType) -> Option<CoercionNode> {
    match data_type {
        DataType::Primitive(primitive) => match primitive {
            PrimitiveType::String => Some(CoercionNode::Coercion(Coercion::ToString)),
            PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => {
                Some(CoercionNode::Coercion(Coercion::ToTimestamp))
            }
            _ => None,
        },
        DataType::Struct(st) => {
            let nested_context = create_coercion_tree(st);
            if !nested_context.root.is_empty() {
                Some(CoercionNode::Tree(nested_context))
            } else {
                None
            }
        }
        DataType::Array(array) => {
            build_coercion_node(array.element_type()).and_then(|node| match node {
                CoercionNode::Coercion(c) => Some(CoercionNode::ArrayPrimitive(c)),
                CoercionNode::Tree(t) => Some(CoercionNode::ArrayTree(t)),
                _ => None,
            })
        }
        // TODO: Map and Variant column types are not coerced. Values inside these columns
        // pass through to the Delta writer unchanged. Add support if these types are needed.
        _ => None,
    }
}

/// Applies all data coercions specified by the [`CoercionTree`] to the [`Value`].
///
/// Returns an error if a string value in a timestamp field cannot be parsed as a timestamp.
/// The caller should reject the record (or batch) on error rather than forwarding an
/// unparsable string to Arrow, which would produce an opaque `ArrowError::JsonError`.
pub(crate) fn coerce(value: &mut Value, coercion_tree: &CoercionTree) -> Result<(), String> {
    if let Some(context) = value.as_object_mut() {
        for (field_name, coercion) in coercion_tree.root.iter() {
            if let Some(value) = context.get_mut(field_name) {
                apply_coercion(value, coercion, field_name)?;
            }
        }
    }
    Ok(())
}

fn apply_coercion(value: &mut Value, node: &CoercionNode, path: &str) -> Result<(), String> {
    match node {
        CoercionNode::Coercion(Coercion::ToString) => {
            if !value.is_null() && !value.is_string() {
                *value = Value::String(value.to_string());
            }
        }
        CoercionNode::Coercion(Coercion::ToTimestamp) => {
            if let Some(as_str) = value.as_str() {
                *value = string_to_timestamp(as_str, path)?;
            } else if value.is_i64() || value.is_u64() {
                // Already epoch microseconds — valid timestamp representation, pass through.
            }
        }
        CoercionNode::Tree(tree) => {
            for (name, node) in tree.root.iter() {
                let child_path = format!("{path}.{name}");
                let fields = value.as_object_mut();
                if let Some(fields) = fields
                    && let Some(value) = fields.get_mut(name)
                {
                    apply_coercion(value, node, &child_path)?;
                }
            }
        }
        CoercionNode::ArrayPrimitive(coercion) => {
            if let Some(values) = value.as_array_mut() {
                let node = CoercionNode::Coercion(coercion.clone());
                for (i, value) in values.iter_mut().enumerate() {
                    apply_coercion(value, &node, &format!("{path}[{i}]"))?;
                }
            }
        }
        CoercionNode::ArrayTree(tree) => {
            if let Some(values) = value.as_array_mut() {
                for (i, value) in values.iter_mut().enumerate() {
                    for (name, node) in tree.root.iter() {
                        let child_path = format!("{path}[{i}].{name}");
                        if let Some(fields) = value.as_object_mut()
                            && let Some(field_value) = fields.get_mut(name)
                        {
                            apply_coercion(field_value, node, &child_path)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn string_to_timestamp(string: &str, path: &str) -> Result<Value, String> {
    // Try strict RFC 3339 / ISO 8601 with T separator first.
    if let Ok(dt) = DateTime::<Utc>::from_str(string) {
        return Ok(Value::Number(dt.timestamp_micros().into()));
    }

    // Arrow's parser also accepts ' ' as a date/time separator (e.g. "2021-11-11 22:11:58").
    // Handle that here so the value is normalised to epoch microseconds rather than left as a
    // raw string that Arrow would silently accept without going through this coercion layer.
    let ndt = NaiveDateTime::parse_from_str(string, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(string, "%Y-%m-%d %H:%M:%S"));
    if let Ok(ndt) = ndt {
        return Ok(Value::Number(ndt.and_utc().timestamp_micros().into()));
    }

    Err(format!("field \"{path}\": cannot parse \"{string}\" as a timestamp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::LazyLock;

    #[test]
    fn test_string_to_timestamp_valid() {
        assert!(string_to_timestamp("2010-01-01T22:11:58Z", "field").is_ok());
        // exceeds nanos size
        assert!(string_to_timestamp("2400-01-01T22:11:58Z", "field").is_ok());
        // Arrow also accepts ' ' as date/time separator
        assert!(string_to_timestamp("2021-11-11 22:11:58", "field").is_ok());
        assert!(string_to_timestamp("2021-11-11 22:11:58.123456", "field").is_ok());
    }

    #[test]
    fn test_string_to_timestamp_invalid() {
        assert!(string_to_timestamp("not a date", "field").is_err());
        assert!(string_to_timestamp("", "field").is_err());
        assert!(string_to_timestamp("2021-13-01T00:00:00Z", "field").is_err());
        assert!(string_to_timestamp("1636668718000000", "field").is_err());
    }

    static SCHEMA: LazyLock<Value> = LazyLock::new(|| {
        json!({
                "type": "struct",
                "fields": [
                    { "name": "level1_string", "type": "string", "nullable": true, "metadata": {} },
                    { "name": "level1_integer", "type": "integer", "nullable": true, "metadata": {} },
                    { "name": "level1_timestamp", "type": "timestamp", "nullable": true, "metadata": {} },
                    {
                        "name": "level2",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "level2_string",
                                    "type": "string",
                                    "nullable": true, "metadata": {}
                                },
                                {
                                    "name": "level2_int",
                                    "type": "integer",
                                    "nullable": true, "metadata": {}
                                },
                                {
                                    "name": "level2_timestamp",
                                    "type": "timestamp",
                                    "nullable": true, "metadata": {}
                                }]
                        },
                        "nullable": true, "metadata": {}
                    },
                    {
                        "name": "array_timestamp",
                        "type": {
                            "type": "array",
                            "containsNull": true,
                            "elementType": "timestamp",
                        },
                        "nullable": true, "metadata": {},
                    },
                    {
                        "name": "array_string",
                        "type": {
                            "type": "array",
                            "containsNull": true,
                            "elementType": "string",
                        },
                        "nullable": true, "metadata": {},
                    },
                    {
                        "name": "array_int",
                        "type": {
                            "type": "array",
                            "containsNull": true,
                            "elementType": "integer",
                        },
                        "nullable": true, "metadata": {},
                    },
                    {
                        "name": "array_struct",
                        "type": {
                            "type": "array",
                            "containsNull": true,
                            "elementType": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "level2_string",
                                        "type": "string",
                                        "nullable": true, "metadata": {}
                                    },
                                    {
                                        "name": "level2_int",
                                        "type": "integer",
                                        "nullable": true, "metadata": {}
                                    },
                                    {
                                        "name": "level2_timestamp",
                                        "type": "timestamp",
                                        "nullable": true, "metadata": {}
                                    },
                                ],
                            },
                        },
                        "nullable": true, "metadata": {},
                    }
                ]
        })
    });

    #[test]
    fn test_coercion_tree() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();

        let tree = create_coercion_tree(&delta_schema);

        let mut top_level_keys: Vec<&String> = tree.root.keys().collect();
        top_level_keys.sort();

        let level2 = tree.root.get("level2");
        let level2_root = match level2 {
            Some(CoercionNode::Tree(tree)) => tree.root.clone(),
            _ => unreachable!(""),
        };
        let mut level2_keys: Vec<&String> = level2_root.keys().collect();
        level2_keys.sort();

        let array_struct = tree.root.get("array_struct");
        let array_struct_root = match array_struct {
            Some(CoercionNode::ArrayTree(tree)) => tree.root.clone(),
            _ => unreachable!(""),
        };

        assert_eq!(
            vec![
                "array_string",
                "array_struct",
                "array_timestamp",
                "level1_string",
                "level1_timestamp",
                "level2"
            ],
            top_level_keys
        );

        assert_eq!(vec!["level2_string", "level2_timestamp"], level2_keys);

        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            tree.root.get("level1_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            tree.root.get("level1_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            level2_root.get("level2_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            level2_root.get("level2_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::ArrayPrimitive(Coercion::ToString),
            tree.root.get("array_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::ArrayPrimitive(Coercion::ToTimestamp),
            tree.root.get("array_timestamp").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToString),
            array_struct_root.get("level2_string").unwrap().to_owned()
        );
        assert_eq!(
            CoercionNode::Coercion(Coercion::ToTimestamp),
            array_struct_root
                .get("level2_timestamp")
                .unwrap()
                .to_owned()
        );
    }

    #[test]
    fn test_coercions() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();

        let coercion_tree = create_coercion_tree(&delta_schema);

        let mut messages = [
            json!({
                "level1_string": "a",
                "level1_integer": 0,
                // Timestamp passed in as an i64. We won't coerce it, but it will work anyway.
                "level1_timestamp": 1636668718000000i64,
                "level2": {
                    "level2_string": { "x": "x", "y": "y" },
                    "level2_timestamp": "2021-11-11T22:11:58Z"
                },
                "array_timestamp": ["2021-11-17T01:02:03Z", "2021-11-17T02:03:04Z"],
                "array_string": ["a", "b", {"a": 1}],
                "array_int": [1, 2, 3],
                "array_struct": [
                    {
                        "level2_string": r#"{"a":1}"#,
                        "level2_int": 1,
                        "level2_timestamp": "2021-11-17T00:00:01Z"
                    },
                    {
                        "level2_string": { "a": 2 },
                        "level2_int": 2,
                        "level2_timestamp": 1637107202000000i64
                    },
                ]
            }),
            json!({
                "level1_string": { "a": "a", "b": "b"},
                "level1_integer": 42,
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58Z"
            }),
            json!({
                "level1_integer": 99,
            }),
            json!({
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58+00:00",
            }),
            json!({
                // RFC 3339 but not ISO 8601. We WILL coerce it.
                "level1_timestamp": "2021-11-11T22:11:58-00:00",
            }),
            json!({
                // Space-separated (SQL/Java style) — Arrow accepts this format, so we coerce it.
                "level1_timestamp": "2021-11-11 22:11:58",
            }),
        ];

        for message in messages.iter_mut() {
            coerce(message, &coercion_tree).unwrap();
        }

        let expected = [
            json!({
                "level1_string": "a",
                "level1_integer": 0,
                // Timestamp passed in as an i64. We won't coerce it, but it will work anyway.
                "level1_timestamp": 1636668718000000i64,
                "level2": {
                    "level2_string": r#"{"x":"x","y":"y"}"#,
                    "level2_timestamp": 1636668718000000i64
                },
                "array_timestamp": [1637110923000000i64, 1637114584000000i64],
                "array_string": ["a", "b", r#"{"a":1}"#],
                "array_int": [1, 2, 3],
                "array_struct": [
                    {
                        "level2_string": "{\"a\":1}",
                        "level2_int": 1,
                        "level2_timestamp": 1637107201000000i64
                    },
                    {
                        "level2_string": r#"{"a":2}"#,
                        "level2_int": 2,
                        "level2_timestamp": 1637107202000000i64
                    },
                ]
            }),
            json!({
                "level1_string": r#"{"a":"a","b":"b"}"#,
                "level1_integer": 42,
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                "level1_integer": 99,
            }),
            json!({
                // Complies with ISO 8601 and RFC 3339. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                // RFC 3339 but not ISO 8601. We WILL coerce it.
                "level1_timestamp": 1636668718000000i64
            }),
            json!({
                // Space-separated (SQL/Java style) — coerced to epoch microseconds.
                "level1_timestamp": 1636668718000000i64,
            }),
        ];

        for i in 0..messages.len() {
            assert_eq!(messages[i], expected[i]);
        }
    }

    #[test]
    fn test_timestamp_coercion_invalid_strings() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let coercion_tree = create_coercion_tree(&delta_schema);

        // These strings are not parseable as timestamps by any format Arrow accepts.
        // coerce() must return Err so callers can reject the batch before Arrow sees the value.
        for bad in &[
            "This definitely is not a timestamp",
            "20211111T22115800Z", // compact ISO 8601, rejected by both chrono and Arrow
            "1636668718000000",   // numeric string, not a timestamp format
        ] {
            let mut value = json!({ "level1_timestamp": bad });
            assert!(
                coerce(&mut value, &coercion_tree).is_err(),
                "expected error for: {bad}"
            );
        }
    }

    #[test]
    fn test_empty_schema() {
        let schema_json = json!({
            "type": "struct",
            "fields": []
        });
        let delta_schema: DeltaSchema = serde_json::from_value(schema_json).unwrap();
        let tree = create_coercion_tree(&delta_schema);
        assert!(tree.root.is_empty());
    }

    #[test]
    fn test_coerce_empty_object() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);
        let mut value = json!({});
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, json!({}));
    }

    #[test]
    fn test_coerce_null_values() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);
        let mut value = json!({
            "level1_string": null,
            "level1_timestamp": null,
            "level2": null,
            "array_timestamp": null,
            "array_struct": null,
        });
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        // Null values should pass through unchanged — null on a string field stays null.
        assert_eq!(value, expected);
    }

    #[test]
    fn test_coerce_nested_struct_with_missing_fields() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);
        let mut value = json!({
            "level2": {
                "level2_int": 42
                // level2_string and level2_timestamp are missing
            }
        });
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, expected);
    }

    #[test]
    fn test_coerce_empty_arrays() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);
        let mut value = json!({
            "array_timestamp": [],
            "array_string": [],
            "array_struct": [],
        });
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, expected);
    }

    #[test]
    fn test_coerce_non_object_top_level() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        // Array at top level
        let mut value = json!([1, 2, 3]);
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, expected);

        // Primitive at top level
        let mut value = json!("hello");
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, expected);

        // Null at top level
        let mut value = json!(null);
        let expected = value.clone();
        coerce(&mut value, &tree).unwrap();
        assert_eq!(value, expected);
    }

    #[test]
    fn test_tostring_coercion() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        // Non-string types get converted; null passes through unchanged.
        let mut value = json!({
            "level1_string": null,
            "level2": {
                "level2_string": null,
            },
            "array_string": ["a", null, 42, true, 3.15, [1, 2, 3], {"x": 1}],
        });
        coerce(&mut value, &tree).unwrap();

        assert_eq!(value["level1_string"], json!(null));
        assert_eq!(value["level2"]["level2_string"], json!(null));
        assert_eq!(value["array_string"][0], json!("a"));
        assert_eq!(value["array_string"][1], json!(null));
        assert_eq!(value["array_string"][2], json!("42"));
        assert_eq!(value["array_string"][3], json!("true"));
        assert_eq!(value["array_string"][4], json!("3.15"));
        assert_eq!(value["array_string"][5], json!("[1,2,3]"));
        assert_eq!(value["array_string"][6], json!(r#"{"x":1}"#));
    }

    #[test]
    fn test_timestamp_coercion_i64_passthrough() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        let epoch_micros = 1636668718000000i64;
        let mut value = json!({ "level1_timestamp": epoch_micros });
        coerce(&mut value, &tree).unwrap();

        assert_eq!(value["level1_timestamp"], json!(epoch_micros));
    }

    #[test]
    fn test_error_path_top_level() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        let mut value = json!({ "level1_timestamp": "not-a-date" });
        let err = coerce(&mut value, &tree).unwrap_err();
        assert!(
            err.contains("\"level1_timestamp\""),
            "expected field name in error, got: {err}"
        );
    }

    #[test]
    fn test_error_path_nested_struct() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        let mut value = json!({ "level2": { "level2_timestamp": "not-a-date" } });
        let err = coerce(&mut value, &tree).unwrap_err();
        assert!(
            err.contains("\"level2.level2_timestamp\""),
            "expected dotted path in error, got: {err}"
        );
    }

    #[test]
    fn test_error_path_array_primitive() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        let mut value = json!({ "array_timestamp": ["2021-11-11T22:11:58Z", "not-a-date"] });
        let err = coerce(&mut value, &tree).unwrap_err();
        assert!(
            err.contains("\"array_timestamp[1]\""),
            "expected indexed path in error, got: {err}"
        );
    }

    #[test]
    fn test_error_path_array_struct() {
        let delta_schema: DeltaSchema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let tree = create_coercion_tree(&delta_schema);

        let mut value = json!({
            "array_struct": [
                { "level2_timestamp": "2021-11-11T22:11:58Z" },
                { "level2_timestamp": "not-a-date" },
            ]
        });
        let err = coerce(&mut value, &tree).unwrap_err();
        assert!(
            err.contains("\"array_struct[1].level2_timestamp\""),
            "expected indexed dotted path in error, got: {err}"
        );
    }
}
