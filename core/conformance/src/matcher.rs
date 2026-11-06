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

//! Compares an expectation from a test file with an actual JSON value.
//!
//! Every field of an expected object must match, and extra actual fields are
//! allowed. Lists match in order and in length. A one-key object whose key
//! starts with `$$` is an operator.

use serde_json::{Number, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

const TYPE_NAMES: [&str; 7] = [
    "null", "boolean", "number", "integer", "string", "array", "object",
];

/// Values that `$$capture` stored for the rest of a test.
#[derive(Debug, Default, Clone)]
pub struct Captures(HashMap<String, Value>);

impl Captures {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    pub fn insert(&mut self, name: &str, value: Value) {
        self.0.insert(name.to_owned(), value);
    }
}

/// Where and why an actual value differs from its expectation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub path: String,
    pub message: String,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            formatter.write_str(&self.message)
        } else {
            write!(formatter, "{}: {}", self.path, self.message)
        }
    }
}

/// Matches `actual` against `expected` and stores the values of `$$capture`.
/// Captures are kept only when the whole match succeeds.
///
/// # Errors
///
/// Returns the first difference found.
pub fn matches(expected: &Value, actual: &Value, captures: &mut Captures) -> Result<(), Mismatch> {
    let mut attempt = captures.clone();
    match_at(expected, actual, &mut attempt, "")?;
    *captures = attempt;
    Ok(())
}

/// Replaces every `{$$ref: name}` in call arguments with its captured value.
///
/// # Errors
///
/// Fails for a name that nothing captured and for any other operator.
pub fn resolve_refs(arguments: &Value, captures: &Captures) -> Result<Value, String> {
    if let Some((operator, operand)) = operator(arguments) {
        return match (operator, operand) {
            ("$$ref", Value::String(name)) => captures
                .get(name)
                .cloned()
                .ok_or_else(|| format!("nothing captured {name:?}")),
            _ => Err(format!("{operator} is not allowed in arguments")),
        };
    }
    match arguments {
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| Ok((key.clone(), resolve_refs(value, captures)?)))
            .collect::<Result<_, String>>()
            .map(Value::Object),
        Value::Array(items) => items
            .iter()
            .map(|item| resolve_refs(item, captures))
            .collect::<Result<_, String>>()
            .map(Value::Array),
        other => Ok(other.clone()),
    }
}

/// Checks that every operator in an expectation is known and well formed, so
/// that a mistake fails the load instead of a run.
///
/// # Errors
///
/// Returns the first invalid operator.
pub fn validate_expectation(expected: &Value) -> Result<(), String> {
    if let Value::Object(fields) = expected
        && fields.len() > 1
        && fields.keys().any(|key| key.starts_with("$$"))
    {
        return Err(format!(
            "an operator object has exactly one key: {}",
            compact(expected)
        ));
    }
    if let Some((operator, operand)) = operator(expected) {
        return validate_operator(operator, operand);
    }
    match expected {
        Value::Object(fields) => fields.values().try_for_each(validate_expectation),
        Value::Array(items) => items.iter().try_for_each(validate_expectation),
        _ => Ok(()),
    }
}

fn validate_operator(operator: &str, operand: &Value) -> Result<(), String> {
    match (operator, operand) {
        ("$$exists", Value::Bool(_))
        | ("$$capture" | "$$ref", Value::String(_))
        | ("$$gte" | "$$lte", Value::Number(_)) => Ok(()),
        ("$$gte" | "$$lte", bound)
            if matches!(self::operator(bound), Some(("$$ref", Value::String(_)))) =>
        {
            Ok(())
        }
        ("$$type", Value::String(name)) => validate_type_name(name),
        ("$$type", Value::Array(names)) if !names.is_empty() => names.iter().try_for_each(|name| {
            name.as_str()
                .ok_or_else(|| "$$type takes type names".to_owned())
                .and_then(validate_type_name)
        }),
        ("$$any", Value::Array(items)) if !items.is_empty() => {
            items.iter().try_for_each(validate_expectation)
        }
        ("$$unordered" | "$$contains", Value::Array(items)) => {
            items.iter().try_for_each(validate_expectation)
        }
        ("$$not" | "$$len", inner) => validate_expectation(inner),
        (
            "$$exists" | "$$capture" | "$$ref" | "$$gte" | "$$lte" | "$$type" | "$$any"
            | "$$unordered" | "$$contains",
            _,
        ) => Err(format!(
            "invalid operand for {operator}: {}",
            compact(operand)
        )),
        _ => Err(format!("unknown operator {operator}")),
    }
}

fn validate_type_name(name: &str) -> Result<(), String> {
    if TYPE_NAMES.contains(&name) {
        Ok(())
    } else {
        Err(format!("unknown type {name:?}"))
    }
}

fn match_at(
    expected: &Value,
    actual: &Value,
    captures: &mut Captures,
    path: &str,
) -> Result<(), Mismatch> {
    if let Some((operator, operand)) = operator(expected) {
        return match_operator(operator, operand, actual, captures, path);
    }
    match expected {
        Value::Object(fields) => {
            let Value::Object(actual_fields) = actual else {
                return Err(mismatch(path, expected, actual));
            };
            for (key, field_expected) in fields {
                let field_path = join(path, key);
                let field_actual = actual_fields.get(key);
                if let Some(("$$exists", Value::Bool(wanted))) = operator(field_expected) {
                    let exists = field_actual.is_some_and(|value| !value.is_null());
                    if exists != *wanted {
                        return Err(Mismatch {
                            path: field_path,
                            message: if *wanted {
                                "is missing".to_owned()
                            } else {
                                format!(
                                    "must be absent, got {}",
                                    compact(field_actual.unwrap_or(&Value::Null))
                                )
                            },
                        });
                    }
                    continue;
                }
                let Some(field_actual) = field_actual else {
                    return Err(Mismatch {
                        path: field_path,
                        message: format!("is missing, expected {}", compact(field_expected)),
                    });
                };
                match_at(field_expected, field_actual, captures, &field_path)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            let Value::Array(actual_items) = actual else {
                return Err(mismatch(path, expected, actual));
            };
            if items.len() != actual_items.len() {
                return Err(Mismatch {
                    path: path.to_owned(),
                    message: format!(
                        "expected {} items, got {}: {}",
                        items.len(),
                        actual_items.len(),
                        compact(actual)
                    ),
                });
            }
            for (index, (item, actual_item)) in items.iter().zip(actual_items).enumerate() {
                match_at(item, actual_item, captures, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        _ if values_equal(expected, actual) => Ok(()),
        _ => Err(mismatch(path, expected, actual)),
    }
}

fn match_operator(
    operator: &str,
    operand: &Value,
    actual: &Value,
    captures: &mut Captures,
    path: &str,
) -> Result<(), Mismatch> {
    let fail = |message: String| {
        Err(Mismatch {
            path: path.to_owned(),
            message,
        })
    };
    match (operator, operand) {
        ("$$exists", Value::Bool(wanted)) => {
            if actual.is_null() == *wanted {
                return fail(format!("$$exists {wanted}, got {}", compact(actual)));
            }
            Ok(())
        }
        ("$$type", names) => {
            let names: Vec<&str> = match names {
                Value::String(name) => vec![name.as_str()],
                Value::Array(names) => names.iter().filter_map(Value::as_str).collect(),
                _ => Vec::new(),
            };
            if names.iter().any(|name| has_type(actual, name)) {
                Ok(())
            } else {
                fail(format!("expected type {names:?}, got {}", compact(actual)))
            }
        }
        ("$$any", Value::Array(alternatives)) => {
            for alternative in alternatives {
                let mut attempt = captures.clone();
                if match_at(alternative, actual, &mut attempt, path).is_ok() {
                    *captures = attempt;
                    return Ok(());
                }
            }
            fail(format!(
                "matches none of {}, got {}",
                compact(operand),
                compact(actual)
            ))
        }
        ("$$not", inner) => {
            let mut attempt = captures.clone();
            if match_at(inner, actual, &mut attempt, path).is_ok() {
                return fail(format!("must not match {}", compact(inner)));
            }
            Ok(())
        }
        ("$$len", inner) => {
            let length = match actual {
                Value::Array(items) => items.len(),
                Value::Object(fields) => fields.len(),
                Value::String(text) => text.chars().count(),
                _ => return fail(format!("$$len needs a list, got {}", compact(actual))),
            };
            match_at(
                inner,
                &Value::from(length),
                captures,
                &format!("{path}.$$len"),
            )
        }
        ("$$gte" | "$$lte", bound) => {
            let bound = match self::operator(bound) {
                Some(("$$ref", Value::String(name))) => match captures.get(name) {
                    Some(Value::Number(captured)) => captured.clone(),
                    Some(other) => {
                        return fail(format!("{name:?} holds {}, not a number", compact(other)));
                    }
                    None => return fail(format!("nothing captured {name:?}")),
                },
                _ => match bound {
                    Value::Number(number) => number.clone(),
                    other => return fail(format!("invalid bound {}", compact(other))),
                },
            };
            let Value::Number(number) = actual else {
                return fail(format!(
                    "{operator} needs a number, got {}",
                    compact(actual)
                ));
            };
            let ordering = compare_numbers(number, &bound);
            let holds = if operator == "$$gte" {
                ordering != Ordering::Less
            } else {
                ordering != Ordering::Greater
            };
            if holds {
                Ok(())
            } else {
                fail(format!("{number} is not {operator} {bound}"))
            }
        }
        ("$$unordered" | "$$contains", Value::Array(items)) => {
            let Value::Array(actual_items) = actual else {
                return fail(format!("{operator} needs a list, got {}", compact(actual)));
            };
            let exact = operator == "$$unordered";
            if exact && items.len() != actual_items.len() {
                return fail(format!(
                    "expected {} items in any order, got {}: {}",
                    items.len(),
                    actual_items.len(),
                    compact(actual)
                ));
            }
            let mut used = vec![false; actual_items.len()];
            if assign(items, actual_items, &mut used, captures, path) {
                Ok(())
            } else {
                fail(format!(
                    "no assignment of {} to {}",
                    compact(operand),
                    compact(actual)
                ))
            }
        }
        ("$$capture", Value::String(name)) => {
            captures.insert(name, actual.clone());
            Ok(())
        }
        ("$$ref", Value::String(name)) => match captures.get(name) {
            Some(stored) if values_equal(stored, actual) => Ok(()),
            Some(stored) => fail(format!(
                "expected the value of {name:?}, {}, got {}",
                compact(stored),
                compact(actual)
            )),
            None => fail(format!("nothing captured {name:?}")),
        },
        _ => fail(format!("invalid operator {operator}")),
    }
}

/// Assigns each expected item to a distinct actual item, with backtracking.
fn assign(
    items: &[Value],
    actual_items: &[Value],
    used: &mut [bool],
    captures: &mut Captures,
    path: &str,
) -> bool {
    let Some((item, rest)) = items.split_first() else {
        return true;
    };
    for index in 0..actual_items.len() {
        if used[index] {
            continue;
        }
        let mut attempt = captures.clone();
        if match_at(item, &actual_items[index], &mut attempt, path).is_err() {
            continue;
        }
        used[index] = true;
        if assign(rest, actual_items, used, &mut attempt, path) {
            *captures = attempt;
            return true;
        }
        used[index] = false;
    }
    false
}

/// The operator of a one-key `$$` object.
fn operator(value: &Value) -> Option<(&str, &Value)> {
    let Value::Object(fields) = value else {
        return None;
    };
    if fields.len() != 1 {
        return None;
    }
    fields
        .iter()
        .next()
        .filter(|(key, _)| key.starts_with("$$"))
        .map(|(key, operand)| (key.as_str(), operand))
}

/// Equality that treats `1` and `1.0` as one number.
#[must_use]
pub fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            compare_numbers(left, right) == Ordering::Equal
        }
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| values_equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .get(key)
                        .is_some_and(|other| values_equal(value, other))
                })
        }
        _ => left == right,
    }
}

fn compare_numbers(left: &Number, right: &Number) -> Ordering {
    match (integer(left), integer(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => {
            let left = left.as_f64().unwrap_or(f64::NAN);
            let right = right.as_f64().unwrap_or(f64::NAN);
            left.partial_cmp(&right).unwrap_or(Ordering::Less)
        }
    }
}

fn integer(number: &Number) -> Option<i128> {
    number
        .as_u64()
        .map(i128::from)
        .or_else(|| number.as_i64().map(i128::from))
}

fn has_type(value: &Value, name: &str) -> bool {
    match name {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "number" => value.is_number(),
        "integer" => value.as_number().and_then(integer).is_some(),
        "string" => value.is_string(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

fn mismatch(path: &str, expected: &Value, actual: &Value) -> Mismatch {
    Mismatch {
        path: path.to_owned(),
        message: format!("expected {}, got {}", compact(expected), compact(actual)),
    }
}

/// JSON text short enough for a failure message.
#[must_use]
pub fn compact(value: &Value) -> String {
    const LIMIT: usize = 300;
    let text = value.to_string();
    if text.len() <= LIMIT {
        return text;
    }
    let mut end = LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(expected: Value, actual: Value) -> Result<Captures, Mismatch> {
        let mut captures = Captures::default();
        matches(&expected, &actual, &mut captures).map(|()| captures)
    }

    #[test]
    fn given_extra_actual_fields_when_matching_should_pass() {
        assert!(check(json!({"name": "a"}), json!({"name": "a", "id": 1})).is_ok());
    }

    #[test]
    fn given_a_missing_field_when_matching_should_name_its_path() {
        let error = check(json!({"topic": {"name": "a"}}), json!({"topic": {}})).unwrap_err();
        assert_eq!(error.path, "topic.name");
    }

    #[test]
    fn given_lists_of_other_lengths_when_matching_should_fail() {
        assert!(check(json!([1, 2]), json!([1, 2, 3])).is_err());
    }

    #[test]
    fn given_integer_and_float_forms_when_matching_should_compare_numbers() {
        assert!(check(json!(10), json!(10.0)).is_ok());
    }

    #[test]
    fn given_exists_false_when_the_field_is_present_should_fail() {
        assert!(check(json!({"key": {"$$exists": false}}), json!({"key": "x"})).is_err());
        assert!(check(json!({"key": {"$$exists": false}}), json!({})).is_ok());
    }

    #[test]
    fn given_a_capture_when_a_later_ref_differs_should_fail() {
        let mut captures = check(json!({"op": {"$$capture": "lost"}}), json!({"op": 7})).unwrap();
        assert!(matches(&json!({"$$ref": "lost"}), &json!(7), &mut captures).is_ok());
        assert!(matches(&json!({"$$ref": "lost"}), &json!(8), &mut captures).is_err());
    }

    #[test]
    fn given_a_failed_match_when_it_captured_should_keep_no_capture() {
        let mut captures = Captures::default();
        let result = matches(
            &json!({"op": {"$$capture": "lost"}, "name": "x"}),
            &json!({"op": 7, "name": "y"}),
            &mut captures,
        );
        assert!(result.is_err());
        assert!(captures.get("lost").is_none());
    }

    #[test]
    fn given_unordered_items_when_matching_should_assign_each_once() {
        assert!(check(json!({"$$unordered": [1, 2]}), json!([2, 1])).is_ok());
        assert!(check(json!({"$$unordered": [1, 1]}), json!([1, 2])).is_err());
    }

    #[test]
    fn given_contains_when_the_list_has_more_items_should_pass() {
        assert!(
            check(
                json!({"$$contains": [{"name": "b"}]}),
                json!([{"name": "a"}, {"name": "b"}])
            )
            .is_ok()
        );
        assert!(
            check(
                json!({"$$contains": [{"name": "c"}]}),
                json!([{"name": "a"}])
            )
            .is_err()
        );
    }

    #[test]
    fn given_a_captured_bound_when_matching_should_compare_with_it() {
        let mut captures = Captures::default();
        captures.insert("own_op", json!(10));
        let below = json!({"$$lte": {"$$ref": "own_op"}});
        assert!(matches(&below, &json!(9), &mut captures).is_ok());
        assert!(matches(&below, &json!(11), &mut captures).is_err());
        assert!(validate_expectation(&below).is_ok());
    }

    #[test]
    fn given_bounds_and_lengths_when_matching_should_compare() {
        assert!(check(json!({"$$gte": 1}), json!(3)).is_ok());
        assert!(check(json!({"$$lte": 1}), json!(3)).is_err());
        assert!(check(json!({"$$len": 2}), json!(["a", "b"])).is_ok());
        assert!(check(json!({"$$len": {"$$gte": 1}}), json!([])).is_err());
    }

    #[test]
    fn given_any_and_not_when_matching_should_combine() {
        assert!(check(json!({"$$any": [1, "x"]}), json!("x")).is_ok());
        assert!(check(json!({"$$not": {"$$ref": "nothing"}}), json!(1)).is_ok());
        assert!(check(json!({"$$not": 1}), json!(1)).is_err());
    }

    #[test]
    fn given_refs_in_arguments_when_resolving_should_substitute() {
        let mut captures = Captures::default();
        captures.insert("lost", json!(5));
        let resolved =
            resolve_refs(&json!({"operation_id": {"$$ref": "lost"}}), &captures).unwrap();
        assert_eq!(resolved, json!({"operation_id": 5}));
        assert!(resolve_refs(&json!({"$$gte": 1}), &captures).is_err());
    }

    #[test]
    fn given_malformed_operators_when_validating_should_fail() {
        assert!(validate_expectation(&json!({"$$type": "text"})).is_err());
        assert!(validate_expectation(&json!({"$$gte": 1, "x": 2})).is_err());
        assert!(validate_expectation(&json!({"$$what": 1})).is_err());
        assert!(validate_expectation(&json!({"a": [{"$$capture": "x"}]})).is_ok());
    }
}
