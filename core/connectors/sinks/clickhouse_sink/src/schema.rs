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

//! ClickHouse column schema model and type string parser used by RowBinary mode.
//!
//!
//! # Type string grammar
//!
//! Grammar followed by ClickHouse type strings (as returned by `SELECT type FROM system.columns`):
//! ```text
//! type  ::= composite | parameterised | primitive
//!
//! composite      ::= "Nullable(" type ")"
//!                  | "Array(" type ")"
//!                  | "Map(" type ", " type ")"
//!                  | "Tuple(" tuple_fields ")"
//!
//! tuple_fields   ::= type ("," type)*            -- unnamed fields
//!                  | ident type ("," ident type)* -- named fields
//!
//! parameterised  ::= "FixedString(" n ")"
//!                  | "DateTime64(" precision ["," tz] ")"
//!                  | "DateTime(" tz ")"
//!                  | "Decimal(" precision "," scale ")"
//!                  | "Decimal32(" scale ")"
//!                  | "Decimal64(" scale ")"
//!                  | "Decimal128(" scale ")"
//!                  | "Enum8(" enum_pairs ")"
//!                  | "Enum16(" enum_pairs ")"
//!
//! enum_pairs     ::= "'" name "' = " int ("," "'" name "' = " int)*
//!
//! primitive      ::= "String" | "Int8" | "Int16" | "Int32" | "Int64"
//!                  | "UInt8" | "UInt16" | "UInt32" | "UInt64"
//!                  | "Float32" | "Float64" | "Bool" | "Boolean"
//!                  | "UUID" | "Date" | "Date32" | "DateTime"
//!                  | "IPv4" | "IPv6"
//! ```
//!
//!
//! ## Example
//!
//! ```text
//! Nullable(Map(String, Array(Tuple(id Int32, ts DateTime64(3, 'UTC')))))
//! ```
//!
//! Parses into the AST:
//!
//! ```text
//! Nullable
//! └── Map
//!     ├── key:   String
//!     └── value: Array
//!                └── Tuple
//!                    ├── [0] Int32
//!                    └── [1] DateTime64(3)
//! ```

use iggy_connector_sdk::Error;
use std::collections::HashMap;
use tracing::error;

/// A single ClickHouse table column.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ch_type: ChType,
    /// True when the column has a DEFAULT or MATERIALIZED expression, meaning
    /// RowBinaryWithDefaults can skip it with a 0x01 prefix byte.
    pub has_default: bool,
}

/// Supported ClickHouse column types.
///
/// Unsupported types (LowCardinality, Variant, the new JSON column type, geo
/// types) cause `parse_type` to return an error, which in turn makes `open()`
/// fail rather than silently producing corrupt data.
#[derive(Debug, Clone)]
pub enum ChType {
    // ── Primitives ──────────────────────────────────────────────────────────
    String,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Uuid,
    /// Days since 1970-01-01 stored as UInt16.
    Date,
    /// Days since 1970-01-01 stored as Int32.
    Date32,
    /// Unix seconds stored as UInt32. Optional timezone suffix is ignored for
    /// serialisation purposes.
    DateTime,
    /// Unix time scaled by 10^precision stored as Int64.
    DateTime64(u8),
    /// Fixed-width byte string padded with zeros.
    FixedString(usize),
    /// Decimal(precision, scale). Serialised as Int32 / Int64 / Int128.
    Decimal(u8, u8),
    /// IPv4 address — UInt32, 4 bytes little-endian.
    IPv4,
    /// IPv6 address — 16 bytes, big-endian.
    IPv6,
    /// Enum8: maps string → i8. Values parsed from the type definition.
    Enum8(HashMap<String, i8>),
    /// Enum16: maps string → i16. Values parsed from the type definition.
    Enum16(HashMap<String, i16>),

    // ── Composites (recursive) ───────────────────────────────────────────────
    Nullable(Box<ChType>),
    Array(Box<ChType>),
    /// Map(key_type, value_type)
    Map(Box<ChType>, Box<ChType>),
    /// Tuple of ordered field types (named or unnamed).
    Tuple(Vec<ChType>),
}

// ─── Public entry point ──────────────────────────────────────────────────────

/// Parse a ClickHouse type string (as returned by `system.columns`) into a
/// `ChType`. Returns `Err(Error::InitError(...))` for unsupported or
/// unrecognised types.
pub fn parse_type(s: &str) -> Result<ChType, Error> {
    parse_type_inner(s.trim())
}

// ─── Recursive descent parser ────────────────────────────────────────────────

fn parse_type_inner(s: &str) -> Result<ChType, Error> {
    // Strip a single pair of outer parentheses if the entire string is wrapped.
    // This shouldn't be needed for well-formed ClickHouse type strings, but is
    // a defensive measure.
    let s = s.trim();
    // e.g. "Nullable(Int32)"
    if let Some(inner) = strip_wrapper(s, "Nullable") {
        return Ok(ChType::Nullable(Box::new(parse_type_inner(inner)?)));
    }
    // e.g. "Array(String)"
    if let Some(inner) = strip_wrapper(s, "Array") {
        return Ok(ChType::Array(Box::new(parse_type_inner(inner)?)));
    }
    // e.g. "Map(String, Int64)"
    if let Some(inner) = strip_wrapper(s, "Map") {
        let (k, v) = split_two_args(inner)?;
        return Ok(ChType::Map(
            Box::new(parse_type_inner(k)?),
            Box::new(parse_type_inner(v)?),
        ));
    }
    // e.g. "Tuple(Int32, String)" or "Tuple(id Int32, name String)"
    if let Some(inner) = strip_wrapper(s, "Tuple") {
        let parts = split_args(inner)?;
        // Named tuples look like `field_name Type, …`. Strip names if present.
        let types: Result<Vec<ChType>, Error> = parts
            .iter()
            .map(|p| {
                let p = p.trim();
                // If the first token is an identifier followed by a space and a
                // valid type keyword, treat it as a named field.
                if let Some(rest) = strip_named_tuple_field(p) {
                    parse_type_inner(rest)
                } else {
                    parse_type_inner(p)
                }
            })
            .collect();
        return Ok(ChType::Tuple(types?));
    }
    // e.g. "Enum8('a' = 1, 'b' = 2)"
    if let Some(inner) = strip_wrapper(s, "Enum8") {
        let map = parse_enum_values_i8(inner)?;
        return Ok(ChType::Enum8(map));
    }
    // e.g. "Enum16('a' = 1, 'b' = 2)"
    if let Some(inner) = strip_wrapper(s, "Enum16") {
        let map = parse_enum_values_i16(inner)?;
        return Ok(ChType::Enum16(map));
    }
    // e.g. "FixedString(16)"
    if let Some(inner) = strip_wrapper(s, "FixedString") {
        let n: usize = inner
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid FixedString length: {inner}")))?;
        return Ok(ChType::FixedString(n));
    }
    // e.g. "DateTime64(3)" or "DateTime64(3, 'UTC')"
    if let Some(inner) = strip_wrapper(s, "DateTime64") {
        // DateTime64(precision) or DateTime64(precision, 'timezone')
        let precision_str = inner.split(',').next().unwrap_or(inner).trim();
        let precision: u8 = precision_str
            .parse()
            .map_err(|_| init_err(format!("Invalid DateTime64 precision: {precision_str}")))?;
        return Ok(ChType::DateTime64(precision));
    }
    // e.g. "DateTime('UTC')"
    if let Some(inner) = strip_wrapper(s, "DateTime") {
        // DateTime('timezone') — timezone is ignored for serialisation.
        let _ = inner;
        return Ok(ChType::DateTime);
    }
    // e.g. "Decimal(18, 4)"
    if let Some(inner) = strip_wrapper(s, "Decimal") {
        let (p_str, s_str) = split_two_args(inner)?;
        let precision: u8 = p_str
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid Decimal precision: {p_str}")))?;
        let scale: u8 = s_str
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid Decimal scale: {s_str}")))?;
        return Ok(ChType::Decimal(precision, scale));
    }
    // e.g. "Decimal32(4)"
    if let Some(inner) = strip_wrapper(s, "Decimal32") {
        let scale: u8 = inner
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid Decimal32 scale: {inner}")))?;
        return Ok(ChType::Decimal(9, scale));
    }
    // e.g. "Decimal64(4)"
    if let Some(inner) = strip_wrapper(s, "Decimal64") {
        let scale: u8 = inner
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid Decimal64 scale: {inner}")))?;
        return Ok(ChType::Decimal(18, scale));
    }
    // e.g. "Decimal128(4)"
    if let Some(inner) = strip_wrapper(s, "Decimal128") {
        let scale: u8 = inner
            .trim()
            .parse()
            .map_err(|_| init_err(format!("Invalid Decimal128 scale: {inner}")))?;
        return Ok(ChType::Decimal(38, scale));
    }

    // Primitive leaf types
    match s {
        "String" => Ok(ChType::String),
        "Int8" => Ok(ChType::Int8),
        "Int16" => Ok(ChType::Int16),
        "Int32" => Ok(ChType::Int32),
        "Int64" => Ok(ChType::Int64),
        "UInt8" => Ok(ChType::UInt8),
        "UInt16" => Ok(ChType::UInt16),
        "UInt32" => Ok(ChType::UInt32),
        "UInt64" => Ok(ChType::UInt64),
        "Float32" => Ok(ChType::Float32),
        "Float64" => Ok(ChType::Float64),
        "Bool" | "Boolean" => Ok(ChType::Boolean),
        "UUID" => Ok(ChType::Uuid),
        "Date" => Ok(ChType::Date),
        "Date32" => Ok(ChType::Date32),
        "DateTime" => Ok(ChType::DateTime),
        "IPv4" => Ok(ChType::IPv4),
        "IPv6" => Ok(ChType::IPv6),

        // ── Explicitly unsupported ─────────────────────────────────────────
        s if s.starts_with("LowCardinality") => {
            error!(
                "Unsupported ClickHouse type: {s}. LowCardinality uses dictionary encoding that is not supported in RowBinary mode."
            );
            Err(init_err(format!("Unsupported type: {s}")))
        }
        s if s.starts_with("Variant") => {
            error!("Unsupported ClickHouse type: {s}. Variant is not supported in RowBinary mode.");
            Err(init_err(format!("Unsupported type: {s}")))
        }
        "JSON" => {
            error!(
                "Unsupported ClickHouse type: JSON. The native JSON column type is not supported in RowBinary mode."
            );
            Err(init_err("Unsupported type: JSON".into()))
        }
        s if matches!(
            s,
            "Point" | "Ring" | "Polygon" | "MultiPolygon" | "LineString" | "MultiLineString"
        ) =>
        {
            error!(
                "Unsupported ClickHouse type: {s}. Geo types are not supported in RowBinary mode."
            );
            Err(init_err(format!("Unsupported type: {s}")))
        }
        other => {
            error!("Unrecognised ClickHouse type: {other}");
            Err(init_err(format!("Unrecognised type: {other}")))
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn init_err(msg: String) -> Error {
    Error::InitError(msg)
}

/// If `s` starts with `prefix(` and ends with `)`, return the inner content.
fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let with_paren = format!("{prefix}(");
    if s.starts_with(with_paren.as_str()) && s.ends_with(')') {
        Some(&s[with_paren.len()..s.len() - 1])
    } else {
        None
    }
}

/// Split a comma-separated argument list, respecting nested parentheses.
/// e.g. `"String, Map(String, Int32)"` → `["String", "Map(String, Int32)"]`
fn split_args(s: &str) -> Result<Vec<&str>, Error> {
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Err(init_err(format!("Unmatched ')' in type string: {s}")));
                }
                depth -= 1;
            }
            ',' if depth == 0 => {
                args.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        args.push(last);
    }
    Ok(args)
}

/// Split exactly two comma-separated arguments (e.g. for Map or Decimal).
fn split_two_args(s: &str) -> Result<(&str, &str), Error> {
    let parts = split_args(s)?;
    if parts.len() != 2 {
        return Err(init_err(format!(
            "Expected exactly 2 arguments, got {}: {s}",
            parts.len()
        )));
    }
    Ok((parts[0], parts[1]))
}

/// If `s` starts with an identifier followed by a space and the rest looks
/// like a type, return the type portion. This handles named Tuple fields like
/// `id Int32`.
fn strip_named_tuple_field(s: &str) -> Option<&str> {
    let mut chars = s.char_indices().peekable();
    // Consume identifier characters (letters, digits, underscore)
    while let Some((_, ch)) = chars.peek() {
        if ch.is_alphanumeric() || *ch == '_' {
            chars.next();
        } else {
            break;
        }
    }
    // Expect a single space after the identifier
    if let Some((idx, ' ')) = chars.next() {
        let rest = s[idx + 1..].trim();
        // Sanity: the rest should start with an uppercase letter (type name)
        if rest.starts_with(|c: char| c.is_uppercase()) {
            return Some(rest);
        }
    }
    None
}

/// Parse `'name' = value, ...` pairs for Enum8.
fn parse_enum_values_i8(s: &str) -> Result<HashMap<String, i8>, Error> {
    let mut map = HashMap::new();
    for pair in split_args(s)? {
        let (name, val) = parse_enum_pair(pair)?;
        let v: i8 = val
            .parse()
            .map_err(|_| init_err(format!("Invalid Enum8 value: {val}")))?;
        map.insert(name, v);
    }
    Ok(map)
}

/// Parse `'name' = value, ...` pairs for Enum16.
fn parse_enum_values_i16(s: &str) -> Result<HashMap<String, i16>, Error> {
    let mut map = HashMap::new();
    for pair in split_args(s)? {
        let (name, val) = parse_enum_pair(pair)?;
        let v: i16 = val
            .parse()
            .map_err(|_| init_err(format!("Invalid Enum16 value: {val}")))?;
        map.insert(name, v);
    }
    Ok(map)
}

/// Parse a single `'name' = value` pair, returning (name, value_str).
fn parse_enum_pair(pair: &str) -> Result<(String, &str), Error> {
    let pair = pair.trim();
    // Format: 'name' = value
    let eq_pos = pair
        .rfind('=')
        .ok_or_else(|| init_err(format!("Invalid enum pair (no '='): {pair}")))?;
    let name_part = pair[..eq_pos].trim();
    let val_part = pair[eq_pos + 1..].trim();
    // Strip surrounding single quotes from name
    let name = if name_part.starts_with('\'') && name_part.ends_with('\'') {
        name_part[1..name_part.len() - 1].to_string()
    } else {
        return Err(init_err(format!("Enum name not quoted: {name_part}")));
    };
    Ok((name, val_part))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_primitives() {
        assert!(matches!(parse_type("String").unwrap(), ChType::String));
        assert!(matches!(parse_type("Int8").unwrap(), ChType::Int8));
        assert!(matches!(parse_type("Int16").unwrap(), ChType::Int16));
        assert!(matches!(parse_type("Int32").unwrap(), ChType::Int32));
        assert!(matches!(parse_type("Int64").unwrap(), ChType::Int64));
        assert!(matches!(parse_type("UInt8").unwrap(), ChType::UInt8));
        assert!(matches!(parse_type("UInt16").unwrap(), ChType::UInt16));
        assert!(matches!(parse_type("UInt32").unwrap(), ChType::UInt32));
        assert!(matches!(parse_type("UInt64").unwrap(), ChType::UInt64));
        assert!(matches!(parse_type("Float32").unwrap(), ChType::Float32));
        assert!(matches!(parse_type("Float64").unwrap(), ChType::Float64));
        assert!(matches!(parse_type("Boolean").unwrap(), ChType::Boolean));
        assert!(matches!(parse_type("Bool").unwrap(), ChType::Boolean));
        assert!(matches!(parse_type("UUID").unwrap(), ChType::Uuid));
        assert!(matches!(parse_type("Date").unwrap(), ChType::Date));
        assert!(matches!(parse_type("Date32").unwrap(), ChType::Date32));
        assert!(matches!(parse_type("DateTime").unwrap(), ChType::DateTime));
        assert!(matches!(parse_type("IPv4").unwrap(), ChType::IPv4));
        assert!(matches!(parse_type("IPv6").unwrap(), ChType::IPv6));
    }

    #[test]
    fn parses_nullable_string() {
        let t = parse_type("Nullable(String)").unwrap();
        assert!(matches!(t, ChType::Nullable(inner) if matches!(*inner, ChType::String)));
    }

    #[test]
    fn parses_nullable_int32() {
        let t = parse_type("Nullable(Int32)").unwrap();
        assert!(matches!(t, ChType::Nullable(inner) if matches!(*inner, ChType::Int32)));
    }

    #[test]
    fn parses_fixed_string() {
        let t = parse_type("FixedString(16)").unwrap();
        assert!(matches!(t, ChType::FixedString(16)));
    }

    #[test]
    fn parses_datetime64_precision() {
        let t = parse_type("DateTime64(3)").unwrap();
        assert!(matches!(t, ChType::DateTime64(3)));
    }

    #[test]
    fn parses_datetime64_with_timezone() {
        let t = parse_type("DateTime64(6, 'UTC')").unwrap();
        assert!(matches!(t, ChType::DateTime64(6)));
    }

    #[test]
    fn parses_datetime_with_timezone() {
        let t = parse_type("DateTime('Europe/London')").unwrap();
        assert!(matches!(t, ChType::DateTime));
    }

    #[test]
    fn parses_decimal() {
        let t = parse_type("Decimal(18, 4)").unwrap();
        assert!(matches!(t, ChType::Decimal(18, 4)));
    }

    #[test]
    fn parses_decimal32() {
        let t = parse_type("Decimal32(4)").unwrap();
        assert!(matches!(t, ChType::Decimal(9, 4)));
    }

    #[test]
    fn parses_decimal64() {
        let t = parse_type("Decimal64(6)").unwrap();
        assert!(matches!(t, ChType::Decimal(18, 6)));
    }

    #[test]
    fn parses_array_of_string() {
        let t = parse_type("Array(String)").unwrap();
        assert!(matches!(t, ChType::Array(inner) if matches!(*inner, ChType::String)));
    }

    #[test]
    fn parses_array_of_nullable_int32() {
        let t = parse_type("Array(Nullable(Int32))").unwrap();
        assert!(matches!(
            t,
            ChType::Array(inner)
            if matches!(*inner, ChType::Nullable(ref i) if matches!(**i, ChType::Int32))
        ));
    }

    #[test]
    fn parses_map_string_int32() {
        let t = parse_type("Map(String, Int32)").unwrap();
        assert!(matches!(t, ChType::Map(k, v)
            if matches!(*k, ChType::String) && matches!(*v, ChType::Int32)));
    }

    #[test]
    fn parses_map_with_complex_value() {
        let t = parse_type("Map(String, Array(Int64))").unwrap();
        assert!(matches!(t, ChType::Map(k, v)
            if matches!(*k, ChType::String) && matches!(*v, ChType::Array(_))));
    }

    #[test]
    fn parses_tuple_unnamed() {
        let t = parse_type("Tuple(String, Int32)").unwrap();
        assert!(matches!(t, ChType::Tuple(fields) if fields.len() == 2));
    }

    #[test]
    fn parses_tuple_named() {
        let t = parse_type("Tuple(id Int32, name String)").unwrap();
        assert!(matches!(t, ChType::Tuple(fields) if fields.len() == 2));
    }

    #[test]
    fn parses_enum8() {
        let t = parse_type("Enum8('active' = 1, 'inactive' = 2)").unwrap();
        if let ChType::Enum8(map) = t {
            assert_eq!(map["active"], 1i8);
            assert_eq!(map["inactive"], 2i8);
        } else {
            panic!("expected Enum8");
        }
    }

    #[test]
    fn parses_enum16() {
        let t = parse_type("Enum16('a' = 100, 'b' = 200)").unwrap();
        if let ChType::Enum16(map) = t {
            assert_eq!(map["a"], 100i16);
            assert_eq!(map["b"], 200i16);
        } else {
            panic!("expected Enum16");
        }
    }

    #[test]
    fn rejects_low_cardinality() {
        assert!(parse_type("LowCardinality(String)").is_err());
    }

    #[test]
    fn rejects_variant() {
        assert!(parse_type("Variant(String, Int32)").is_err());
    }

    #[test]
    fn rejects_json_column_type() {
        assert!(parse_type("JSON").is_err());
    }

    #[test]
    fn rejects_geo_types() {
        assert!(parse_type("Point").is_err());
        assert!(parse_type("Polygon").is_err());
    }

    #[test]
    fn rejects_unknown_type() {
        assert!(parse_type("WeirdType").is_err());
    }

    // Parse deeply-nested expressions ──────────────────────────────────

    /// Validates the exact example shown in the module-level grammar comment.
    #[test]
    fn parses_doc_comment_example() {
        // Nullable(Map(String, Array(Tuple(id Int32, ts DateTime64(3, 'UTC')))))
        let t =
            parse_type("Nullable(Map(String, Array(Tuple(id Int32, ts DateTime64(3, 'UTC')))))")
                .unwrap();
        let ChType::Nullable(inner) = t else {
            panic!("expected Nullable")
        };
        let ChType::Map(k, v) = *inner else {
            panic!("expected Map")
        };
        assert!(matches!(*k, ChType::String));
        let ChType::Array(inner) = *v else {
            panic!("expected Array")
        };
        let ChType::Tuple(fields) = *inner else {
            panic!("expected Tuple")
        };
        assert_eq!(fields.len(), 2);
        assert!(matches!(fields[0], ChType::Int32));
        assert!(matches!(fields[1], ChType::DateTime64(3)));
    }

    /// Nullable wrapping a composite type (not just a primitive).
    #[test]
    fn parses_nullable_wrapping_composite() {
        let t = parse_type("Nullable(Array(Int32))").unwrap();
        let ChType::Nullable(inner) = t else {
            panic!("expected Nullable")
        };
        assert!(matches!(*inner, ChType::Array(_)));
    }

    /// Named tuple whose fields are themselves composite types.
    #[test]
    fn parses_named_tuple_with_composite_fields() {
        // strip_named_tuple_field must correctly skip names whose type contains parens
        let t = parse_type("Tuple(tags Array(String), meta Map(String, Int32))").unwrap();
        let ChType::Tuple(fields) = t else {
            panic!("expected Tuple")
        };
        assert_eq!(fields.len(), 2);
        assert!(matches!(fields[0], ChType::Array(_)));
        assert!(matches!(fields[1], ChType::Map(_, _)));
    }

    /// Map whose value type is itself a Map — exercises that split_two_args
    /// does not split on the comma inside the nested Map's argument list.
    #[test]
    fn parses_map_of_maps() {
        let t = parse_type("Map(String, Map(String, Int32))").unwrap();
        assert!(matches!(t, ChType::Map(k, v)
            if matches!(*k, ChType::String) && matches!(*v, ChType::Map(_, _))));
    }

    /// Array wrapping an unnamed tuple with more than two elements.
    #[test]
    fn parses_array_of_three_element_tuple() {
        let t = parse_type("Array(Tuple(Float32, Float32, Float32))").unwrap();
        let ChType::Array(inner) = t else {
            panic!("expected Array")
        };
        let ChType::Tuple(fields) = *inner else {
            panic!("expected Tuple")
        };
        assert_eq!(fields.len(), 3);
        assert!(fields.iter().all(|f| matches!(f, ChType::Float32)));
    }
}
