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

//! RowBinary / RowBinaryWithDefaults byte serialization.
//!
//! Follows the ClickHouse binary format specification:
//! <https://clickhouse.com/docs/en/interfaces/formats#rowbinary>
//!
//! Key layout rules:
//! - All integers are **little-endian**.
//! - Strings are prefixed with an **unsigned LEB128 varint** length.
//! - `Nullable(T)`: 1-byte null marker (`0x01` = null, `0x00` = not null)
//!   followed by T bytes when not null.
//! - `RowBinaryWithDefaults`: each top-level column is preceded by a 1-byte
//!   flag (`0x01` = use server DEFAULT, `0x00` = value follows).

use crate::schema::{ChType, Column};
use iggy_connector_sdk::Error;
use simd_json::OwnedValue;
use simd_json::prelude::{TypedScalarValue, ValueAsArray, ValueAsObject};
use tracing::error;

// ─── Public API ──────────────────────────────────────────────────────────────

/// Serialise one message (a JSON object) as a RowBinaryWithDefaults row.
///
/// Columns are written in schema order. When a column is absent from the JSON
/// object and `has_default` is true the DEFAULT prefix byte (`0x01`) is
/// written and the column value is skipped. When a column is absent but has no
/// default and is not Nullable this is an error.
pub fn serialize_row(
    value: &OwnedValue,
    columns: &[Column],
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    let obj = value.as_object().ok_or_else(|| {
        error!("RowBinary: message payload is not a JSON object");
        Error::InvalidRecord
    })?;

    for col in columns {
        let field_value = obj.get(col.name.as_str());

        // RowBinaryWithDefaults prefix byte
        let is_null_or_absent = field_value.map(|v| v.is_null()).unwrap_or(true);
        if is_null_or_absent && col.has_default {
            buf.push(0x01); // use DEFAULT
            continue;
        }
        buf.push(0x00); // value follows

        match field_value {
            Some(v) if !v.is_null() => serialize_value(v, &col.ch_type, buf)?,
            _ => {
                // Field is absent or null — write zero value if Nullable,
                // otherwise error.
                write_zero_or_null(&col.ch_type, buf, &col.name)?;
            }
        }
    }
    Ok(())
}

// ─── Core recursive serializer ────────────────────────────────────────────────

pub(crate) fn serialize_value(
    value: &OwnedValue,
    ch_type: &ChType,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    match ch_type {
        // ── Nullable ─────────────────────────────────────────────────────────
        ChType::Nullable(inner) => {
            if value.is_null() {
                buf.push(0x01); // null
            } else {
                buf.push(0x00); // not null
                serialize_value(value, inner, buf)?;
            }
        }

        // ── String ───────────────────────────────────────────────────────────
        ChType::String => {
            let s = coerce_to_string(value)?;
            write_string(s.as_bytes(), buf);
        }
        ChType::FixedString(n) => {
            let s = coerce_to_string(value)?;
            let bytes = s.as_bytes();
            // Pad or truncate to exactly n bytes
            let mut fixed = vec![0u8; *n];
            let copy_len = bytes.len().min(*n);
            fixed[..copy_len].copy_from_slice(&bytes[..copy_len]);
            buf.extend_from_slice(&fixed);
        }

        // ── Integers ─────────────────────────────────────────────────────────
        ChType::Int8 => buf.push(coerce_i64(value)? as i8 as u8),
        ChType::Int16 => buf.extend_from_slice(&(coerce_i64(value)? as i16).to_le_bytes()),
        ChType::Int32 => buf.extend_from_slice(&(coerce_i64(value)? as i32).to_le_bytes()),
        ChType::Int64 => buf.extend_from_slice(&coerce_i64(value)?.to_le_bytes()),
        ChType::UInt8 => buf.push(coerce_u64(value)? as u8),
        ChType::UInt16 => buf.extend_from_slice(&(coerce_u64(value)? as u16).to_le_bytes()),
        ChType::UInt32 => buf.extend_from_slice(&(coerce_u64(value)? as u32).to_le_bytes()),
        ChType::UInt64 => buf.extend_from_slice(&coerce_u64(value)?.to_le_bytes()),

        // ── Floats ───────────────────────────────────────────────────────────
        ChType::Float32 => {
            let f = coerce_f64(value)? as f32;
            buf.extend_from_slice(&f.to_le_bytes());
        }
        ChType::Float64 => {
            let f = coerce_f64(value)?;
            buf.extend_from_slice(&f.to_le_bytes());
        }

        // ── Boolean ──────────────────────────────────────────────────────────
        ChType::Boolean => {
            let b = match value {
                OwnedValue::Static(simd_json::StaticNode::Bool(b)) => *b,
                OwnedValue::Static(simd_json::StaticNode::I64(n)) => *n != 0,
                OwnedValue::Static(simd_json::StaticNode::U64(n)) => *n != 0,
                _ => {
                    error!("Cannot convert to Boolean: {value:?}");
                    return Err(Error::InvalidRecord);
                }
            };
            buf.push(b as u8);
        }

        // ── UUID ─────────────────────────────────────────────────────────────
        // ClickHouse stores UUID as two little-endian 64-bit words.
        // Input: standard hyphenated UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        ChType::Uuid => {
            let s = coerce_to_string(value)?;
            let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            if hex.len() != 32 {
                error!("Invalid UUID string: {s}");
                return Err(Error::InvalidRecord);
            }
            let bytes = hex::decode(&hex).map_err(|_| {
                error!("Cannot decode UUID hex: {hex}");
                Error::InvalidRecord
            })?;
            // ClickHouse UUID layout: first 8 bytes reversed, second 8 bytes reversed
            let mut uuid_buf = [0u8; 16];
            uuid_buf[..8].copy_from_slice(&bytes[..8]);
            uuid_buf[8..].copy_from_slice(&bytes[8..]);
            uuid_buf[..8].reverse();
            uuid_buf[8..].reverse();
            buf.extend_from_slice(&uuid_buf);
        }

        // ── Date types ───────────────────────────────────────────────────────
        ChType::Date => {
            // Days since 1970-01-01 as UInt16. Accept integer or "YYYY-MM-DD".
            let days = coerce_to_days(value)? as u16;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        ChType::Date32 => {
            let days = coerce_to_days(value)? as i32;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        ChType::DateTime => {
            // Unix seconds as UInt32. Accept integer or RFC 3339 string.
            let secs = coerce_to_unix_seconds(value)? as u32;
            buf.extend_from_slice(&secs.to_le_bytes());
        }
        ChType::DateTime64(precision) => {
            // Unix time scaled by 10^precision as Int64.
            let secs_f64 = coerce_to_unix_seconds_f64(value)?;
            let scale = 10i64.pow(*precision as u32) as f64;
            let scaled = (secs_f64 * scale).round() as i64;
            buf.extend_from_slice(&scaled.to_le_bytes());
        }

        // ── Decimal ──────────────────────────────────────────────────────────
        ChType::Decimal(precision, scale) => {
            let f = coerce_f64(value)?;
            let scale_factor = 10f64.powi(*scale as i32);
            let int_val = (f * scale_factor).round() as i128;
            if *precision <= 9 {
                buf.extend_from_slice(&(int_val as i32).to_le_bytes());
            } else if *precision <= 18 {
                buf.extend_from_slice(&(int_val as i64).to_le_bytes());
            } else {
                // Int128: two little-endian 64-bit words, low word first
                let lo = int_val as i64;
                let hi = (int_val >> 64) as i64;
                buf.extend_from_slice(&lo.to_le_bytes());
                buf.extend_from_slice(&hi.to_le_bytes());
            }
        }

        // ── IP addresses ─────────────────────────────────────────────────────
        ChType::IPv4 => {
            let s = coerce_to_string(value)?;
            let addr: std::net::Ipv4Addr = s.parse().map_err(|_| {
                error!("Invalid IPv4 address: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&addr.octets()); // big-endian
        }
        ChType::IPv6 => {
            let s = coerce_to_string(value)?;
            let addr: std::net::Ipv6Addr = s.parse().map_err(|_| {
                error!("Invalid IPv6 address: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&addr.octets()); // big-endian
        }

        // ── Enums ────────────────────────────────────────────────────────────
        ChType::Enum8(map) => {
            let s = coerce_to_string(value)?;
            let v = map.get(s.as_str()).ok_or_else(|| {
                error!("Unknown Enum8 value: {s}");
                Error::InvalidRecord
            })?;
            buf.push(*v as u8);
        }
        ChType::Enum16(map) => {
            let s = coerce_to_string(value)?;
            let v = map.get(s.as_str()).ok_or_else(|| {
                error!("Unknown Enum16 value: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&(*v as i16).to_le_bytes());
        }

        // ── Composites ───────────────────────────────────────────────────────
        ChType::Array(elem_type) => {
            let arr = value.as_array().ok_or_else(|| {
                error!("Expected JSON array for Array type, got: {value:?}");
                Error::InvalidRecord
            })?;
            write_varint(arr.len() as u64, buf);
            for elem in arr {
                serialize_value(elem, elem_type, buf)?;
            }
        }
        ChType::Map(key_type, val_type) => {
            let obj = value.as_object().ok_or_else(|| {
                error!("Expected JSON object for Map type, got: {value:?}");
                Error::InvalidRecord
            })?;
            write_varint(obj.len() as u64, buf);
            for (k, v) in obj {
                // Map keys must be serialisable as the key type. JSON object
                // keys are always strings, so we wrap them in OwnedValue::String.
                let key_val = OwnedValue::String(k.clone());
                serialize_value(&key_val, key_type, buf)?;
                serialize_value(v, val_type, buf)?;
            }
        }
        ChType::Tuple(field_types) => {
            // Tuples may arrive as JSON arrays (unnamed) or objects (named).
            match value {
                OwnedValue::Array(arr) => {
                    if arr.len() != field_types.len() {
                        error!(
                            "Tuple length mismatch: expected {}, got {}",
                            field_types.len(),
                            arr.len()
                        );
                        return Err(Error::InvalidRecord);
                    }
                    for (elem, ft) in arr.iter().zip(field_types.iter()) {
                        serialize_value(elem, ft, buf)?;
                    }
                }
                OwnedValue::Object(obj) => {
                    // Named tuple: fields are matched by position (insertion order).
                    let values: Vec<&OwnedValue> = obj.values().collect();
                    if values.len() != field_types.len() {
                        error!(
                            "Tuple length mismatch: expected {}, got {}",
                            field_types.len(),
                            values.len()
                        );
                        return Err(Error::InvalidRecord);
                    }
                    for (v, ft) in values.iter().zip(field_types.iter()) {
                        serialize_value(v, ft, buf)?;
                    }
                }
                other => {
                    error!("Expected JSON array or object for Tuple type, got: {other:?}");
                    return Err(Error::InvalidRecord);
                }
            }
        }
    }
    Ok(())
}

// ─── Low-level helpers ────────────────────────────────────────────────────────

/// Write a ClickHouse-style unsigned LEB128 varint (7 bits per byte, MSB = continuation).
pub fn write_varint(mut n: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// Write a string: varint length prefix + UTF-8 bytes.
fn write_string(bytes: &[u8], buf: &mut Vec<u8>) {
    write_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

/// Write a zero / null value for a column that is absent and has no default.
/// Nullable columns get the null marker; non-nullable columns are an error.
fn write_zero_or_null(ch_type: &ChType, buf: &mut Vec<u8>, col_name: &str) -> Result<(), Error> {
    match ch_type {
        ChType::Nullable(_) => {
            buf.push(0x01); // null
            Ok(())
        }
        _ => {
            error!(
                "Column '{col_name}' is non-nullable with no default, but is absent from the message"
            );
            Err(Error::InvalidRecord)
        }
    }
}

// ─── Value coercion helpers ───────────────────────────────────────────────────

fn coerce_i64(value: &OwnedValue) -> Result<i64, Error> {
    match value {
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(*n),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(*n as i64),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as i64),
        OwnedValue::String(s) => s.parse::<i64>().map_err(|_| {
            error!("Cannot parse '{s}' as integer");
            Error::InvalidRecord
        }),
        other => {
            error!("Cannot coerce {other:?} to integer");
            Err(Error::InvalidRecord)
        }
    }
}

fn coerce_u64(value: &OwnedValue) -> Result<u64, Error> {
    match value {
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(*n),
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(*n as u64),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f as u64),
        OwnedValue::String(s) => s.parse::<u64>().map_err(|_| {
            error!("Cannot parse '{s}' as unsigned integer");
            Error::InvalidRecord
        }),
        other => {
            error!("Cannot coerce {other:?} to unsigned integer");
            Err(Error::InvalidRecord)
        }
    }
}

fn coerce_f64(value: &OwnedValue) -> Result<f64, Error> {
    match value {
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f),
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(*n as f64),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(*n as f64),
        OwnedValue::String(s) => s.parse::<f64>().map_err(|_| {
            error!("Cannot parse '{s}' as float");
            Error::InvalidRecord
        }),
        other => {
            error!("Cannot coerce {other:?} to float");
            Err(Error::InvalidRecord)
        }
    }
}

fn coerce_to_string(value: &OwnedValue) -> Result<String, Error> {
    match value {
        OwnedValue::String(s) => Ok(s.to_string()),
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(n.to_string()),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(n.to_string()),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(f.to_string()),
        OwnedValue::Static(simd_json::StaticNode::Bool(b)) => Ok(b.to_string()),
        other => {
            error!("Cannot coerce {other:?} to string");
            Err(Error::InvalidRecord)
        }
    }
}

/// Returns days since 1970-01-01. Accepts integer (days) or "YYYY-MM-DD" string.
fn coerce_to_days(value: &OwnedValue) -> Result<i64, Error> {
    match value {
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(*n),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(*n as i64),
        OwnedValue::String(s) => {
            // Parse "YYYY-MM-DD" manually
            let parts: Vec<&str> = s.splitn(3, '-').collect();
            if parts.len() != 3 {
                error!("Invalid date string: {s}");
                return Err(Error::InvalidRecord);
            }
            let (y, m, d) = parse_ymd(parts[0], parts[1], parts[2]).map_err(|_| {
                error!("Cannot parse date: {s}");
                Error::InvalidRecord
            })?;
            Ok(ymd_to_days(y, m, d))
        }
        other => {
            error!("Cannot coerce {other:?} to Date");
            Err(Error::InvalidRecord)
        }
    }
}

/// Returns Unix seconds as f64 (fractional seconds for sub-second precision).
/// Accepts integer, float, or RFC 3339 / ISO 8601 string.
fn coerce_to_unix_seconds_f64(value: &OwnedValue) -> Result<f64, Error> {
    match value {
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(*n as f64),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(*n as f64),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(*f),
        OwnedValue::String(s) => parse_datetime_string(s),
        other => {
            error!("Cannot coerce {other:?} to DateTime");
            Err(Error::InvalidRecord)
        }
    }
}

/// Returns Unix seconds as i64 (truncates fractional seconds).
fn coerce_to_unix_seconds(value: &OwnedValue) -> Result<i64, Error> {
    Ok(coerce_to_unix_seconds_f64(value)? as i64)
}

/// Parse "YYYY-MM-DDThh:mm:ss[.frac][Z|±hh:mm]" into Unix seconds (f64).
/// This is a minimal parser sufficient for common ISO 8601 / RFC 3339 formats.
fn parse_datetime_string(s: &str) -> Result<f64, Error> {
    // Split on 'T' or ' ' for date-time separator
    let (date_part, time_part) = if let Some(idx) = s.find('T').or_else(|| s.find(' ')) {
        (&s[..idx], &s[idx + 1..])
    } else {
        // Date-only string — treat as midnight UTC
        (s, "00:00:00")
    };

    let date_parts: Vec<&str> = date_part.splitn(3, '-').collect();
    if date_parts.len() != 3 {
        error!("Cannot parse datetime string: {s}");
        return Err(Error::InvalidRecord);
    }
    let (y, m, d) = parse_ymd(date_parts[0], date_parts[1], date_parts[2]).map_err(|_| {
        error!("Cannot parse date component of: {s}");
        Error::InvalidRecord
    })?;

    // Strip timezone suffix and optional fractional seconds
    let (time_no_tz, tz_offset_secs) = strip_timezone(time_part);
    let frac_secs = parse_time(time_no_tz).map_err(|_| {
        error!("Cannot parse time component of: {s}");
        Error::InvalidRecord
    })?;

    let days = ymd_to_days(y, m, d) as f64;
    Ok(days * 86400.0 + frac_secs - tz_offset_secs as f64)
}

fn parse_ymd(y: &str, m: &str, d: &str) -> Result<(i32, u32, u32), ()> {
    let year: i32 = y.trim().parse().map_err(|_| ())?;
    let month: u32 = m.trim().parse().map_err(|_| ())?;
    let day: u32 = d.trim().parse().map_err(|_| ())?;
    Ok((year, month, day))
}

/// Days since Unix epoch for a proleptic Gregorian date (Gregorian calendar).
fn ymd_to_days(y: i32, m: u32, d: u32) -> i64 {
    // Algorithm from https://www.tondering.dk/claus/cal/julperiod.php
    let y = if m <= 2 { y as i64 - 1 } else { y as i64 };
    let m = m as i64;
    let d = d as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parse "hh:mm:ss[.frac]" into total seconds (f64).
fn parse_time(s: &str) -> Result<f64, ()> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() < 2 {
        return Err(());
    }
    let h: f64 = parts[0].parse().map_err(|_| ())?;
    let min: f64 = parts[1].parse().map_err(|_| ())?;
    let sec: f64 = parts.get(2).unwrap_or(&"0").parse().map_err(|_| ())?;
    Ok(h * 3600.0 + min * 60.0 + sec)
}

/// Strip a timezone suffix from a time string. Returns (time_without_tz, offset_seconds).
fn strip_timezone(s: &str) -> (&str, i64) {
    if let Some(stripped) = s.strip_suffix('Z') {
        return (stripped, 0);
    }
    // Look for ±hh:mm or ±hhmm suffix
    for (sign, ch) in [(1i64, '+'), (-1i64, '-')] {
        #[allow(clippy::collapsible_if)]
        if let Some(pos) = s.rfind(ch) {
            if pos > 0 {
                let tz = &s[pos + 1..];
                let secs = if tz.contains(':') {
                    let parts: Vec<&str> = tz.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        if let (Ok(h), Ok(m)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                            Some(sign * (h * 3600 + m * 60))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else if tz.len() == 4 {
                    if let Ok(hhmm) = tz.parse::<i64>() {
                        Some(sign * ((hhmm / 100) * 3600 + (hhmm % 100) * 60))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(offset) = secs {
                    return (&s[..pos], offset);
                }
            }
        }
    }
    (s, 0)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ChType, Column};
    use simd_json::{OwnedValue, StaticNode};

    fn json_str(s: &str) -> OwnedValue {
        OwnedValue::String(s.into())
    }
    fn json_i64(n: i64) -> OwnedValue {
        OwnedValue::Static(StaticNode::I64(n))
    }
    fn json_u64(n: u64) -> OwnedValue {
        OwnedValue::Static(StaticNode::U64(n))
    }
    fn json_f64(f: f64) -> OwnedValue {
        OwnedValue::Static(StaticNode::F64(f))
    }
    fn json_bool(b: bool) -> OwnedValue {
        OwnedValue::Static(StaticNode::Bool(b))
    }
    fn json_null() -> OwnedValue {
        OwnedValue::Static(StaticNode::Null)
    }

    fn col(name: &str, ch_type: ChType, has_default: bool) -> Column {
        Column {
            name: name.into(),
            ch_type,
            has_default,
        }
    }

    // ── varint ───────────────────────────────────────────────────────────────
    #[test]
    fn varint_single_byte() {
        let mut buf = vec![];
        write_varint(0, &mut buf);
        assert_eq!(buf, [0x00]);

        buf.clear();
        write_varint(127, &mut buf);
        assert_eq!(buf, [0x7F]);
    }

    #[test]
    fn varint_multi_byte() {
        let mut buf = vec![];
        write_varint(128, &mut buf);
        assert_eq!(buf, [0x80, 0x01]);

        buf.clear();
        write_varint(300, &mut buf);
        assert_eq!(buf, [0xAC, 0x02]);
    }

    // ── primitives ───────────────────────────────────────────────────────────
    #[test]
    fn serialize_int32_little_endian() {
        let mut buf = vec![];
        serialize_value(&json_i64(1000), &ChType::Int32, &mut buf).unwrap();
        assert_eq!(buf, 1000i32.to_le_bytes());
    }

    #[test]
    fn serialize_uint64_little_endian() {
        let mut buf = vec![];
        serialize_value(&json_u64(u64::MAX), &ChType::UInt64, &mut buf).unwrap();
        assert_eq!(buf, u64::MAX.to_le_bytes());
    }

    #[test]
    fn serialize_float32() {
        let mut buf = vec![];
        serialize_value(&json_f64(3.15), &ChType::Float32, &mut buf).unwrap();
        assert_eq!(buf, (3.15f64 as f32).to_le_bytes());
    }

    #[test]
    fn serialize_float64() {
        let mut buf = vec![];
        serialize_value(&json_f64(2.318281828), &ChType::Float64, &mut buf).unwrap();
        assert_eq!(buf, 2.318281828f64.to_le_bytes());
    }

    #[test]
    fn serialize_boolean_true() {
        let mut buf = vec![];
        serialize_value(&json_bool(true), &ChType::Boolean, &mut buf).unwrap();
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn serialize_boolean_false() {
        let mut buf = vec![];
        serialize_value(&json_bool(false), &ChType::Boolean, &mut buf).unwrap();
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn serialize_boolean_from_nonzero_i64_is_true() {
        let mut buf = vec![];
        serialize_value(&json_i64(1), &ChType::Boolean, &mut buf).unwrap();
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn serialize_boolean_from_zero_u64_is_false() {
        let mut buf = vec![];
        serialize_value(&json_u64(0), &ChType::Boolean, &mut buf).unwrap();
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn serialize_string_with_varint_prefix() {
        let mut buf = vec![];
        serialize_value(&json_str("hi"), &ChType::String, &mut buf).unwrap();
        // length=2 as varint, then "hi"
        assert_eq!(buf, [0x02, b'h', b'i']);
    }

    #[test]
    fn serialize_fixed_string_pads_to_length() {
        let mut buf = vec![];
        serialize_value(&json_str("ab"), &ChType::FixedString(4), &mut buf).unwrap();
        assert_eq!(buf, [b'a', b'b', 0x00, 0x00]);
    }

    #[test]
    fn serialize_fixed_string_truncates_to_length() {
        let mut buf = vec![];
        serialize_value(&json_str("abcdef"), &ChType::FixedString(3), &mut buf).unwrap();
        assert_eq!(buf, [b'a', b'b', b'c']);
    }

    // ── nullable ─────────────────────────────────────────────────────────────
    #[test]
    fn serialize_nullable_null_writes_marker() {
        let mut buf = vec![];
        serialize_value(
            &json_null(),
            &ChType::Nullable(Box::new(ChType::Int32)),
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn serialize_nullable_non_null_writes_zero_then_value() {
        let mut buf = vec![];
        serialize_value(
            &json_i64(42),
            &ChType::Nullable(Box::new(ChType::Int32)),
            &mut buf,
        )
        .unwrap();
        let mut expected = vec![0x00u8];
        expected.extend_from_slice(&42i32.to_le_bytes());
        assert_eq!(buf, expected);
    }

    // ── uuid ─────────────────────────────────────────────────────────────────

    #[test]
    fn serialize_uuid_writes_split_reversed_halves() {
        let mut buf = vec![];
        // Raw bytes: [55 0e 84 00 e2 9b 41 d4] [a7 16 44 66 55 44 00 00]
        // First half reversed:  [d4 41 9b e2 00 84 0e 55]
        // Second half reversed: [00 00 44 55 66 44 16 a7]
        serialize_value(
            &json_str("550e8400-e29b-41d4-a716-446655440000"),
            &ChType::Uuid,
            &mut buf,
        )
        .unwrap();
        assert_eq!(
            buf,
            [
                0xd4, 0x41, 0x9b, 0xe2, 0x00, 0x84, 0x0e, 0x55, 0x00, 0x00, 0x44, 0x55, 0x66, 0x44,
                0x16, 0xa7,
            ]
        );
    }

    #[test]
    fn serialize_uuid_invalid_string_is_error() {
        let mut buf = vec![];
        let result = serialize_value(&json_str("not-a-uuid"), &ChType::Uuid, &mut buf);
        assert!(result.is_err());
    }

    // ── enum ─────────────────────────────────────────────────────────────────

    #[test]
    fn serialize_enum8_known_value() {
        let mut map = std::collections::HashMap::new();
        map.insert("active".to_string(), 1i8);
        map.insert("inactive".to_string(), 2i8);
        let mut buf = vec![];
        serialize_value(&json_str("active"), &ChType::Enum8(map), &mut buf).unwrap();
        assert_eq!(buf, [0x01]);
    }

    #[test]
    fn serialize_enum8_unknown_value_is_error() {
        let mut map = std::collections::HashMap::new();
        map.insert("active".to_string(), 1i8);
        let mut buf = vec![];
        let result = serialize_value(&json_str("deleted"), &ChType::Enum8(map), &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_enum16_known_value_little_endian() {
        let mut map = std::collections::HashMap::new();
        map.insert("low".to_string(), 300i16);
        let mut buf = vec![];
        serialize_value(&json_str("low"), &ChType::Enum16(map), &mut buf).unwrap();
        assert_eq!(buf, 300i16.to_le_bytes());
    }

    // ── RowBinary row ────────────────────────────────────────────────────────
    #[test]
    fn serialize_row_with_default_column_absent() {
        use simd_json::OwnedValue;
        let mut obj = simd_json::owned::Object::new();
        obj.insert("name".into(), json_str("alice"));
        let value = OwnedValue::Object(Box::new(obj));

        let columns = vec![
            col("name", ChType::String, false),
            col("age", ChType::Int32, true), // has_default=true, absent → 0x01
        ];
        let mut buf = vec![];
        serialize_row(&value, &columns, &mut buf).unwrap();

        // name: 0x00 prefix + varint(5) + "alice"
        // age:  0x01 (use DEFAULT)
        assert_eq!(buf[0], 0x00); // name: value follows
        assert_eq!(buf[1], 5); // varint length of "alice"
        assert_eq!(&buf[2..7], b"alice");
        assert_eq!(buf[7], 0x01); // age: use DEFAULT
    }

    #[test]
    fn serialize_row_non_nullable_absent_no_default_is_error() {
        use simd_json::OwnedValue;
        let value = OwnedValue::Object(Box::new(simd_json::owned::Object::new()));
        let columns = vec![col("id", ChType::Int32, false)];
        let mut buf = vec![];
        // 0x00 prefix written, then error on missing non-nullable non-default column
        let result = serialize_row(&value, &columns, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_row_nullable_absent_writes_null_marker() {
        // Absent field + Nullable column + no default → 0x00 prefix + 0x01 null marker.
        let value = OwnedValue::Object(Box::new(simd_json::owned::Object::new()));
        let columns = vec![col("x", ChType::Nullable(Box::new(ChType::Int32)), false)];
        let mut buf = vec![];
        serialize_row(&value, &columns, &mut buf).unwrap();
        assert_eq!(buf, [0x00, 0x01]);
    }

    #[test]
    fn serialize_row_non_nullable_explicit_null_is_error() {
        // Field present in JSON but set to null, column is non-nullable → error.
        let mut obj = simd_json::owned::Object::new();
        obj.insert("id".into(), json_null());
        let value = OwnedValue::Object(Box::new(obj));
        let columns = vec![col("id", ChType::Int32, false)];
        let mut buf = vec![];
        let result = serialize_row(&value, &columns, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_row_non_object_payload_is_error() {
        let value = OwnedValue::Array(Box::default());
        let columns = vec![col("x", ChType::Int32, false)];
        let mut buf = vec![];
        let result = serialize_row(&value, &columns, &mut buf);
        assert!(result.is_err());
    }

    // ── date / datetime ──────────────────────────────────────────────────────
    #[test]
    fn serialize_date_from_integer() {
        let mut buf = vec![];
        serialize_value(&json_u64(19000), &ChType::Date, &mut buf).unwrap();
        assert_eq!(buf, 19000u16.to_le_bytes());
    }

    #[test]
    fn serialize_date_from_string() {
        let mut buf = vec![];
        // 1970-01-02 = day 1
        serialize_value(&json_str("1970-01-02"), &ChType::Date, &mut buf).unwrap();
        assert_eq!(buf, 1u16.to_le_bytes());
    }

    #[test]
    fn serialize_datetime_from_integer() {
        let mut buf = vec![];
        serialize_value(&json_u64(1_700_000_000), &ChType::DateTime, &mut buf).unwrap();
        assert_eq!(buf, 1_700_000_000u32.to_le_bytes());
    }

    #[test]
    fn serialize_datetime64_millis() {
        let mut buf = vec![];
        // 1000 seconds → 1_000_000 milliseconds at precision=3
        serialize_value(&json_u64(1000), &ChType::DateTime64(3), &mut buf).unwrap();
        assert_eq!(buf, 1_000_000i64.to_le_bytes());
    }

    #[test]
    fn serialize_date32_from_string() {
        let mut buf = vec![];
        // 1970-01-02 = day 1 as i32 (Date32 uses signed Int32, not UInt16)
        serialize_value(&json_str("1970-01-02"), &ChType::Date32, &mut buf).unwrap();
        assert_eq!(buf, 1i32.to_le_bytes());
    }

    #[test]
    fn serialize_datetime_from_iso8601_utc_string() {
        let mut buf = vec![];
        // "1970-01-02T00:00:00Z" = 86400 seconds
        serialize_value(
            &json_str("1970-01-02T00:00:00Z"),
            &ChType::DateTime,
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf, 86400u32.to_le_bytes());
    }

    #[test]
    fn serialize_datetime_from_iso8601_positive_offset() {
        let mut buf = vec![];
        // "1970-01-01T01:00:00+01:00" = midnight UTC = 0 seconds
        serialize_value(
            &json_str("1970-01-01T01:00:00+01:00"),
            &ChType::DateTime,
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf, 0u32.to_le_bytes());
    }

    #[test]
    fn serialize_datetime64_from_string_with_fractional_seconds() {
        let mut buf = vec![];
        // 1.5 seconds at precision=3 → 1500 milliseconds
        serialize_value(
            &json_str("1970-01-01T00:00:01.500Z"),
            &ChType::DateTime64(3),
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf, 1500i64.to_le_bytes());
    }

    // ── decimal ──────────────────────────────────────────────────────────────
    #[test]
    fn serialize_decimal32_scale2() {
        let mut buf = vec![];
        // 3.15 * 10^2 = 314 → Int32
        serialize_value(&json_f64(3.15), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 315i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal64_scale4() {
        let mut buf = vec![];
        serialize_value(&json_f64(1.2345), &ChType::Decimal(18, 4), &mut buf).unwrap();
        assert_eq!(buf, 12345i64.to_le_bytes());
    }

    #[test]
    fn serialize_decimal128_two_word_layout() {
        let mut buf = vec![];
        // Decimal(38, 2): 1.0 → int_val = 100 → fits in i128
        // Written as two little-endian i64 words: lo=100, hi=0
        serialize_value(&json_f64(1.0), &ChType::Decimal(38, 2), &mut buf).unwrap();
        let mut expected = 100i64.to_le_bytes().to_vec();
        expected.extend_from_slice(&0i64.to_le_bytes());
        assert_eq!(buf, expected);
    }

    // ── array ────────────────────────────────────────────────────────────────
    #[test]
    fn serialize_array_of_int32() {
        let arr = OwnedValue::Array(Box::new(vec![json_i64(1), json_i64(2), json_i64(3)]));
        let mut buf = vec![];
        serialize_value(&arr, &ChType::Array(Box::new(ChType::Int32)), &mut buf).unwrap();
        // varint(3) + 3×Int32
        assert_eq!(buf[0], 3); // varint
        assert_eq!(&buf[1..5], 1i32.to_le_bytes());
        assert_eq!(&buf[5..9], 2i32.to_le_bytes());
        assert_eq!(&buf[9..13], 3i32.to_le_bytes());
    }

    // ── map ──────────────────────────────────────────────────────────────────

    #[test]
    fn serialize_map_string_to_int32() {
        // Map(String, Int32): {"k": 1}
        // → varint(1) + string("k") + Int32(1)
        let mut obj = simd_json::owned::Object::new();
        obj.insert("k".into(), json_i64(1));
        let value = OwnedValue::Object(Box::new(obj));
        let mut buf = vec![];
        serialize_value(
            &value,
            &ChType::Map(Box::new(ChType::String), Box::new(ChType::Int32)),
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], 1); // varint: 1 entry
        assert_eq!(buf[1], 1); // varint: key length 1
        assert_eq!(buf[2], b'k');
        assert_eq!(&buf[3..7], 1i32.to_le_bytes());
    }

    #[test]
    fn serialize_map_non_object_is_error() {
        let value = OwnedValue::Array(Box::default());
        let mut buf = vec![];
        let result = serialize_value(
            &value,
            &ChType::Map(Box::new(ChType::String), Box::new(ChType::Int32)),
            &mut buf,
        );
        assert!(result.is_err());
    }

    // ── tuple ─────────────────────────────────────────────────────────────────

    #[test]
    fn serialize_tuple_from_json_array() {
        // Tuple(String, Int32): ["hi", 7]
        let arr = OwnedValue::Array(Box::new(vec![json_str("hi"), json_i64(7)]));
        let mut buf = vec![];
        serialize_value(
            &arr,
            &ChType::Tuple(vec![ChType::String, ChType::Int32]),
            &mut buf,
        )
        .unwrap();
        assert_eq!(&buf[..3], &[0x02, b'h', b'i']); // string "hi"
        assert_eq!(&buf[3..], 7i32.to_le_bytes()); // Int32 7
    }

    #[test]
    fn serialize_tuple_from_json_object() {
        // Tuple(String, Int32) as named object: {"a": "hi", "b": 7}
        let mut obj = simd_json::owned::Object::new();
        obj.insert("a".into(), json_str("hi"));
        obj.insert("b".into(), json_i64(7));
        let value = OwnedValue::Object(Box::new(obj));
        let mut buf = vec![];
        serialize_value(
            &value,
            &ChType::Tuple(vec![ChType::String, ChType::Int32]),
            &mut buf,
        )
        .unwrap();
        assert_eq!(&buf[..3], &[0x02, b'h', b'i']);
        assert_eq!(&buf[3..], 7i32.to_le_bytes());
    }

    #[test]
    fn serialize_tuple_length_mismatch_is_error() {
        // Schema expects 2 fields, array has 1 → error
        let arr = OwnedValue::Array(Box::new(vec![json_i64(1)]));
        let mut buf = vec![];
        let result = serialize_value(
            &arr,
            &ChType::Tuple(vec![ChType::Int32, ChType::String]),
            &mut buf,
        );
        assert!(result.is_err());
    }

    // ── ipv4 / ipv6 ──────────────────────────────────────────────────────────
    #[test]
    fn serialize_ipv4() {
        let mut buf = vec![];
        serialize_value(&json_str("127.0.0.1"), &ChType::IPv4, &mut buf).unwrap();
        assert_eq!(buf, [127, 0, 0, 1]);
    }

    #[test]
    fn serialize_ipv6_loopback() {
        let mut buf = vec![];
        serialize_value(&json_str("::1"), &ChType::IPv6, &mut buf).unwrap();
        assert_eq!(buf.len(), 16);
        assert_eq!(buf[15], 1);
        assert!(buf[..15].iter().all(|&b| b == 0));
    }
}

// Hex decoding helper (no extra dep — manual implementation for UUID)
mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if !s.len().is_multiple_of(2) {
            return Err(());
        }
        s.as_bytes()
            .chunks(2)
            .map(|chunk| {
                let hi = from_hex_digit(chunk[0])?;
                let lo = from_hex_digit(chunk[1])?;
                Ok((hi << 4) | lo)
            })
            .collect()
    }

    fn from_hex_digit(b: u8) -> Result<u8, ()> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err(()),
        }
    }
}
