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

use std::borrow::Cow;

use crate::schema::{ChType, Column};
use iggy_connector_sdk::Error;
use simd_json::OwnedValue;
use simd_json::prelude::{TypedScalarValue, ValueAsArray, ValueAsObject};
use tracing::{error, warn};

// ─── Public API ──────────────────────────────────────────────────────────────

/// Serialise one message (a JSON object) as a RowBinaryWithDefaults row.
///
/// Columns are written in schema order. When a column is absent from the JSON
/// object and `has_default` is true the DEFAULT prefix byte (`0x01`) is
/// written and the column value is skipped. When a column is absent but has no
/// default and is not Nullable this is an error.
pub(crate) fn serialize_row(
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

        // RowBinaryWithDefaults prefix byte. Only an absent field defers to the
        // server DEFAULT; an explicit JSON null must fall through to the
        // Nullable path so it is stored as NULL rather than the default value.
        if field_value.is_none() && col.has_default {
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
            // ClickHouse rejects over-length values with TOO_LARGE_STRING_SIZE
            // rather than truncating, so reject here instead of dropping bytes.
            if bytes.len() > *n {
                error!("Value too large for FixedString({n})");
                return Err(Error::InvalidRecord);
            }
            buf.extend_from_slice(bytes);
            buf.resize(buf.len() + (n - bytes.len()), 0u8);
        }

        // ── Integers ─────────────────────────────────────────────────────────
        ChType::Int8 => buf.push(i8::try_from(coerce_i64(value)?).map_err(|_| {
            error!("Value out of range for Int8");
            Error::InvalidRecord
        })? as u8),
        ChType::Int16 => buf.extend_from_slice(
            &i16::try_from(coerce_i64(value)?)
                .map_err(|_| {
                    error!("Value out of range for Int16");
                    Error::InvalidRecord
                })?
                .to_le_bytes(),
        ),
        ChType::Int32 => buf.extend_from_slice(
            &i32::try_from(coerce_i64(value)?)
                .map_err(|_| {
                    error!("Value out of range for Int32");
                    Error::InvalidRecord
                })?
                .to_le_bytes(),
        ),
        ChType::Int64 => buf.extend_from_slice(&coerce_i64(value)?.to_le_bytes()),
        ChType::UInt8 => buf.push(u8::try_from(coerce_u64(value)?).map_err(|_| {
            error!("Value out of range for UInt8");
            Error::InvalidRecord
        })?),
        ChType::UInt16 => buf.extend_from_slice(
            &u16::try_from(coerce_u64(value)?)
                .map_err(|_| {
                    error!("Value out of range for UInt16");
                    Error::InvalidRecord
                })?
                .to_le_bytes(),
        ),
        ChType::UInt32 => buf.extend_from_slice(
            &u32::try_from(coerce_u64(value)?)
                .map_err(|_| {
                    error!("Value out of range for UInt32");
                    Error::InvalidRecord
                })?
                .to_le_bytes(),
        ),
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
            let b = s.as_bytes();
            // Validate hyphenated UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
                error!("Invalid UUID string (expected xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx): {s}");
                return Err(Error::InvalidRecord);
            }
            // Copy hex segments into a stack buffer, skipping the 4 dashes
            let mut hex = [0u8; 32];
            let segments: [&[u8]; 5] = [&b[0..8], &b[9..13], &b[14..18], &b[19..23], &b[24..36]];
            let mut i = 0;
            for seg in segments {
                hex[i..i + seg.len()].copy_from_slice(seg);
                i += seg.len();
            }
            let hex_str = std::str::from_utf8(&hex).map_err(|_| {
                error!("Cannot decode UUID hex: {s}");
                Error::InvalidRecord
            })?;
            // ClickHouse UUID: two LE UInt64 words, low word (bytes 8-15) first.
            // hex_str[..16] is the high u64, hex_str[16..] the low u64 (both big-endian).
            let hi = u64::from_str_radix(&hex_str[..16], 16).map_err(|_| {
                error!("Cannot decode UUID hex: {s}");
                Error::InvalidRecord
            })?;
            let lo = u64::from_str_radix(&hex_str[16..], 16).map_err(|_| {
                error!("Cannot decode UUID hex: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&lo.to_le_bytes());
            buf.extend_from_slice(&hi.to_le_bytes());
        }

        // ── Date types ───────────────────────────────────────────────────────
        ChType::Date => {
            // Days since 1970-01-01 as UInt16. Accept integer or "YYYY-MM-DD".
            let days = u16::try_from(coerce_to_days(value)?).map_err(|_| {
                error!("Value out of range for Date (1970-01-01..2149-06-06)");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        ChType::Date32 => {
            let days = i32::try_from(coerce_to_days(value)?).map_err(|_| {
                error!("Value out of range for Date32");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        ChType::DateTime => {
            // Unix seconds as UInt32. Accept integer or RFC 3339 string.
            let secs = u32::try_from(coerce_to_unix_seconds(value)?).map_err(|_| {
                error!("Value out of range for DateTime (1970-01-01..2106-02-07)");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&secs.to_le_bytes());
        }
        ChType::DateTime64(precision) => {
            // Unix time scaled by 10^precision as Int64.
            // precision is validated to be 0-9 by the schema parser, so
            // 10i64.pow(*precision as u32) cannot overflow i64::MAX.
            let scale = 10i64.pow(*precision as u32);
            let scaled = match value {
                OwnedValue::Static(simd_json::StaticNode::I64(n)) => {
                    // Integer fast path: multiply in i64 to avoid f64 precision loss.
                    // At precision=9, a current-era timestamp (~1.7e9 s) * 1e9 = ~1.7e18,
                    // which exceeds 2^53 and would lose ~256 ns if routed through f64.
                    n.checked_mul(scale).ok_or_else(|| {
                        error!("DateTime64 overflow");
                        Error::InvalidRecord
                    })?
                }
                OwnedValue::Static(simd_json::StaticNode::U64(n)) => i64::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_mul(scale))
                    .ok_or_else(|| {
                        error!("DateTime64 overflow");
                        Error::InvalidRecord
                    })?,
                _ => {
                    // Float or string inputs: f64 path is acceptable since floats
                    // already carry sub-second fractions, and strings go through
                    // parse_datetime_string which returns fractional seconds.
                    let secs_f64 = coerce_to_unix_seconds_f64(value)?;
                    (secs_f64 * scale as f64).round() as i64
                }
            };
            buf.extend_from_slice(&scaled.to_le_bytes());
        }

        // ── Decimal ──────────────────────────────────────────────────────────
        ChType::Decimal(precision, scale) => {
            let int_val = coerce_decimal(value, *precision, *scale)?;
            if *precision <= 9 {
                buf.extend_from_slice(
                    &i32::try_from(int_val)
                        .map_err(|_| {
                            error!("Decimal value out of range for precision {precision}");
                            Error::InvalidRecord
                        })?
                        .to_le_bytes(),
                );
            } else if *precision <= 18 {
                buf.extend_from_slice(
                    &i64::try_from(int_val)
                        .map_err(|_| {
                            error!("Decimal value out of range for precision {precision}");
                            Error::InvalidRecord
                        })?
                        .to_le_bytes(),
                );
            } else {
                buf.extend_from_slice(&int_val.to_le_bytes());
            }
        }

        // ── IP addresses ─────────────────────────────────────────────────────
        ChType::IPv4 => {
            let s = coerce_to_string(value)?;
            let addr: std::net::Ipv4Addr = s.parse().map_err(|_| {
                error!("Invalid IPv4 address: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&u32::from(addr).to_le_bytes());
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
            let v = map.get(&*s).ok_or_else(|| {
                error!("Unknown Enum8 value: {s}");
                Error::InvalidRecord
            })?;
            buf.push(*v as u8);
        }
        ChType::Enum16(map) => {
            let s = coerce_to_string(value)?;
            let v = map.get(&*s).ok_or_else(|| {
                error!("Unknown Enum16 value: {s}");
                Error::InvalidRecord
            })?;
            buf.extend_from_slice(&v.to_le_bytes());
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
            let mut entries: Vec<(&str, &OwnedValue)> =
                obj.iter().map(|(k, v)| (k.as_ref(), v)).collect();
            // Simple string-key sort; serialized-key sorting would differ for
            // non-string key types (e.g. Int32 "10" < "2" by bytes but not by string),
            // but non-string-keyed maps are uncommon in practice.
            entries.sort_unstable_by_key(|(k, _)| *k);
            write_varint(entries.len() as u64, buf);
            for (k, v) in entries {
                match key_type.as_ref() {
                    ChType::String => write_string(k.as_bytes(), buf),
                    _ => {
                        let key_val = OwnedValue::String(k.to_owned());
                        serialize_value(&key_val, key_type, buf)?;
                    }
                }
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
                    for (elem, (_, ft)) in arr.iter().zip(field_types.iter()) {
                        serialize_value(elem, ft, buf)?;
                    }
                }
                OwnedValue::Object(obj) => {
                    // Named tuple: look up each field by name to guarantee
                    // correct ordering regardless of JSON key iteration order.
                    if obj.len() != field_types.len() {
                        error!(
                            "Tuple length mismatch: expected {}, got {}",
                            field_types.len(),
                            obj.len()
                        );
                        return Err(Error::InvalidRecord);
                    }
                    for (name_opt, ft) in field_types.iter() {
                        let Some(name) = name_opt else {
                            error!("Cannot serialise unnamed tuple fields from a JSON object");
                            return Err(Error::InvalidRecord);
                        };
                        let Some(v) = obj.get(name.as_str()) else {
                            error!("Tuple field '{name}' not found in JSON object");
                            return Err(Error::InvalidRecord);
                        };
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
pub(crate) fn write_varint(mut n: u64, buf: &mut Vec<u8>) {
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
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => i64::try_from(*n).map_err(|_| {
            error!("Cannot coerce unsigned integer {n} to signed integer");
            Error::InvalidRecord
        }),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => {
            let n = *f as i64;
            if n as f64 != *f {
                error!("Float {f} is not a finite whole number within i64 range");
                return Err(Error::InvalidRecord);
            }
            Ok(n)
        }
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
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => u64::try_from(*n).map_err(|_| {
            error!("Cannot coerce negative integer {n} to unsigned integer");
            Error::InvalidRecord
        }),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => {
            let n = *f as u64;
            if n as f64 != *f {
                error!("Float {f} is not a finite whole number within u64 range");
                return Err(Error::InvalidRecord);
            }
            Ok(n)
        }
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

/// Convert a JSON value to the scaled integer representation used by ClickHouse Decimal types.
///
/// String and integer inputs are converted without going through f64, preserving full precision.
/// Float inputs are accepted with a warning for high-precision columns because the JSON parser
/// has already rounded the value to ~15 significant digits before this function is called.
fn coerce_decimal(value: &OwnedValue, precision: u8, scale: u8) -> Result<i128, Error> {
    let scale_pow = 10i128.pow(u32::from(scale));
    match value {
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => {
            i128::from(*n).checked_mul(scale_pow).ok_or_else(|| {
                error!("Decimal overflow for Decimal({precision}, {scale})");
                Error::InvalidRecord
            })
        }
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => {
            i128::from(*n).checked_mul(scale_pow).ok_or_else(|| {
                error!("Decimal overflow for Decimal({precision}, {scale})");
                Error::InvalidRecord
            })
        }
        OwnedValue::String(s) => parse_decimal_str(s, scale).map_err(|_| {
            error!("Cannot parse '{s}' as Decimal({precision}, {scale})");
            Error::InvalidRecord
        }),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => {
            // f64 has ~15-16 significant digits; values with more will be silently rounded.
            // Decimal64 supports 18 and Decimal128 supports 38. Warn so the caller can act.
            if precision > 15 {
                warn!(
                    "Decimal({precision}, {scale}) received as f64; precision beyond ~15 \
                     significant digits is silently lost. Send the value as a JSON string \
                     to preserve full precision."
                );
            }
            Ok((f * 10f64.powi(i32::from(scale))).round() as i128)
        }
        other => {
            error!("Cannot coerce {other:?} to Decimal({precision}, {scale})");
            Err(Error::InvalidRecord)
        }
    }
}

/// Parse a decimal string (e.g. `"-1234.56"`) into a scaled integer without going through f64.
///
/// The result equals `value * 10^scale`, rounded half-up when the string has more fractional
/// digits than `scale`. Returns `Err(())` for malformed input.
fn parse_decimal_str(s: &str, scale: u8) -> Result<i128, ()> {
    let s = s.trim();
    let (negative, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    if s.is_empty() {
        return Err(());
    }

    let (int_str, frac_str) = match s.find('.') {
        Some(pos) => (&s[..pos], &s[pos + 1..]),
        None => (s, ""),
    };

    let int_val: i128 = if int_str.is_empty() {
        0
    } else {
        int_str.parse().map_err(|_| ())?
    };

    let scale = u32::from(scale);
    let mut result = int_val.checked_mul(10i128.pow(scale)).ok_or(())?;

    if !frac_str.is_empty() {
        // Cap at 38 digits: Decimal128(38) is the widest ClickHouse type, and
        // 10^38 < i128::MAX, so no intermediate value overflows.
        let frac_str = if frac_str.len() > 38 {
            &frac_str[..38]
        } else {
            frac_str
        };
        let frac_len = frac_str.len() as u32;
        let frac_val: i128 = frac_str.parse().map_err(|_| ())?;
        let frac_scaled = if frac_len <= scale {
            frac_val
                .checked_mul(10i128.pow(scale - frac_len))
                .ok_or(())?
        } else {
            // More digits than scale: round half-up.
            let divisor = 10i128.pow(frac_len - scale);
            (frac_val + divisor / 2) / divisor
        };
        result = result.checked_add(frac_scaled).ok_or(())?;
    }

    Ok(if negative { -result } else { result })
}

fn coerce_to_string(value: &OwnedValue) -> Result<Cow<'_, str>, Error> {
    match value {
        OwnedValue::String(s) => Ok(Cow::Borrowed(s.as_str())),
        OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(Cow::Owned(n.to_string())),
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(Cow::Owned(n.to_string())),
        OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(Cow::Owned(f.to_string())),
        OwnedValue::Static(simd_json::StaticNode::Bool(b)) => Ok(Cow::Owned(b.to_string())),
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
        OwnedValue::Static(simd_json::StaticNode::U64(n)) => i64::try_from(*n).map_err(|_| {
            error!("Cannot coerce unsigned integer {n} to signed integer");
            Error::InvalidRecord
        }),
        OwnedValue::String(s) => {
            // Parse "YYYY-MM-DD" manually
            let mut it = s.splitn(3, '-');
            let (y_s, m_s, d_s) = match (it.next(), it.next(), it.next()) {
                (Some(y), Some(m), Some(d)) => (y, m, d),
                _ => {
                    error!("Invalid date string: {s}");
                    return Err(Error::InvalidRecord);
                }
            };
            let (y, m, d) = parse_ymd(y_s, m_s, d_s).map_err(|_| {
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

    let mut it = date_part.splitn(3, '-');
    let (y_s, m_s, d_s) = match (it.next(), it.next(), it.next()) {
        (Some(y), Some(m), Some(d)) => (y, m, d),
        _ => {
            error!("Cannot parse datetime string: {s}");
            return Err(Error::InvalidRecord);
        }
    };
    let (y, m, d) = parse_ymd(y_s, m_s, d_s).map_err(|_| {
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
    let mut it = s.splitn(3, ':');
    let (Some(h_s), Some(min_s)) = (it.next(), it.next()) else {
        return Err(());
    };
    let h: f64 = h_s.parse().map_err(|_| ())?;
    let min: f64 = min_s.parse().map_err(|_| ())?;
    let sec: f64 = it.next().unwrap_or("0").parse().map_err(|_| ())?;
    Ok(h * 3600.0 + min * 60.0 + sec)
}

/// Strip a timezone suffix from a time string. Returns (time_without_tz, offset_seconds).
fn strip_timezone(s: &str) -> (&str, i64) {
    if let Some(stripped) = s.strip_suffix('Z') {
        return (stripped, 0);
    }
    // Look for ±hh:mm or ±hhmm suffix
    #[allow(clippy::collapsible_if)]
    for (sign, ch) in [(1i64, '+'), (-1i64, '-')] {
        if let Some(pos) = s.rfind(ch) {
            if pos > 0 {
                let tz = &s[pos + 1..];
                let secs = if tz.contains(':') {
                    let mut it = tz.splitn(2, ':');
                    match (it.next(), it.next()) {
                        (Some(h_s), Some(m_s)) => {
                            if let (Ok(h), Ok(m)) = (h_s.parse::<i64>(), m_s.parse::<i64>()) {
                                Some(sign * (h * 3600 + m * 60))
                            } else {
                                None
                            }
                        }
                        _ => None,
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
    fn serialize_int8_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(serialize_value(&json_i64(300), &ChType::Int8, &mut buf).is_err());
        assert!(serialize_value(&json_i64(-129), &ChType::Int8, &mut buf).is_err());
    }

    #[test]
    fn serialize_int16_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(serialize_value(&json_i64(32768), &ChType::Int16, &mut buf).is_err());
        assert!(serialize_value(&json_i64(-32769), &ChType::Int16, &mut buf).is_err());
    }

    #[test]
    fn serialize_int32_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(
            serialize_value(&json_i64(i64::from(i32::MAX) + 1), &ChType::Int32, &mut buf).is_err()
        );
        assert!(
            serialize_value(&json_i64(i64::from(i32::MIN) - 1), &ChType::Int32, &mut buf).is_err()
        );
    }

    #[test]
    fn serialize_uint8_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(serialize_value(&json_i64(256), &ChType::UInt8, &mut buf).is_err());
        assert!(serialize_value(&json_i64(-1), &ChType::UInt8, &mut buf).is_err());
    }

    #[test]
    fn serialize_uint16_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(serialize_value(&json_i64(65536), &ChType::UInt16, &mut buf).is_err());
        assert!(serialize_value(&json_i64(-1), &ChType::UInt16, &mut buf).is_err());
    }

    #[test]
    fn serialize_int64_rejects_u64_above_i64_max() {
        let mut buf = vec![];
        assert!(serialize_value(&json_u64(u64::MAX), &ChType::Int64, &mut buf).is_err());
        assert!(serialize_value(&json_u64(i64::MAX as u64 + 1), &ChType::Int64, &mut buf).is_err());
    }

    #[test]
    fn serialize_int64_accepts_u64_at_i64_max() {
        let mut buf = vec![];
        serialize_value(&json_u64(i64::MAX as u64), &ChType::Int64, &mut buf).unwrap();
        assert_eq!(buf, i64::MAX.to_le_bytes());
    }

    #[test]
    fn serialize_int8_rejects_u64_above_i64_max() {
        // Regression: the u64->i64 wrap must not turn u64::MAX into -1 and slip past the i8 range check.
        let mut buf = vec![];
        assert!(serialize_value(&json_u64(u64::MAX), &ChType::Int8, &mut buf).is_err());
    }

    #[test]
    fn float_whole_number_accepted_for_int64() {
        let mut buf = vec![];
        serialize_value(&json_f64(42.0), &ChType::Int64, &mut buf).unwrap();
        assert_eq!(buf, 42i64.to_le_bytes());
    }

    #[test]
    fn float_whole_number_accepted_for_uint64() {
        let mut buf = vec![];
        serialize_value(&json_f64(42.0), &ChType::UInt64, &mut buf).unwrap();
        assert_eq!(buf, 42u64.to_le_bytes());
    }

    #[test]
    fn float_nan_rejected_for_signed_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(f64::NAN), &ChType::Int32, &mut buf).is_err());
    }

    #[test]
    fn float_inf_rejected_for_signed_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(f64::INFINITY), &ChType::Int32, &mut buf).is_err());
        assert!(serialize_value(&json_f64(f64::NEG_INFINITY), &ChType::Int32, &mut buf).is_err());
    }

    #[test]
    fn float_fractional_rejected_for_signed_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(3.15), &ChType::Int32, &mut buf).is_err());
    }

    #[test]
    fn float_nan_rejected_for_unsigned_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(f64::NAN), &ChType::UInt32, &mut buf).is_err());
    }

    #[test]
    fn float_inf_rejected_for_unsigned_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(f64::INFINITY), &ChType::UInt32, &mut buf).is_err());
    }

    #[test]
    fn float_negative_rejected_for_unsigned_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(-1.0), &ChType::UInt32, &mut buf).is_err());
    }

    #[test]
    fn float_fractional_rejected_for_unsigned_integer() {
        let mut buf = vec![];
        assert!(serialize_value(&json_f64(1.5), &ChType::UInt32, &mut buf).is_err());
    }

    #[test]
    fn serialize_uint32_rejects_out_of_range() {
        let mut buf = vec![];
        assert!(
            serialize_value(
                &json_u64(u64::from(u32::MAX) + 1),
                &ChType::UInt32,
                &mut buf
            )
            .is_err()
        );
        assert!(serialize_value(&json_i64(-1), &ChType::UInt32, &mut buf).is_err());
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
    fn serialize_fixed_string_rejects_oversized() {
        let mut buf = vec![];
        let result = serialize_value(&json_str("abcdef"), &ChType::FixedString(3), &mut buf);
        assert!(matches!(result, Err(Error::InvalidRecord)));
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
    fn serialize_uuid_clickhouse_wire_format() {
        let mut buf = vec![];
        // ClickHouse UUID wire format: two LE UInt64 words, low word (bytes 8-15) first.
        // 550e8400-e29b-41d4-a716-446655440000
        //   low  u64 = 0xa716446655440000 → LE: [00 00 44 55 66 44 16 a7]
        //   high u64 = 0x550e8400e29b41d4 → LE: [d4 41 9b e2 00 84 0e 55]
        serialize_value(
            &json_str("550e8400-e29b-41d4-a716-446655440000"),
            &ChType::Uuid,
            &mut buf,
        )
        .unwrap();
        assert_eq!(
            buf,
            [
                0x00, 0x00, 0x44, 0x55, 0x66, 0x44, 0x16, 0xa7, 0xd4, 0x41, 0x9b, 0xe2, 0x00, 0x84,
                0x0e, 0x55,
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
    fn serialize_row_nullable_with_default_explicit_null_writes_null_marker() {
        // Field explicitly null + Nullable column + has_default=true.
        // Explicit null must be stored as NULL, not the server DEFAULT.
        let mut obj = simd_json::owned::Object::new();
        obj.insert("x".into(), json_null());
        let value = OwnedValue::Object(Box::new(obj));
        let columns = vec![col("x", ChType::Nullable(Box::new(ChType::Int32)), true)];
        let mut buf = vec![];
        serialize_row(&value, &columns, &mut buf).unwrap();
        assert_eq!(buf, [0x00, 0x01]); // value follows + null marker, not 0x01 DEFAULT
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
    fn serialize_date_rejects_u64_above_i64_max() {
        let mut buf = vec![];
        assert!(serialize_value(&json_u64(u64::MAX), &ChType::Date, &mut buf).is_err());
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
    fn serialize_datetime64_nanos_exact() {
        let mut buf = vec![];
        // Current-era timestamp: 1_700_000_000 s * 10^9 = 1_700_000_000_000_000_000 ns.
        // The f64 path would round to the nearest 256 ns boundary; the integer path is exact.
        serialize_value(&json_i64(1_700_000_000), &ChType::DateTime64(9), &mut buf).unwrap();
        assert_eq!(buf, 1_700_000_000_000_000_000i64.to_le_bytes());

        buf.clear();
        serialize_value(&json_u64(1_700_000_000), &ChType::DateTime64(9), &mut buf).unwrap();
        assert_eq!(buf, 1_700_000_000_000_000_000i64.to_le_bytes());
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
    fn serialize_pre_1970_datetime_rejected() {
        // "1969-12-31T00:00:00Z" is negative unix seconds. `as u32` used to wrap
        // it to 4294880896; try_from must reject it as out of DateTime range.
        let mut buf = vec![];
        let err = serialize_value(
            &json_str("1969-12-31T00:00:00Z"),
            &ChType::DateTime,
            &mut buf,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidRecord));
        assert!(buf.is_empty());
    }

    #[test]
    fn serialize_pre_1970_date_rejected() {
        let mut buf = vec![];
        let err = serialize_value(&json_str("1969-12-31"), &ChType::Date, &mut buf).unwrap_err();
        assert!(matches!(err, Error::InvalidRecord));
        assert!(buf.is_empty());
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
        // 3.15 * 10^2 = 315 → Int32
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
    fn serialize_decimal32_rejects_out_of_range() {
        let mut buf = vec![];
        // i32::MAX = 2_147_483_647; value 1e9 * 100 (scale=2) = 1e11 > i32::MAX
        assert!(
            serialize_value(&json_f64(1_000_000_000.0), &ChType::Decimal(9, 2), &mut buf).is_err()
        );
    }

    #[test]
    fn serialize_decimal64_rejects_out_of_range() {
        let mut buf = vec![];
        // i64::MAX ~ 9.2e18; value 1e16 * 10^4 (scale=4) = 1e20 > i64::MAX
        assert!(serialize_value(&json_f64(1e16), &ChType::Decimal(18, 4), &mut buf).is_err());
    }

    #[test]
    fn serialize_decimal128_two_word_layout() {
        let mut buf = vec![];
        // Decimal(38, 2): 1.0 → int_val = 100 → written as i128::to_le_bytes()
        serialize_value(&json_f64(1.0), &ChType::Decimal(38, 2), &mut buf).unwrap();
        let mut expected = 100i64.to_le_bytes().to_vec();
        expected.extend_from_slice(&0i64.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn serialize_decimal_from_string_integer() {
        // "315" with scale 2 → 31500 (no fractional part)
        let mut buf = vec![];
        serialize_value(&json_str("315"), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 31500i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_string_exact_scale() {
        let mut buf = vec![];
        serialize_value(&json_str("3.15"), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 315i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_string_fewer_frac_digits_than_scale() {
        // "3.1" with scale 2 → 310 (pad with trailing zero)
        let mut buf = vec![];
        serialize_value(&json_str("3.1"), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 310i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_string_rounds_extra_frac_digits() {
        // "3.155" with scale 2 → 316 (rounds 3.155 → 3.16)
        let mut buf = vec![];
        serialize_value(&json_str("3.155"), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 316i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_string_negative() {
        let mut buf = vec![];
        serialize_value(&json_str("-3.15"), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, (-315i32).to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_i64() {
        // Integer input: 123 with scale 2 → 12300
        let mut buf = vec![];
        serialize_value(&json_i64(123), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 12300i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_from_u64() {
        let mut buf = vec![];
        serialize_value(&json_u64(456), &ChType::Decimal(9, 2), &mut buf).unwrap();
        assert_eq!(buf, 45600i32.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_i64_above_f64_precision() {
        // 2^53 + 1 = 9_007_199_254_740_993 cannot be represented exactly in f64.
        // The integer path must preserve it exactly.
        let v = 9_007_199_254_740_993i64;
        let mut buf = vec![];
        serialize_value(&json_i64(v), &ChType::Decimal(18, 0), &mut buf).unwrap();
        assert_eq!(buf, v.to_le_bytes());
    }

    #[test]
    fn serialize_decimal128_high_precision_string() {
        // 19 significant digits — beyond f64's ~15-digit precision.
        // "123456789012345678.90" with scale 2 must encode as 12345678901234567890.
        // The old f64 path would give 12345678901234568000 (off by 110 units).
        let mut buf = vec![];
        serialize_value(
            &json_str("123456789012345678.90"),
            &ChType::Decimal(38, 2),
            &mut buf,
        )
        .unwrap();
        let expected: i128 = 12_345_678_901_234_567_890;
        assert_eq!(buf, expected.to_le_bytes());
    }

    #[test]
    fn serialize_decimal_invalid_string_is_error() {
        let mut buf = vec![];
        assert!(
            serialize_value(&json_str("not-a-number"), &ChType::Decimal(9, 2), &mut buf).is_err()
        );
    }

    // ── parse_decimal_str unit tests ─────────────────────────────────────────
    #[test]
    fn parse_decimal_str_integer_only() {
        assert_eq!(parse_decimal_str("42", 2), Ok(4200));
    }

    #[test]
    fn parse_decimal_str_exact_scale() {
        assert_eq!(parse_decimal_str("1.23", 2), Ok(123));
    }

    #[test]
    fn parse_decimal_str_fewer_frac_digits() {
        assert_eq!(parse_decimal_str("1.2", 2), Ok(120));
    }

    #[test]
    fn parse_decimal_str_more_frac_digits_rounds_up() {
        assert_eq!(parse_decimal_str("1.235", 2), Ok(124));
    }

    #[test]
    fn parse_decimal_str_more_frac_digits_rounds_down() {
        assert_eq!(parse_decimal_str("1.234", 2), Ok(123));
    }

    #[test]
    fn parse_decimal_str_negative() {
        assert_eq!(parse_decimal_str("-1.23", 2), Ok(-123));
    }

    #[test]
    fn parse_decimal_str_leading_dot() {
        assert_eq!(parse_decimal_str(".5", 0), Ok(1));
    }

    #[test]
    fn parse_decimal_str_zero_scale() {
        assert_eq!(parse_decimal_str("7.9", 0), Ok(8));
    }

    #[test]
    fn parse_decimal_str_empty_is_error() {
        assert!(parse_decimal_str("", 2).is_err());
    }

    #[test]
    fn parse_decimal_str_non_numeric_is_error() {
        assert!(parse_decimal_str("abc", 2).is_err());
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
    fn serialize_map_entries_written_in_sorted_key_order() {
        // Keys inserted out of order; output must be in lexicographic string order.
        // "10" sorts before "a" (string order), not after "1" in numeric order.
        let mut obj = simd_json::owned::Object::new();
        obj.insert("z".into(), json_i64(3));
        obj.insert("m".into(), json_i64(2));
        obj.insert("a".into(), json_i64(1));
        obj.insert("10".into(), json_i64(4));
        let value = OwnedValue::Object(Box::new(obj));
        let mut buf = vec![];
        serialize_value(
            &value,
            &ChType::Map(Box::new(ChType::String), Box::new(ChType::Int32)),
            &mut buf,
        )
        .unwrap();
        let mut expected = vec![4u8]; // varint: 4 entries
        expected.extend_from_slice(&[2, b'1', b'0']); // key "10"
        expected.extend_from_slice(&4i32.to_le_bytes());
        expected.extend_from_slice(&[1, b'a']); // key "a"
        expected.extend_from_slice(&1i32.to_le_bytes());
        expected.extend_from_slice(&[1, b'm']); // key "m"
        expected.extend_from_slice(&2i32.to_le_bytes());
        expected.extend_from_slice(&[1, b'z']); // key "z"
        expected.extend_from_slice(&3i32.to_le_bytes());
        assert_eq!(buf, expected);
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
            &ChType::Tuple(vec![(None, ChType::String), (None, ChType::Int32)]),
            &mut buf,
        )
        .unwrap();
        assert_eq!(&buf[..3], &[0x02, b'h', b'i']); // string "hi"
        assert_eq!(&buf[3..], 7i32.to_le_bytes()); // Int32 7
    }

    #[test]
    fn serialize_tuple_from_json_object() {
        // Tuple(a String, b Int32) as named object: {"b": 7, "a": "hi"}
        // Keys are inserted in reverse order to verify name-based (not positional) lookup.
        let mut obj = simd_json::owned::Object::new();
        obj.insert("b".into(), json_i64(7));
        obj.insert("a".into(), json_str("hi"));
        let value = OwnedValue::Object(Box::new(obj));
        let mut buf = vec![];
        serialize_value(
            &value,
            &ChType::Tuple(vec![
                (Some("a".into()), ChType::String),
                (Some("b".into()), ChType::Int32),
            ]),
            &mut buf,
        )
        .unwrap();
        // Schema order is (a String, b Int32), so "hi" must come before 7
        // regardless of object key insertion order.
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
            &ChType::Tuple(vec![(None, ChType::Int32), (None, ChType::String)]),
            &mut buf,
        );
        assert!(result.is_err());
    }

    // ── ipv4 / ipv6 ──────────────────────────────────────────────────────────
    #[test]
    fn serialize_ipv4() {
        let mut buf = vec![];
        serialize_value(&json_str("127.0.0.1"), &ChType::IPv4, &mut buf).unwrap();
        assert_eq!(buf, [1, 0, 0, 127]); // little-endian u32
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
