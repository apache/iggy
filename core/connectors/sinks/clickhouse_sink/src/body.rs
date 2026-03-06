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

//! HTTP request body builders for each INSERT format.
//!
//! Each function accepts a slice of [`ConsumedMessage`]s and returns the raw
//! bytes that will be sent to ClickHouse. Payloads of the wrong type for the
//! chosen format are skipped with a warning rather than erroring, so a mixed
//! batch never causes a complete failure.

use crate::StringFormat;
use crate::schema::Column;
use iggy_connector_sdk::{ConsumedMessage, Error, Payload};
use tracing::warn;

// ─── Body builders ───────────────────────────────────────────────────────────

/// Build a newline-delimited JSON body for `FORMAT JSONEachRow`.
/// Each `Payload::Json` message becomes one line. Other payload types are skipped.
pub(crate) fn build_json_body(messages: &[ConsumedMessage]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                if simd_json::to_writer(&mut buf, value).is_ok() {
                    buf.push(b'\n');
                } else {
                    warn!("Failed to serialise JSON payload at offset {}", msg.offset);
                }
            }
            other => {
                warn!(
                    "JSONEachRow mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    buf
}

/// Build a RowBinaryWithDefaults body.
/// Each `Payload::Json` message is serialised to binary using the table schema.
pub(crate) fn build_row_binary_body(
    messages: &[ConsumedMessage],
    schema: &[Column],
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(messages.len() * 128);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                crate::binary::serialize_row(value, schema, &mut buf)?;
            }
            other => {
                warn!(
                    "RowBinary mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    Ok(buf)
}

/// Build a raw string body for CSV / TSV / JSONEachRow string passthrough.
/// Each `Payload::Text` message is written as-is with a trailing newline
/// appended for CSV/TSV if not already present.
pub(crate) fn build_string_body(
    messages: &[ConsumedMessage],
    string_format: StringFormat,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Text(s) => {
                buf.extend_from_slice(s.as_bytes());
                if string_format.requires_newline() && !s.ends_with('\n') {
                    buf.push(b'\n');
                }
            }
            other => {
                warn!(
                    "String passthrough mode: skipping unsupported payload type {:?} at offset {}",
                    payload_type_name(other),
                    msg.offset
                );
            }
        }
    }
    buf
}

fn payload_type_name(p: &Payload) -> &'static str {
    match p {
        Payload::Json(_) => "Json",
        Payload::Raw(_) => "Raw",
        Payload::Text(_) => "Text",
        Payload::Proto(_) => "Proto",
        Payload::FlatBuffer(_) => "FlatBuffer",
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringFormat;
    use crate::schema::{ChType, Column};
    use iggy_connector_sdk::{ConsumedMessage, Payload};
    use simd_json::{OwnedValue, StaticNode};

    fn msg(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 0,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    fn col(name: &str, ch_type: ChType) -> Column {
        Column {
            name: name.into(),
            ch_type,
            has_default: false,
        }
    }

    fn json_null() -> OwnedValue {
        OwnedValue::Static(StaticNode::Null)
    }

    // ── build_json_body ──────────────────────────────────────────────────────

    /// Expects each line to be a valid JSON object
    #[test]
    fn json_body_empty_input_returns_empty_buf() {
        assert!(build_json_body(&[]).is_empty());
    }

    #[test]
    fn json_body_null_payload_produces_null_line() {
        let messages = vec![msg(Payload::Json(json_null()))];
        assert_eq!(build_json_body(&messages), b"null\n");
    }

    #[test]
    fn json_body_appends_one_line_per_message() {
        let messages = vec![
            msg(Payload::Json(json_null())),
            msg(Payload::Json(json_null())),
        ];
        assert_eq!(build_json_body(&messages), b"null\nnull\n");
    }

    #[test]
    fn json_body_non_json_payload_is_skipped() {
        let messages = vec![msg(Payload::Text("hello".into()))];
        assert!(build_json_body(&messages).is_empty());
    }

    #[test]
    fn json_body_mixed_payloads_only_json_included() {
        let messages = vec![
            msg(Payload::Json(json_null())),
            msg(Payload::Text("skipped".into())),
            msg(Payload::Raw(vec![1, 2, 3])),
            msg(Payload::Json(json_null())),
        ];
        assert_eq!(build_json_body(&messages), b"null\nnull\n");
    }

    // ── build_string_body ────────────────────────────────────────────────────

    #[test]
    fn string_body_empty_input_returns_empty_buf() {
        assert!(build_string_body(&[], StringFormat::Csv).is_empty());
    }

    #[test]
    fn string_body_csv_appends_newline_when_missing() {
        let messages = vec![msg(Payload::Text("a,b,c".into()))];
        assert_eq!(build_string_body(&messages, StringFormat::Csv), b"a,b,c\n");
    }

    #[test]
    fn string_body_csv_does_not_double_newline() {
        let messages = vec![msg(Payload::Text("a,b,c\n".into()))];
        assert_eq!(build_string_body(&messages, StringFormat::Csv), b"a,b,c\n");
    }

    #[test]
    fn string_body_tsv_appends_newline_when_missing() {
        let messages = vec![msg(Payload::Text("a\tb\tc".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::Tsv),
            b"a\tb\tc\n"
        );
    }

    #[test]
    fn string_body_json_each_row_does_not_append_newline() {
        // JSONEachRow strings are already self-delimiting; no newline should be added.
        let messages = vec![msg(Payload::Text("{\"k\":1}".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::JsonEachRow),
            b"{\"k\":1}"
        );
    }

    #[test]
    fn string_body_non_text_payload_is_skipped() {
        let messages = vec![
            msg(Payload::Raw(vec![1, 2, 3])),
            msg(Payload::Json(json_null())),
        ];
        assert!(build_string_body(&messages, StringFormat::Csv).is_empty());
    }

    // ── build_row_binary_body ────────────────────────────────────────────────

    #[test]
    fn row_binary_body_empty_input_returns_empty_buf() {
        assert!(build_row_binary_body(&[], &[]).unwrap().is_empty());
    }

    #[test]
    fn row_binary_body_non_json_payload_is_skipped() {
        let messages = vec![
            msg(Payload::Text("hello".into())),
            msg(Payload::Raw(vec![0xFF])),
        ];
        let body = build_row_binary_body(&messages, &[col("x", ChType::String)]).unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn row_binary_body_json_payload_writes_bytes() {
        // Schema: one non-nullable String column named "name".
        // JSON: {"name": "alice"}
        let mut obj = simd_json::owned::Object::with_capacity(1);
        obj.insert("name".to_string(), OwnedValue::String("alice".into()));
        let messages = vec![msg(Payload::Json(OwnedValue::Object(Box::new(obj))))];
        let schema = vec![col("name", ChType::String)];
        let body = build_row_binary_body(&messages, &schema).unwrap();
        // RowBinaryWithDefaults: 0x00 (value follows) + LEB128 length (5) + UTF-8 bytes
        assert_eq!(body, b"\x00\x05alice");
    }

    #[test]
    fn row_binary_body_multiple_rows_concatenated() {
        let mut obj1 = simd_json::owned::Object::with_capacity(1);
        obj1.insert("n".to_string(), OwnedValue::String("x".into()));
        let mut obj2 = simd_json::owned::Object::with_capacity(1);
        obj2.insert("n".to_string(), OwnedValue::String("y".into()));
        let messages = vec![
            msg(Payload::Json(OwnedValue::Object(Box::new(obj1)))),
            msg(Payload::Json(OwnedValue::Object(Box::new(obj2)))),
        ];
        let schema = vec![col("n", ChType::String)];
        let body = build_row_binary_body(&messages, &schema).unwrap();
        // Two rows: 0x00 (value follows) + \x01x and 0x00 + \x01y
        assert_eq!(body, b"\x00\x01x\x00\x01y");
    }
}