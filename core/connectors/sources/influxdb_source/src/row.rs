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

//! Query-response parsers for InfluxDB V2 (annotated CSV) and V3 (JSONL).
//!
//! Both parsers produce `Vec<Row>` — a list of field-name → string-value maps.
//! The cursor-tracking and payload-building logic in the source connector
//! operates on this common representation so it runs unchanged regardless of
//! which InfluxDB version is in use.

use csv::StringRecord;
use iggy_connector_sdk::Error;
use simd_json::BorrowedValue;
use std::collections::HashMap;

/// A single row returned by a query, field name → typed JSON value.
///
/// V2 (annotated CSV) stores all values as `Value::String` since CSV has no
/// type information; `parse_scalar` in `build_payload` converts them to typed
/// values when building the message payload. V3 (JSONL) stores typed values
/// directly — numbers, booleans, and nulls arrive pre-typed from SQL, so no
/// string-round-trip parse is needed.
pub(crate) type Row = HashMap<String, serde_json::Value>;

// ── InfluxDB V2 — annotated CSV ───────────────────────────────────────────────

/// Return `true` if `record` is a CSV header row.
///
/// Checks for any of the standard InfluxDB temporal column names:
/// `_time`, `_start`, or `_stop`. Regular time-series queries include `_time`;
/// Flux window-aggregate queries (`count()`, `mean()`, `distinct()`) produce
/// result tables with `_start` and `_stop` but no `_time`. Requiring only
/// `_time` would cause those header rows to be missed, silently dropping all
/// subsequent data rows until the next recognised header.
///
/// InfluxDB annotation rows (`#group`, `#datatype`, `#default`) are already
/// filtered out earlier in [`parse_csv_rows`] by the leading-`#` check, so
/// they will never reach this function.
fn is_header_record(record: &StringRecord) -> bool {
    record
        .iter()
        .any(|v| v == "_time" || v == "_start" || v == "_stop")
}

/// Parse an InfluxDB V2 annotated-CSV response body into a list of rows.
///
/// - Annotation rows (first field starts with `#`) are skipped.
/// - Blank lines are skipped.
/// - The first non-annotation row containing `_time`, `_start`, or `_stop` becomes the header.
/// - Repeated identical header rows (multi-table result format) are skipped.
/// - Each subsequent data row is mapped `header[i] → row[i]`.
pub(crate) fn parse_csv_rows(csv_text: &str) -> Result<Vec<Row>, Error> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // multi-table results have variable column counts per table
        .from_reader(csv_text.as_bytes());

    let mut headers: Option<StringRecord> = None;
    let mut rows = Vec::new();

    for result in reader.records() {
        let record =
            result.map_err(|e| Error::InvalidRecordValue(format!("Invalid CSV record: {e}")))?;

        if record.is_empty() {
            continue;
        }

        if let Some(first) = record.get(0)
            && first.starts_with('#')
        {
            continue;
        }

        if is_header_record(&record) {
            headers = Some(record.clone());
            continue;
        }

        let Some(active_headers) = headers.as_ref() else {
            continue;
        };

        // Skip repeated header rows (multi-table result format)
        if record == *active_headers {
            continue;
        }

        let mut mapped = Row::with_capacity(active_headers.len());
        for (idx, key) in active_headers.iter().enumerate() {
            if key.is_empty() {
                continue;
            }
            let value = record.get(idx).unwrap_or("").to_string();
            mapped.insert(key.to_string(), serde_json::Value::String(value));
        }

        if !mapped.is_empty() {
            rows.push(mapped);
        }
    }

    Ok(rows)
}

// ── InfluxDB V3 — JSONL (newline-delimited JSON) ──────────────────────────────

/// Parse an InfluxDB V3 JSONL response body into a list of rows.
///
/// Each non-empty line must be a JSON object. Field values of any JSON type
/// are stringified to `String`:
/// - `null` → `"null"`
/// - `bool` → `"true"` / `"false"`
/// - `number` → decimal representation
/// - `string` → value as-is (no extra quotes)
/// - `array` / `object` → compact JSON representation
///
/// Blank lines are silently skipped. Lines that fail to parse as JSON objects
/// return an error.
///
/// Uses `simd_json` for accelerated JSON tokenization in the hot path.
/// `simd_json::from_slice` requires `&mut [u8]` and modifies the bytes in
/// place for zero-copy SIMD parsing; we clone each line into a `Vec<u8>` to
/// satisfy the mutability requirement without borrowing the original string.
fn parse_object(value: BorrowedValue<'_>) -> Result<Row, Error> {
    let BorrowedValue::Object(map) = value else {
        return Err(Error::InvalidRecordValue(
            "expected a JSON object in JSONL response".to_string(),
        ));
    };
    let mut row = Row::with_capacity(map.len());
    for (k, v) in map.iter() {
        // Convert simd_json BorrowedValue → serde_json::Value directly, preserving
        // the original type. Numbers and booleans are kept typed so build_payload
        // can emit them without a string-round-trip through parse_scalar.
        let json_val = match v {
            BorrowedValue::Static(simd_json::StaticNode::Null) => serde_json::Value::Null,
            BorrowedValue::Static(simd_json::StaticNode::Bool(b)) => serde_json::Value::Bool(*b),
            BorrowedValue::Static(simd_json::StaticNode::I64(n)) => {
                serde_json::Value::Number((*n).into())
            }
            BorrowedValue::Static(simd_json::StaticNode::U64(n)) => {
                serde_json::Value::Number((*n).into())
            }
            BorrowedValue::Static(simd_json::StaticNode::F64(n)) => {
                serde_json::Number::from_f64(*n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            BorrowedValue::String(s) => serde_json::Value::String(s.to_string()),
            // Arrays/objects: serialize via simd_json then re-parse as serde_json.
            // InfluxDB V3 SQL results are flat, so this path is rarely hit.
            other => serde_json::to_value(other)
                .map_err(|e| Error::InvalidRecordValue(format!("JSON conversion error: {e}")))?,
        };
        row.insert(k.to_string(), json_val);
    }
    Ok(row)
}

pub fn parse_jsonl_rows(data: &str) -> Result<Vec<Row>, Error> {
    let mut rows = Vec::new();
    let mut scratch = Vec::new(); // reused across lines
    for line in data.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(trimmed.as_bytes());
        let obj: simd_json::BorrowedValue = simd_json::to_borrowed_value(&mut scratch)
            .map_err(|e| Error::InvalidRecordValue(format!("JSON parse error: {e}")))?;
        rows.push(parse_object(obj)?);
    }
    Ok(rows)
}
#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_csv_rows ───────────────────────────────────────────────────────

    #[test]
    fn csv_empty_string_returns_empty() {
        assert!(parse_csv_rows("").unwrap().is_empty());
    }

    #[test]
    fn csv_skips_annotation_rows() {
        let csv = "#group,false\n#datatype,string\n_time,_value\n2024-01-01T00:00:00Z,42\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_value").and_then(|v| v.as_str()), Some("42"));
    }

    #[test]
    fn csv_skips_blank_lines() {
        let csv = "_time,_value\n2024-01-01T00:00:00Z,1\n\n_time,_value\n2024-01-01T00:00:01Z,2\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2, "expected 2 data rows, got {}", rows.len());
    }

    #[test]
    fn csv_skips_repeated_header_rows() {
        let csv = "_time,_value\n2024-01-01T00:00:00Z,10\n_time,_value\n2024-01-01T00:00:01Z,20\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn csv_new_table_different_columns_updates_headers() {
        // Multi-table result: second table has an extra _measurement column.
        // The parser should recognise the new header row and update accordingly.
        let csv = "_time,_value\n\
                   2024-01-01T00:00:00Z,10\n\
                   _time,_measurement,_value\n\
                   2024-01-01T00:00:01Z,cpu,20\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2);
        //assert!(rows[0].contains_key("_measurement"));
        assert!(!rows[0].contains_key("_measurement"));
        assert_eq!(
            rows[1].get("_measurement").and_then(|v| v.as_str()),
            Some("cpu")
        );
    }

    #[test]
    fn csv_maps_all_columns() {
        let csv = "_time,_measurement,_field,_value\n2024-01-01T00:00:00Z,cpu,usage,75.0\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.get("_measurement").and_then(|v| v.as_str()),
            Some("cpu")
        );
        assert_eq!(row.get("_field").and_then(|v| v.as_str()), Some("usage"));
        assert_eq!(row.get("_value").and_then(|v| v.as_str()), Some("75.0"));
    }

    #[test]
    fn csv_no_data_rows_returns_empty() {
        let csv = "_time,_value\n"; // header only
        let rows = parse_csv_rows(csv).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn csv_aggregation_query_without_time_column_parses_rows() {
        // Flux window-aggregate queries (count(), mean(), etc.) produce result
        // tables with _start and _stop but no _time. Before the _start/_stop fix,
        // is_header_record returned false, headers stayed None, and all data rows
        // were silently dropped.
        let csv = "_start,_stop,_field,_value\n\
                   2024-01-01T00:00:00Z,2024-01-01T01:00:00Z,usage,42\n\
                   2024-01-01T01:00:00Z,2024-01-01T02:00:00Z,usage,55\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2, "rows must not be silently dropped");
        assert_eq!(rows[0].get("_value").and_then(|v| v.as_str()), Some("42"));
        assert_eq!(rows[1].get("_value").and_then(|v| v.as_str()), Some("55"));
    }

    #[test]
    fn csv_stop_only_header_is_recognised() {
        let csv = "_stop,_count\n2024-01-01T01:00:00Z,7\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_count").and_then(|v| v.as_str()), Some("7"));
    }

    // ── parse_jsonl_rows ─────────────────────────────────────────────────────

    #[test]
    fn jsonl_empty_string_returns_empty() {
        assert!(parse_jsonl_rows("").unwrap().is_empty());
    }

    #[test]
    fn jsonl_single_row() {
        let jsonl = r#"{"_time":"2024-01-01T00:00:00Z","_measurement":"cpu","_value":75.5}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("_measurement").and_then(|v| v.as_str()),
            Some("cpu")
        );
        // Numbers remain typed — no string round-trip.
        assert_eq!(rows[0].get("_value").and_then(|v| v.as_f64()), Some(75.5));
    }

    #[test]
    fn jsonl_multiple_rows() {
        let jsonl = "{\"_time\":\"2024-01-01T00:00:00Z\",\"v\":1}\n{\"_time\":\"2024-01-01T00:00:01Z\",\"v\":2}\n";
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("v").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(rows[1].get("v").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn jsonl_skips_blank_lines() {
        let jsonl = "{\"v\":1}\n\n{\"v\":2}\n";
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn jsonl_bool_values_remain_typed() {
        let jsonl = r#"{"active":true,"disabled":false}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("active"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(
            rows[0].get("disabled"),
            Some(&serde_json::Value::Bool(false))
        );
    }

    #[test]
    fn jsonl_null_value_remains_typed() {
        let jsonl = r#"{"field":null}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("field"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn jsonl_string_values_unquoted() {
        let jsonl = r#"{"host":"server1"}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(
            rows[0].get("host").and_then(|v| v.as_str()),
            Some("server1")
        );
    }

    #[test]
    fn jsonl_invalid_json_returns_error() {
        let jsonl = "not json\n";
        assert!(parse_jsonl_rows(jsonl).is_err());
    }

    #[test]
    fn jsonl_trailing_newline_ok() {
        let jsonl = "{\"v\":42}\n";
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // ── parse_jsonl_rows — additional type coverage ──────────────────────────

    #[test]
    fn jsonl_negative_integer_uses_i64_branch() {
        // Negative integers are stored as I64 by simd_json; positive ones are U64.
        let jsonl = r#"{"delta":-42,"count":7}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("delta").and_then(|v| v.as_i64()), Some(-42));
        assert_eq!(rows[0].get("count").and_then(|v| v.as_u64()), Some(7));
    }

    #[test]
    fn jsonl_nested_array_value_is_serialized_as_json() {
        // The "other" arm in parse_object handles arrays and nested objects.
        let jsonl = r#"{"tags":["a","b"]}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        let tags = rows[0].get("tags").unwrap();
        assert!(tags.is_array(), "expected array value, got {tags:?}");
    }

    #[test]
    fn jsonl_non_object_line_returns_error() {
        // A JSONL line that is not a JSON object (e.g. a bare array) must be rejected.
        let jsonl = "[1,2,3]\n";
        assert!(parse_jsonl_rows(jsonl).is_err());
    }

    // ── parse_csv_rows — path coverage ──────────────────────────────────────

    #[test]
    fn csv_data_row_before_any_header_is_skipped() {
        // A data row that appears before any _time/_start/_stop header row must be
        // silently ignored (headers is None → continue).
        let csv = "random,data\n_time,_value\n2024-01-01T00:00:00Z,1\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "only the row after the header should be returned"
        );
        assert_eq!(rows[0].get("_value").and_then(|v| v.as_str()), Some("1"));
    }

    #[test]
    fn csv_empty_header_column_name_is_skipped() {
        // InfluxDB annotated CSV sometimes has a leading empty column (annotation prefix).
        // Empty column names must be skipped so the row map stays clean.
        let csv = "_time,,_value\n2024-01-01T00:00:00Z,extra,42\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            !rows[0].contains_key(""),
            "empty key must not appear in row"
        );
        assert_eq!(rows[0].get("_value").and_then(|v| v.as_str()), Some("42"));
    }
}
