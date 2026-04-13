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

use crate::adapter::Row;
use csv::StringRecord;
use iggy_connector_sdk::Error;

// ---------------------------------------------------------------------------
// InfluxDB V2 — annotated CSV
// ---------------------------------------------------------------------------

/// Return `true` if `record` is a CSV header row (contains `"_time"`).
///
/// A header row must contain a `_time` column. The `_value` column is
/// intentionally **not** required: Flux aggregation queries (`count()`,
/// `mean()`, `group()`) produce result tables with columns like `_count` or
/// `_mean` instead of `_value`. Requiring `_value` would cause those header
/// rows to be missed, silently skipping all subsequent data rows until the
/// next recognised header.
///
/// InfluxDB annotation rows (`#group`, `#datatype`, `#default`) are already
/// filtered out earlier in [`parse_csv_rows`] by the leading-`#` check, so
/// they will never reach this function.
fn is_header_record(record: &StringRecord) -> bool {
    record.iter().any(|v| v == "_time")
}

/// Parse an InfluxDB V2 annotated-CSV response body into a list of rows.
///
/// - Annotation rows (first field starts with `#`) are skipped.
/// - Blank lines are skipped.
/// - The first non-annotation row containing `_time` becomes the header.
/// - Repeated identical header rows (multi-table result format) are skipped.
/// - Each subsequent data row is mapped `header[i] → row[i]`.
pub fn parse_csv_rows(csv_text: &str) -> Result<Vec<Row>, Error> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
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

        let mut mapped = Row::new();
        for (idx, key) in active_headers.iter().enumerate() {
            if key.is_empty() {
                continue;
            }
            let value = record.get(idx).unwrap_or("").to_string();
            mapped.insert(key.to_string(), value);
        }

        if !mapped.is_empty() {
            rows.push(mapped);
        }
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// InfluxDB V3 — JSONL (newline-delimited JSON)
// ---------------------------------------------------------------------------

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
pub fn parse_jsonl_rows(jsonl_text: &str) -> Result<Vec<Row>, Error> {
    let mut rows = Vec::new();

    for (line_no, line) in jsonl_text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(line).map_err(|e| {
                Error::InvalidRecordValue(format!(
                    "JSONL parse error on line {}: {e} — raw: {line:?}",
                    line_no + 1
                ))
            })?;

        let row: Row = obj
            .into_iter()
            .map(|(k, v)| {
                let s = match v {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Null => "null".to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    other => other.to_string(),
                };
                (k, s)
            })
            .collect();

        rows.push(row);
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

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
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("42"));
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
    fn csv_maps_all_columns() {
        let csv = "_time,_measurement,_field,_value\n2024-01-01T00:00:00Z,cpu,usage,75.0\n";
        let rows = parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get("_measurement").map(String::as_str), Some("cpu"));
        assert_eq!(row.get("_field").map(String::as_str), Some("usage"));
        assert_eq!(row.get("_value").map(String::as_str), Some("75.0"));
    }

    #[test]
    fn csv_no_data_rows_returns_empty() {
        let csv = "_time,_value\n"; // header only
        let rows = parse_csv_rows(csv).unwrap();
        assert!(rows.is_empty());
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
        assert_eq!(rows[0].get("_measurement").map(String::as_str), Some("cpu"));
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("75.5"));
    }

    #[test]
    fn jsonl_multiple_rows() {
        let jsonl = "{\"_time\":\"2024-01-01T00:00:00Z\",\"v\":1}\n{\"_time\":\"2024-01-01T00:00:01Z\",\"v\":2}\n";
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("v").map(String::as_str), Some("1"));
        assert_eq!(rows[1].get("v").map(String::as_str), Some("2"));
    }

    #[test]
    fn jsonl_skips_blank_lines() {
        let jsonl = "{\"v\":1}\n\n{\"v\":2}\n";
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn jsonl_stringifies_bool_values() {
        let jsonl = r#"{"active":true,"disabled":false}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("active").map(String::as_str), Some("true"));
        assert_eq!(rows[0].get("disabled").map(String::as_str), Some("false"));
    }

    #[test]
    fn jsonl_stringifies_null() {
        let jsonl = r#"{"field":null}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("field").map(String::as_str), Some("null"));
    }

    #[test]
    fn jsonl_string_values_unquoted() {
        let jsonl = r#"{"host":"server1"}"#;
        let rows = parse_jsonl_rows(jsonl).unwrap();
        assert_eq!(rows[0].get("host").map(String::as_str), Some("server1"));
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
}
