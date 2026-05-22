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

//! InfluxDB line-protocol escaping helpers.
//!
//! Both InfluxDB V2 and V3 use the same line-protocol format for writes, so
//! these functions are shared by both connector versions.

use iggy_connector_sdk::Error;

/// Write an escaped measurement name into `buf`.
///
/// Escapes: `\` → `\\`, `,` → `\,`, ` ` → `\ `, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline and carriage-return are the InfluxDB line-protocol record
/// delimiters that can corrupt parsing; a literal newline inside
/// a measurement name would split the line and corrupt the batch.
/// Tab characters are rejected with an error as they are not valid
/// in the InfluxDB line-protocol spec and would corrupt tag-set parsing.
#[inline]
pub(crate) fn write_measurement(buf: &mut String, value: &str) -> Result<(), Error> {
    if value.contains('\t') {
        return Err(Error::InvalidConfigValue(
            "measurement name must not contain tab characters — tabs are not valid \
             in the InfluxDB line-protocol spec and would corrupt tag-set parsing"
                .into(),
        ));
    }
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            ' ' => buf.push_str("\\ "),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
    Ok(())
}

/// Write an escaped tag key/value into `buf`.
///
/// Escapes: `\` → `\\`, `,` → `\,`, `=` → `\=`, ` ` → `\ `, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline and carriage-return are InfluxDB line-protocol record delimiters that
/// can corrupt tag-set parsing.
/// Tab characters are rejected with an error as they are not valid
/// in the InfluxDB line-protocol spec and would corrupt tag-set parsing.
#[inline]
pub(crate) fn write_tag_value(buf: &mut String, value: &str) -> Result<(), Error> {
    if value.contains('\t') {
        return Err(Error::InvalidConfigValue(
            "tag value must not contain tab characters — tabs are not valid \
             in the InfluxDB line-protocol spec and would corrupt tag-set parsing"
                .into(),
        ));
    }
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            '=' => buf.push_str("\\="),
            ' ' => buf.push_str("\\ "),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
    Ok(())
}

/// Write an escaped string field value (without surrounding quotes) into `buf`.
///
/// Escapes: `\` → `\\`, `"` → `\"`, `\n` → `\\n`, `\r` → `\\r`
///
/// NOTE: Escaping `\n` and `\r` inside quoted string field values is a
/// deliberate divergence from the InfluxDB line-protocol spec (which only
/// mandates `\\` and `\"` inside quoted strings). The divergence prevents
/// line-splitting in downstream consumers that use raw newlines as record
/// delimiters — e.g. log shippers, stream processors, and InfluxDB's own
/// bulk-write HTTP parser if the body is re-streamed line by line.
///
/// Tab (`\t`) is intentionally NOT escaped here. String field values are
/// double-quoted in line protocol, and the spec permits literal tabs inside
/// quoted strings. Measurement names and tag values (see [`write_measurement`]
/// and [`write_tag_value`]) are unquoted, so tabs must be escaped there to
/// avoid misparsing the tag set.
#[inline]
pub(crate) fn write_field_string(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            '"' => buf.push_str("\\\""),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_escapes_comma_space_backslash() {
        let mut buf = String::new();
        write_measurement(&mut buf, "m\\eas,urea meant").unwrap();
        assert_eq!(buf, "m\\\\eas\\,urea\\ meant");
    }

    #[test]
    fn measurement_escapes_newlines() {
        let mut buf = String::new();
        write_measurement(&mut buf, "meas\nurea\rment").unwrap();
        assert_eq!(buf, "meas\\nurea\\rment");
    }

    #[test]
    fn tag_value_escapes_equals_sign() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "a=b,c d\\e").unwrap();
        assert_eq!(buf, "a\\=b\\,c\\ d\\\\e");
    }

    #[test]
    fn tag_value_escapes_newlines() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "line1\nline2\r").unwrap();
        assert_eq!(buf, "line1\\nline2\\r");
    }

    #[test]
    fn field_string_escapes_quote_and_backslash() {
        let mut buf = String::new();
        write_field_string(&mut buf, r#"say "hello" \world\"#);
        assert_eq!(buf, r#"say \"hello\" \\world\\"#);
    }

    #[test]
    fn field_string_escapes_newlines() {
        let mut buf = String::new();
        write_field_string(&mut buf, "line1\nline2\r");
        assert_eq!(buf, "line1\\nline2\\r");
    }

    #[test]
    fn measurement_plain_ascii_unchanged() {
        let mut buf = String::new();
        write_measurement(&mut buf, "cpu_usage").unwrap();
        assert_eq!(buf, "cpu_usage");
    }

    #[test]
    fn tag_value_plain_ascii_unchanged() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "server01").unwrap();
        assert_eq!(buf, "server01");
    }

    #[test]
    fn field_string_plain_ascii_unchanged() {
        let mut buf = String::new();
        write_field_string(&mut buf, "hello world");
        assert_eq!(buf, "hello world");
    }

    #[test]
    fn measurement_empty_string_produces_empty_output() {
        let mut buf = String::new();
        write_measurement(&mut buf, "").unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn tag_value_empty_string_produces_empty_output() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "").unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn field_string_empty_string_produces_empty_output() {
        let mut buf = String::new();
        write_field_string(&mut buf, "");
        assert!(buf.is_empty());
    }

    #[test]
    fn measurement_rejects_tab() {
        let mut buf = String::new();
        assert!(write_measurement(&mut buf, "m\teasure").is_err());
    }

    #[test]
    fn tag_value_rejects_tab() {
        let mut buf = String::new();
        assert!(write_tag_value(&mut buf, "val\tue").is_err());
    }

    #[test]
    fn measurement_unicode_passthrough() {
        let mut buf = String::new();
        write_measurement(&mut buf, "温度").unwrap();
        assert_eq!(buf, "温度");
    }

    #[test]
    fn tag_value_unicode_passthrough() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "µ-sensor").unwrap();
        assert_eq!(buf, "µ-sensor");
    }

    #[test]
    fn field_string_unicode_passthrough() {
        let mut buf = String::new();
        write_field_string(&mut buf, "café");
        assert_eq!(buf, "café");
    }
}
