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

/// Write an escaped measurement name into `buf`.
///
/// Escapes: `\` → `\\`, `,` → `\,`, ` ` → `\ `, `\t` → `\\t`, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline, carriage-return, and tab are the InfluxDB line-protocol record
/// delimiters or whitespace that can corrupt parsing; a literal newline inside
/// a measurement name would split the line and corrupt the batch.
pub fn write_measurement(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            ' ' => buf.push_str("\\ "),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped tag key/value into `buf`.
///
/// Escapes: `\` → `\\`, `,` → `\,`, `=` → `\=`, ` ` → `\ `, `\t` → `\\t`, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline, carriage-return, and tab are escaped for the same reason as in
/// [`write_measurement`]: they are InfluxDB line-protocol record delimiters or
/// whitespace that can corrupt tag-set parsing.
pub fn write_tag_value(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            '=' => buf.push_str("\\="),
            ' ' => buf.push_str("\\ "),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped string field value (without surrounding quotes) into `buf`.
///
/// Escapes: `\` → `\\`, `"` → `\"`, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline and carriage-return are the InfluxDB line-protocol record
/// delimiters; a literal newline inside a string field value would split the
/// line and corrupt the batch.
///
/// Tab (`\t`) is intentionally NOT escaped here. String field values are
/// double-quoted in line protocol, and the spec permits literal tabs inside
/// quoted strings. Measurement names and tag values (see [`write_measurement`]
/// and [`write_tag_value`]) are unquoted, so tabs must be escaped there to
/// avoid mis-parsing the tag set.
pub fn write_field_string(buf: &mut String, value: &str) {
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
        write_measurement(&mut buf, "m\\eas,urea meant");
        assert_eq!(buf, "m\\\\eas\\,urea\\ meant");
    }

    #[test]
    fn measurement_escapes_newlines() {
        let mut buf = String::new();
        write_measurement(&mut buf, "meas\nurea\rment");
        assert_eq!(buf, "meas\\nurea\\rment");
    }

    #[test]
    fn tag_value_escapes_equals_sign() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "a=b,c d\\e");
        assert_eq!(buf, "a\\=b\\,c\\ d\\\\e");
    }

    #[test]
    fn tag_value_escapes_newlines() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "line1\nline2\r");
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
        write_measurement(&mut buf, "cpu_usage");
        assert_eq!(buf, "cpu_usage");
    }

    #[test]
    fn tag_value_plain_ascii_unchanged() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "server01");
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
        write_measurement(&mut buf, "");
        assert!(buf.is_empty());
    }

    #[test]
    fn tag_value_empty_string_produces_empty_output() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "");
        assert!(buf.is_empty());
    }

    #[test]
    fn field_string_empty_string_produces_empty_output() {
        let mut buf = String::new();
        write_field_string(&mut buf, "");
        assert!(buf.is_empty());
    }

    #[test]
    fn measurement_escapes_tab() {
        let mut buf = String::new();
        write_measurement(&mut buf, "m\teasure");
        assert_eq!(buf, "m\\teasure");
    }

    #[test]
    fn tag_value_escapes_tab() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "val\tue");
        assert_eq!(buf, "val\\tue");
    }

    #[test]
    fn measurement_unicode_passthrough() {
        let mut buf = String::new();
        write_measurement(&mut buf, "温度");
        assert_eq!(buf, "温度");
    }

    #[test]
    fn tag_value_unicode_passthrough() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "µ-sensor");
        assert_eq!(buf, "µ-sensor");
    }

    #[test]
    fn field_string_unicode_passthrough() {
        let mut buf = String::new();
        write_field_string(&mut buf, "café");
        assert_eq!(buf, "café");
    }
}
