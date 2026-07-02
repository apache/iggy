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

use crate::OutputFormat;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::Error;

pub(crate) struct PathContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
    pub first_timestamp_micros: u64,
}

pub(crate) fn render_s3_key(
    prefix: Option<&str>,
    template: &str,
    ctx: &PathContext<'_>,
    offset_start: u64,
    offset_end: u64,
    format: OutputFormat,
) -> Result<String, Error> {
    let rendered = render_template(template, ctx)?;

    // Partition ID is always embedded in the filename to prevent cross-partition
    // key collisions (partitions have independent offset spaces starting at 0).
    let filename = format!(
        "{:05}-{:020}-{:020}.{}",
        ctx.partition_id,
        offset_start,
        offset_end,
        format.file_extension()
    );

    let key = match prefix {
        Some(p) => {
            let p = p.trim_matches('/');
            if p.is_empty() {
                format!("{rendered}/{filename}")
            } else {
                format!("{p}/{rendered}/{filename}")
            }
        }
        None => format!("{rendered}/{filename}"),
    };

    Ok(key)
}

fn render_template(template: &str, ctx: &PathContext<'_>) -> Result<String, Error> {
    let dt = timestamp_to_datetime(ctx.first_timestamp_micros)?;
    let date = dt.format("%Y-%m-%d").to_string();
    let hour = dt.format("%H").to_string();
    let ts_millis = (ctx.first_timestamp_micros / 1_000).to_string();

    Ok(template
        .replace("{stream}", &sanitize_key_segment(ctx.stream))
        .replace("{topic}", &sanitize_key_segment(ctx.topic))
        .replace("{partition}", &ctx.partition_id.to_string())
        .replace("{date}", &date)
        .replace("{hour}", &hour)
        .replace("{timestamp}", &ts_millis))
}

/// Replace characters that produce ambiguous or hard-to-list S3 key segments.
/// Keeps `[a-zA-Z0-9._-]`, replaces everything else with `_`.
fn sanitize_key_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn timestamp_to_datetime(micros: u64) -> Result<DateTime<Utc>, Error> {
    let secs = (micros / 1_000_000) as i64;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
        Error::CannotStoreData(format!(
            "Invalid message timestamp: {micros} micros is out of range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> PathContext<'static> {
        PathContext {
            stream: "app_logs",
            topic: "api_requests",
            partition_id: 1,
            first_timestamp_micros: 1_710_597_600_000_000, // 2024-03-16T14:00:00Z
        }
    }

    #[test]
    fn render_default_template() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some("iggy/raw"),
            "{stream}/{topic}/{date}/{hour}",
            &ctx,
            0,
            99,
            OutputFormat::JsonLines,
        )
        .unwrap();
        assert_eq!(
            key,
            "iggy/raw/app_logs/api_requests/2024-03-16/14/00001-00000000000000000000-00000000000000000099.jsonl"
        );
    }

    #[test]
    fn render_with_partition() {
        let ctx = test_ctx();
        let key = render_s3_key(
            None,
            "{stream}/{topic}/{partition}/{date}",
            &ctx,
            100,
            199,
            OutputFormat::JsonArray,
        )
        .unwrap();
        assert_eq!(
            key,
            "app_logs/api_requests/1/2024-03-16/00001-00000000000000000100-00000000000000000199.json"
        );
    }

    #[test]
    fn render_no_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(None, "{stream}/{topic}", &ctx, 0, 9, OutputFormat::Raw).unwrap();
        assert_eq!(
            key,
            "app_logs/api_requests/00001-00000000000000000000-00000000000000000009.bin"
        );
    }

    #[test]
    fn render_empty_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(Some(""), "{stream}", &ctx, 0, 0, OutputFormat::JsonLines).unwrap();
        assert_eq!(
            key,
            "app_logs/00001-00000000000000000000-00000000000000000000.jsonl"
        );
    }

    #[test]
    fn render_prefix_with_trailing_slash() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some("data/"),
            "{topic}",
            &ctx,
            5,
            10,
            OutputFormat::JsonLines,
        )
        .unwrap();
        assert_eq!(
            key,
            "data/api_requests/00001-00000000000000000005-00000000000000000010.jsonl"
        );
    }

    #[test]
    fn timestamp_deterministic_from_message() {
        let ctx = test_ctx();
        let key1 = render_s3_key(None, "{timestamp}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        let key2 = render_s3_key(None, "{timestamp}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn timestamp_to_datetime_zero() {
        let dt = timestamp_to_datetime(0).unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "1970-01-01");
    }

    #[test]
    fn timestamp_to_datetime_known() {
        let dt = timestamp_to_datetime(1_710_597_600_000_000).unwrap();
        assert_eq!(dt.format("%Y-%m-%dT%H").to_string(), "2024-03-16T14");
    }

    #[test]
    fn sanitize_stream_topic_names() {
        let ctx = PathContext {
            stream: "my//stream",
            topic: "topic with spaces",
            partition_id: 0,
            first_timestamp_micros: 1_710_597_600_000_000,
        };
        let key = render_s3_key(None, "{stream}/{topic}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        assert!(
            !key.contains("//"),
            "Sanitized key must not contain '//' from stream name: {key}"
        );
        assert!(
            !key.contains(' '),
            "Sanitized key must not contain spaces: {key}"
        );
    }

    #[test]
    fn lex_sort_correct_with_large_offsets() {
        let ctx = test_ctx();
        let key_small =
            render_s3_key(None, "{stream}", &ctx, 999_900, 999_999, OutputFormat::Raw).unwrap();
        let key_large = render_s3_key(
            None,
            "{stream}",
            &ctx,
            1_000_000,
            1_001_000,
            OutputFormat::Raw,
        )
        .unwrap();
        assert!(key_small < key_large, "Lexicographic sort must be correct");
    }
}
