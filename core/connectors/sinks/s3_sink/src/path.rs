/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::OutputFormat;
use chrono::{DateTime, Utc};

pub struct PathContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
    pub first_timestamp_micros: u64,
}

pub fn render_s3_key(
    prefix: Option<&str>,
    template: &str,
    ctx: &PathContext<'_>,
    offset_start: u64,
    offset_end: u64,
    format: OutputFormat,
) -> String {
    let rendered = render_template(template, ctx);

    let filename = format!(
        "{:06}-{:06}.{}",
        offset_start,
        offset_end,
        format.file_extension()
    );

    match prefix {
        Some(p) => {
            let p = p.trim_matches('/');
            if p.is_empty() {
                format!("{rendered}/{filename}")
            } else {
                format!("{p}/{rendered}/{filename}")
            }
        }
        None => format!("{rendered}/{filename}"),
    }
}

fn render_template(template: &str, ctx: &PathContext<'_>) -> String {
    let dt = timestamp_to_datetime(ctx.first_timestamp_micros);
    let date = dt.format("%Y-%m-%d").to_string();
    let hour = dt.format("%H").to_string();
    let now_millis = Utc::now().timestamp_millis().to_string();

    template
        .replace("{stream}", ctx.stream)
        .replace("{topic}", ctx.topic)
        .replace("{partition}", &ctx.partition_id.to_string())
        .replace("{date}", &date)
        .replace("{hour}", &hour)
        .replace("{timestamp}", &now_millis)
}

fn timestamp_to_datetime(micros: u64) -> DateTime<Utc> {
    let secs = (micros / 1_000_000) as i64;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_else(Utc::now)
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
        );
        assert_eq!(
            key,
            "iggy/raw/app_logs/api_requests/2024-03-16/14/000000-000099.jsonl"
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
        );
        assert_eq!(
            key,
            "app_logs/api_requests/1/2024-03-16/000100-000199.json"
        );
    }

    #[test]
    fn render_no_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(
            None,
            "{stream}/{topic}",
            &ctx,
            0,
            9,
            OutputFormat::Raw,
        );
        assert_eq!(key, "app_logs/api_requests/000000-000009.bin");
    }

    #[test]
    fn render_empty_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some(""),
            "{stream}",
            &ctx,
            0,
            0,
            OutputFormat::JsonLines,
        );
        assert_eq!(key, "app_logs/000000-000000.jsonl");
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
        );
        assert_eq!(key, "data/api_requests/000005-000010.jsonl");
    }

    #[test]
    fn timestamp_to_datetime_zero() {
        let dt = timestamp_to_datetime(0);
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "1970-01-01");
    }

    #[test]
    fn timestamp_to_datetime_known() {
        let dt = timestamp_to_datetime(1_710_597_600_000_000);
        assert_eq!(dt.format("%Y-%m-%dT%H").to_string(), "2024-03-16T14");
    }
}
