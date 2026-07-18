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

//! Kafka response frame parsing and human-readable summaries for `send` / `verify`.

use bytes::Bytes;
use iggy_gateway_kafka::protocol::header::response_header_version;
use kafka_protocol::messages::{
    ApiVersionsResponse, CreateTopicsResponse, FetchResponse, ListOffsetsResponse,
    MetadataResponse, ProduceResponse,
};
use kafka_protocol::protocol::Decodable;

/// Parsed view of one length-prefixed Kafka response payload (excluding the 4-byte frame length).
pub struct ResponseSummary {
    pub frame_bytes: usize,
    pub correlation_id: i32,
    pub response_header_version: i16,
    pub correlation_match: bool,
    /// Highest-severity non-zero error found, or `0` when all decoded codes are zero.
    pub primary_error_code: i16,
    pub details: Vec<String>,
    pub decode_note: Option<String>,
}

impl ResponseSummary {
    #[must_use]
    pub fn has_nonzero_error(&self) -> bool {
        self.primary_error_code != 0
    }

    /// Reasons `verify` should count this response as a failure.
    ///
    /// Fails on correlation mismatch, schema decode failure, and unexpected non-zero
    /// error codes. Stub APIs may return documented non-zero codes (Produce 6,
    /// Metadata 3, CreateTopics 41).
    #[must_use]
    pub fn verify_failure_reason(&self, api_key: i16) -> Option<String> {
        if !self.correlation_match {
            return Some("correlation_id mismatch".into());
        }
        if let Some(note) = &self.decode_note {
            return Some(format!("schema decode failure: {note}"));
        }
        if !is_acceptable_verify_error(api_key, self.primary_error_code) {
            return Some(format!(
                "unexpected error_code={} ({})",
                self.primary_error_code,
                format_error_code(self.primary_error_code)
            ));
        }
        None
    }

    pub fn print(&self, api_name: &str, version: i16, quiet: bool) {
        let sym = if self.has_nonzero_error() {
            "⚠"
        } else {
            "✓"
        };
        let ec_label = format_error_code(self.primary_error_code);
        let corr = if self.correlation_match {
            format!("{}", self.correlation_id)
        } else {
            format!("{} (expected correlation mismatch)", self.correlation_id)
        };

        if quiet {
            println!(
                "{sym} {api_name} v{version} → {}B  ec={} ({ec_label})",
                self.frame_bytes, self.primary_error_code
            );
            return;
        }

        println!(
            "{sym} {api_name} v{version}  frame={}B  correlation={corr}  resp_hdr=v{}  primary_ec={} ({ec_label})",
            self.frame_bytes, self.response_header_version, self.primary_error_code
        );
        for line in &self.details {
            println!("    {line}");
        }
        if let Some(note) = &self.decode_note {
            println!("    note: {note}");
        }
    }
}

fn is_acceptable_verify_error(api_key: i16, error_code: i16) -> bool {
    if error_code == 0 {
        return true;
    }
    match api_key {
        0 => error_code == 6,   // Produce stub: NOT_LEADER_OR_FOLLOWER
        3 => error_code == 3,   // Metadata stub: UNKNOWN_TOPIC_OR_PARTITION
        19 => error_code == 41, // CreateTopics stub: NOT_CONTROLLER
        _ => false,
    }
}

/// Analyze a response payload for the given request `(api_key, api_version)`.
pub fn analyze_response(
    api_key: i16,
    api_version: i16,
    request_correlation_id: i32,
    payload: &[u8],
) -> ResponseSummary {
    let frame_bytes = payload.len();
    if payload.len() < 4 {
        return ResponseSummary {
            frame_bytes,
            correlation_id: 0,
            response_header_version: 0,
            correlation_match: false,
            primary_error_code: -1,
            details: vec!["payload shorter than correlation_id".into()],
            decode_note: Some("truncated response".into()),
        };
    }

    let correlation_id = i32::from_be_bytes(payload[0..4].try_into().expect("4 bytes"));
    let resp_hdr_ver = response_header_version(api_key, api_version);
    let body_start = if resp_hdr_ver >= 1 {
        5 // correlation_id + empty tagged fields (0x00)
    } else {
        4
    };

    if payload.len() < body_start {
        return ResponseSummary {
            frame_bytes,
            correlation_id,
            response_header_version: resp_hdr_ver,
            correlation_match: correlation_id == request_correlation_id,
            primary_error_code: -1,
            details: vec![format!(
                "truncated after correlation (need {body_start} bytes)"
            )],
            decode_note: None,
        };
    }

    let body = &payload[body_start..];
    let mut details = Vec::new();
    let mut codes = Vec::new();
    let mut decode_note = None;

    if body.len() == 2 {
        let ec = i16::from_be_bytes(body.try_into().expect("2 bytes"));
        codes.push(ec);
        details.push(format!(
            "error-only body: error_code={ec} ({})",
            format_error_code(ec)
        ));
    } else {
        match decode_body(api_key, api_version, body, &mut details, &mut codes) {
            Ok(()) => {}
            Err(e) => {
                decode_note = Some(format!("schema decode failed: {e:#}"));
                details.push(format!("raw_body_hex={}", hex::encode(body)));
            }
        }
    }

    let primary_error_code = codes.iter().copied().filter(|&c| c != 0).max().unwrap_or(0);

    ResponseSummary {
        frame_bytes,
        correlation_id,
        response_header_version: resp_hdr_ver,
        correlation_match: correlation_id == request_correlation_id,
        primary_error_code,
        details,
        decode_note,
    }
}

fn optional_topic_name(name: &Option<kafka_protocol::messages::TopicName>) -> String {
    name.as_ref()
        .map(|n| n.0.as_str().to_string())
        .unwrap_or_else(|| "<null>".into())
}

fn topic_name(name: &kafka_protocol::messages::TopicName) -> String {
    name.0.as_str().to_string()
}

fn decode_body(
    api_key: i16,
    api_version: i16,
    body: &[u8],
    details: &mut Vec<String>,
    codes: &mut Vec<i16>,
) -> anyhow::Result<()> {
    let mut buf = Bytes::copy_from_slice(body);
    match api_key {
        18 => {
            let resp = ApiVersionsResponse::decode(&mut buf, api_version)?;
            codes.push(resp.error_code);
            details.push(format!(
                "top_level.error_code={} ({})",
                resp.error_code,
                format_error_code(resp.error_code)
            ));
            details.push(format!("api_keys={}", resp.api_keys.len()));
            if api_version >= 1 {
                details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            }
            for (i, k) in resp.api_keys.iter().enumerate().take(8) {
                details.push(format!(
                    "api_keys[{i}]: key={} min={} max={}",
                    k.api_key, k.min_version, k.max_version
                ));
            }
            if resp.api_keys.len() > 8 {
                details.push(format!("… {} more api_keys", resp.api_keys.len() - 8));
            }
        }
        3 => {
            let resp = MetadataResponse::decode(&mut buf, api_version)?;
            if api_version >= 3 {
                details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            }
            details.push(format!("brokers={}", resp.brokers.len()));
            if let Some(b) = resp.brokers.first() {
                details.push(format!(
                    "brokers[0]: id={} host={} port={}",
                    b.node_id.0, b.host, b.port
                ));
            }
            details.push(format!("topics={}", resp.topics.len()));
            for (i, t) in resp.topics.iter().enumerate().take(4) {
                codes.push(t.error_code);
                let name = optional_topic_name(&t.name);
                details.push(format!(
                    "topics[{i}]: name={name} ec={} ({}) partitions={}",
                    t.error_code,
                    format_error_code(t.error_code),
                    t.partitions.len()
                ));
            }
            if resp.topics.len() > 4 {
                details.push(format!("… {} more topics", resp.topics.len() - 4));
            }
        }
        0 => {
            let resp = ProduceResponse::decode(&mut buf, api_version)?;
            if api_version >= 1 {
                details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            }
            details.push(format!("topics={}", resp.responses.len()));
            for (ti, topic) in resp.responses.iter().enumerate().take(4) {
                let name = topic_name(&topic.name);
                details.push(format!(
                    "topics[{ti}]: name={name} partitions={}",
                    topic.partition_responses.len()
                ));
                for (pi, p) in topic.partition_responses.iter().enumerate().take(4) {
                    codes.push(p.error_code);
                    details.push(format!(
                        "  partitions[{pi}]: index={} ec={} ({}) offset={}",
                        p.index,
                        p.error_code,
                        format_error_code(p.error_code),
                        p.base_offset
                    ));
                }
            }
        }
        1 => {
            let resp = FetchResponse::decode(&mut buf, api_version)?;
            if api_version >= 1 {
                details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            }
            if api_version >= 7 {
                codes.push(resp.error_code);
                details.push(format!(
                    "top_level.error_code={} ({}) session_id={}",
                    resp.error_code,
                    format_error_code(resp.error_code),
                    resp.session_id
                ));
            }
            details.push(format!("topics={}", resp.responses.len()));
            for (ti, topic) in resp.responses.iter().enumerate().take(4) {
                let name = topic_name(&topic.topic);
                details.push(format!(
                    "topics[{ti}]: name={name} partitions={}",
                    topic.partitions.len()
                ));
                for (pi, p) in topic.partitions.iter().enumerate().take(4) {
                    codes.push(p.error_code);
                    details.push(format!(
                        "  partitions[{pi}]: index={} ec={} ({}) hw={}",
                        p.partition_index,
                        p.error_code,
                        format_error_code(p.error_code),
                        p.high_watermark
                    ));
                }
            }
        }
        2 => {
            let resp = ListOffsetsResponse::decode(&mut buf, api_version)?;
            if api_version >= 2 {
                details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            }
            details.push(format!("topics={}", resp.topics.len()));
            for (ti, topic) in resp.topics.iter().enumerate().take(4) {
                let name = topic_name(&topic.name);
                details.push(format!(
                    "topics[{ti}]: name={name} partitions={}",
                    topic.partitions.len()
                ));
                for (pi, p) in topic.partitions.iter().enumerate().take(4) {
                    codes.push(p.error_code);
                    details.push(format!(
                        "  partitions[{pi}]: index={} ec={} ({}) offset={}",
                        p.partition_index,
                        p.error_code,
                        format_error_code(p.error_code),
                        p.offset
                    ));
                }
            }
        }
        19 => {
            let resp = CreateTopicsResponse::decode(&mut buf, api_version)?;
            details.push(format!("throttle_time_ms={}", resp.throttle_time_ms));
            details.push(format!("topics={}", resp.topics.len()));
            for (i, t) in resp.topics.iter().enumerate().take(4) {
                codes.push(t.error_code);
                let name = topic_name(&t.name);
                details.push(format!(
                    "topics[{i}]: name={name} ec={} ({})",
                    t.error_code,
                    format_error_code(t.error_code)
                ));
            }
        }
        other => {
            details.push(format!("no schema decoder for api_key={other}"));
            if body.len() >= 2 {
                let ec = i16::from_be_bytes(body[0..2].try_into().expect("2 bytes"));
                codes.push(ec);
                details.push(format!(
                    "body[0..2] as i16={ec} ({}) — may not be top-level error_code",
                    format_error_code(ec)
                ));
            }
        }
    }
    Ok(())
}

fn format_error_code(code: i16) -> &'static str {
    match code {
        0 => "NONE",
        1 => "OFFSET_OUT_OF_RANGE",
        2 => "CORRUPT_MESSAGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        6 => "NOT_LEADER_OR_FOLLOWER",
        35 => "UNSUPPORTED_VERSION",
        36 => "TOPIC_ALREADY_EXISTS",
        37 => "INVALID_PARTITIONS",
        38 => "INVALID_REPLICATION_FACTOR",
        41 => "NOT_CONTROLLER",
        42 => "INVALID_REQUEST",
        -1 => "UNKNOWN",
        _ => "OTHER",
    }
}
