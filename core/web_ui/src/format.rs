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

use std::fmt::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::DateTime;

use crate::models::ResourceOptions;

const MICROS_PER_SECOND: u64 = 1_000_000;

/// Formats a server timestamp (Unix microseconds) in UTC.
pub fn timestamp(micros: u64) -> String {
    format_micros(micros, "%Y-%m-%d %H:%M:%S UTC")
}

/// Like [`timestamp`], keeping the microseconds that order messages.
pub fn timestamp_precise(micros: u64) -> String {
    format_micros(micros, "%Y-%m-%d %H:%M:%S%.6f UTC")
}

/// Renders the `message_expiry` option (microseconds, 0 or `u64::MAX` means
/// never) as a short duration.
pub fn message_expiry(options: &ResourceOptions) -> String {
    let micros = options
        .get("message_expiry")
        .and_then(|option| option.value.parse::<u64>().ok())
        .unwrap_or(0);
    if micros == 0 || micros == u64::MAX {
        return "never".to_owned();
    }
    duration(micros / MICROS_PER_SECOND)
}

/// Shows the payload as text when it is UTF-8 and as hex otherwise.
pub fn payload(base64_payload: &str) -> String {
    match STANDARD.decode(base64_payload) {
        Ok(bytes) => String::from_utf8(bytes).unwrap_or_else(|error| hex(error.as_bytes())),
        Err(_) => base64_payload.to_owned(),
    }
}

/// Up to `max_chars` characters of the payload, decoding only as much of it
/// as that needs, so a list of large messages stays cheap. Text or hex is
/// picked from the decoded prefix alone, so a payload whose invalid UTF-8
/// comes later previews as text while [`payload`] shows it as hex.
pub fn payload_preview(base64_payload: &str, max_chars: usize) -> String {
    // A character is at most 4 bytes of UTF-8, and every 3 bytes take 4
    // base64 characters, so this prefix always holds `max_chars` characters.
    let prefix_len = (max_chars * 4).div_ceil(3) * 4;
    if base64_payload.len() <= prefix_len {
        return truncate(&payload(base64_payload), max_chars);
    }
    let Ok(bytes) = STANDARD.decode(&base64_payload[..prefix_len]) else {
        return truncate(base64_payload, max_chars);
    };
    let preview = match String::from_utf8(bytes) {
        Ok(text) => text,
        // The cut can split the last character, which is still text.
        Err(error) if error.utf8_error().error_len().is_none() => {
            let valid = error.utf8_error().valid_up_to();
            let mut bytes = error.into_bytes();
            bytes.truncate(valid);
            String::from_utf8(bytes).unwrap_or_default()
        }
        Err(error) => hex(error.as_bytes()),
    };
    // The payload goes on past the prefix, so the preview always ends in "…".
    match truncate(&preview, max_chars) {
        cut if cut.ends_with('…') => cut,
        whole => format!("{whole}…"),
    }
}

/// Message IDs are 128-bit and usually UUIDs, so they read best in that form.
pub fn message_id(id: u128) -> String {
    let hex = format!("{id:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 3);
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            text.push(' ');
        }
        // Writing into a String cannot fail.
        let _ = write!(text, "{byte:02x}");
    }
    text
}

pub fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_owned(),
    }
}

fn format_micros(micros: u64, pattern: &str) -> String {
    i64::try_from(micros)
        .ok()
        .and_then(DateTime::from_timestamp_micros)
        .map(|date| date.format(pattern).to_string())
        .unwrap_or_else(|| micros.to_string())
}

fn duration(total_seconds: u64) -> String {
    const UNITS: [(u64, &str); 4] = [(86_400, "d"), (3_600, "h"), (60, "m"), (1, "s")];
    let mut remaining = total_seconds;
    let parts: Vec<String> = UNITS
        .iter()
        .filter_map(|&(unit_seconds, suffix)| {
            let amount = remaining / unit_seconds;
            remaining %= unit_seconds;
            (amount > 0).then(|| format!("{amount}{suffix}"))
        })
        .collect();
    if parts.is_empty() {
        "0s".to_owned()
    } else {
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OptionValue;

    fn expiry_options(micros: u64) -> ResourceOptions {
        ResourceOptions::from([(
            "message_expiry".to_owned(),
            OptionValue {
                value: micros.to_string(),
            },
        )])
    }

    #[test]
    fn given_zero_or_max_expiry_should_render_never() {
        assert_eq!(message_expiry(&expiry_options(0)), "never");
        assert_eq!(message_expiry(&expiry_options(u64::MAX)), "never");
        assert_eq!(message_expiry(&ResourceOptions::new()), "never");
    }

    #[test]
    fn given_expiry_in_micros_should_render_largest_units_first() {
        let micros = (86_400 + 3_600 + 5) * MICROS_PER_SECOND;
        assert_eq!(message_expiry(&expiry_options(micros)), "1d 1h 5s");
    }

    #[test]
    fn given_utf8_payload_should_render_text() {
        assert_eq!(payload(&STANDARD.encode("hello")), "hello");
    }

    #[test]
    fn given_binary_payload_should_render_hex() {
        assert_eq!(payload(&STANDARD.encode([0xff, 0x00, 0x10])), "ff 00 10");
    }

    #[test]
    fn given_message_id_should_render_uuid_form() {
        assert_eq!(
            message_id(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            "01234567-89ab-cdef-0123-456789abcdef"
        );
        assert_eq!(message_id(1), "00000000-0000-0000-0000-000000000001");
    }

    #[test]
    fn given_long_payload_should_preview_from_a_prefix() {
        let text = "zażółć gęślą jaźń ".repeat(20);
        let preview = payload_preview(&STANDARD.encode(&text), 10);
        assert_eq!(preview, "zażółć gęś…");

        let binary: Vec<u8> = (0xf8..=0xff).cycle().take(4096).collect();
        let preview = payload_preview(&STANDARD.encode(&binary), 11);
        assert_eq!(preview, "f8 f9 fa fb…");
    }

    #[test]
    fn given_short_payload_should_preview_all_of_it() {
        assert_eq!(payload_preview(&STANDARD.encode("hello"), 60), "hello");
        assert_eq!(payload_preview(&STANDARD.encode([0xff, 0x00]), 60), "ff 00");
    }

    #[test]
    fn given_prefix_cut_inside_a_character_should_keep_it_as_text() {
        // 60 two-byte characters make a prefix cut inside a character likely.
        let text = "ł".repeat(400);
        let preview = payload_preview(&STANDARD.encode(&text), 60);
        assert_eq!(preview, format!("{}…", "ł".repeat(60)));
    }

    #[test]
    fn given_long_text_should_truncate_on_char_boundary() {
        assert_eq!(truncate("zażółć", 3), "zaż…");
        assert_eq!(truncate("short", 10), "short");
    }

    #[test]
    fn given_micros_should_format_utc() {
        assert_eq!(timestamp(1_700_000_000_123_456), "2023-11-14 22:13:20 UTC");
        assert_eq!(
            timestamp_precise(1_700_000_000_123_456),
            "2023-11-14 22:13:20.123456 UTC"
        );
    }
}
