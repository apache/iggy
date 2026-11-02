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

//! Fixture loaders - compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::path::PathBuf;

use bytes::Bytes;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::Decodable;

use iggy_gateway_kafka::protocol::header::request_header_version;

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tools/kafka-tool/kafka_messages")
}

pub fn fixture_exists(api_key: i16, api_name: &str, version: i16) -> bool {
    let filename = format!("{api_key:03}_{api_name}_v{version}.bin");
    fixtures_dir().join(filename).is_file()
}

/// Load request body bytes from a kafka-tool `.bin` fixture (skips frame header).
pub fn load_fixture_body(api_key: i16, api_name: &str, version: i16) -> Bytes {
    let filename = format!("{api_key:03}_{api_name}_v{version}.bin");
    let path = fixtures_dir().join(&filename);
    let data = std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {filename}: {e}"));
    extract_body_from_framed_message(api_key, version, &data)
}

/// Standardized note emitted when a gitignored wire fixture is absent, pointing at the
/// generation script so a fresh clone knows how to produce it.
pub const FIXTURE_SKIP_HINT: &str = "generate with `gateways/kafka/scripts/ci-wire-fixtures.sh generate` (or the kafka-tool `generate` subcommand)";

/// True when the fixtures directory contains at least one `.bin` wire fixture.
pub fn any_wire_fixture_present() -> bool {
    let Ok(entries) = std::fs::read_dir(fixtures_dir()) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .path()
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
    })
}

/// Set by CI (after `ci-wire-fixtures.sh generate`) to turn missing-fixture skips into hard
/// failures, so a broken generation step cannot leave these suites green with zero assertions.
const FIXTURES_REQUIRED_ENV: &str = "KAFKA_FIXTURES_REQUIRED";

/// Load a fixture body, or return `None` after printing a standardized skip note when the
/// fixture is missing. Unifies the missing-fixture policy across suites: every suite skips
/// explicitly instead of panicking or silently passing with zero assertions. Panics instead of
/// skipping when `KAFKA_FIXTURES_REQUIRED` is set, so CI catches a silently broken fixture
/// generation step rather than passing an empty suite.
pub fn load_fixture_body_or_skip(api_key: i16, api_name: &str, version: i16) -> Option<Bytes> {
    if fixture_exists(api_key, api_name, version) {
        Some(load_fixture_body(api_key, api_name, version))
    } else if std::env::var(FIXTURES_REQUIRED_ENV).is_ok_and(|v| v == "1") {
        panic!(
            "missing required fixture {api_key:03}_{api_name}_v{version}.bin ({FIXTURES_REQUIRED_ENV}=1 set): {FIXTURE_SKIP_HINT}"
        );
    } else {
        eprintln!("skipping {api_key:03}_{api_name}_v{version}.bin: {FIXTURE_SKIP_HINT}");
        None
    }
}

/// Strip the 4-byte length prefix and Kafka request header from a framed message.
fn extract_body_from_framed_message(api_key: i16, api_version: i16, data: &[u8]) -> Bytes {
    let mut frame = Bytes::copy_from_slice(&data[4..]);
    let hdr_ver = request_header_version(api_key, api_version);
    RequestHeader::decode(&mut frame, hdr_ver).expect("fixture request header must decode");
    frame
}
