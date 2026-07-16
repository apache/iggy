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

//! Fixture loaders — compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::path::PathBuf;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::Decoder;
use iggy_gateway_kafka::protocol::header::{RequestHeader, request_header_version};

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

/// Load a fixture body, or return `None` after printing a standardized skip note when the
/// fixture is missing. Unifies the missing-fixture policy across suites: every suite skips
/// explicitly instead of panicking or silently passing with zero assertions.
pub fn load_fixture_body_or_skip(api_key: i16, api_name: &str, version: i16) -> Option<Bytes> {
    if fixture_exists(api_key, api_name, version) {
        Some(load_fixture_body(api_key, api_name, version))
    } else {
        eprintln!("skipping {api_key:03}_{api_name}_v{version}.bin: {FIXTURE_SKIP_HINT}");
        None
    }
}

/// Strip the 4-byte length prefix and Kafka request header from a framed message.
pub fn extract_body_from_framed_message(api_key: i16, api_version: i16, data: &[u8]) -> Bytes {
    let frame = Bytes::copy_from_slice(&data[4..]);
    let hdr_ver = request_header_version(api_key, api_version);
    let mut decoder = Decoder::new(frame);
    RequestHeader::decode_from(&mut decoder, hdr_ver).expect("fixture request header must decode");
    decoder
        .read_bytes(decoder.remaining())
        .expect("fixture request body must decode")
}
