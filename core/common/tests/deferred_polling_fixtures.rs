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

#[path = "../examples/deferred_polling_fixtures/schema.rs"]
mod schema;

use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchRef};
use iggy_binary_protocol::requests::messages::{DeferredPollMessagesRequest, FetchMessagesRequest};
use iggy_binary_protocol::responses::messages::FetchMessagesResponse;
use iggy_binary_protocol::{HEADER_SIZE, WireDecode};
use schema::{CANONICAL_PATH, LANGUAGE_COPIES, build, render};
use std::fs;
use std::path::Path;

#[test]
fn given_live_codecs_when_rendering_fixture_should_match_the_canonical_file() {
    let rendered = render().expect("fixture builds");
    assert!(
        read_manifest_file(CANONICAL_PATH) == rendered.as_bytes(),
        "{CANONICAL_PATH} is stale, rerun: cargo run -p iggy_common --example deferred_polling_fixtures"
    );
}

#[test]
fn given_language_copies_when_comparing_should_match_canonical_bytes() {
    let canonical = read_manifest_file(CANONICAL_PATH);
    for copy in LANGUAGE_COPIES {
        assert!(
            read_manifest_file(copy) == canonical,
            "{copy} differs from {CANONICAL_PATH}, rerun the deferred_polling_fixtures example"
        );
    }
}

#[test]
fn given_request_vectors_when_decoding_should_reach_their_fields() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.deferred_polls {
        let bytes = hex::decode(&vector.body_hex).expect("hex");
        let request = DeferredPollMessagesRequest::decode_from(&bytes).expect("poll decodes");
        assert_eq!(
            request.poll.partition_id, vector.request.partition_id,
            "{}",
            vector.case
        );
        assert_eq!(
            request.poll.strategy.kind, vector.request.strategy.kind,
            "{}",
            vector.case
        );
        assert_eq!(
            request.poll.auto_commit, vector.request.auto_commit,
            "{}",
            vector.case
        );
        assert_eq!(
            request.max_bytes, vector.request.max_bytes,
            "{}",
            vector.case
        );
    }
    for vector in &fixture.fetches {
        let bytes = hex::decode(&vector.body_hex).expect("hex");
        let request = FetchMessagesRequest::decode_from(&bytes).expect("fetch decodes");
        request.validate().expect("a fetch vector is valid");
        let targets: Vec<_> = request
            .targets
            .iter()
            .map(|target| {
                (
                    target.partition_id,
                    target.strategy.kind,
                    target.owner_generation,
                )
            })
            .collect();
        let expected: Vec<_> = vector
            .request
            .targets
            .iter()
            .map(|target| {
                (
                    target.partition_id,
                    target.strategy.kind,
                    target.owner_generation,
                )
            })
            .collect();
        assert_eq!(targets, expected, "{}", vector.case);
        assert_eq!(
            request.auto_commit, vector.request.auto_commit,
            "{}",
            vector.case
        );
    }
    for vector in &fixture.cancellations {
        assert_eq!(
            hex::decode(&vector.header_hex).expect("hex").len(),
            HEADER_SIZE
        );
    }
}

#[test]
fn given_reply_vectors_when_decoding_should_reach_their_sections_and_messages() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.fetch_replies {
        let bytes = hex::decode(&vector.body_hex).expect("hex");
        let reply = FetchMessagesResponse::decode(&bytes).expect("reply decodes");
        assert_eq!(
            reply.sections.len(),
            vector.sections.len(),
            "{}",
            vector.case
        );
        for (section, expected) in reply.sections.iter().zip(&vector.sections) {
            assert_eq!(
                (
                    section.header.partition_id,
                    section.header.status,
                    section.header.messages_count,
                    section.header.next_offset,
                ),
                (
                    expected.partition_id,
                    expected.status,
                    expected.messages_count,
                    expected.next_offset,
                ),
                "{}",
                vector.case
            );
            let messages: Vec<_> = if section.records.is_empty() {
                Vec::new()
            } else {
                let header = BatchHeader::decode(section.records).expect("batch header decodes");
                BatchRef::new(header, &section.records[BATCH_HEADER_SIZE..])
                    .iter_with_offsets()
                    .map(|record| {
                        (
                            header.base_offset + u64::from(record.message.header.offset_delta),
                            record.message.header.id.to_string(),
                            String::from_utf8(record.message.payload.to_vec()).expect("utf8"),
                        )
                    })
                    .collect()
            };
            let expected: Vec<_> = expected
                .messages
                .iter()
                .map(|message| (message.offset, message.id.clone(), message.payload.clone()))
                .collect();
            assert_eq!(messages, expected, "{}", vector.case);
        }
    }
}

#[test]
fn given_refused_vectors_when_decoding_or_validating_should_refuse_each() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.refused.fetches {
        let bytes = hex::decode(&vector.body_hex).expect("hex");
        let refused = FetchMessagesRequest::decode_from(&bytes)
            .and_then(|request| request.validate())
            .is_err();
        assert!(refused, "{}", vector.case);
    }
    for vector in &fixture.refused.fetch_replies {
        let bytes = hex::decode(&vector.body_hex).expect("hex");
        assert!(
            FetchMessagesResponse::decode(&bytes).is_err(),
            "{}",
            vector.case
        );
    }
}

fn read_manifest_file(relative: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}
