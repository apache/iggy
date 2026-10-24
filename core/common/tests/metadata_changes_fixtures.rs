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

#[path = "../examples/metadata_changes_fixtures/schema.rs"]
mod schema;

use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMetadataChange, WireMetadataChangeCursor, WireMetadataCurrentPageToken,
};
use iggy_binary_protocol::requests::system::GetMetadataChangesRequest;
use iggy_binary_protocol::responses::system::{
    GetMetadataChangesResponse, WireMetadataChangeRecord, WireMetadataItem,
};
use iggy_common::{
    MetadataChange, MetadataChangeCursor, MetadataChangeRecord, MetadataChanges,
    MetadataChangesQuery, MetadataCurrentItem, MetadataCurrentPageToken,
};
use schema::{CANONICAL_PATH, LANGUAGE_COPIES, build, render};
use std::fs;
use std::path::Path;
use std::str::FromStr;

#[test]
fn given_live_codecs_when_rendering_fixture_should_match_the_canonical_file() {
    let rendered = render().expect("fixture builds");
    assert!(
        read_manifest_file(CANONICAL_PATH) == rendered.as_bytes(),
        "{CANONICAL_PATH} is stale, rerun: cargo run -p iggy_common --example metadata_changes_fixtures"
    );
}

#[test]
fn given_language_copies_when_comparing_should_match_canonical_bytes() {
    let canonical = read_manifest_file(CANONICAL_PATH);
    for copy in LANGUAGE_COPIES {
        assert!(
            read_manifest_file(copy) == canonical,
            "{copy} differs from {CANONICAL_PATH}, rerun the metadata_changes_fixtures example"
        );
    }
}

#[test]
fn given_record_and_item_vectors_when_decoding_should_reach_the_expected_json() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.records {
        let bytes = hex::decode(&vector.record_hex).expect("hex");
        let wire = WireMetadataChangeRecord::decode_from(&bytes).expect("record decodes");
        assert_eq!(hex::encode(&wire.body), vector.body_hex, "{}", vector.case);
        let record = MetadataChangeRecord::try_from(wire).expect("record converts");
        assert_eq!(record, vector.expected, "{}", vector.case);
        let json = serde_json::to_string(&record).expect("record renders");
        let parsed: MetadataChangeRecord = serde_json::from_str(&json).expect("record parses");
        assert_eq!(parsed, vector.expected, "{}", vector.case);
    }
    for vector in &fixture.items {
        let bytes = hex::decode(&vector.item_hex).expect("hex");
        let wire = WireMetadataItem::decode_from(&bytes).expect("item decodes");
        let item = MetadataCurrentItem::try_from(wire).expect("item converts");
        assert_eq!(item, vector.expected, "{}", vector.case);
    }
}

#[test]
fn given_every_kind_when_counting_record_vectors_should_cover_the_whole_catalog() {
    let fixture = build().expect("fixture builds");
    let kinds: std::collections::BTreeSet<u8> =
        fixture.records.iter().map(|vector| vector.kind).collect();
    let catalog: std::collections::BTreeSet<u8> = fixture
        .constants
        .kinds
        .values()
        .map(|kind| kind.code)
        .collect();
    assert_eq!(kinds, catalog);
    let groups: std::collections::BTreeSet<u8> =
        fixture.items.iter().map(|vector| vector.group).collect();
    assert_eq!(groups.len(), fixture.constants.groups.len());
}

#[test]
fn given_cursor_and_token_vectors_when_parsing_text_should_match_their_bytes() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.cursors {
        let cursor = MetadataChangeCursor::from_str(&vector.text).expect("cursor parses");
        let bytes = hex::decode(&vector.hex).expect("hex");
        let (wire, _) = WireMetadataChangeCursor::decode(&bytes).expect("cursor decodes");
        assert_eq!(MetadataChangeCursor::from(wire), cursor, "{}", vector.case);
        assert_eq!(cursor.op, vector.op, "{}", vector.case);
    }
    for vector in &fixture.page_tokens {
        let token = MetadataCurrentPageToken::from_str(&vector.text).expect("token parses");
        let bytes = hex::decode(&vector.hex).expect("hex");
        let (wire, _) = WireMetadataCurrentPageToken::decode(&bytes).expect("token decodes");
        assert_eq!(
            MetadataCurrentPageToken::from(wire),
            token,
            "{}",
            vector.case
        );
    }
}

#[test]
fn given_request_and_response_vectors_when_decoding_should_match_inputs_and_answers() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.requests {
        let bytes = hex::decode(&vector.request_hex).expect("hex");
        let request = GetMetadataChangesRequest::decode_from(&bytes).expect("request decodes");
        let mut decoded = MetadataChangesQuery::try_from(request).expect("request converts");
        decoded.groups = decoded.validate().expect("valid request").iter().collect();
        let mut expected = vector.input.query().expect("input builds");
        expected.groups = expected.validate().expect("valid input").iter().collect();
        assert_eq!(decoded, expected, "{}", vector.case);
        let mut from_http = MetadataChangesQuery::from_http_query(
            vector
                .http_query
                .split('&')
                .map(|pair| pair.split_once('=').expect("key and value")),
        )
        .expect("http query parses");
        from_http.groups = from_http
            .validate()
            .expect("valid HTTP query")
            .iter()
            .collect();
        assert_eq!(from_http, decoded, "{}", vector.case);
    }
    for vector in &fixture.responses {
        let bytes = hex::decode(&vector.response_hex).expect("hex");
        let response = GetMetadataChangesResponse::decode_from(&bytes).expect("response decodes");
        let answer = MetadataChanges::try_from(response).expect("response converts");
        assert_eq!(answer, vector.expected, "{}", vector.case);
        let json = serde_json::to_string(&answer).expect("answer renders");
        let parsed: MetadataChanges = serde_json::from_str(&json).expect("answer parses");
        let known = answer
            .records
            .iter()
            .all(|record| record.change.kind().is_some());
        if known {
            assert_eq!(parsed, vector.expected, "{}", vector.case);
        } else {
            assert!(
                parsed
                    .records
                    .iter()
                    .all(|record| record.change.kind().is_none())
            );
        }
    }
    let unknown = fixture
        .responses
        .iter()
        .find(|vector| vector.case == "unknown_kind_and_group")
        .expect("unknown vector");
    assert!(
        unknown
            .expected
            .records
            .iter()
            .all(|record| matches!(record.change, MetadataChange::Unknown { .. }))
    );
}

#[test]
fn given_large_values_when_decoding_should_keep_every_bit() {
    let fixture = build().expect("fixture builds");
    let vector = &fixture.large_values;
    let bytes = hex::decode(&vector.record_hex).expect("hex");
    let wire = WireMetadataChangeRecord::decode_from(&bytes).expect("record decodes");
    let record = MetadataChangeRecord::try_from(wire).expect("record converts");
    assert_eq!(record.op.to_string(), vector.op);
    assert_eq!(record.timestamp.to_string(), vector.timestamp);
    assert_eq!(format!("{:032x}", record.checksum), vector.checksum);
    let MetadataChange::SessionRegistered(session) = record.change else {
        panic!("expected a session record");
    };
    assert_eq!(session.user_created_at.to_string(), vector.user_created_at);
    assert_eq!(session.session.to_string(), vector.session);
    assert_eq!(format!("{:032x}", session.client_id), vector.client_id);
}

#[test]
fn given_refused_vectors_when_decoding_or_validating_should_refuse_each() {
    let fixture = build().expect("fixture builds");
    let refused = &fixture.refused;
    for vector in &refused.cursors {
        assert!(
            MetadataChangeCursor::from_str(&vector.text).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &refused.page_tokens {
        assert!(
            MetadataCurrentPageToken::from_str(&vector.text).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &refused.record_bodies {
        let body = hex::decode(&vector.body_hex).expect("hex");
        assert!(
            WireMetadataChange::decode(vector.group, vector.kind, &body).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &refused.requests {
        let bytes = hex::decode(&vector.hex).expect("hex");
        assert!(
            GetMetadataChangesRequest::decode_from(&bytes).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &refused.responses {
        let bytes = hex::decode(&vector.hex).expect("hex");
        assert!(
            GetMetadataChangesResponse::decode_from(&bytes).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &refused.queries {
        let refused_query = vector
            .input
            .query()
            .and_then(|query| query.validate().map(|_| query));
        assert!(refused_query.is_err(), "{}", vector.case);
    }
    for vector in &refused.http_queries {
        let parsed = MetadataChangesQuery::from_http_query(
            vector
                .query
                .split('&')
                .map(|pair| pair.split_once('=').unwrap_or((pair, ""))),
        )
        .and_then(|query| query.validate().map(|_| query));
        assert!(parsed.is_err(), "{}", vector.case);
    }
}

fn read_manifest_file(relative: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read {}: {error}. Generate it with: cargo run -p iggy_common --example metadata_changes_fixtures",
            path.display()
        )
    })
}
