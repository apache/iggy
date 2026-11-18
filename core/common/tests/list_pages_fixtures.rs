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

#[path = "../examples/list_pages_fixtures/schema.rs"]
mod schema;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use iggy_binary_protocol::primitives::page::{WirePageCursor, WirePageOrder, WirePageQuery};
use iggy_binary_protocol::requests::consumer_groups::GetConsumerGroupsRequest;
use iggy_binary_protocol::requests::partitions::GetPartitionsRequest;
use iggy_binary_protocol::requests::streams::GetStreamsRequest;
use iggy_binary_protocol::requests::topics::GetTopicsRequest;
use iggy_binary_protocol::requests::users::GetUsersRequest;
use iggy_binary_protocol::responses::PageResponse;
use iggy_binary_protocol::responses::consumer_groups::ConsumerGroupResponse;
use iggy_binary_protocol::responses::streams::{StreamResponse, TopicResponse};
use iggy_binary_protocol::responses::topics::PartitionResponse;
use iggy_binary_protocol::responses::users::UserResponse;
use iggy_binary_protocol::{WireDecode, WireEncode, WireError, WireIdentifier};
use iggy_common::{
    DEFAULT_PAGE_LIMIT, NamedPageQuery, PAGE_CURSOR_PREFIX, PageCursor, PageOrder, PageQuery,
};
use schema::{
    CANONICAL_PATH, ItemFields, LANGUAGE_COPIES, ListCommand, MAX_CURSOR_BYTES, PageItem, build,
    render,
};
use std::fs;
use std::path::Path;
use std::str::FromStr;

#[test]
fn given_live_codecs_when_rendering_fixture_should_match_the_canonical_file() {
    let rendered = render().expect("fixture builds");
    assert!(
        read_manifest_file(CANONICAL_PATH) == rendered.as_bytes(),
        "{CANONICAL_PATH} is stale, rerun: cargo run -p iggy_common --example list_pages_fixtures"
    );
}

#[test]
fn given_language_copies_when_comparing_should_match_canonical_bytes() {
    let canonical = read_manifest_file(CANONICAL_PATH);
    for copy in LANGUAGE_COPIES {
        assert!(
            read_manifest_file(copy) == canonical,
            "{copy} differs from {CANONICAL_PATH}, rerun the list_pages_fixtures example"
        );
    }
}

#[test]
fn given_cursor_vectors_when_parsing_and_printing_should_round_trip() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.cursors {
        let bytes = hex::decode(&vector.hex).expect("hex");
        let cursor = PageCursor::from_str(&vector.text).expect(&vector.case);
        assert_eq!(
            WirePageCursor::from(cursor).to_bytes()[..],
            bytes[..],
            "{}",
            vector.case
        );
        assert_eq!(cursor.to_string(), vector.text, "{}", vector.case);
        assert_eq!(text_rule(&vector.text), Some(bytes), "{}", vector.case);
    }
    let texts: String = fixture
        .cursors
        .iter()
        .map(|vector| &vector.text[PAGE_CURSOR_PREFIX.len()..])
        .collect();
    assert!(
        texts.contains('-') && texts.contains('_'),
        "the cursor texts hold both characters that only Base64url has"
    );
}

#[test]
fn given_opaque_cursors_when_parsing_or_decoding_a_request_should_pass_only_the_text_rule() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.cursors {
        let request = first_page_block_with_cursor(&hex::decode(&vector.hex).expect("hex"));
        assert!(
            GetStreamsRequest::decode_from(&request).is_ok(),
            "{}: the same block with a valid cursor decodes",
            vector.case
        );
    }
    for vector in &fixture.opaque_cursors {
        let bytes = hex::decode(&vector.hex).expect("hex");
        assert!(
            PageCursor::from_str(&vector.text).is_err(),
            "{}",
            vector.case
        );
        let request = first_page_block_with_cursor(&bytes);
        assert!(
            GetStreamsRequest::decode_from(&request).is_err(),
            "{}: the server decodes the request",
            vector.case
        );
        assert_eq!(text_rule(&vector.text), Some(bytes), "{}", vector.case);
    }
}

#[test]
fn given_request_vectors_when_decoding_should_reach_a_query_that_a_server_serves() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.requests {
        let bytes = hex::decode(&vector.request_hex).expect("hex");
        let (stream_id, topic_id, page) = decode_request(vector.command, &bytes)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.case));
        let parent = |id: Option<&schema::Identifier>| {
            id.map(|id| WireIdentifier::try_from(id).expect("identifier"))
        };
        assert_eq!(
            (stream_id, topic_id),
            (
                parent(vector.input.stream_id.as_ref()),
                parent(vector.input.topic_id.as_ref())
            ),
            "{}",
            vector.case
        );
        let query = vector.input.query();
        assert_eq!(
            page,
            WirePageQuery::try_from(&query).expect("valid query"),
            "{}",
            vector.case
        );
        assert_served(vector.command, &query, &vector.case);
        let pairs = vector
            .http_query
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()));
        let parsed = match vector.command {
            ListCommand::Partitions => PageQuery::from_http_query(pairs).map(NamedPageQuery::from),
            _ => NamedPageQuery::from_http_query(pairs),
        };
        assert_eq!(parsed.expect(&vector.case), query, "{}", vector.case);
    }
}

#[test]
fn given_response_vectors_when_decoding_should_reach_their_next_and_items() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.responses {
        let bytes = hex::decode(&vector.response_hex).expect("hex");
        let reply = decode_reply(vector.command, &bytes)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.case));
        assert_eq!(
            (reply.next, reply.items, reply.consumed),
            (vector.next.clone(), vector.items.clone(), bytes.len()),
            "{}",
            vector.case
        );
    }
    for command in ListCommand::ALL {
        assert!(
            fixture
                .responses
                .iter()
                .any(|vector| vector.command == command && vector.items.len() > 1),
            "{} has a page of more than one item",
            command.name()
        );
    }
    assert!(
        fixture
            .responses
            .iter()
            .any(|vector| vector.items.is_empty() && vector.next.is_some()),
        "an empty page with a next"
    );
}

#[test]
fn given_opaque_responses_when_decoding_should_hold_a_valid_page_after_a_next_rust_refuses() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.opaque_responses {
        let bytes = hex::decode(&vector.response_hex).expect("hex");
        assert!(
            decode_reply(vector.command, &bytes).is_err(),
            "{}",
            vector.case
        );
        let next_end = 1 + usize::from(bytes[0]);
        let next = vector.next.as_deref().expect("an opaque next");
        assert_eq!(
            text_rule(next).as_deref(),
            Some(&bytes[1..next_end]),
            "{}",
            vector.case
        );
        let without_next = [&[0][..], &bytes[next_end..]].concat();
        let reply = decode_reply(vector.command, &without_next)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.case));
        assert_eq!(
            (reply.next, reply.items, reply.consumed),
            (None, vector.items.clone(), without_next.len()),
            "{}",
            vector.case
        );
    }
}

#[test]
fn given_refused_vectors_when_parsing_validating_or_decoding_should_refuse_each() {
    let fixture = build().expect("fixture builds");
    for vector in &fixture.refused.cursors {
        assert_eq!(text_rule(&vector.text), None, "{}", vector.case);
        assert!(
            PageCursor::from_str(&vector.text).is_err(),
            "{}",
            vector.case
        );
    }
    for vector in &fixture.refused.queries {
        assert!(vector.input.query().validate().is_err(), "{}", vector.case);
    }
    for vector in &fixture.refused.responses {
        let bytes = hex::decode(&vector.response_hex).expect("hex");
        assert!(
            decode_reply(vector.command, &bytes).is_err(),
            "{}",
            vector.case
        );
    }
}

/// The cursor check of a foreign SDK: the prefix, then canonical unpadded
/// Base64url of 1 to 255 bytes. It does not read the bytes.
fn text_rule(text: &str) -> Option<Vec<u8>> {
    let encoded = text.strip_prefix(PAGE_CURSOR_PREFIX)?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    (URL_SAFE_NO_PAD.encode(&bytes) == encoded && (1..=MAX_CURSOR_BYTES).contains(&bytes.len()))
        .then_some(bytes)
}

/// The page block that a client which does not read the cursor bytes sends.
fn first_page_block_with_cursor(cursor: &[u8]) -> Vec<u8> {
    let cursor_len = u8::try_from(cursor.len()).expect("a cursor length fits one byte");
    let no_name = 0;
    [
        &[WirePageOrder::Id.as_u8(), u8::from(false)][..],
        &DEFAULT_PAGE_LIMIT.to_le_bytes(),
        &[cursor_len],
        cursor,
        &[no_name],
    ]
    .concat()
}

/// The server refuses a cursor of the other walk and a name filter on
/// partitions, so every request vector must be one that a server serves.
fn assert_served(command: ListCommand, query: &NamedPageQuery, case: &str) {
    let id_walk = matches!(
        command,
        ListCommand::Partitions | ListCommand::ConsumerGroups
    ) || query.page.order == PageOrder::Id;
    let cursor_fits = match query.page.cursor.map(WirePageCursor::from) {
        None => true,
        Some(WirePageCursor::Id(_)) => id_walk,
        Some(WirePageCursor::Created { .. }) => !id_walk,
    };
    assert!(cursor_fits, "{case}: the cursor kind does not fit the walk");
    assert!(
        command != ListCommand::Partitions || query.name.is_none(),
        "{case}: partitions have no name filter"
    );
}

type DecodedRequest = (
    Option<WireIdentifier>,
    Option<WireIdentifier>,
    WirePageQuery,
);

fn decode_request(command: ListCommand, bytes: &[u8]) -> Result<DecodedRequest, WireError> {
    Ok(match command {
        ListCommand::Users => (None, None, GetUsersRequest::decode_from(bytes)?.page),
        ListCommand::Streams => (None, None, GetStreamsRequest::decode_from(bytes)?.page),
        ListCommand::Topics => {
            let request = GetTopicsRequest::decode_from(bytes)?;
            (Some(request.stream_id), None, request.page)
        }
        ListCommand::Partitions => {
            let request = GetPartitionsRequest::decode_from(bytes)?;
            (
                Some(request.stream_id),
                Some(request.topic_id),
                request.page,
            )
        }
        ListCommand::ConsumerGroups => {
            let request = GetConsumerGroupsRequest::decode_from(bytes)?;
            (
                Some(request.stream_id),
                Some(request.topic_id),
                request.page,
            )
        }
    })
}

struct DecodedReply {
    next: Option<String>,
    items: Vec<ItemFields>,
    consumed: usize,
}

fn decode_reply(command: ListCommand, bytes: &[u8]) -> Result<DecodedReply, WireError> {
    match command {
        ListCommand::Users => decode_page::<UserResponse>(bytes),
        ListCommand::Streams => decode_page::<StreamResponse>(bytes),
        ListCommand::Topics => decode_page::<TopicResponse>(bytes),
        ListCommand::Partitions => decode_page::<PartitionResponse>(bytes),
        ListCommand::ConsumerGroups => decode_page::<ConsumerGroupResponse>(bytes),
    }
}

fn decode_page<T: PageItem>(bytes: &[u8]) -> Result<DecodedReply, WireError> {
    let (page, consumed) = PageResponse::<T>::decode(bytes)?;
    Ok(DecodedReply {
        next: page.next.map(|cursor| PageCursor::from(cursor).to_string()),
        items: page.items.iter().map(PageItem::fields).collect(),
        consumed,
    })
}

fn read_manifest_file(relative: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}
