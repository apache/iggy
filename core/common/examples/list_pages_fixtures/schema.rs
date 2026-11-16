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

//! Shape and builder of the list page golden vectors.
//!
//! Shared by the generator example and the drift test, so the test rebuilds
//! the fixture in process and compares it byte for byte with the canonical
//! file and every SDK copy.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use iggy_binary_protocol::codes::{
    GET_CONSUMER_GROUPS_CODE, GET_PARTITIONS_CODE, GET_STREAMS_CODE, GET_TOPICS_CODE,
    GET_USERS_CODE,
};
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
use iggy_binary_protocol::{
    MAX_WIRE_NAME_LENGTH, PartitionContext, WireDecode, WireEncode, WireError, WireIdentifier,
    WireName, WireOptions,
};
use iggy_common::{
    DEFAULT_PAGE_LIMIT, IggyError, MAX_PAGE_LIMIT, NamedPageQuery, PAGE_CURSOR_PREFIX, PageCursor,
    PageOrder, PageQuery,
};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;

pub const SCHEMA_VERSION: u32 = 1;
pub const GENERATOR: &str = "core/common/examples/list_pages_fixtures.rs";
/// Relative to the `iggy_common` manifest directory.
pub const CANONICAL_PATH: &str = "fixtures/list_pages.json";
/// Byte-identical copies each foreign SDK loads from its own test resources.
pub const LANGUAGE_COPIES: [&str; 8] = [
    "../../foreign/go/contracts/testdata/list_pages.json",
    "../../foreign/java/java-sdk/src/test/resources/list_pages.json",
    "../../foreign/node/src/wire/fixtures/list_pages.json",
    "../../foreign/csharp/Iggy_SDK_Tests/Fixtures/list_pages.json",
    "../../foreign/swift/Tests/IggyTests/Fixtures/list_pages.json",
    "../../foreign/python/tests/fixtures/list_pages.json",
    "../../foreign/cpp/tests/fixtures/list_pages.json",
    "../../foreign/php/tests/fixtures/list_pages.json",
];
/// The length of a cursor on the wire is one byte.
pub const MAX_CURSOR_BYTES: usize = u8::MAX as usize;

const CREATED_AT: u64 = 1_710_000_000_000_000;
/// The cursor of the CLI help and docs examples.
const ID_SEVEN: WirePageCursor = WirePageCursor::Id(7);
const ID_DISTINCT_BYTES: WirePageCursor = WirePageCursor::Id(0x0807_0605_0403_0201);
/// Its text holds `_` and `-`, the two characters in which Base64url differs
/// from standard Base64.
const ID_URL_SAFE_CHARACTERS: WirePageCursor = WirePageCursor::Id(0x00f8_ff00);
const CREATED_DISTINCT_BYTES: WirePageCursor = WirePageCursor::Created {
    created_at: 0x1817_1615_1413_1211,
    id: 0x2827_2625_2423_2221,
};
/// 17 bytes, 9 characters and 10 UTF-16 units, so a length in anything but
/// bytes shows.
const MULTI_BYTE_NAME: &str = "café-注文-📦";

pub type FixtureResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Serialize)]
pub struct ListPagesFixture {
    pub schema_version: u32,
    pub generated_by: String,
    pub conventions: BTreeMap<String, String>,
    pub constants: Constants,
    pub cursors: Vec<CursorVector>,
    pub opaque_cursors: Vec<CursorVector>,
    pub requests: Vec<RequestVector>,
    pub responses: Vec<ResponseVector>,
    pub opaque_responses: Vec<ResponseVector>,
    pub refused: RefusedVectors,
}

#[derive(Debug, Serialize)]
pub struct Constants {
    pub codes: BTreeMap<String, u32>,
    pub orders: BTreeMap<String, u8>,
    pub cursor_prefix: String,
    pub max_cursor_bytes: usize,
    pub default_limit: u32,
    pub max_limit: u32,
    pub max_name_bytes: usize,
    pub errors: BTreeMap<String, u32>,
}

/// The five list commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListCommand {
    Users,
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
}

/// A cursor text and the bytes it stands for.
#[derive(Debug, Serialize)]
pub struct CursorVector {
    pub case: String,
    pub text: String,
    pub hex: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Identifier {
    Numeric(u32),
    Named(String),
}

/// The parent ids and the page query of one list request.
#[derive(Debug, Clone, Serialize)]
pub struct PageInput {
    pub stream_id: Option<Identifier>,
    pub topic_id: Option<Identifier>,
    #[serde(serialize_with = "as_text")]
    pub order: PageOrder,
    pub descending: bool,
    pub limit: u32,
    pub cursor: Option<PageCursor>,
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RequestVector {
    pub case: String,
    pub command: ListCommand,
    pub input: PageInput,
    pub request_hex: String,
    pub http_query: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ResponseVector {
    pub case: String,
    pub command: ListCommand,
    pub response_hex: String,
    pub next: Option<String>,
    pub items: Vec<ItemFields>,
}

/// What a reply vector pins of one item. The item codecs do not change, so
/// the id and the name show where each item starts and ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ItemFields {
    pub id: u32,
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RefusedVectors {
    pub cursors: Vec<RefusedText>,
    pub queries: Vec<RefusedQuery>,
    pub responses: Vec<RefusedResponse>,
}

#[derive(Debug, Serialize)]
pub struct RefusedText {
    pub case: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct RefusedQuery {
    pub case: String,
    pub input: PageInput,
}

#[derive(Debug, Serialize)]
pub struct RefusedResponse {
    pub case: String,
    pub command: ListCommand,
    pub response_hex: String,
}

/// An item of a list page.
pub trait PageItem: WireEncode + WireDecode {
    fn fields(&self) -> ItemFields;
}

impl ListCommand {
    pub const ALL: [Self; 5] = [
        Self::Users,
        Self::Streams,
        Self::Topics,
        Self::Partitions,
        Self::ConsumerGroups,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Users => "get_users",
            Self::Streams => "get_streams",
            Self::Topics => "get_topics",
            Self::Partitions => "get_partitions",
            Self::ConsumerGroups => "get_consumer_groups",
        }
    }

    pub const fn code(self) -> u32 {
        match self {
            Self::Users => GET_USERS_CODE,
            Self::Streams => GET_STREAMS_CODE,
            Self::Topics => GET_TOPICS_CODE,
            Self::Partitions => GET_PARTITIONS_CODE,
            Self::ConsumerGroups => GET_CONSUMER_GROUPS_CODE,
        }
    }
}

impl Serialize for ListCommand {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl TryFrom<&Identifier> for WireIdentifier {
    type Error = WireError;

    fn try_from(id: &Identifier) -> Result<Self, Self::Error> {
        match id {
            Identifier::Numeric(value) => Ok(Self::numeric(*value)),
            Identifier::Named(value) => Self::named(value.as_str()),
        }
    }
}

impl PageInput {
    fn new(stream_id: Option<Identifier>, topic_id: Option<Identifier>) -> Self {
        Self {
            stream_id,
            topic_id,
            order: PageOrder::Id,
            descending: false,
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
            name: None,
        }
    }

    pub fn query(&self) -> NamedPageQuery {
        NamedPageQuery {
            page: PageQuery {
                order: self.order,
                descending: self.descending,
                limit: self.limit,
                cursor: self.cursor,
            },
            name: self.name.clone(),
        }
    }

    fn request_bytes(&self, command: ListCommand) -> FixtureResult<Vec<u8>> {
        let page = WirePageQuery::try_from(&self.query())?;
        let stream_id = || parent(self.stream_id.as_ref());
        let topic_id = || parent(self.topic_id.as_ref());
        let bytes = match command {
            ListCommand::Users => GetUsersRequest { page }.to_bytes(),
            ListCommand::Streams => GetStreamsRequest { page }.to_bytes(),
            ListCommand::Topics => GetTopicsRequest {
                stream_id: stream_id()?,
                page,
            }
            .to_bytes(),
            ListCommand::Partitions => GetPartitionsRequest {
                stream_id: stream_id()?,
                topic_id: topic_id()?,
                page,
            }
            .to_bytes(),
            ListCommand::ConsumerGroups => GetConsumerGroupsRequest {
                stream_id: stream_id()?,
                topic_id: topic_id()?,
                page,
            }
            .to_bytes(),
        };
        Ok(bytes.to_vec())
    }
}

impl PageItem for StreamResponse {
    fn fields(&self) -> ItemFields {
        named_item(self.id, &self.name)
    }
}

impl PageItem for TopicResponse {
    fn fields(&self) -> ItemFields {
        named_item(self.id, &self.name)
    }
}

impl PageItem for PartitionResponse {
    fn fields(&self) -> ItemFields {
        ItemFields {
            id: self.id,
            name: None,
        }
    }
}

impl PageItem for ConsumerGroupResponse {
    fn fields(&self) -> ItemFields {
        named_item(self.id, &self.name)
    }
}

impl PageItem for UserResponse {
    fn fields(&self) -> ItemFields {
        named_item(self.id, &self.username)
    }
}

pub fn render() -> FixtureResult<String> {
    let mut json = serde_json::to_string_pretty(&build()?)?;
    json.push('\n');
    Ok(json)
}

/// The fixture, built from the live codecs, so a codec change shows up as a
/// stale file in the drift test.
pub fn build() -> FixtureResult<ListPagesFixture> {
    Ok(ListPagesFixture {
        schema_version: SCHEMA_VERSION,
        generated_by: GENERATOR.to_owned(),
        conventions: conventions(),
        constants: constants(),
        cursors: cursor_vectors(),
        opaque_cursors: opaque_cursor_vectors(),
        requests: request_vectors()?,
        responses: response_vectors()?,
        opaque_responses: opaque_response_vectors()?,
        refused: RefusedVectors {
            cursors: refused_cursors()?,
            queries: refused_queries(),
            responses: refused_responses()?,
        },
    })
}

/// The text form of any byte string, also one that is not a valid cursor.
pub fn cursor_text(bytes: &[u8]) -> String {
    format!("{PAGE_CURSOR_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn conventions() -> BTreeMap<String, String> {
    [
        (
            "hex",
            "Every *_hex field is lowercase hex without separators. request_hex is the command payload without framing, and response_hex is the reply payload.",
        ),
        (
            "identifiers",
            "stream_id and topic_id are {kind, value}: kind numeric with a u32 value, or kind named with a string value. They are null for a command that has no such field.",
        ),
        (
            "cursor_text",
            "A cursor text is cursor_prefix and then the cursor bytes as unpadded Base64url. The unused bits of the last character are zero. A client refuses a text that does not encode back to the same text, and a text of 0 bytes or of more than max_cursor_bytes bytes.",
        ),
        (
            "opaque_cursors",
            "A client that encodes the wire format itself does not read the bytes of a cursor. It keeps the bytes of every opaque_cursors entry and of every opaque_responses next. It sends them as it got them, and the server refuses them with invalid_command. The Rust SDK and the SDKs that wrap it read the bytes, so they refuse these cursors and replies.",
        ),
        (
            "next",
            "A client sends the next of a page as the cursor of the next request, with the same other fields. A page can be empty and still have a next. A walk ends at the first page without a next.",
        ),
        (
            "walk",
            "Over TCP, QUIC and WebSocket, a list method that returns a whole list walks the pages with order id, ascending and limit max_limit. Over HTTP, it reads the route without limit and cursor, which answers the whole list as a bare JSON array.",
        ),
        (
            "name",
            "A name is UTF-8 text of 1 to max_name_bytes bytes. Its length on the wire counts bytes, not characters. GetPartitions has no name filter, so its query has no name and its page block has a name length of 0. The server refuses a GetPartitions page block with a name.",
        ),
        (
            "http_query",
            "http_query holds the page keys and their text values that a client sends to the HTTP list route. A request with limit or cursor gets one page. A request without both gets the whole list as a bare JSON array, in the order of its order and descending keys and filtered by its name key. The partitions route always answers a page.",
        ),
        (
            "items",
            "items hold the id and the name of each item of the page, in reply order. A partition has no name.",
        ),
        (
            "refused",
            "A client refuses every text of refused.cursors, every input of refused.queries before it sends a request, and every body of refused.responses. An input with a name is a query of a list with a name filter.",
        ),
        (
            "errors",
            "The server answers invalid_command to a page block that it cannot read or serve. Examples are a cursor of an unknown kind or length, a cursor whose kind does not fit the order, and a name filter on GetPartitions. reply_too_large is new in this protocol version. The server answers it in place of a GetClients reply, or of a reply to a read of streams, topics, partitions, consumer groups, users, tokens, options, stats or cluster metadata, whose frame is above the reply size cap. A page reply stays far below that cap.",
        ),
    ]
    .into_iter()
    .map(|(key, text)| (key.to_owned(), text.to_owned()))
    .collect()
}

fn constants() -> Constants {
    Constants {
        codes: ListCommand::ALL
            .into_iter()
            .map(|command| (command.name().to_owned(), command.code()))
            .collect(),
        orders: [PageOrder::Id, PageOrder::Created]
            .into_iter()
            .map(|order| (order.to_string(), WirePageOrder::from(order).as_u8()))
            .collect(),
        cursor_prefix: PAGE_CURSOR_PREFIX.to_owned(),
        max_cursor_bytes: MAX_CURSOR_BYTES,
        default_limit: DEFAULT_PAGE_LIMIT,
        max_limit: MAX_PAGE_LIMIT,
        max_name_bytes: MAX_WIRE_NAME_LENGTH,
        errors: [IggyError::InvalidCommand, IggyError::ReplyTooLarge]
            .into_iter()
            .map(|error| (<&'static str>::from(&error).to_owned(), error.as_code()))
            .collect(),
    }
}

fn cursor_vectors() -> Vec<CursorVector> {
    [
        ("id_seven", ID_SEVEN),
        ("id_distinct_bytes", ID_DISTINCT_BYTES),
        ("id_url_safe_characters", ID_URL_SAFE_CHARACTERS),
        ("created_distinct_bytes", CREATED_DISTINCT_BYTES),
    ]
    .into_iter()
    .map(|(case, cursor)| CursorVector {
        case: case.to_owned(),
        text: PageCursor::from(cursor).to_string(),
        hex: hex::encode(cursor.to_bytes()),
    })
    .collect()
}

fn opaque_cursor_vectors() -> Vec<CursorVector> {
    let id_seven = ID_SEVEN.to_bytes();
    [
        ("kind_zero", with_kind(0, &id_seven)),
        ("unknown_kind", unknown_kind_cursor()),
        ("created_kind_with_the_id_length", with_kind(2, &id_seven)),
        (
            "id_kind_with_the_created_length",
            with_kind(1, &CREATED_DISTINCT_BYTES.to_bytes()),
        ),
        ("one_byte", vec![1]),
        ("most_bytes", longest_cursor()),
    ]
    .into_iter()
    .map(|(case, bytes)| CursorVector {
        case: case.to_owned(),
        text: cursor_text(&bytes),
        hex: hex::encode(&bytes),
    })
    .collect()
}

fn request_vectors() -> FixtureResult<Vec<RequestVector>> {
    let stream_id = || Some(Identifier::Numeric(7));
    let stream_name = || Some(Identifier::Named("orders".to_owned()));
    let topic_id = || Some(Identifier::Numeric(3));
    let topic_name = || Some(Identifier::Named("events".to_owned()));
    let no_parent = || PageInput::new(None, None);
    let cases = [
        ("streams_first_page", ListCommand::Streams, no_parent()),
        (
            "streams_whole_list_walk",
            ListCommand::Streams,
            PageInput {
                limit: MAX_PAGE_LIMIT,
                ..no_parent()
            },
        ),
        (
            "streams_created_descending_one_item",
            ListCommand::Streams,
            PageInput {
                order: PageOrder::Created,
                descending: true,
                limit: 1,
                ..no_parent()
            },
        ),
        (
            "streams_after_id_cursor",
            ListCommand::Streams,
            PageInput {
                cursor: Some(ID_DISTINCT_BYTES.into()),
                ..no_parent()
            },
        ),
        (
            "streams_created_after_cursor_with_name",
            ListCommand::Streams,
            PageInput {
                order: PageOrder::Created,
                descending: true,
                limit: 20,
                cursor: Some(CREATED_DISTINCT_BYTES.into()),
                name: Some("ord".to_owned()),
                ..no_parent()
            },
        ),
        (
            "streams_multi_byte_name",
            ListCommand::Streams,
            PageInput {
                name: Some(MULTI_BYTE_NAME.to_owned()),
                ..no_parent()
            },
        ),
        (
            "streams_longest_name",
            ListCommand::Streams,
            PageInput {
                name: Some(longest_name()),
                ..no_parent()
            },
        ),
        ("users_first_page", ListCommand::Users, no_parent()),
        (
            "users_created_after_cursor_with_name",
            ListCommand::Users,
            PageInput {
                order: PageOrder::Created,
                cursor: Some(
                    WirePageCursor::Created {
                        created_at: CREATED_AT,
                        id: 1,
                    }
                    .into(),
                ),
                name: Some("adm".to_owned()),
                ..no_parent()
            },
        ),
        (
            "topics_numeric_stream_first_page",
            ListCommand::Topics,
            PageInput::new(stream_id(), None),
        ),
        (
            "topics_named_stream_created_descending_with_name",
            ListCommand::Topics,
            PageInput {
                order: PageOrder::Created,
                descending: true,
                limit: 50,
                cursor: Some(
                    WirePageCursor::Created {
                        created_at: CREATED_AT + 9,
                        id: 3,
                    }
                    .into(),
                ),
                name: Some("sens".to_owned()),
                ..PageInput::new(stream_name(), None)
            },
        ),
        (
            "partitions_numeric_ids_first_page",
            ListCommand::Partitions,
            PageInput::new(stream_id(), topic_id()),
        ),
        (
            "partitions_named_ids_descending_after_cursor",
            ListCommand::Partitions,
            PageInput {
                descending: true,
                limit: MAX_PAGE_LIMIT,
                cursor: Some(WirePageCursor::Id(999).into()),
                ..PageInput::new(stream_name(), topic_name())
            },
        ),
        (
            "partitions_created_order_after_id_cursor",
            ListCommand::Partitions,
            PageInput {
                order: PageOrder::Created,
                cursor: Some(ID_SEVEN.into()),
                ..PageInput::new(stream_id(), topic_id())
            },
        ),
        (
            "consumer_groups_numeric_ids_first_page",
            ListCommand::ConsumerGroups,
            PageInput::new(stream_id(), topic_id()),
        ),
        (
            "consumer_groups_named_ids_descending_with_name",
            ListCommand::ConsumerGroups,
            PageInput {
                descending: true,
                cursor: Some(WirePageCursor::Id(3).into()),
                name: Some("bill".to_owned()),
                ..PageInput::new(stream_name(), topic_name())
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(case, command, input)| {
            let request_hex = hex::encode(input.request_bytes(command)?);
            let http_query = input
                .query()
                .http_query_pairs()
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect();
            Ok(RequestVector {
                case: case.to_owned(),
                command,
                input,
                request_hex,
                http_query,
            })
        })
        .collect()
}

fn response_vectors() -> FixtureResult<Vec<ResponseVector>> {
    let orders = || stream(1, CREATED_AT + 5, "orders");
    let payments = || stream(4, CREATED_AT, "payments-eu");
    let events = || topic(1, CREATED_AT, "events");
    let full_partition = || partition(0, vec![0, 1, 2], vec![0, 1]);
    let lagging_partition = || partition(1, vec![1], vec![]);
    let iggy = || user(1, CREATED_AT, "iggy");
    let alice = || user(2, CREATED_AT + 1, "alice-ops");
    Ok(vec![
        response(
            "streams_with_next",
            ListCommand::Streams,
            vec![orders()?, payments()?],
            Some(WirePageCursor::Id(4)),
        ),
        response(
            "streams_created_order_with_next",
            ListCommand::Streams,
            vec![payments()?, orders()?],
            Some(WirePageCursor::Created {
                created_at: CREATED_AT + 5,
                id: 1,
            }),
        ),
        response::<StreamResponse>(
            "streams_empty_with_next",
            ListCommand::Streams,
            vec![],
            Some(WirePageCursor::Id(1000)),
        ),
        response::<StreamResponse>(
            "streams_empty_last_page",
            ListCommand::Streams,
            vec![],
            None,
        ),
        response(
            "topics_last_page",
            ListCommand::Topics,
            vec![events()?, topic(2, CREATED_AT, "sensor-readings")?],
            None,
        ),
        response(
            "topics_created_descending_with_next",
            ListCommand::Topics,
            vec![topic(3, CREATED_AT + 9, "audit")?, events()?],
            Some(WirePageCursor::Created {
                created_at: CREATED_AT,
                id: 1,
            }),
        ),
        response(
            "partitions_with_next",
            ListCommand::Partitions,
            vec![full_partition(), lagging_partition()],
            Some(WirePageCursor::Id(1)),
        ),
        response(
            "partitions_descending_last_page",
            ListCommand::Partitions,
            vec![lagging_partition(), full_partition()],
            None,
        ),
        response(
            "consumer_groups_with_next",
            ListCommand::ConsumerGroups,
            vec![group(1, "billing")?, group(3, "analytics-eu")?],
            Some(WirePageCursor::Id(3)),
        ),
        response(
            "users_with_next",
            ListCommand::Users,
            vec![iggy()?, alice()?],
            Some(WirePageCursor::Id(2)),
        ),
        response(
            "users_created_descending_last_page",
            ListCommand::Users,
            vec![alice()?, iggy()?],
            None,
        ),
    ])
}

/// Pages whose next only a server refuses, built around a valid page.
fn opaque_response_vectors() -> FixtureResult<Vec<ResponseVector>> {
    [
        ("streams_with_unknown_cursor_kind", unknown_kind_cursor()),
        ("streams_with_most_cursor_bytes", longest_cursor()),
    ]
    .into_iter()
    .map(|(case, next)| {
        let items = vec![
            stream(1, CREATED_AT + 5, "orders")?,
            stream(4, CREATED_AT, "payments-eu")?,
        ];
        let page = PageResponse { items, next: None };
        let last_page = page.to_bytes();
        let mut body = vec![u8::try_from(next.len())?];
        body.extend_from_slice(&next);
        // The first byte of the last page is the length 0 of its absent next.
        body.extend_from_slice(&last_page[1..]);
        Ok(ResponseVector {
            case: case.to_owned(),
            command: ListCommand::Streams,
            response_hex: hex::encode(body),
            next: Some(cursor_text(&next)),
            items: page.items.iter().map(PageItem::fields).collect(),
        })
    })
    .collect()
}

fn refused_cursors() -> FixtureResult<Vec<RefusedText>> {
    let id_seven = PageCursor::from(ID_SEVEN).to_string();
    let id_seven_body = &id_seven[PAGE_CURSOR_PREFIX.len()..];
    let url_safe = PageCursor::from(ID_URL_SAFE_CHARACTERS).to_string();
    let standard_body = url_safe[PAGE_CURSOR_PREFIX.len()..]
        .replace('_', "/")
        .replace('-', "+");
    // 17 bytes end in a character with 4 data bits and 2 unused bits. Here
    // all six are zero, so `B` in place of `A` sets an unused bit and keeps
    // the bytes.
    let unused_bits_zero = PageCursor::from(WirePageCursor::Created {
        created_at: 9,
        id: 3,
    })
    .to_string();
    let unused_bit_set = format!(
        "{}B",
        unused_bits_zero
            .strip_suffix('A')
            .ok_or("the cursor text ends with A")?
    );
    Ok([
        ("empty", String::new()),
        ("missing_prefix", id_seven_body.to_owned()),
        ("other_prefix", format!("mc1_{id_seven_body}")),
        ("later_version_prefix", format!("pg2_{id_seven_body}")),
        ("upper_case_prefix", format!("PG1_{id_seven_body}")),
        (
            "padded",
            format!("{}=", PageCursor::from(CREATED_DISTINCT_BYTES)),
        ),
        (
            "standard_alphabet",
            format!("{PAGE_CURSOR_PREFIX}{standard_body}"),
        ),
        (
            "character_outside_the_alphabet",
            format!("{}*", &id_seven[..id_seven.len() - 1]),
        ),
        ("trailing_newline", format!("{id_seven}\n")),
        ("unused_bit_set", unused_bit_set),
        ("no_bytes", PAGE_CURSOR_PREFIX.to_owned()),
        (
            "more_than_max_cursor_bytes",
            cursor_text(&[0x5a; MAX_CURSOR_BYTES + 1]),
        ),
    ]
    .into_iter()
    .map(|(case, text)| RefusedText {
        case: case.to_owned(),
        text,
    })
    .collect())
}

fn refused_queries() -> Vec<RefusedQuery> {
    let input = || PageInput::new(None, None);
    [
        (
            "limit_zero",
            PageInput {
                limit: 0,
                ..input()
            },
        ),
        (
            "limit_above_max_limit",
            PageInput {
                limit: MAX_PAGE_LIMIT + 1,
                ..input()
            },
        ),
        (
            "empty_name",
            PageInput {
                name: Some(String::new()),
                ..input()
            },
        ),
        (
            // 128 characters, so a length in characters passes it.
            "name_above_max_name_bytes",
            PageInput {
                name: Some("é".repeat(MAX_WIRE_NAME_LENGTH / 2 + 1)),
                ..input()
            },
        ),
    ]
    .into_iter()
    .map(|(case, input)| RefusedQuery {
        case: case.to_owned(),
        input,
    })
    .collect()
}

fn refused_responses() -> FixtureResult<Vec<RefusedResponse>> {
    let one_group = PageResponse {
        items: vec![group(1, "billing")?],
        next: None,
    }
    .to_bytes();
    let with_count = |count: u32| {
        let mut body = one_group.to_vec();
        body[1..5].copy_from_slice(&count.to_le_bytes());
        body
    };
    let two_partitions = PageResponse {
        items: vec![
            partition(0, vec![0, 1, 2], vec![0, 1]),
            partition(1, vec![1], vec![]),
        ],
        next: Some(WirePageCursor::Id(1)),
    }
    .to_bytes();
    let created_next = PageResponse::<UserResponse> {
        items: vec![],
        next: Some(CREATED_DISTINCT_BYTES),
    }
    .to_bytes();
    Ok([
        ("empty_body", ListCommand::Streams, Vec::new()),
        ("truncated_count", ListCommand::Topics, vec![0, 2, 0]),
        (
            "truncated_next",
            ListCommand::Users,
            created_next[..6].to_vec(),
        ),
        (
            "count_above_items",
            ListCommand::ConsumerGroups,
            with_count(2),
        ),
        (
            "count_of_u32_max",
            ListCommand::ConsumerGroups,
            with_count(u32::MAX),
        ),
        (
            "truncated_item",
            ListCommand::Partitions,
            two_partitions[..two_partitions.len() - 1].to_vec(),
        ),
        (
            "trailing_bytes",
            ListCommand::Partitions,
            [&two_partitions[..], &[0]].concat(),
        ),
    ]
    .into_iter()
    .map(|(case, command, body)| RefusedResponse {
        case: case.to_owned(),
        command,
        response_hex: hex::encode(body),
    })
    .collect())
}

fn response<T: PageItem>(
    case: &str,
    command: ListCommand,
    items: Vec<T>,
    next: Option<WirePageCursor>,
) -> ResponseVector {
    let page = PageResponse { items, next };
    ResponseVector {
        case: case.to_owned(),
        command,
        response_hex: hex::encode(page.to_bytes()),
        next: page.next.map(|cursor| PageCursor::from(cursor).to_string()),
        items: page.items.iter().map(PageItem::fields).collect(),
    }
}

fn stream(id: u32, created_at: u64, name: &str) -> FixtureResult<StreamResponse> {
    Ok(StreamResponse {
        id,
        created_at,
        topics_count: 2,
        size_bytes: 4096,
        messages_count: 100,
        name: WireName::new(name)?,
        options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    })
}

fn topic(id: u32, created_at: u64, name: &str) -> FixtureResult<TopicResponse> {
    Ok(TopicResponse {
        id,
        created_at,
        partitions_count: 3,
        size_bytes: 2048,
        messages_count: 50,
        name: WireName::new(name)?,
        options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    })
}

fn partition(id: u32, members: Vec<u8>, in_sync_members: Vec<u8>) -> PartitionResponse {
    PartitionResponse {
        context: PartitionContext {
            incarnation: 1,
            owner_generation: 0,
            metadata_op: 42,
        },
        id,
        created_at: CREATED_AT + u64::from(id),
        segments_count: 1,
        current_offset: 99,
        size_bytes: 1024,
        messages_count: 100,
        members,
        in_sync_members,
    }
}

fn group(id: u32, name: &str) -> FixtureResult<ConsumerGroupResponse> {
    Ok(ConsumerGroupResponse {
        id,
        partitions_count: 3,
        members_count: 1,
        name: WireName::new(name)?,
    })
}

fn user(id: u32, created_at: u64, username: &str) -> FixtureResult<UserResponse> {
    Ok(UserResponse {
        id,
        created_at,
        status: 1,
        username: WireName::new(username)?,
        options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    })
}

fn named_item(id: u32, name: &WireName) -> ItemFields {
    ItemFields {
        id,
        name: Some(name.as_str().to_owned()),
    }
}

fn parent(id: Option<&Identifier>) -> FixtureResult<WireIdentifier> {
    let id = id.ok_or("the command needs this parent id")?;
    Ok(WireIdentifier::try_from(id)?)
}

fn with_kind(kind: u8, cursor: &[u8]) -> Vec<u8> {
    [&[kind][..], &cursor[1..]].concat()
}

fn unknown_kind_cursor() -> Vec<u8> {
    with_kind(3, &ID_SEVEN.to_bytes())
}

fn longest_cursor() -> Vec<u8> {
    (1..=u8::MAX).collect()
}

/// 255 bytes but 128 characters, so a length in characters shows.
fn longest_name() -> String {
    let mut name = "é".repeat(MAX_WIRE_NAME_LENGTH / 2);
    name.push('a');
    name
}

fn as_text<T: Display, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
