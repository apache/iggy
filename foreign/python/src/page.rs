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

use iggy::prelude::{
    ConsumerGroup as RustConsumerGroup, NamedPageQuery, Page, PageCursor,
    PageOrder as RustPageOrder, PageQuery, Partition as RustPartition, Stream as RustStream,
    Topic as RustTopic, UserInfo as RustUserInfo,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum};
use std::str::FromStr;

use crate::client::to_runtime_error;
use crate::consumer::ConsumerGroup;
use crate::event_loop::attach_unless_exiting;
use crate::stream::Stream;
use crate::topic::{Partition, Topic};
use crate::user::UserInfo;

/// The order of a list page: `Id` by id, `Created` by creation time and then
/// by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, from_py_object)]
pub enum PageOrder {
    Id,
    Created,
}

impl From<PageOrder> for RustPageOrder {
    fn from(order: PageOrder) -> Self {
        match order {
            PageOrder::Id => Self::Id,
            PageOrder::Created => Self::Created,
        }
    }
}

/// The query of a page method. A bad cursor or query fails here, at the call,
/// so every transport raises the same invalid command code before any I/O.
pub(crate) fn validated_page_query(
    order: Option<PageOrder>,
    descending: bool,
    limit: Option<u32>,
    cursor: Option<&str>,
) -> PyResult<PageQuery> {
    let defaults = PageQuery::default();
    let query = PageQuery {
        order: order.map_or(defaults.order, Into::into),
        descending,
        limit: limit.unwrap_or(defaults.limit),
        cursor: cursor
            .map(PageCursor::from_str)
            .transpose()
            .map_err(to_runtime_error)?,
    };
    query.validate().map_err(to_runtime_error)?;
    Ok(query)
}

/// [`validated_page_query`] with the name filter of the lists of named
/// entities.
pub(crate) fn validated_named_page_query(
    order: Option<PageOrder>,
    descending: bool,
    limit: Option<u32>,
    cursor: Option<&str>,
    name: Option<String>,
) -> PyResult<NamedPageQuery> {
    let query = NamedPageQuery {
        page: validated_page_query(order, descending, limit, cursor)?,
        name,
    };
    query.validate().map_err(to_runtime_error)?;
    Ok(query)
}

macro_rules! list_page {
    ($page:ident, $item:ident, $rust_item:ty, $method:literal) => {
        #[doc = concat!("One page of a list, as `IggyClient.", $method, "` returns it.")]
        #[gen_stub_pyclass]
        #[pyclass(frozen)]
        pub struct $page {
            /// The items of the page, in the order of the query.
            #[pyo3(get)]
            items: Vec<Py<$item>>,
            /// Pass it as `cursor`, with the same other arguments, to read the
            /// next page. `None` on the last page.
            #[pyo3(get)]
            next: Option<String>,
        }

        impl TryFrom<Page<$rust_item>> for $page {
            type Error = PyErr;

            fn try_from(page: Page<$rust_item>) -> PyResult<Self> {
                let items = attach_unless_exiting(|py| {
                    page.items
                        .into_iter()
                        .map(|item| Py::new(py, $item::from(item)))
                        .collect()
                })?;
                Ok(Self {
                    items,
                    next: page.next.map(|cursor| cursor.to_string()),
                })
            }
        }
    };
}

list_page!(StreamsPage, Stream, RustStream, "get_streams_page");
list_page!(TopicsPage, Topic, RustTopic, "get_topics_page");
list_page!(
    PartitionsPage,
    Partition,
    RustPartition,
    "get_partitions_page"
);
list_page!(
    ConsumerGroupsPage,
    ConsumerGroup,
    RustConsumerGroup,
    "get_consumer_groups_page"
);
list_page!(UsersPage, UserInfo, RustUserInfo, "get_users_page");

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{Identifier, IggyError};
    use iggy_binary_protocol::primitives::page::WirePageQuery;
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
    use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
    use iggy_common::wire_conversions::{identifier_to_wire, page_from_wire};
    use pyo3::PyClass;
    use serde_json::Value;

    use crate::client::PyIggyError;
    use crate::identifier::PyIdentifier;

    const FIXTURE: &str = include_str!("../tests/fixtures/list_pages.json");

    #[test]
    fn given_request_vectors_when_building_queries_should_encode_fixture_bytes() {
        let fixture = fixture();
        for vector in vectors(&fixture["requests"]) {
            let input = &vector["input"];
            let page = WirePageQuery::try_from(&query(input).expect("query is accepted"))
                .expect("query encodes");
            let stream_id = || parent(&input["stream_id"]);
            let topic_id = || parent(&input["topic_id"]);
            let request = match vector["command"].as_str().expect("command") {
                "get_streams" => GetStreamsRequest { page }.to_bytes(),
                "get_users" => GetUsersRequest { page }.to_bytes(),
                "get_topics" => GetTopicsRequest {
                    stream_id: stream_id(),
                    page,
                }
                .to_bytes(),
                "get_partitions" => GetPartitionsRequest {
                    stream_id: stream_id(),
                    topic_id: topic_id(),
                    page,
                }
                .to_bytes(),
                "get_consumer_groups" => GetConsumerGroupsRequest {
                    stream_id: stream_id(),
                    topic_id: topic_id(),
                    page,
                }
                .to_bytes(),
                command => panic!("unknown command {command}"),
            };
            assert_eq!(
                request.to_vec(),
                decode_hex(&vector["request_hex"]),
                "{}",
                vector["case"]
            );
        }
    }

    #[test]
    fn given_response_vectors_when_converting_pages_should_expose_next_and_items() {
        Python::initialize();
        let fixture = fixture();
        Python::attach(|py| {
            for vector in vectors(&fixture["responses"]) {
                let case = vector["case"].as_str().expect("case");
                let reply = decode_hex(&vector["response_hex"]);
                let (page, name) = match vector["command"].as_str().expect("command") {
                    "get_streams" => (
                        converted::<StreamResponse, _, StreamsPage>(
                            py,
                            &reply,
                            RustStream::try_from,
                        ),
                        Some("name"),
                    ),
                    "get_topics" => (
                        converted::<TopicResponse, _, TopicsPage>(py, &reply, RustTopic::try_from),
                        Some("name"),
                    ),
                    "get_partitions" => (
                        converted::<PartitionResponse, _, PartitionsPage>(py, &reply, |item| {
                            Ok(RustPartition::from(item))
                        }),
                        None,
                    ),
                    "get_consumer_groups" => (
                        converted::<ConsumerGroupResponse, _, ConsumerGroupsPage>(
                            py,
                            &reply,
                            |item| Ok(RustConsumerGroup::from(item)),
                        ),
                        Some("name"),
                    ),
                    "get_users" => (
                        converted::<UserResponse, _, UsersPage>(py, &reply, RustUserInfo::try_from),
                        Some("username"),
                    ),
                    command => panic!("unknown command {command}"),
                };

                let next: Option<String> = page
                    .getattr("next")
                    .and_then(|next| next.extract())
                    .expect("next");
                assert_eq!(next.as_deref(), vector["next"].as_str(), "{case}");
                let items = page.getattr("items").expect("items");
                let expected = vector["items"].as_array().expect("items");
                assert_eq!(items.len().expect("length"), expected.len(), "{case}");
                for (item, expected) in items.try_iter().expect("items iterate").zip(expected) {
                    let item = item.expect("item");
                    let id: u64 = item.getattr("id").and_then(|id| id.extract()).expect("id");
                    assert_eq!(Some(id), expected["id"].as_u64(), "{case}");
                    let item_name = name.map(|attribute| {
                        item.getattr(attribute)
                            .and_then(|name| name.extract::<String>())
                            .expect("name")
                    });
                    assert_eq!(item_name.as_deref(), expected["name"].as_str(), "{case}");
                }
            }
        });
    }

    #[test]
    fn given_refused_cursors_and_queries_when_building_queries_should_raise_invalid_command() {
        Python::initialize();
        let fixture = fixture();
        let invalid_command = fixture["constants"]["errors"]["invalid_command"].as_u64();
        let cursors = vectors(&fixture["refused"]["cursors"])
            .iter()
            .chain(vectors(&fixture["opaque_cursors"]));
        Python::attach(|py| {
            for vector in cursors {
                let case = vector["case"].as_str().expect("case");
                let refused = validated_page_query(None, false, None, vector["text"].as_str())
                    .expect_err(case);
                assert_eq!(Some(code(py, &refused)), invalid_command, "{case}");
            }
            for vector in vectors(&fixture["refused"]["queries"]) {
                let case = vector["case"].as_str().expect("case");
                let refused = query(&vector["input"]).expect_err(case);
                assert_eq!(Some(code(py, &refused)), invalid_command, "{case}");
            }
        });
    }

    /// The query of a fixture input, through the function the page methods of
    /// the named lists use. A partitions input has no name, so its query
    /// encodes the page block of the partitions page method.
    fn query(input: &Value) -> PyResult<NamedPageQuery> {
        let order = match input["order"].as_str().expect("order") {
            "id" => PageOrder::Id,
            "created" => PageOrder::Created,
            order => panic!("unknown order {order}"),
        };
        validated_named_page_query(
            Some(order),
            input["descending"].as_bool().expect("descending"),
            input["limit"]
                .as_u64()
                .map(|limit| u32::try_from(limit).expect("u32 limit")),
            input["cursor"].as_str(),
            input["name"].as_str().map(str::to_owned),
        )
    }

    /// A parent id of a request, through the conversion the page methods use.
    fn parent(id: &Value) -> WireIdentifier {
        let id = match id["kind"].as_str().expect("identifier kind") {
            "numeric" => PyIdentifier::Int(
                id["value"]
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok())
                    .expect("u32 id"),
            ),
            "named" => PyIdentifier::String(id["value"].as_str().expect("name").to_owned()),
            kind => panic!("unknown identifier kind {kind}"),
        };
        identifier_to_wire(&Identifier::try_from(id).expect("identifier")).expect("wire id")
    }

    /// A reply decoded and converted as the page methods do it.
    fn converted<'py, W, R, P>(
        py: Python<'py>,
        reply: &[u8],
        item: impl FnMut(W) -> Result<R, IggyError>,
    ) -> Bound<'py, PyAny>
    where
        W: WireDecode,
        P: PyClass + Into<PyClassInitializer<P>> + TryFrom<Page<R>, Error = PyErr>,
    {
        let (wire, consumed) = PageResponse::<W>::decode(reply).expect("reply decodes");
        assert_eq!(consumed, reply.len());
        let page = page_from_wire(wire, item).expect("items convert");
        Bound::new(py, P::try_from(page).expect("page converts"))
            .expect("page wraps")
            .into_any()
    }

    fn code(py: Python<'_>, error: &PyErr) -> u64 {
        assert!(error.is_instance_of::<PyIggyError>(py));
        error
            .value(py)
            .getattr("code")
            .and_then(|code| code.extract())
            .expect("code")
    }

    fn fixture() -> Value {
        serde_json::from_str(FIXTURE).expect("fixture parses")
    }

    fn vectors(section: &Value) -> &Vec<Value> {
        section.as_array().expect("vector list")
    }

    fn decode_hex(text: &Value) -> Vec<u8> {
        let text = text.as_str().expect("hex text");
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
