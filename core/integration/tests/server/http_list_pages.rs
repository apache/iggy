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

//! The query of the HTTP list routes: page keys, the JSON page and the strict
//! parser. The SDK only sends well-formed keys, so these requests are raw.

use crate::server::http_client::HttpClient;
use futures::future::{join_all, try_join_all};
use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use reqwest::{Response, StatusCode};
use serde_json::Value;

const STREAM: &str = "http-pages";
const TOPIC: &str = "events";
const PARTITIONS: u32 = 4;
const PARTITIONS_PATH: &str = "/streams/http-pages/topics/events/partitions";
/// The routes that answer the bare array of the legacy server without `limit`
/// and `cursor`.
const LIST_PATHS: [&str; 4] = [
    "/streams",
    "/streams/http-pages/topics",
    "/streams/http-pages/topics/events/consumer-groups",
    "/users",
];
/// Clients that create the streams of the long list at the same time. One
/// client alone waits about 100 ms for each create.
const CREATORS: usize = 8;

#[iggy_harness]
async fn given_the_partitions_route_when_reading_should_serve_pages_and_refuse_bad_keys(
    harness: &TestHarness,
) {
    seed(harness).await;
    let http = HttpClient::login_root(harness).await;

    let page = json_of(http.get(PARTITIONS_PATH).await).await;
    assert_eq!(ids(&page), [0, 1, 2, 3], "no page key reads the first page");
    assert_eq!(page.get("next"), None);

    let first = json_of(
        http.get(&format!(
            "{PARTITIONS_PATH}?limit=3&consistency=serializable"
        ))
        .await,
    )
    .await;
    assert_eq!(ids(&first), [0, 1, 2]);
    let next = first["next"].as_str().expect("a next cursor");
    assert!(next.starts_with(PAGE_CURSOR_PREFIX), "{next}");
    let last = json_of(
        http.get(&format!("{PARTITIONS_PATH}?limit=3&cursor={next}"))
            .await,
    )
    .await;
    assert_eq!(ids(&last), [3]);
    assert_eq!(last.get("next"), None);

    let newest = json_of(
        http.get(&format!(
            "{PARTITIONS_PATH}?descending=true&limit=2&consistency=linearizable"
        ))
        .await,
    )
    .await;
    assert_eq!(ids(&newest), [3, 2]);

    for query in [
        "offset=3",
        "limit=3&limit=3",
        "limit=0",
        "limit=1001",
        "limit=03",
        "order=name",
        "descending=1",
        "cursor=pg1_",
        "cursor=mc1_AQcAAAAAAAAA",
        "name=p",
        "consistency=strong",
        "consistency=serializable&consistency=serializable",
    ] {
        let response = http.get(&format!("{PARTITIONS_PATH}?{query}")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "?{query}");
    }
}

#[iggy_harness]
async fn given_the_list_routes_when_reading_should_answer_the_bare_array_or_one_page(
    harness: &TestHarness,
) {
    seed(harness).await;
    let http = HttpClient::login_root(harness).await;
    for path in LIST_PATHS {
        let page = json_of(http.get(&format!("{path}?limit={MAX_PAGE_LIMIT}")).await).await;
        assert_eq!(page.get("next"), None, "{path}: one page holds the list");
        let items = page["items"].as_array().expect("an items array");
        assert_ne!(items.len(), 0, "{path}: the seed fills each list");
        for query in ["", "?consistency=serializable"] {
            let bare = json_of(http.get(&format!("{path}{query}")).await).await;
            assert_eq!(
                bare.as_array(),
                Some(items),
                "{path}{query}: without limit and cursor, the bare array of the page items"
            );
        }
        let newest_first: Vec<Value> = items.iter().rev().cloned().collect();
        let descending = json_of(http.get(&format!("{path}?descending=true")).await).await;
        assert_eq!(
            descending.as_array(),
            Some(&newest_first),
            "{path}: without limit and cursor, the whole list in the asked order"
        );
        let filtered = json_of(http.get(&format!("{path}?name=no-such-name")).await).await;
        assert_eq!(
            filtered.as_array(),
            Some(&Vec::new()),
            "{path}: without limit and cursor, the whole list with the asked filter"
        );
        for query in ["offset=3", "limit=0", "consistency=strong", "name="] {
            let response = http.get(&format!("{path}?{query}")).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}?{query}");
        }
    }
}

#[iggy_harness]
async fn given_more_streams_than_one_page_when_listing_should_return_every_stream(
    harness: &TestHarness,
) {
    let names: Vec<String> = (0..=MAX_PAGE_LIMIT)
        .map(|index| format!("many-{index:04}"))
        .collect();
    let creators = try_join_all((0..CREATORS).map(|_| harness.tcp_root_client()))
        .await
        .expect("tcp root clients");
    let chunks = names.chunks(names.len().div_ceil(CREATORS));
    join_all(
        creators
            .iter()
            .zip(chunks)
            .map(|(client, chunk)| async move {
                for name in chunk {
                    client
                        .create_stream(name, &StreamCreateOptions::default())
                        .await
                        .expect("create stream");
                }
            }),
    )
    .await;

    let listed = creators[0].get_streams().await.expect("get streams");
    let mut listed: Vec<&str> = listed.iter().map(|stream| stream.name.as_str()).collect();
    listed.sort_unstable();
    assert_eq!(listed, names, "the list reads past its first page");

    let http = HttpClient::login_root(harness).await;
    let reply = json_of(http.get("/streams").await).await;
    let mut bare: Vec<&str> = reply
        .as_array()
        .expect("the bare array")
        .iter()
        .map(|stream| stream["name"].as_str().expect("a stream name"))
        .collect();
    bare.sort_unstable();
    assert_eq!(bare, names, "the bare array holds every page");
}

/// Stream `http-pages` with topic `events` of [`PARTITIONS`] partitions and
/// one consumer group, created over TCP.
async fn seed(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream_id = Identifier::named(STREAM).unwrap();
    let topic_id = Identifier::named(TOPIC).unwrap();
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    client
        .create_topic(
            &stream_id,
            TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    client
        .create_consumer_group(&stream_id, &topic_id, "workers")
        .await
        .expect("create consumer group");
}

async fn json_of(response: Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.expect("a JSON reply")
}

fn ids(page: &Value) -> Vec<u64> {
    page["items"]
        .as_array()
        .expect("an items array")
        .iter()
        .map(|item| item["id"].as_u64().expect("an item id"))
        .collect()
}
