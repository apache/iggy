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

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const GRANT_BUDGET: Duration = Duration::from_secs(20);
const RETRY_PAUSE: Duration = Duration::from_millis(200);

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_one_readable_stream_when_listing_should_return_that_stream(harness: &TestHarness) {
    let root = harness.root_client().await.expect("root client");
    let stream = root
        .create_stream("orders", &StreamCreateOptions::default())
        .await
        .expect("create orders");
    root.create_stream("archive", &StreamCreateOptions::default())
        .await
        .expect("create archive");
    let reader = scoped_reader(
        harness,
        &root,
        stream.id,
        StreamPermissions {
            read_stream: true,
            ..Default::default()
        },
    )
    .await;
    let visible = reader.get_streams().await.expect("list readable streams");
    assert_eq!(
        visible
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        [stream.id]
    );
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_one_readable_topic_when_listing_should_return_that_topic(harness: &TestHarness) {
    let root = harness.root_client().await.expect("root client");
    let stream = root
        .create_stream("orders", &StreamCreateOptions::default())
        .await
        .expect("create orders");
    let stream_id = Identifier::numeric(stream.id).expect("stream id");
    let topic = root
        .create_topic(&stream_id, "events", &TopicCreateOptions::default())
        .await
        .expect("create events");
    root.create_topic(&stream_id, "history", &TopicCreateOptions::default())
        .await
        .expect("create history");
    let reader = scoped_reader(
        harness,
        &root,
        stream.id,
        StreamPermissions {
            topics: Some(BTreeMap::from([(
                topic.id as usize,
                TopicPermissions {
                    read_topic: true,
                    ..Default::default()
                },
            )])),
            ..Default::default()
        },
    )
    .await;
    let visible = reader
        .get_topics(&stream_id)
        .await
        .expect("list readable topics");
    assert_eq!(
        visible
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        [topic.id]
    );
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_no_grant_when_listing_should_return_empty_lists(harness: &TestHarness) {
    let root = harness.root_client().await.expect("root client");
    let stream = root
        .create_stream("orders", &StreamCreateOptions::default())
        .await
        .expect("create orders");
    let stream_id = Identifier::numeric(stream.id).expect("stream id");
    root.create_topic(&stream_id, "events", &TopicCreateOptions::default())
        .await
        .expect("create events");
    let reader = reader(harness, &root, None).await;

    let streams = reader.get_streams().await.expect("list streams");
    let topics = reader.get_topics(&stream_id).await.expect("list topics");
    let missing = reader
        .get_topics(&Identifier::numeric(stream.id + 7).expect("stream id"))
        .await
        .expect("list topics of a missing stream");

    assert!(streams.is_empty(), "{streams:?}");
    assert!(topics.is_empty(), "{topics:?}");
    assert!(missing.is_empty(), "{missing:?}");
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_topic_grant_when_listing_streams_should_leave_its_stream_out(harness: &TestHarness) {
    let root = harness.root_client().await.expect("root client");
    let stream = root
        .create_stream("orders", &StreamCreateOptions::default())
        .await
        .expect("create orders");
    let stream_id = Identifier::numeric(stream.id).expect("stream id");
    let topic = root
        .create_topic(&stream_id, "events", &TopicCreateOptions::default())
        .await
        .expect("create events");
    let reader = scoped_reader(
        harness,
        &root,
        stream.id,
        StreamPermissions {
            topics: Some(BTreeMap::from([(
                topic.id as usize,
                TopicPermissions {
                    read_topic: true,
                    ..Default::default()
                },
            )])),
            ..Default::default()
        },
    )
    .await;

    let streams = reader.get_streams().await.expect("list streams");

    assert!(streams.is_empty(), "{streams:?}");
    assert!(matches!(
        reader.get_stream(&stream_id).await,
        Err(IggyError::Unauthorized)
    ));
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_a_grant_moved_to_another_stream_when_listing_again_should_match_the_direct_reads(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    let mut streams = Vec::new();
    for name in ["orders", "archive"] {
        let stream = root
            .create_stream(name, &StreamCreateOptions::default())
            .await
            .expect("create a stream");
        streams.push(stream.id);
    }
    let read_stream = || StreamPermissions {
        read_stream: true,
        ..Default::default()
    };
    let reader = scoped_reader(harness, &root, streams[0], read_stream()).await;
    assert_reads_only(&reader, &streams, streams[0]).await;

    root.update_permissions(
        &Identifier::named("reader").expect("reader name"),
        Some(stream_grant(streams[1], read_stream())),
    )
    .await
    .expect("move the grant to the other stream");

    assert_reads_only(&reader, &streams, streams[1]).await;
}

/// Waits until the list and the direct reads both serve only `expected`. The
/// node serving the reader applies a grant change after the write returns.
async fn assert_reads_only(reader: &IggyClient, candidates: &[u32], expected: u32) {
    let deadline = Instant::now() + GRANT_BUDGET;
    loop {
        let served = (
            listed_streams(reader).await,
            readable_streams(reader, candidates).await,
        );
        if served == (vec![expected], vec![expected]) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the list and the direct reads serve {served:?}, expected only stream {expected}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

async fn listed_streams(reader: &IggyClient) -> Vec<u32> {
    reader
        .get_streams()
        .await
        .expect("list streams")
        .into_iter()
        .map(|entry| entry.id)
        .collect()
}

/// The streams of `candidates` a direct read serves the reader.
async fn readable_streams(reader: &IggyClient, candidates: &[u32]) -> Vec<u32> {
    let mut readable = Vec::new();
    for &stream_id in candidates {
        match reader
            .get_stream(&Identifier::numeric(stream_id).expect("stream id"))
            .await
        {
            Ok(Some(_)) => readable.push(stream_id),
            Err(IggyError::Unauthorized) => {}
            other => panic!("unexpected direct read of stream {stream_id}: {other:?}"),
        }
    }
    readable
}

fn stream_grant(stream_id: u32, permissions: StreamPermissions) -> Permissions {
    Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(stream_id as usize, permissions)])),
    }
}

async fn scoped_reader(
    harness: &TestHarness,
    root: &IggyClient,
    stream_id: u32,
    permissions: StreamPermissions,
) -> IggyClient {
    reader(harness, root, Some(stream_grant(stream_id, permissions))).await
}

async fn reader(
    harness: &TestHarness,
    root: &IggyClient,
    permissions: Option<Permissions>,
) -> IggyClient {
    root.create_user(
        "reader",
        "example-password",
        UserStatus::Active,
        permissions,
        &UserCreateOptions::default(),
    )
    .await
    .expect("create reader");
    let reader = harness.new_client().await.expect("reader client");
    reader
        .login_user("reader", "example-password")
        .await
        .expect("sign in reader");
    reader
}
