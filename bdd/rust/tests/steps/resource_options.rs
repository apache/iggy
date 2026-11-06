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

use crate::common::global_context::GlobalContext;
use cucumber::{given, then, when};
use iggy::prelude::{
    Consumer, HeaderKey, HeaderValue, Identifier, IggyExpiry, IggyMessage, MessageClient,
    OptionKey, OptionSpec, OptionsScope, Partitioning, PollOptions, PollingStrategy, StreamClient,
    StreamCreateOptions, StreamDetails, StreamEncryptionKey, StreamEncryptionSource,
    StreamUpdateOptions, SystemClient, TopicClient, TopicCreateOptions,
};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

const KEY_LENGTH: usize = 32;
const PARTITION_ID: u32 = 0;

#[when(
    regex = r#"^I create a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+)$"#
)]
pub async fn when_create_stream_with_key(world: &mut GlobalContext, name: String, byte: u8) {
    create_keyed_stream(world, &name, byte).await;
}

#[given(
    regex = r#"^a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+) exists$"#
)]
pub async fn given_keyed_stream(world: &mut GlobalContext, name: String, byte: u8) {
    create_keyed_stream(world, &name, byte).await;
}

#[when(regex = r#"^I create a stream named "([^"]+)" without an encryption key$"#)]
pub async fn when_create_stream_without_key(world: &mut GlobalContext, name: String) {
    let client = world.client.as_ref().expect("client");
    let stream = client
        .create_stream(&name, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    world.last_stream_id = Some(stream.id);
    world.last_stream_name = Some(stream.name);
    world.last_stream_key = None;
}

#[then(regex = r#"^the stream should report encryption key source "([^"]+)"$"#)]
pub async fn then_stream_source(world: &mut GlobalContext, source: String) {
    let stream = fetch_stream(world).await;
    let expected = StreamEncryptionSource::from_str(&source).expect("known source");
    assert_eq!(stream.encryption_key_source(), Some(expected));
}

#[then("the stream encryption key fingerprint should match the key")]
pub async fn then_fingerprint_matches_key(world: &mut GlobalContext) {
    let stream = fetch_stream(world).await;
    let key = world.last_stream_key.as_ref().expect("stream key");
    assert_eq!(stream.encryption_key_fingerprint(), Some(key.fingerprint()));
}

#[then("the stream should report an encryption key fingerprint")]
pub async fn then_stream_reports_fingerprint(world: &mut GlobalContext) {
    let stream = fetch_stream(world).await;
    let fingerprint = stream.encryption_key_fingerprint().expect("fingerprint");
    assert_eq!(fingerprint.len(), 16);
}

#[then("the stream should report at least one KEK fingerprint")]
pub async fn then_stream_reports_kek(world: &mut GlobalContext) {
    let stream = fetch_stream(world).await;
    assert!(!stream.encryption_kek_fingerprints().is_empty());
}

#[then("the stream should expose no encryption key material")]
pub async fn then_no_key_material(world: &mut GlobalContext) {
    let stream = fetch_stream(world).await;
    for secret in [OptionKey::EncryptionKey, OptionKey::EncryptionKeyWrapped] {
        assert!(
            !stream.options.contains_key(&HeaderKey::from(secret)),
            "{secret} must never be returned"
        );
    }
}

#[when(regex = r#"^I create a topic named "([^"]+)" with (\d+) partitions? in the stream$"#)]
pub async fn when_create_topic_in_stream(
    world: &mut GlobalContext,
    name: String,
    partitions_count: u32,
) {
    let client = world.client.as_ref().expect("client");
    let topic = client
        .create_topic(
            &stream_identifier(world),
            &name,
            &TopicCreateOptions {
                partitions_count: Some(partitions_count),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    world.last_topic_id = Some(topic.id);
    world.last_topic_name = Some(topic.name);
}

#[when(regex = r#"^I send a message with payload "([^"]+)" to the topic$"#)]
pub async fn when_send_payload(world: &mut GlobalContext, payload: String) {
    send_message(world, &payload, BTreeMap::new()).await;
}

#[when(
    regex = r#"^I send a message with payload "([^"]+)" and header "([^"]+)" set to "([^"]+)" to the topic$"#
)]
pub async fn when_send_payload_with_header(
    world: &mut GlobalContext,
    payload: String,
    header: String,
    value: String,
) {
    let headers = BTreeMap::from([(
        HeaderKey::from_str(&header).expect("header key"),
        HeaderValue::from_str(&value).expect("header value"),
    )]);
    send_message(world, &payload, headers).await;
}

#[then(regex = r#"^polling the topic should return the payload "([^"]+)"$"#)]
pub async fn then_poll_returns_payload(world: &mut GlobalContext, payload: String) {
    let message = poll_first(world).await;
    assert_eq!(message.payload.as_ref(), payload.as_bytes());
}

#[then(
    regex = r#"^polling the topic should return the payload "([^"]+)" and header "([^"]+)" set to "([^"]+)"$"#
)]
pub async fn then_poll_returns_payload_and_header(
    world: &mut GlobalContext,
    payload: String,
    header: String,
    value: String,
) {
    let message = poll_first(world).await;
    assert_eq!(message.payload.as_ref(), payload.as_bytes());
    let headers = message
        .user_headers_map()
        .expect("readable headers")
        .expect("headers present");
    assert_eq!(
        headers.get(&HeaderKey::from_str(&header).expect("header key")),
        Some(&HeaderValue::from_str(&value).expect("header value"))
    );
}

#[when(regex = r#"^I rename the stream to "([^"]+)"$"#)]
pub async fn when_rename_stream(world: &mut GlobalContext, name: String) {
    let client = world.client.as_ref().expect("client");
    client
        .update_stream(
            &stream_identifier(world),
            &StreamUpdateOptions {
                name: Some(name),
                ..StreamUpdateOptions::default()
            },
        )
        .await
        .expect("rename stream");
}

#[then(regex = r#"^the stream name should be "([^"]+)"$"#)]
pub async fn then_stream_name(world: &mut GlobalContext, name: String) {
    assert_eq!(fetch_stream(world).await.name, name);
}

#[when(regex = r#"^I request a reset of the stream option "([^"]+)"$"#)]
pub async fn when_reset_stream_option(world: &mut GlobalContext, key: String) {
    let client = world.client.as_ref().expect("client");
    let result = client
        .update_stream(
            &stream_identifier(world),
            &StreamUpdateOptions {
                reset: BTreeSet::from([OptionKey::from_str(&key).expect("known key")]),
                ..StreamUpdateOptions::default()
            },
        )
        .await;
    world.last_update_error = result.err();
}

#[then("the stream update should be rejected")]
pub async fn then_stream_update_rejected(world: &mut GlobalContext) {
    assert!(world.last_update_error.is_some(), "the update must fail");
}

#[when("I describe the stream options")]
pub async fn when_describe_stream_options(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("client");
    world.last_option_specs = client
        .describe_options(OptionsScope::Stream)
        .await
        .expect("describe options");
}

#[then(regex = r#"^the stream option "([^"]+)" should be updatable and not secret$"#)]
pub async fn then_option_updatable(world: &mut GlobalContext, key: String) {
    let spec = option_spec(world, &key);
    assert!(spec.updatable && !spec.secret && !spec.read_only);
}

#[then(regex = r#"^the stream option "([^"]+)" should be create only and secret$"#)]
pub async fn then_option_create_only_secret(world: &mut GlobalContext, key: String) {
    let spec = option_spec(world, &key);
    assert!(!spec.updatable && !spec.resettable && spec.secret && !spec.read_only);
}

#[then(regex = r#"^the stream option "([^"]+)" should be read only$"#)]
pub async fn then_option_read_only(world: &mut GlobalContext, key: String) {
    let spec = option_spec(world, &key);
    assert!(spec.read_only && !spec.updatable && !spec.secret);
}

async fn create_keyed_stream(world: &mut GlobalContext, name: &str, byte: u8) {
    let key = StreamEncryptionKey::try_from([byte; KEY_LENGTH].as_slice()).expect("32 bytes");
    let client = world.client.as_ref().expect("client");
    let stream = client
        .create_stream(
            name,
            &StreamCreateOptions {
                encryption_key: Some(key.clone()),
                ..StreamCreateOptions::default()
            },
        )
        .await
        .expect("create keyed stream");
    world.last_stream_id = Some(stream.id);
    world.last_stream_name = Some(stream.name);
    world.last_stream_key = Some(key);
}

async fn fetch_stream(world: &GlobalContext) -> StreamDetails {
    let client = world.client.as_ref().expect("client");
    client
        .get_stream(&stream_identifier(world))
        .await
        .expect("get stream")
        .expect("stream exists")
}

async fn send_message(
    world: &GlobalContext,
    payload: &str,
    headers: BTreeMap<HeaderKey, HeaderValue>,
) {
    let client = world.client.as_ref().expect("client");
    let builder = IggyMessage::builder().payload(payload.to_owned().into());
    let message = if headers.is_empty() {
        builder.build()
    } else {
        builder.user_headers(headers).build()
    }
    .expect("message");
    client
        .send_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            &Partitioning::partition_id(PARTITION_ID),
            &mut [message],
        )
        .await
        .expect("send message");
}

async fn poll_first(world: &GlobalContext) -> IggyMessage {
    let client = world.client.as_ref().expect("client");
    let polled = client
        .poll_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll messages");
    polled
        .messages
        .into_iter()
        .next()
        .expect("one message polled")
}

fn option_spec<'a>(world: &'a GlobalContext, key: &str) -> &'a OptionSpec {
    world
        .last_option_specs
        .iter()
        .find(|spec| spec.key == key)
        .expect("described key")
}

fn stream_identifier(world: &GlobalContext) -> Identifier {
    Identifier::numeric(world.last_stream_id.expect("stream created")).expect("stream id")
}

fn topic_identifier(world: &GlobalContext) -> Identifier {
    Identifier::numeric(world.last_topic_id.expect("topic created")).expect("topic id")
}
