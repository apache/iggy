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

use std::{process::Command, time::Duration};

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Source, source::SourceBatchResult,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

use crate::{
    S3Source, client,
    config::{ResolvedConfig, S3SourceConfig},
    state::{SourceState, StateTracker},
};

fn config(server: &MockServer) -> S3SourceConfig {
    serde_json::from_value(serde_json::json!({
        "bucket": "events", "region": "us-east-1", "endpoint": server.uri(),
        "prefix": "logs/", "poll_interval": "1ms",
        "max_record_bytes": 16, "max_batch_bytes": 32, "max_batch_messages": 1
    }))
    .expect("config")
}

async fn source(server: &MockServer, checkpoint: Option<ConnectorState>) -> S3Source {
    let config = config(server);
    let resolved = ResolvedConfig::try_from(&config).expect("resolved config");
    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(server.uri())
            .force_path_style(true)
            .credentials_provider(Credentials::new("test", "test", None, None, "unit-test"))
            .retry_config(RetryConfig::disabled())
            .build(),
    );
    let restored = checkpoint
        .map(|checkpoint| {
            checkpoint
                .deserialize::<SourceState>("S3 source", 7)
                .expect("checkpoint")
        })
        .unwrap_or_default();
    let mut source = S3Source::new(7, config, None);
    source.client = Some(client);
    source.resolved_config = Some(resolved);
    *source.state.get_mut() = StateTracker::new(restored);
    source
}

async fn listing(server: &MockServer, size: usize) {
    Mock::given(method("GET")).and(path("/events/"))
        .and(query_param("list-type", "2")).and(query_param("prefix", "logs/"))
        .and(query_param("encoding-type", "url"))
        .and(query_param("max-keys", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>logs/a</Key><ETag>"v1"</ETag><Size>{size}</Size></Contents></ListBucketResult>"#
        ))).mount(server).await;
}

async fn full_get(server: &MockServer, body: &'static [u8], etag: &str) {
    Mock::given(method("GET"))
        .and(path("/events/logs/a"))
        .and(header("if-match", etag))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", etag)
                .set_body_bytes(body),
        )
        .with_priority(10)
        .mount(server)
        .await;
}

async fn poll_with_paused_delay(source: &S3Source) -> Result<ProducedMessages, Error> {
    let delay = source
        .recovery
        .lock()
        .await
        .retry_delay
        .max(Duration::from_millis(1));
    tokio::time::pause();
    let poll = source.poll();
    tokio::pin!(poll);
    tokio::select! {
        biased;
        result = &mut poll => panic!("poll returned before its delay: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    tokio::time::advance(delay).await;
    // Network I/O uses real time so SDK timers cannot outrun the local server.
    tokio::time::resume();
    poll.await
}

#[test]
fn given_fresh_source_when_opened_should_probe_access_without_advancing_state() {
    const CHILD_PROCESS_ENV: &str = "IGGY_S3_PROBE_TEST_CHILD";
    if std::env::var_os(CHILD_PROCESS_ENV).is_none() {
        // AWS credentials are process-global, so isolate discovery from other tests.
        let output = Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg(std::thread::current().name().expect("test name"))
            .env_clear()
            .env(CHILD_PROCESS_ENV, "1")
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_CONFIG_FILE", "/dev/null")
            .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            .output()
            .expect("isolated probe test");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        for scenario in [
            "nonempty",
            "empty_object",
            "empty_listing",
            "denied_list",
            "denied_get",
        ] {
            let server = MockServer::start().await;
            let has_object = !matches!(scenario, "empty_listing" | "denied_list");
            if has_object {
                listing(
                    &server,
                    if scenario == "empty_object" {
                        0
                    } else {
                        1_000_000
                    },
                )
                .await;
                let response = match scenario {
                    "empty_object" => ResponseTemplate::new(200)
                        .insert_header("etag", "\"v1\"")
                        .set_body_bytes(b""),
                    "denied_get" => ResponseTemplate::new(403)
                        .set_body_string("<Error><Code>AccessDenied</Code></Error>"),
                    _ => ResponseTemplate::new(206)
                        .insert_header("etag", "\"v1\"")
                        .insert_header("content-range", "bytes 0-0/1000000")
                        .set_body_bytes(b"a"),
                };
                let mut mock = Mock::given(method("GET"))
                    .and(path("/events/logs/a"))
                    .and(header("if-match", "\"v1\""));
                if scenario == "empty_object" {
                    mock = mock
                        .and(|request: &wiremock::Request| !request.headers.contains_key("range"));
                } else {
                    mock = mock.and(header("range", "bytes=0-0"));
                }
                mock.respond_with(response).expect(1).mount(&server).await;
            } else {
                let response = if scenario == "denied_list" {
                    ResponseTemplate::new(403)
                        .set_body_string("<Error><Code>AccessDenied</Code></Error>")
                } else {
                    ResponseTemplate::new(200).set_body_string(
                        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
                    )
                };
                Mock::given(method("GET"))
                    .and(query_param("list-type", "2"))
                    .respond_with(response)
                    .expect(1)
                    .mount(&server)
                    .await;
            }
            let mut source = S3Source::new(7, config(&server), None);
            let result = source.open().await;
            if scenario.starts_with("denied") {
                assert!(
                    matches!(result, Err(Error::InitError(_))),
                    "{scenario}: {result:?}"
                );
                assert!(source.client.is_none());
            } else {
                result.expect(scenario);
                assert!(source.client.is_some());
            }
            let tracker = source.state.lock().await;
            assert!(tracker.active_object().is_none());
            assert!(tracker.pending_checkpoint().is_none());
            assert!(!source.scan.lock().await.is_exhausted());
            assert_eq!(
                server.received_requests().await.expect("requests").len(),
                if has_object { 2 } else { 1 }
            );
        }
    });
}

#[test]
fn given_probe_object_replaced_when_refreshed_should_recompute_range_for_its_size() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        for (old_size, new_size) in [(4, 0), (0, 4)] {
            let server = MockServer::start().await;
            listing(&server, old_size).await;
            Mock::given(method("GET"))
                .and(path("/events/logs/a"))
                .and(header("if-match", "\"v1\""))
                .respond_with(
                    ResponseTemplate::new(412)
                        .set_body_string("<Error><Code>PreconditionFailed</Code></Error>"),
                )
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("HEAD"))
                .and(path("/events/logs/a"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("etag", "\"v2\"")
                        .insert_header("content-length", new_size.to_string()),
                )
                .expect(1)
                .mount(&server)
                .await;
            let mock = Mock::given(method("GET"))
                .and(path("/events/logs/a"))
                .and(header("if-match", "\"v2\""));
            if new_size == 0 {
                mock.and(|request: &wiremock::Request| !request.headers.contains_key("range"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("etag", "\"v2\"")
                            .set_body_bytes(b""),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
            } else {
                mock.and(header("range", "bytes=0-0"))
                    .respond_with(
                        ResponseTemplate::new(206)
                            .insert_header("etag", "\"v2\"")
                            .insert_header("content-range", "bytes 0-0/4")
                            .set_body_bytes(b"x"),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
            }
            let source = source(&server, None).await;
            client::validate_access(
                source.client.as_ref().expect("client"),
                source.resolved_config.as_ref().expect("config"),
            )
            .await
            .expect("probe replacement");
            assert_eq!(server.received_requests().await.expect("requests").len(), 4);
        }
    });
}

#[test]
fn given_invalid_listings_when_polled_should_reject_without_advancing_or_exhausting() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        for (key, cursor, expected) in [
            (None, None, "empty truncated listing"),
            (Some("outside/a"), None, "prefix/StartAfter"),
            (Some("logs/a"), Some("logs/a"), "prefix/StartAfter"),
            (Some("logs/0"), Some("logs/a"), "prefix/StartAfter"),
        ] {
            let server = MockServer::start().await;
            let contents = key.map_or_else(String::new, |key| {
                format!("<Contents><Key>{key}</Key><ETag>\"v1\"</ETag><Size>4</Size></Contents>")
            });
            Mock::given(method("GET"))
                .and(query_param("list-type", "2"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "<ListBucketResult><IsTruncated>true</IsTruncated>{contents}</ListBucketResult>"
                )))
                .expect(1)
                .mount(&server)
                .await;
            let source = source(&server, None).await;
            if let Some(cursor) = cursor {
                source.scan.lock().await.advance_after(cursor.into());
            }
            let error = source.poll().await.expect_err("invalid listing");
            assert!(error.to_string().contains(expected), "{error}");
            let scan = source.scan.lock().await;
            assert_eq!(scan.start_after(), cursor);
            assert!(!scan.is_exhausted());
            let tracker = source.state.lock().await;
            assert!(tracker.active_object().is_none());
            assert!(tracker.pending_checkpoint().is_none());
            assert_eq!(server.received_requests().await.expect("requests").len(), 1);
        }
    });
}

#[test]
fn given_refreshed_object_when_get_fails_should_cache_identity_until_ack() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        let source = source(&server, None).await;
        source.poll().await.expect("initial batch");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        let committed = source.state.lock().await.active_object().cloned();
        server.reset().await;
        Mock::given(method("GET"))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(412)
                    .set_body_string("<Error><Code>PreconditionFailed</Code></Error>"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/events/logs/a"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v2\"")
                    .insert_header("content-length", "4"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(header("if-match", "\"v2\""))
            .respond_with(
                ResponseTemplate::new(503).set_body_string("<Error><Code>SlowDown</Code></Error>"),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        full_get(&server, b"x\ny\n", "\"v2\"").await;
        let error = source.poll().await.expect_err("replacement GET failure");
        assert!(
            error
                .to_string()
                .contains("requested offset 2, retry offset 0"),
            "{error}"
        );
        assert_eq!(
            source.state.lock().await.active_object(),
            committed.as_ref()
        );
        assert!(source.state.lock().await.pending_checkpoint().is_none());
        let first = poll_with_paused_delay(&source)
            .await
            .expect("cached replacement");
        assert_eq!(first.messages[0].payload, b"x");
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack");
        assert_eq!(
            source.state.lock().await.active_object(),
            committed.as_ref()
        );
        let replay = source.poll().await.expect("replay replacement");
        assert_eq!(replay.messages[0].payload, b"x");
        assert_eq!(replay.messages[0].id, first.messages[0].id);
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack replacement");
        assert!(source.recovery.lock().await.refreshed_object.is_none());
        let active = source
            .state
            .lock()
            .await
            .active_object()
            .cloned()
            .expect("active");
        assert_eq!(active.etag, "\"v2\"");
        assert_eq!(active.next_byte_offset, 2);
        Mock::given(method("GET"))
            .and(header("range", "bytes=2-"))
            .and(header("if-match", "\"v2\""))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "\"v2\"")
                    .insert_header("content-range", "bytes 2-3/4")
                    .set_body_bytes(b"y\n"),
            )
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            source.poll().await.expect("resume new version").messages[0].payload,
            b"y"
        );
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("completion");
        assert_eq!(source.scan.lock().await.start_after(), Some("logs/a"));
    });
}

#[test]
fn given_batches_when_nacked_then_acked_should_replay_resume_and_exhaust() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a"))
            .and(header("range", "bytes=2-"))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "\"v1\"")
                    .insert_header("content-range", "bytes 2-3/4")
                    .set_body_bytes(b"b\n"),
            )
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/events/"))
            .and(query_param("start-after", "logs/a"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            ))
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        let source = source(&server, None).await;

        let first = source.poll().await.expect("first batch");
        assert_eq!(first.messages[0].payload, b"a");
        assert!(source.state.lock().await.active_object().is_none());
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack");
        let replay = source.poll().await.expect("replay");
        assert_eq!(first.messages[0].id, replay.messages[0].id);
        assert_eq!(
            first.state.expect("state").0,
            replay.state.expect("state").0
        );
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");

        assert_eq!(
            source
                .state
                .lock()
                .await
                .active_object()
                .expect("active")
                .next_byte_offset,
            2
        );
        let last = source.poll().await.expect("last batch");
        assert_eq!(last.messages[0].payload, b"b");
        assert_eq!(
            last.state
                .expect("state")
                .deserialize::<SourceState>("S3 source", 7),
            Some(SourceState::default())
        );
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("completion ack");
        assert_eq!(source.scan.lock().await.start_after(), Some("logs/a"));
        let empty = source.poll().await.expect("exhausted");
        assert!(empty.messages.is_empty());
        assert!(empty.state.is_none());
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("idle ack");
        let request_count = server.received_requests().await.expect("requests").len();
        assert!(source.poll().await.expect("idle").messages.is_empty());
        assert_eq!(
            server.received_requests().await.expect("requests").len(),
            request_count
        );
    });
}

#[test]
fn given_encoded_key_when_listed_should_decode_once_and_preserve_checkpoint_and_cursor() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        let key = "logs/a %25+雪\u{1}";
        Mock::given(method("GET"))
            .and(query_param("list-type", "2"))
            .and(query_param("encoding-type", "url"))
            .and(query_param("prefix", "logs/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>logs%2Fa%20%2525%2B%E9%9B%AA%01</Key><ETag>"v1"</ETag><Size>4</Size></Contents></ListBucketResult>"#,
            ))
            .with_priority(2)
            .expect(1)
            .mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a%20%2525%2B%E9%9B%AA%01"))
            .and(header("if-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"v1\"").set_body_bytes(b"a\nb\n"))
            .with_priority(2)
            .expect(1)
            .mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a%20%2525%2B%E9%9B%AA%01"))
            .and(header("range", "bytes=2-"))
            .respond_with(ResponseTemplate::new(206).insert_header("etag", "\"v1\"")
                .insert_header("content-range", "bytes 2-3/4").set_body_bytes(b"b\n"))
            .with_priority(1)
            .expect(1)
            .mount(&server).await;
        Mock::given(method("GET"))
            .and(query_param("start-after", key))
            .and(query_param("encoding-type", "url"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<ListBucketResult/>"))
            .with_priority(1)
            .expect(1)
            .mount(&server).await;
        let source = source(&server, None).await;
        let batch = source.poll().await.expect("encoded key batch");
        let checkpoint = batch.state.expect("checkpoint")
            .deserialize::<SourceState>("S3 source", 7).expect("valid checkpoint");
        assert_eq!(checkpoint.active_object.expect("active object").key, key);
        source.on_batch_result(SourceBatchResult::Ack).await.expect("ack");
        assert_eq!(source.poll().await.expect("remaining batch").messages[0].payload, b"b");
        source.on_batch_result(SourceBatchResult::Ack).await.expect("completion ack");
        assert_eq!(source.scan.lock().await.start_after(), Some(key));
        assert!(source.poll().await.expect("exhausted").state.is_none());
    });
}

#[test]
fn given_unencoded_key_when_listed_should_preserve_literal_percent_and_plus() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("encoding-type", "url"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<ListBucketResult><Contents><Key>logs/a%25+雪</Key><ETag>"v1"</ETag><Size>4</Size></Contents></ListBucketResult>"#,
            ))
            .mount(&server).await;
        let source = source(&server, None).await;
        let object = client::list_next_object(source.client.as_ref().expect("client"),
            source.resolved_config.as_ref().expect("config"), None)
            .await.expect("listing").expect("object");
        assert_eq!(object.key, "logs/a%25+雪");
    });
}

#[test]
fn given_encoded_non_utf8_key_when_listed_should_reject_without_exhausting_scan() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("encoding-type", "url"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>logs%2F%FF</Key><ETag>"v1"</ETag><Size>4</Size></Contents></ListBucketResult>"#,
            ))
            .mount(&server).await;
        let source = source(&server, None).await;
        assert!(matches!(source.poll().await, Err(Error::PermanentHttpError(_))));
        assert!(!source.scan.lock().await.is_exhausted());
        assert!(source.state.lock().await.pending_checkpoint().is_none());
    });
}

#[test]
fn given_saved_checkpoint_when_restarted_should_resume_before_listing() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        let initial = source(&server, None).await;
        let first = initial.poll().await.expect("first batch");
        let checkpoint = first.state.expect("state");
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a"))
            .and(header("range", "bytes=2-"))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "\"v1\"")
                    .insert_header("content-range", "bytes 2-3/4")
                    .set_body_bytes(b"b\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let resumed = source(&server, Some(checkpoint)).await;
        assert_eq!(
            resumed.poll().await.expect("resume").messages[0].payload,
            b"b"
        );
        assert_eq!(server.received_requests().await.expect("requests").len(), 1);
    });
}

#[test]
fn given_replaced_active_object_when_polled_should_restart_new_version_at_zero() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        let source = source(&server, None).await;
        source.poll().await.expect("first batch");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        server.reset().await;
        Mock::given(method("GET"))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(412)
                    .set_body_string("<Error><Code>PreconditionFailed</Code></Error>"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/events/logs/a"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v2\"")
                    .insert_header("content-length", "4"),
            )
            .expect(1)
            .mount(&server)
            .await;
        full_get(&server, b"x\ny\n", "\"v2\"").await;
        let batch = source.poll().await.expect("replacement");
        assert_eq!(batch.messages[0].payload, b"x");
        assert_eq!(
            source
                .state
                .lock()
                .await
                .active_object()
                .expect("active")
                .etag,
            "\"v1\""
        );
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        assert_eq!(
            source
                .state
                .lock()
                .await
                .active_object()
                .expect("active")
                .etag,
            "\"v2\""
        );
    });
}

#[test]
fn given_oversized_record_when_repolled_should_latch_without_more_requests() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        let body = b"01234567890123456\n";
        listing(&server, body.len()).await;
        full_get(&server, body, "\"v1\"").await;
        let source = source(&server, None).await;
        assert!(matches!(
            source.poll().await,
            Err(Error::PermanentHttpError(_))
        ));
        let requests = server.received_requests().await.expect("requests").len();
        assert!(matches!(
            source.poll().await,
            Err(Error::PermanentHttpError(_))
        ));
        assert_eq!(
            server.received_requests().await.expect("requests").len(),
            requests
        );
        assert!(source.state.lock().await.pending_checkpoint().is_none());
    });
}

#[test]
fn given_bad_resumed_range_when_polled_should_preserve_committed_offset() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        let source = source(&server, None).await;
        source.poll().await.expect("first batch");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "\"v1\"")
                    .insert_header("content-range", "bytes 0-1/4")
                    .set_body_bytes(b"a\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        assert!(matches!(
            source.poll().await,
            Err(Error::PermanentHttpError(_))
        ));
        let tracker = source.state.lock().await;
        assert_eq!(tracker.active_object().expect("active").next_byte_offset, 2);
        assert!(tracker.pending_checkpoint().is_none());
    });
}

#[test]
fn given_empty_object_when_polled_should_stage_completion_until_ack() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 0).await;
        full_get(&server, b"", "\"v1\"").await;
        let source = source(&server, None).await;
        let batch = source.poll().await.expect("empty object");
        assert!(batch.messages.is_empty());
        assert!(batch.state.is_some());
        assert_eq!(source.scan.lock().await.start_after(), None);
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack");
        assert_eq!(source.scan.lock().await.start_after(), None);
        source.poll().await.expect("replay empty object");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        assert_eq!(source.scan.lock().await.start_after(), Some("logs/a"));
    });
}

#[test]
fn given_list_failure_when_retried_should_back_off_without_exhausting() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(503).set_body_string("<Error><Code>SlowDown</Code></Error>"),
            )
            .mount(&server)
            .await;
        let source = source(&server, None).await;
        assert!(matches!(
            source.poll().await,
            Err(Error::HttpRequestFailed(_))
        ));
        assert_eq!(
            source.recovery.lock().await.retry_delay,
            Duration::from_secs(1)
        );
        assert!(!source.scan.lock().await.is_exhausted());
        server.reset().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        source.poll().await.expect("recovery");
        assert_eq!(source.recovery.lock().await.retry_delay, Duration::ZERO);
    });
}

#[test]
fn given_cancelled_body_read_when_retried_should_preserve_committed_progress() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        listing(&server, 4).await;
        Mock::given(method("GET"))
            .and(path("/events/logs/a"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(b"a\nb\n")
                    .set_delay(Duration::from_secs(1)),
            )
            .mount(&server)
            .await;
        let source = source(&server, None).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), source.poll())
                .await
                .is_err()
        );
        assert!(source.state.lock().await.active_object().is_none());
        assert!(source.state.lock().await.pending_checkpoint().is_none());
        server.reset().await;
        listing(&server, 4).await;
        full_get(&server, b"a\nb\n", "\"v1\"").await;
        assert_eq!(
            source.poll().await.expect("retry").messages[0].payload,
            b"a"
        );
    });
}

#[test]
fn given_denied_list_when_polled_should_not_advance_or_exhaust() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string("<Error><Code>AccessDenied</Code></Error>"),
            )
            .mount(&server)
            .await;
        let source = source(&server, None).await;
        assert!(matches!(
            source.poll().await,
            Err(Error::PermanentHttpError(_))
        ));
        assert!(!source.scan.lock().await.is_exhausted());
        assert!(source.state.lock().await.pending_checkpoint().is_none());
        assert_eq!(
            source.recovery.lock().await.retry_delay,
            Duration::from_secs(1)
        );
    });
}

#[test]
fn given_restored_active_object_when_unavailable_should_retry_and_recover_without_restart() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        for (status, code) in [(403, "AccessDenied"), (404, "NoSuchKey")] {
            let server = MockServer::start().await;
            listing(&server, 4).await;
            full_get(&server, b"a\nb\n", "\"v1\"").await;
            let mut source = source(&server, None).await;
            let first = source.poll().await.expect("first batch");
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ack");
            let committed = source.state.lock().await.active_object().cloned();
            let client = source.client.clone();
            source.close().await.expect("close before restart");
            server.reset().await;
            Mock::given(method("GET"))
                .and(path("/events/logs/a"))
                .and(header("range", "bytes=2-"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .set_body_string(format!("<Error><Code>{code}</Code></Error>")),
                )
                .expect(7)
                .mount(&server)
                .await;

            let mut source = S3Source::new(7, source.config, first.state);
            source.open().await.expect("resume unavailable object");
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests")
                    .is_empty()
            );
            // Poll with fixture credentials without mutating the process environment.
            source.client = client;

            for seconds in [1, 2, 4, 8, 16, 30, 30] {
                assert!(matches!(
                    poll_with_paused_delay(&source).await,
                    Err(Error::PermanentHttpError(_))
                ));
                assert_eq!(
                    source.recovery.lock().await.retry_delay,
                    Duration::from_secs(seconds)
                );
                assert!(source.recovery.lock().await.latched_error.is_none());
                let tracker = source.state.lock().await;
                assert_eq!(tracker.active_object(), committed.as_ref());
                assert!(tracker.pending_checkpoint().is_none());
                assert!(!source.scan.lock().await.is_exhausted());
                assert_eq!(source.scan.lock().await.start_after(), None);
            }

            server.reset().await;
            Mock::given(method("GET"))
                .and(path("/events/logs/a"))
                .and(header("range", "bytes=2-"))
                .respond_with(
                    ResponseTemplate::new(206)
                        .insert_header("etag", "\"v1\"")
                        .insert_header("content-range", "bytes 2-3/4")
                        .set_body_bytes(b"b\n"),
                )
                .expect(1)
                .mount(&server)
                .await;
            let batch = poll_with_paused_delay(&source)
                .await
                .expect("recovered read");
            assert_eq!(batch.messages[0].payload, b"b");
            assert_eq!(source.recovery.lock().await.retry_delay, Duration::ZERO);
            assert_eq!(
                source.state.lock().await.active_object(),
                committed.as_ref()
            );
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("completion ack");
            assert_eq!(source.scan.lock().await.start_after(), Some("logs/a"));
        }
    });
}
