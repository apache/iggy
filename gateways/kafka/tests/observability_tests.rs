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

//! The endpoint of metrics, readiness and liveness of a gateway that runs in-process, without a
//! bridge. `readiness_real_server_tests.rs` covers the gateway binary against an Iggy server.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/http.rs"]
mod http;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::{Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{SemaphorePermit, broadcast};

use iggy_gateway_kafka::auth::{AuthError, AuthenticatedPrincipal, SaslAuthenticator};
use iggy_gateway_kafka::listener::SecurityProtocol;
use iggy_gateway_kafka::observability::{HttpEndpoint, Observability};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_METADATA, API_KEY_PRODUCE, API_KEY_SASL_HANDSHAKE,
};
use iggy_gateway_kafka::protocol::sasl::PlainCredentials;
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

use http::{get, sample, wait_for};
use server::{listener, spawn_gateway};
use tcp::{build_request_frame, hold_answers, read_response_frame};

const WAIT: Duration = Duration::from_secs(10);

/// A gateway without a bridge, the address of its Kafka listener, and its HTTP endpoint, once
/// `/ready` answers 200.
struct Observed {
    kafka: String,
    http: String,
    shutdown: broadcast::Sender<()>,
    _endpoint: HttpEndpoint,
}

async fn observed(config: GatewayConfig) -> Observed {
    observed_with(config, KafkaGateway::new).await
}

/// [`observed`], with the gateway built by `build`.
async fn observed_with(
    config: GatewayConfig,
    build: impl FnOnce(GatewayConfig) -> KafkaGateway,
) -> Observed {
    let observability = Arc::new(Observability::new());
    let endpoint = HttpEndpoint::start(
        "127.0.0.1:0".parse().expect("an address"),
        Arc::clone(&observability),
    )
    .await
    .expect("bind the HTTP endpoint");
    let http = endpoint.local_addr().to_string();
    let (addrs, shutdown) = spawn_gateway(
        |config| build(config).with_observability(observability),
        config,
    );
    wait_for(&http, "/ready", WAIT, |status, _| status == 200).await;
    Observed {
        kafka: addrs[0].to_string(),
        http,
        shutdown,
        _endpoint: endpoint,
    }
}

fn config() -> GatewayConfig {
    GatewayConfig {
        max_frame_size: 64 * 1024,
        shutdown_drain_timeout: Duration::from_secs(2),
        ..GatewayConfig::default()
    }
}

/// A Metadata v9 request for `topics`.
fn metadata_request(topics: &[&'static str]) -> Vec<u8> {
    let topics = topics
        .iter()
        .map(|name| {
            MetadataRequestTopic::default().with_name(Some(StrBytes::from_static_str(name).into()))
        })
        .collect();
    let mut body = bytes::BytesMut::new();
    MetadataRequest::default()
        .with_topics(Some(topics))
        .encode(&mut body, 9)
        .expect("a Metadata v9 request encodes");
    body.to_vec()
}

#[tokio::test]
async fn given_failed_requests_when_scraped_should_show_their_error_codes_per_api() {
    let gateway = observed(config()).await;
    let mut client = TcpStream::connect(&gateway.kafka).await.expect("connect");
    let frame = build_request_frame(
        API_KEY_METADATA,
        9,
        1,
        Some("metrics-test"),
        &metadata_request(&["orders", "refunds"]),
    );
    client.write_all(&frame).await.expect("send Metadata");
    read_response_frame(&mut client, 64 * 1024).await;

    let (_, metrics, _) = wait_for(&gateway.http, "/metrics", WAIT, |_, metrics| {
        sample(
            metrics,
            "iggy_kafka_gateway_request_duration_seconds_count{api=\"Metadata\"}",
        ) == Some(1.0)
    })
    .await;

    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_response_errors_total{api=\"Metadata\",code=\"3\",\
             error=\"UNKNOWN_TOPIC_OR_PARTITION\"}"
        ),
        Some(2.0),
        "a stub gateway holds no topic, so each one answers 3:\n{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_requests_total{api=\"Metadata\",version=\"9\"}"
        ),
        Some(1.0),
        "{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_open{listener=\"PLAINTEXT\"}"
        ),
        Some(1.0),
        "{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_limit{listener=\"PLAINTEXT\"}"
        ),
        Some(1024.0),
        "{metrics}"
    );
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_ready"),
        Some(1.0),
        "{metrics}"
    );
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_iggy_reachable"),
        None,
        "a gateway without a bridge reaches no Iggy:\n{metrics}"
    );
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_groups{state=\"Stable\"}"),
        Some(0.0),
        "{metrics}"
    );
    assert_eq!(
        sample(&metrics, "iggy_kafka_gateway_group_members"),
        Some(0.0),
        "{metrics}"
    );
}

#[tokio::test]
async fn given_connections_that_close_should_count_each_by_its_cause() {
    let gateway = observed(config()).await;

    // A length prefix above the 64 KiB cap, as a TLS client on a plaintext listener sends.
    let mut oversized = TcpStream::connect(&gateway.kafka).await.expect("connect");
    oversized
        .write_all(&[0x16, 0x03, 0x01, 0x02])
        .await
        .expect("send a TLS record header");
    let mut rest = Vec::new();
    tokio::time::timeout(WAIT, oversized.read_to_end(&mut rest))
        .await
        .expect("the gateway closes the connection")
        .ok();

    let closed_by_client = TcpStream::connect(&gateway.kafka).await.expect("connect");
    drop(closed_by_client);

    let (_, metrics, _) = wait_for(&gateway.http, "/metrics", WAIT, |_, metrics| {
        sample(
            metrics,
            "iggy_kafka_gateway_connections_closed_total{listener=\"PLAINTEXT\",\
             cause=\"client_closed\"}",
        ) == Some(1.0)
    })
    .await;
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_closed_total{listener=\"PLAINTEXT\",\
             cause=\"frame_too_large\"}"
        ),
        Some(1.0),
        "{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_accepted_total{listener=\"PLAINTEXT\"}"
        ),
        Some(2.0),
        "{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_open{listener=\"PLAINTEXT\"}"
        ),
        Some(0.0),
        "{metrics}"
    );
}

/// Whether `metrics` counts a request of `api`, at any version.
fn answered(metrics: &str, api: &str) -> bool {
    let series = format!("iggy_kafka_gateway_requests_total{{api=\"{api}\",");
    metrics.lines().any(|line| line.starts_with(&series))
}

#[tokio::test]
async fn given_a_shutdown_should_answer_not_ready_before_the_drain_ends() {
    let gateway = observed(GatewayConfig {
        write_timeout: Duration::from_secs(4),
        shutdown_drain_timeout: Duration::from_secs(10),
        ..GatewayConfig::default()
    })
    .await;
    let _held = hold_answers(gateway.kafka.parse().expect("an address")).await;
    wait_for(&gateway.http, "/metrics", WAIT, |_, metrics| {
        answered(metrics, "Metadata")
    })
    .await;
    // Past the answers that the socket buffers take, so the gateway waits in a write.
    tokio::time::sleep(Duration::from_millis(300)).await;

    gateway.shutdown.send(()).expect("the gateway listens");

    // Well inside the 4 s write timeout that holds the drain open.
    let (_, body, _) = wait_for(
        &gateway.http,
        "/ready",
        Duration::from_secs(2),
        |status, _| status == 503,
    )
    .await;
    assert!(body.contains("shutting down"), "{body}");
    let metrics = get(&gateway.http, "/metrics").await.expect("metrics").1;
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_open{listener=\"PLAINTEXT\"}"
        ),
        Some(1.0),
        "the drain still waits for the held connection:\n{metrics}"
    );
    assert_eq!(
        get(&gateway.http, "/live").await.map(|(status, _)| status),
        Some(200)
    );
}

/// Refuses every login. The test needs a SASL listener, not a login.
#[derive(Debug)]
struct NoLogins;

#[async_trait]
impl SaslAuthenticator for NoLogins {
    async fn authenticate(
        &self,
        _credentials: &PlainCredentials,
        _slot: SemaphorePermit<'_>,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        Err(AuthError::Rejected)
    }
}

/// A refused request waits for the client to close after its answer. That wait is not the
/// request's latency.
#[tokio::test]
async fn given_a_refused_request_when_the_client_keeps_the_connection_should_record_its_latency_at_once()
 {
    let gateway = observed_with(
        GatewayConfig {
            listeners: vec![listener(SecurityProtocol::SaslPlaintext)],
            ..config()
        },
        |config| KafkaGateway::new(config).with_authenticator(Arc::new(NoLogins)),
    )
    .await;
    let mut client = TcpStream::connect(&gateway.kafka).await.expect("connect");
    let mut mechanism = BytesMut::new();
    mechanism.put_i16(6);
    mechanism.put_slice(b"GSSAPI");
    let frame = build_request_frame(API_KEY_SASL_HANDSHAKE, 1, 1, None, &mechanism);
    client.write_all(&frame).await.expect("send SaslHandshake");
    read_response_frame(&mut client, 64 * 1024).await;

    let count = "iggy_kafka_gateway_request_duration_seconds_count{api=\"SaslHandshake\"}";
    let (_, metrics, _) = wait_for(
        &gateway.http,
        "/metrics",
        Duration::from_secs(2),
        |_, metrics| sample(metrics, count) == Some(1.0),
    )
    .await;
    let took = sample(
        &metrics,
        "iggy_kafka_gateway_request_duration_seconds_sum{api=\"SaslHandshake\"}",
    )
    .expect("a sum");
    assert!(took < 1.0, "recorded {took} s:\n{metrics}");
    drop(client);
}

/// A Produce body that does not decode is the client's fault, not a failed write to Iggy.
#[tokio::test]
async fn given_a_produce_body_that_does_not_decode_should_count_a_refused_request() {
    let gateway = observed(config()).await;
    let mut client = TcpStream::connect(&gateway.kafka).await.expect("connect");
    let frame = build_request_frame(API_KEY_PRODUCE, 9, 1, None, &[0xFF]);
    client.write_all(&frame).await.expect("send Produce");
    let mut rest = Vec::new();
    tokio::time::timeout(WAIT, client.read_to_end(&mut rest))
        .await
        .expect("the gateway closes the connection")
        .ok();

    let refused = "iggy_kafka_gateway_connections_closed_total{listener=\"PLAINTEXT\",cause=\"request_refused\"}";
    let (_, metrics, _) = wait_for(&gateway.http, "/metrics", WAIT, |_, metrics| {
        sample(metrics, refused) == Some(1.0)
    })
    .await;
    assert_eq!(
        sample(
            &metrics,
            "iggy_kafka_gateway_connections_closed_total{listener=\"PLAINTEXT\",\
             cause=\"unacknowledged_produce_failed\"}"
        ),
        None,
        "{metrics}"
    );
}
