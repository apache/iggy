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

use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::{Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use iggy_gateway_kafka::observability::{HttpEndpoint, Observability};
use iggy_gateway_kafka::protocol::api::API_KEY_METADATA;
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

use http::{get, sample, wait_for};
use server::spawn_gateway;
use tcp::{build_request_frame, read_response_frame};

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
    let observability = Arc::new(Observability::new());
    let endpoint = HttpEndpoint::start(
        "127.0.0.1:0".parse().expect("an address"),
        Arc::clone(&observability),
    )
    .await
    .expect("bind the HTTP endpoint");
    let http = endpoint.local_addr().to_string();
    let (addrs, shutdown) = spawn_gateway(
        |config| KafkaGateway::new(config).with_observability(observability),
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

#[tokio::test]
async fn given_a_shutdown_should_answer_not_ready_while_it_stays_live() {
    let gateway = observed(config()).await;
    let _held = TcpStream::connect(&gateway.kafka).await.expect("connect");

    gateway.shutdown.send(()).expect("the gateway listens");

    let (status, body, _) =
        wait_for(&gateway.http, "/ready", WAIT, |status, _| status == 503).await;
    assert_eq!(status, 503);
    assert!(body.contains("shutting down"), "{body}");
    assert_eq!(
        get(&gateway.http, "/live").await.map(|(status, _)| status),
        Some(200)
    );
}
