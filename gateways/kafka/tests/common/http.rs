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

//! A bare HTTP client for the gateway's endpoint of metrics, readiness and liveness, compiled into
//! each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// One `GET path` on the endpoint at `addr`: the status and the body. `None` when nothing answers.
pub async fn get(addr: &str, path: &str) -> Option<(u16, String)> {
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await.ok()?;
        let status = response.split_whitespace().nth(1)?.parse().ok()?;
        let body = response
            .split_once("\r\n\r\n")
            .map_or(String::new(), |(_, body)| body.to_string());
        Some((status, body))
    };
    timeout(Duration::from_secs(5), exchange)
        .await
        .ok()
        .flatten()
}

/// Asks `GET path` every 100 ms until `done` holds for the answer, at most for `within`. Returns
/// that answer, and how long it took.
///
/// # Panics
///
/// When `within` passes first, with the last answer.
pub async fn wait_for(
    addr: &str,
    path: &str,
    within: Duration,
    done: impl Fn(u16, &str) -> bool,
) -> (u16, String, Duration) {
    let started = Instant::now();
    loop {
        let answer = get(addr, path).await;
        if let Some((status, body)) = &answer
            && done(*status, body)
        {
            return (*status, body.clone(), started.elapsed());
        }
        assert!(
            started.elapsed() < within,
            "GET {path} on {addr} did not reach the awaited answer within {within:?}, last \
             answer: {answer:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// The value of the sample `series` in the metrics text, such as
/// `iggy_kafka_gateway_requests_total{api="Metadata",version="9"}`.
pub fn sample(metrics: &str, series: &str) -> Option<f64> {
    metrics.lines().find_map(|line| {
        let value = line.strip_prefix(series)?.strip_prefix(' ')?;
        value.split_whitespace().next()?.parse().ok()
    })
}
