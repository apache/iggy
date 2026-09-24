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

//! A minimal external auth HTTP server for integration tests.
//!
//! Spawns a tokio task that accepts external auth callouts over raw TCP and
//! returns a fully-specified inline grant for known credentials or a denial
//! for everything else. Binds an ephemeral port; the drop impl signals the
//! task to shut down.

use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

pub const EXT_AUTH_USERNAME: &str = "ext-alice";
pub const EXT_AUTH_PASSWORD: &str = "ext-pass";

fn grant_response() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    format!(
        r#"{{"decision":"inline_grant","principal":"test-device","permissions":{{"global":{{"manage_servers":false,"read_servers":false,"manage_users":false,"read_users":false,"manage_streams":false,"read_streams":true,"manage_topics":false,"read_topics":true,"poll_messages":true,"send_messages":true}}}},"expires_at":{now}}}"#
    )
}

const DENY_RESPONSE: &str = r#"{"decision":"deny","reason":"unknown user or bad credentials"}"#;

fn http_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

pub struct ExtAuthServer {
    pub port: u16,
    shutdown: Arc<Notify>,
}

impl ExtAuthServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port for ext auth server");
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(Notify::new());
        let shutdown_rx = Arc::clone(&shutdown);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, _)) = accept else { continue };
                        tokio::spawn(handle_connection(stream));
                    }
                    () = shutdown_rx.notified() => break,
                }
            }
        });

        Self { port, shutdown }
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}/auth", self.port)
    }
}

impl Drop for ExtAuthServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

async fn handle_connection(stream: tokio::net::TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut content_length: usize = 0;

    // Read HTTP headers.
    let mut line = String::new();
    loop {
        line.clear();
        if buf_reader.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        if let Some(val) = line
            .strip_prefix("Content-Length:")
            .or_else(|| line.strip_prefix("content-length:"))
        {
            content_length = val.trim().parse().unwrap_or(0);
        }
        if line == "\r\n" {
            break;
        }
    }

    // Read body.
    let mut body = vec![0u8; content_length];
    if tokio::io::AsyncReadExt::read_exact(&mut buf_reader, &mut body)
        .await
        .is_err()
    {
        return;
    }

    let resp = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(req)
            if req.get("username").and_then(|v| v.as_str()) == Some(EXT_AUTH_USERNAME)
                && req.get("credential").and_then(|v| v.as_str()) == Some(EXT_AUTH_PASSWORD) =>
        {
            http_response(&grant_response())
        }
        _ => http_response(DENY_RESPONSE),
    };

    let _ = writer.write_all(resp.as_bytes()).await;
}
