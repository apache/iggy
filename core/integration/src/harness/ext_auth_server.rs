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
//! returns a decision based on the presented username. Binds an ephemeral
//! port; the drop impl signals the task to shut down.
//!
//! The simplified design maps every recognized test user to a pre-provisioned
//! Iggy username via `{"decision": "allow", "username": "..."}`. Tests must
//! create the Iggy user before attempting the ext-auth login.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// External identity presented to the ext-auth service. This username must
/// NOT exist as a local Iggy user, so `verify_login_credentials` returns
/// `InvalidCredentials` and the ext-auth guard enters the callout path.
pub const EXT_AUTH_USERNAME: &str = "ext-alice";
pub const EXT_AUTH_PASSWORD: &str = "ext-pass";
/// The pre-provisioned Iggy user the ext-auth service maps `ext-alice` to.
pub const EXT_AUTH_IGGY_USERNAME: &str = "iggy-alice";

pub const EXT_DENIED_USERNAME: &str = "ext-denied";
pub const EXT_DENIED_PASSWORD: &str = "ext-denied-pass";

fn allow_response(iggy_username: &str) -> String {
    serde_json::json!({
        "decision": "allow",
        "username": iggy_username,
    })
    .to_string()
}

fn deny_response(reason: &str) -> String {
    serde_json::json!({
        "decision": "deny",
        "reason": reason,
    })
    .to_string()
}

fn error_body() -> String {
    serde_json::json!({ "error": "internal server error" }).to_string()
}

fn http_response(status: u16, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        reason = if status == 200 {
            "OK"
        } else {
            "Internal Server Error"
        },
        len = body.len(),
    )
}

#[derive(Clone, Copy)]
enum Mode {
    Normal,
    Failing,
    Slow(Duration),
}

pub struct ExtAuthServer {
    pub port: u16,
    shutdown: Arc<Notify>,
    request_count: Arc<AtomicU32>,
}

impl ExtAuthServer {
    pub async fn start() -> Self {
        Self::start_with_mode(Mode::Normal).await
    }

    pub async fn start_failing() -> Self {
        Self::start_with_mode(Mode::Failing).await
    }

    pub async fn start_slow(delay: Duration) -> Self {
        Self::start_with_mode(Mode::Slow(delay)).await
    }

    async fn start_with_mode(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port for ext auth server");
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(Notify::new());
        let shutdown_rx = Arc::clone(&shutdown);
        let request_count = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&request_count);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, _)) = accept else { continue };
                        let counter = Arc::clone(&counter);
                        tokio::spawn(async move {
                            counter.fetch_add(1, Ordering::Relaxed);
                            handle_connection(stream, mode).await;
                        });
                    }
                    () = shutdown_rx.notified() => break,
                }
            }
        });

        Self {
            port,
            shutdown,
            request_count,
        }
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}/auth", self.port)
    }

    /// Number of HTTP requests the server has received.
    pub fn request_count(&self) -> u32 {
        self.request_count.load(Ordering::Relaxed)
    }
}

impl Drop for ExtAuthServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, mode: Mode) {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut content_length: usize = 0;

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

    let mut body = vec![0u8; content_length];
    if tokio::io::AsyncReadExt::read_exact(&mut buf_reader, &mut body)
        .await
        .is_err()
    {
        return;
    }

    let resp = match mode {
        Mode::Failing => http_response(500, &error_body()),
        Mode::Slow(delay) => {
            tokio::time::sleep(delay).await;
            http_response(200, &allow_response(EXT_AUTH_IGGY_USERNAME))
        }
        Mode::Normal => {
            let decision = match serde_json::from_slice::<serde_json::Value>(&body) {
                Ok(req) => {
                    let creds = req.get("credentials");
                    let username = creds
                        .and_then(|c| c.get("username"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let password = creds
                        .and_then(|c| c.get("password"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    match username {
                        EXT_AUTH_USERNAME if password == EXT_AUTH_PASSWORD => {
                            allow_response(EXT_AUTH_IGGY_USERNAME)
                        }
                        EXT_DENIED_USERNAME if password == EXT_DENIED_PASSWORD => {
                            deny_response("access denied by policy")
                        }
                        _ => deny_response("unknown user or bad credentials"),
                    }
                }
                _ => deny_response("malformed request"),
            };
            http_response(200, &decision)
        }
    };

    let _ = writer.write_all(resp.as_bytes()).await;
}
