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

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

pub const EXT_AUTH_USERNAME: &str = "ext-alice";
pub const EXT_AUTH_PASSWORD: &str = "ext-pass";

pub const EXT_SCOPED_USERNAME: &str = "ext-scoped";
pub const EXT_SCOPED_PASSWORD: &str = "ext-scoped-pass";

pub const EXT_MAPPED_USERNAME: &str = "ext-mapped";
pub const EXT_MAPPED_PASSWORD: &str = "ext-mapped-pass";

pub const EXT_SHORT_USERNAME: &str = "ext-short";
pub const EXT_SHORT_PASSWORD: &str = "ext-short-pass";

/// User id the IggyUser mapping variant returns. Tests must create an Iggy
/// user with this id before logging in with `EXT_MAPPED_USERNAME`.
pub const EXT_MAPPED_USER_ID: u32 = 42;

/// Stream id the scoped grant targets. Tests must create a stream whose
/// numeric id matches (the first stream created gets slab index 0).
pub const EXT_SCOPED_STREAM_ID: usize = 0;

fn now_plus(secs: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + secs
}

fn global_perms() -> serde_json::Value {
    serde_json::json!({
        "manage_servers": false, "read_servers": false,
        "manage_users": false,   "read_users": false,
        "manage_streams": false, "read_streams": true,
        "manage_topics": false,  "read_topics": true,
        "poll_messages": true,   "send_messages": true,
    })
}

fn global_grant_response() -> String {
    serde_json::json!({
        "decision": "inline_grant",
        "principal": "test-device",
        "permissions": { "global": global_perms() },
        "expires_at": now_plus(3600),
    })
    .to_string()
}

fn scoped_grant_response() -> String {
    serde_json::json!({
        "decision": "inline_grant",
        "principal": "scoped-device",
        "permissions": {
            "global": {
                "manage_servers": false, "read_servers": false,
                "manage_users": false,   "read_users": false,
                "manage_streams": false, "read_streams": false,
                "manage_topics": false,  "read_topics": false,
                "poll_messages": false,  "send_messages": false,
            },
            "streams": {
                EXT_SCOPED_STREAM_ID.to_string(): {
                    "manage_stream": false, "read_stream": true,
                    "manage_topics": false, "read_topics": true,
                    "poll_messages": true,  "send_messages": true,
                }
            }
        },
        "expires_at": now_plus(3600),
    })
    .to_string()
}

fn short_grant_response() -> String {
    serde_json::json!({
        "decision": "inline_grant",
        "principal": "test-device",
        "permissions": { "global": global_perms() },
        "expires_at": now_plus(2),
    })
    .to_string()
}

fn iggy_user_response() -> String {
    serde_json::json!({
        "decision": "iggy_user",
        "user_id": EXT_MAPPED_USER_ID,
    })
    .to_string()
}

fn deny_response() -> String {
    serde_json::json!({
        "decision": "deny",
        "reason": "unknown user or bad credentials",
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

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, _)) = accept else { continue };
                        tokio::spawn(handle_connection(stream, mode));
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
            http_response(200, &global_grant_response())
        }
        Mode::Normal => {
            let decision = match serde_json::from_slice::<serde_json::Value>(&body) {
                Ok(req) => {
                    let username = req.get("username").and_then(|v| v.as_str()).unwrap_or("");
                    let credential = req.get("credential").and_then(|v| v.as_str()).unwrap_or("");
                    match username {
                        EXT_AUTH_USERNAME if credential == EXT_AUTH_PASSWORD => {
                            global_grant_response()
                        }
                        EXT_SCOPED_USERNAME if credential == EXT_SCOPED_PASSWORD => {
                            scoped_grant_response()
                        }
                        EXT_MAPPED_USERNAME if credential == EXT_MAPPED_PASSWORD => {
                            iggy_user_response()
                        }
                        EXT_SHORT_USERNAME if credential == EXT_SHORT_PASSWORD => {
                            short_grant_response()
                        }
                        _ => deny_response(),
                    }
                }
                _ => deny_response(),
            };
            http_response(200, &decision)
        }
    };

    let _ = writer.write_all(resp.as_bytes()).await;
}
