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

//! A scripted HTTP/1.1 peer for the HTTP client tests. It answers each request with the next
//! scripted reply on its own connection and records what the client sent.

use crate::http::http_client::HttpClient;
use crate::prelude::{
    CompressionAlgorithm, HttpClientConfig, IggyByteSize, IggyError, IggyExpiry, IggyTimestamp,
    MaxTopicSize, Partition, PartitionContext, StreamDetails, TopicDetails,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The answer past the end of the script. The retry middleware never resends a 4xx, so the
/// client fails at once and the test sees the extra request.
const UNSCRIPTED_STATUS: u16 = 418;
const HEAD_END: &[u8] = b"\r\n\r\n";
const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;

/// One request the client sent.
#[derive(Debug)]
pub(super) struct Recorded {
    /// For example `POST /streams/1/topics/1/messages HTTP/1.1`.
    pub(super) line: String,
    pub(super) body: Vec<u8>,
    /// Time since the server started, on the clock of the test runtime.
    pub(super) at: Duration,
}

impl Recorded {
    /// The `context` the request body carries, `None` when the field is absent.
    pub(super) fn context(&self) -> Option<PartitionContext> {
        let body: serde_json::Value = serde_json::from_slice(&self.body).unwrap();
        body.get("context")
            .map(|context| serde_json::from_value(context.clone()).unwrap())
    }
}

pub(super) struct Reply {
    status: u16,
    body: String,
}

impl Reply {
    pub(super) fn empty(status: u16) -> Self {
        Self {
            status,
            body: String::new(),
        }
    }

    /// The error body the server renders for `error`.
    pub(super) fn error(status: u16, error: &IggyError) -> Self {
        Self {
            status,
            body: serde_json::json!({
                "id": error.as_code(),
                "code": error.as_string(),
                "reason": error.to_string(),
                "field": null,
            })
            .to_string(),
        }
    }

    pub(super) fn topic(contexts: &[(u32, PartitionContext)]) -> Self {
        Self {
            status: 200,
            body: topic_details_json(contexts),
        }
    }

    pub(super) fn stream() -> Self {
        let stream = StreamDetails {
            id: STREAM_ID,
            created_at: IggyTimestamp::zero(),
            name: "stream".to_owned(),
            size: IggyByteSize::default(),
            messages_count: 0,
            topics_count: 0,
            topics: Vec::new(),
            options: Default::default(),
        };
        Self {
            status: 200,
            body: serde_json::to_string(&stream).unwrap(),
        }
    }
}

pub(super) struct ScriptedServer {
    api_url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    task: JoinHandle<()>,
}

impl ScriptedServer {
    pub(super) async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let started = Instant::now();
        let task = tokio::spawn(async move {
            let mut replies = replies.into_iter();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (line, body) = read_request(&mut stream).await;
                recorded.lock().unwrap().push(Recorded {
                    line,
                    body,
                    at: started.elapsed(),
                });
                let reply = replies
                    .next()
                    .unwrap_or_else(|| Reply::empty(UNSCRIPTED_STATUS));
                let response = format!(
                    "HTTP/1.1 {} Scripted\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{}",
                    reply.status,
                    reply.body.len(),
                    reply.body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            api_url,
            requests,
            task,
        }
    }

    /// A client logged in with a token, so every path passes the authentication check.
    pub(super) fn client(&self) -> HttpClient {
        HttpClient::create(Arc::new(HttpClientConfig {
            api_url: self.api_url.clone(),
            jwt: Some("token".to_owned()),
            ..Default::default()
        }))
        .unwrap()
    }

    /// Takes the requests recorded so far.
    pub(super) fn requests(&self) -> Vec<Recorded> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

impl Drop for ScriptedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Topic details whose partitions carry `contexts`, keyed by partition id.
pub(crate) fn topic_details_json(contexts: &[(u32, PartitionContext)]) -> String {
    let partitions = contexts
        .iter()
        .map(|&(id, context)| Partition {
            id,
            created_at: IggyTimestamp::zero(),
            segments_count: 1,
            current_offset: 0,
            size: IggyByteSize::default(),
            messages_count: 0,
            context,
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&TopicDetails {
        id: TOPIC_ID,
        created_at: IggyTimestamp::zero(),
        name: "topic".to_owned(),
        size: IggyByteSize::default(),
        message_expiry: IggyExpiry::NeverExpire,
        compression_algorithm: CompressionAlgorithm::None,
        max_topic_size: MaxTopicSize::Unlimited,
        messages_count: 0,
        partitions_count: u32::try_from(partitions.len()).unwrap(),
        partitions,
        options: Default::default(),
    })
    .unwrap()
}

/// Reads one request whose body, if any, has a `content-length`.
async fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut request = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the client closed the connection mid-request");
        request.extend_from_slice(&chunk[..read]);
        let Some(head_len) = request
            .windows(HEAD_END.len())
            .position(|window| window == HEAD_END)
        else {
            continue;
        };
        let head = String::from_utf8_lossy(&request[..head_len]).into_owned();
        let body_len = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
        let body_start = head_len + HEAD_END.len();
        if request.len() >= body_start + body_len {
            let line = head.lines().next().unwrap_or_default().to_owned();
            return (line, request[body_start..body_start + body_len].to_vec());
        }
    }
}
