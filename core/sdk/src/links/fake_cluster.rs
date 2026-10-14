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

//! Fake replicas over in-memory streams, for the tests of the links and the
//! driver.

use super::dial::test_support::ScriptedDialer;
use super::test_support::{CLIENT, DUPLEX_CAPACITY, read_request_header, refusal, reply};
use super::{Dialer, ReconnectPolicy, ReplicaLinks};
use crate::session_owner::SessionOwner;
use futures::FutureExt;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, GET_PARTITION_ROUTE_CODE, GET_STATS_CODE,
    LOGIN_REGISTER_CODE,
};
use iggy_binary_protocol::consensus::{
    Command, HEADER_SIZE, NON_REPLICATED_CODE_RANGE, Operation, ReplyHeader, RequestHeader,
};
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::responses::system::{ClusterMetadataResponse, ClusterNodeResponse};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{WireEncode, WireName};
use iggy_common::{AutoLogin, ClusterNodeRole, ClusterNodeStatus, Credentials, IggyError};
use secrecy::SecretString;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep, timeout};

pub(crate) const PORT: u16 = 8090;
pub(crate) const FIRST_SESSION: u64 = 100;
pub(crate) const INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const REESTABLISH_AFTER: Duration = Duration::from_secs(5);
pub(crate) const TEST_BUDGET: Duration = Duration::from_secs(120);
/// A request code that the fake replicas never answer.
pub(crate) const UNANSWERED_CODE: u32 = GET_STATS_CODE;
const USER_ID: u32 = 1;
const POLL_STEP: Duration = Duration::from_millis(10);

/// How a replica answers one scripted request.
#[derive(Debug, Clone)]
pub(crate) enum Answer {
    Body(Vec<u8>),
    /// A refusal with the status of this error.
    Refuse(IggyError),
    /// The reply of a committed metadata request at this commit.
    Commit(u64),
    /// The connection ends once the request arrives.
    HangUp,
    Silence,
}

/// A request as a replica received it.
#[derive(Debug, Clone)]
pub(crate) struct Received {
    pub(crate) endpoint: String,
    pub(crate) header: RequestHeader,
    pub(crate) body: Vec<u8>,
}

/// Replicas that answer each request by a script, else by its code. The
/// cluster keeps one session: a Register starts a new one, and a bind of any
/// other session fails with `Unauthenticated`.
#[derive(Default)]
pub(crate) struct FakeCluster {
    pub(crate) nodes: Mutex<Vec<ClusterNodeResponse>>,
    /// Endpoints that refuse each dial.
    pub(crate) down: Mutex<HashSet<String>>,
    /// Endpoints that never finish a dial.
    pub(crate) silent: Mutex<HashSet<String>>,
    /// Endpoints that close the connection on a Register.
    pub(crate) hang_up_on_register: Mutex<HashSet<String>>,
    pub(crate) reject_credentials: Mutex<bool>,
    pub(crate) session: Mutex<Option<u64>>,
    /// The answer of every replica to a route lookup without a script.
    pub(crate) route: Mutex<Option<PartitionRouteResponse>>,
    /// Answers by endpoint and code, used before the answer by code.
    script: Mutex<HashMap<(String, u32), VecDeque<Answer>>>,
    registered: AtomicU64,
    received: Mutex<Vec<(u32, Received)>>,
    pub(crate) dials: Mutex<Vec<(String, Instant)>>,
    connections: Mutex<Vec<(String, AbortHandle)>>,
    open_dials: AtomicUsize,
    pub(crate) max_open_dials: AtomicUsize,
    pub(crate) cut_dials: Mutex<Vec<Duration>>,
}

impl FakeCluster {
    /// `count` replicas, the first one the metadata primary.
    pub(crate) fn new(count: u8) -> Arc<Self> {
        let cluster = Self::default();
        *cluster.nodes.lock().unwrap() = (0..count)
            .map(|id| node(id, &ip(id), PORT, id == 0))
            .collect();
        Arc::new(cluster)
    }

    pub(crate) fn dialer(self: &Arc<Self>) -> Dialer {
        let cluster = Arc::clone(self);
        Dialer::Scripted(ScriptedDialer::new(move |endpoint: &str| {
            Arc::clone(&cluster).accept(endpoint.to_owned()).boxed()
        }))
    }

    /// Queues `answers` for the next requests of `code` to `endpoint`.
    pub(crate) fn script(
        &self,
        endpoint: &str,
        code: u32,
        answers: impl IntoIterator<Item = Answer>,
    ) {
        self.script
            .lock()
            .unwrap()
            .entry((endpoint.to_owned(), code))
            .or_default()
            .extend(answers);
    }

    /// Makes `id` the metadata primary of the roster that the replicas send.
    pub(crate) fn elect(&self, id: u8) {
        for node in self.nodes.lock().unwrap().iter_mut() {
            let role = if node.replica_id == id {
                ClusterNodeRole::Leader
            } else {
                ClusterNodeRole::Follower
            };
            node.role = role as u8;
        }
    }

    /// Every request of `code`, in arrival order.
    pub(crate) fn received(&self, code: u32) -> Vec<Received> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|(received, _)| *received == code)
            .map(|(_, request)| request.clone())
            .collect()
    }

    /// Ends every connection to `endpoint`, as a replica restart does.
    pub(crate) fn drop_connections(&self, endpoint: &str) {
        self.connections.lock().unwrap().retain(|(to, served)| {
            let dropped = to == endpoint;
            if dropped {
                served.abort();
            }
            !dropped
        });
    }

    pub(crate) fn open_connections(&self, endpoint: &str) -> usize {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, served)| to == endpoint && !served.is_finished())
            .count()
    }

    pub(crate) fn codes(&self, endpoint: &str) -> Vec<u32> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, request)| request.endpoint == endpoint)
            .map(|(code, _)| *code)
            .collect()
    }

    pub(crate) fn count(&self, code: u32) -> usize {
        self.received(code).len()
    }

    pub(crate) fn dials_to(&self, endpoint: &str) -> Vec<Instant> {
        self.dials
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, _)| to == endpoint)
            .map(|(_, at)| *at)
            .collect()
    }

    async fn accept(self: Arc<Self>, endpoint: String) -> Result<DuplexStream, IggyError> {
        self.dials
            .lock()
            .unwrap()
            .push((endpoint.clone(), Instant::now()));
        if self.silent.lock().unwrap().contains(&endpoint) {
            let _open = OpenDial::new(&self);
            return std::future::pending().await;
        }
        if self.down.lock().unwrap().contains(&endpoint) {
            return Err(IggyError::CannotEstablishConnection);
        }
        let (client, server) = tokio::io::duplex(DUPLEX_CAPACITY);
        let served = tokio::spawn(Arc::clone(&self).serve(endpoint.clone(), server));
        self.connections
            .lock()
            .unwrap()
            .push((endpoint, served.abort_handle()));
        Ok(client)
    }

    async fn serve(self: Arc<Self>, endpoint: String, stream: DuplexStream) {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut header = [0; HEADER_SIZE];
        while reader.read_exact(&mut header).await.is_ok() {
            let request = read_request_header(&header);
            let mut body = vec![0; request.size as usize - HEADER_SIZE];
            if reader.read_exact(&mut body).await.is_err() {
                return;
            }
            let code = request_code(&request);
            self.received.lock().unwrap().push((
                code,
                Received {
                    endpoint: endpoint.clone(),
                    header: request,
                    body,
                },
            ));
            let frame = match self.answer(&endpoint, &request, code) {
                Answer::Body(body) => reply(request.request, &body),
                Answer::Refuse(error) => refusal(request.request, &error),
                Answer::Commit(commit) => committed(&request, commit),
                Answer::HangUp => return,
                Answer::Silence => continue,
            };
            if writer.write_all(&frame).await.is_err() {
                return;
            }
        }
    }

    fn answer(&self, endpoint: &str, request: &RequestHeader, code: u32) -> Answer {
        let scripted = self
            .script
            .lock()
            .unwrap()
            .get_mut(&(endpoint.to_owned(), code))
            .and_then(VecDeque::pop_front);
        if let Some(answer) = scripted {
            return answer;
        }
        let mut session = self.session.lock().unwrap();
        match code {
            LOGIN_REGISTER_CODE if self.hang_up_on_register.lock().unwrap().contains(endpoint) => {
                Answer::HangUp
            }
            LOGIN_REGISTER_CODE if *self.reject_credentials.lock().unwrap() => {
                Answer::Refuse(IggyError::InvalidCredentials)
            }
            LOGIN_REGISTER_CODE => {
                let registered = FIRST_SESSION + self.registered.fetch_add(1, Ordering::SeqCst);
                *session = Some(registered);
                Answer::Body(binding(registered))
            }
            BIND_SESSION_CODE if *session == Some(request.session) => {
                Answer::Body(binding(request.session))
            }
            BIND_SESSION_CODE => Answer::Refuse(IggyError::Unauthenticated),
            GET_CLUSTER_METADATA_CODE => Answer::Body(self.roster()),
            GET_PARTITION_ROUTE_CODE => match *self.route.lock().unwrap() {
                Some(route) => Answer::Body(route.to_bytes().to_vec()),
                None => Answer::Refuse(IggyError::TransientNotAccepted),
            },
            UNANSWERED_CODE => Answer::Silence,
            _ => Answer::Body(Vec::new()),
        }
    }

    fn roster(&self) -> Vec<u8> {
        ClusterMetadataResponse {
            name: "fake".to_owned(),
            nodes: self.nodes.lock().unwrap().clone(),
        }
        .to_bytes()
        .to_vec()
    }
}

/// A dial that has not finished, counted while it lasts.
struct OpenDial {
    cluster: Arc<FakeCluster>,
    started: Instant,
}

impl OpenDial {
    fn new(cluster: &Arc<FakeCluster>) -> Self {
        let open = cluster.open_dials.fetch_add(1, Ordering::SeqCst) + 1;
        cluster.max_open_dials.fetch_max(open, Ordering::SeqCst);
        Self {
            cluster: Arc::clone(cluster),
            started: Instant::now(),
        }
    }
}

impl Drop for OpenDial {
    fn drop(&mut self) {
        self.cluster.open_dials.fetch_sub(1, Ordering::SeqCst);
        self.cluster
            .cut_dials
            .lock()
            .unwrap()
            .push(self.started.elapsed());
    }
}

pub(crate) fn ip(id: u8) -> String {
    format!("10.0.0.{}", u16::from(id) + 1)
}

pub(crate) fn endpoint(id: u8) -> String {
    format!("{}:{PORT}", ip(id))
}

pub(crate) fn node(id: u8, ip: &str, tcp_port: u16, leader: bool) -> ClusterNodeResponse {
    let role = if leader {
        ClusterNodeRole::Leader
    } else {
        ClusterNodeRole::Follower
    };
    ClusterNodeResponse {
        replica_id: id,
        name: format!("node-{id}"),
        ip: ip.to_owned(),
        tcp_port,
        quic_port: 0,
        http_port: 0,
        websocket_port: 0,
        role: role as u8,
        status: ClusterNodeStatus::Healthy as u8,
    }
}

pub(crate) fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        enabled: true,
        max_retries: None,
        interval: INTERVAL,
        reestablish_after: REESTABLISH_AFTER,
    }
}

pub(crate) fn links(
    cluster: &Arc<FakeCluster>,
    configured: &str,
    policy: ReconnectPolicy,
) -> ReplicaLinks {
    let credentials = Credentials::UsernamePassword("iggy".to_owned(), SecretString::from("iggy"));
    let session = Arc::new(SessionOwner::new(AutoLogin::Enabled(credentials)));
    ReplicaLinks::new(cluster.dialer(), configured.to_owned(), policy, session)
}

/// Connects, and waits until `count` slots serve the session.
pub(crate) async fn connected(cluster: &Arc<FakeCluster>, count: usize) -> ReplicaLinks {
    let links = links(cluster, &endpoint(0), policy());
    links.connect().await.unwrap();
    eventually("every slot is ready", || {
        links.ready_replicas().len() == count
    })
    .await;
    links
}

pub(crate) async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let waited = timeout(TEST_BUDGET, async {
        while !condition() {
            sleep(POLL_STEP).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{what}");
}

fn binding(session: u64) -> Vec<u8> {
    LoginRegisterResponse {
        user_id: USER_ID,
        session,
        server_protocol_version: 1,
        server_version: WireName::new("fake").unwrap(),
    }
    .to_bytes()
    .to_vec()
}

fn request_code(request: &RequestHeader) -> u32 {
    match request.operation {
        Operation::Register => LOGIN_REGISTER_CODE,
        Operation::NonReplicated => u32::from_le_bytes(
            request.reserved[NON_REPLICATED_CODE_RANGE]
                .try_into()
                .unwrap(),
        ),
        operation => operation.to_command_code().unwrap_or_default(),
    }
}

/// The reply of a committed request at `commit`. A metadata reply raises the
/// metadata watermark of the client.
fn committed(request: &RequestHeader, commit: u64) -> Vec<u8> {
    // An empty result section: no error, then the payload.
    let body = if request.operation.is_result_framed() {
        0_u32.to_le_bytes().to_vec()
    } else {
        Vec::new()
    };
    let header = ReplyHeader {
        command: Command::Reply,
        operation: request.operation,
        client: CLIENT,
        request: request.request,
        commit,
        size: u32::try_from(HEADER_SIZE + body.len()).unwrap(),
        ..Default::default()
    };
    let mut frame = bytemuck::bytes_of(&header).to_vec();
    frame.extend_from_slice(&body);
    frame
}
