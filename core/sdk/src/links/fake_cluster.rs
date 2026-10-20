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
use super::test_support::{DUPLEX_CAPACITY, read_request_header, refusal, reply};
use super::{Dialer, ReconnectPolicy, ReplicaLinks};
use crate::session_owner::SessionOwner;
use futures::FutureExt;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, GET_PARTITION_ROUTE_CODE, GET_STATS_CODE,
    LOGIN_REGISTER_CODE, LOGOUT_USER_CODE,
};
use iggy_binary_protocol::consensus::{
    EvictionHeader, EvictionReason, HEADER_SIZE, NON_REPLICATED_CODE_RANGE, Operation,
    PrepareHeader, REJECTION_SECTION_LEN, ReplyHeader, RequestHeader, write_rejection_section,
};
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::responses::streams::get_stream::TopicHeader;
use iggy_binary_protocol::responses::system::{ClusterMetadataResponse, ClusterNodeResponse};
use iggy_binary_protocol::responses::topics::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{PartitionContext, WireDecode, WireEncode, WireName, WireOptions};
use iggy_common::{
    AutoLogin, ClusterNodeRole, ClusterNodeStatus, CompressionAlgorithm, Credentials, IggyError,
};
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
    CommitBody(u64, Vec<u8>),
    CommitRejected(u64, IggyError),
    /// A refusal with `StaleClient` from a replica that replaced its client
    /// table. The session stays.
    Detached,
    Evict(EvictionReason),
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
/// cluster keeps one session: a Register starts a new one, a bind of any
/// other session fails with `Unauthenticated`, and a scripted refusal or
/// rejection with `StaleClient` ends it.
#[derive(Default)]
pub(crate) struct FakeCluster {
    pub(crate) nodes: Mutex<Vec<ClusterNodeResponse>>,
    /// Endpoints that refuse each dial.
    pub(crate) down: Mutex<HashSet<String>>,
    /// Endpoints that never finish a dial.
    pub(crate) silent: Mutex<HashSet<String>>,
    /// Endpoints that close the connection on a Register.
    pub(crate) hang_up_on_register: Mutex<HashSet<String>>,
    /// Usernames whose Register registers a session at once, but whose reply
    /// waits until the name leaves the set.
    pub(crate) held_registers: Mutex<HashSet<String>>,
    pub(crate) reject_credentials: Mutex<bool>,
    pub(crate) session: Mutex<Option<u64>>,
    /// The username of each client id that registered with a password. As on
    /// a server, a Register for another user under that client id ends in an
    /// eviction.
    registered_users: Mutex<HashMap<u128, String>>,
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
            let username = register_username(code, &body);
            self.received.lock().unwrap().push((
                code,
                Received {
                    endpoint: endpoint.clone(),
                    header: request,
                    body,
                },
            ));
            let frame = match self.answer(&endpoint, &request, code, username.as_deref()) {
                Answer::Body(body) => reply(request.request, &body),
                Answer::Refuse(error) => refusal(request.request, &error),
                Answer::Detached => refusal(request.request, &IggyError::StaleClient),
                Answer::Commit(commit) => committed(&request, commit, None, &[]),
                Answer::CommitBody(commit, body) => committed(&request, commit, None, &body),
                Answer::CommitRejected(commit, error) => {
                    committed(&request, commit, Some(&error), &[])
                }
                Answer::Evict(reason) => {
                    let header = EvictionHeader::new(0, 0, 0, request.client, reason);
                    let _ = writer.write_all(bytemuck::bytes_of(&header)).await;
                    return;
                }
                Answer::HangUp => return,
                Answer::Silence => continue,
            };
            while username
                .as_ref()
                .is_some_and(|username| self.held_registers.lock().unwrap().contains(username))
            {
                sleep(POLL_STEP).await;
            }
            if writer.write_all(&frame).await.is_err() {
                return;
            }
        }
    }

    fn answer(
        &self,
        endpoint: &str,
        request: &RequestHeader,
        code: u32,
        username: Option<&str>,
    ) -> Answer {
        let scripted = self
            .script
            .lock()
            .unwrap()
            .get_mut(&(endpoint.to_owned(), code))
            .and_then(VecDeque::pop_front);
        let mut session = self.session.lock().unwrap();
        if let Some(answer) = scripted {
            if let Answer::Refuse(IggyError::StaleClient)
            | Answer::CommitRejected(_, IggyError::StaleClient) = answer
            {
                *session = None;
            }
            return answer;
        }
        match code {
            LOGIN_REGISTER_CODE if self.hang_up_on_register.lock().unwrap().contains(endpoint) => {
                Answer::HangUp
            }
            LOGIN_REGISTER_CODE if *self.reject_credentials.lock().unwrap() => {
                Answer::Refuse(IggyError::InvalidCredentials)
            }
            LOGIN_REGISTER_CODE if self.registered_to_another_user(request.client, username) => {
                Answer::Evict(EvictionReason::SessionError)
            }
            LOGIN_REGISTER_CODE => {
                let registered = FIRST_SESSION + self.registered.fetch_add(1, Ordering::SeqCst);
                *session = Some(registered);
                if let Some(username) = username {
                    self.registered_users
                        .lock()
                        .unwrap()
                        .insert(request.client, username.to_owned());
                }
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
            _ if !matches!(
                request.operation,
                Operation::NonReplicated | Operation::Logout
            ) =>
            {
                Answer::Commit(1)
            }
            _ => Answer::Body(Vec::new()),
        }
    }

    fn registered_to_another_user(&self, client: u128, username: Option<&str>) -> bool {
        username.is_some_and(|username| {
            self.registered_users
                .lock()
                .unwrap()
                .get(&client)
                .is_some_and(|registered| registered != username)
        })
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

pub(crate) fn topic_body(partitions_count: u32) -> Vec<u8> {
    GetTopicResponse {
        topic: TopicHeader {
            id: 1,
            created_at: 0,
            partitions_count,
            message_expiry: 0,
            compression_algorithm: CompressionAlgorithm::None.as_code(),
            max_topic_size: 0,
            size_bytes: 0,
            messages_count: 0,
            name: WireName::new("topic").unwrap(),
            options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
        partitions: (0..partitions_count)
            .map(|id| PartitionResponse {
                id,
                created_at: 0,
                segments_count: 0,
                current_offset: 0,
                size_bytes: 0,
                messages_count: 0,
                context: PartitionContext {
                    incarnation: 7,
                    owner_generation: 0,
                    metadata_op: 11,
                },
            })
            .collect(),
    }
    .to_bytes()
    .to_vec()
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

/// The username of a Register with a password.
fn register_username(code: u32, body: &[u8]) -> Option<String> {
    if code != LOGIN_REGISTER_CODE {
        return None;
    }
    LoginRegisterRequest::decode_from(body)
        .ok()
        .map(|request| request.username.as_str().to_owned())
}

fn request_code(request: &RequestHeader) -> u32 {
    match request.operation {
        Operation::Register => LOGIN_REGISTER_CODE,
        Operation::Logout => LOGOUT_USER_CODE,
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
fn committed(
    request: &RequestHeader,
    commit: u64,
    rejection: Option<&IggyError>,
    payload: &[u8],
) -> Vec<u8> {
    // An empty result section: no error, then the payload.
    let mut body = if let Some(error) = rejection {
        assert!(request.operation.is_result_framed());
        let mut body = vec![0; REJECTION_SECTION_LEN];
        write_rejection_section(&mut body, error.as_code());
        body
    } else if request.operation.is_result_framed() {
        0_u32.to_le_bytes().to_vec()
    } else {
        Vec::new()
    };
    body.extend_from_slice(payload);
    let prepare = PrepareHeader {
        operation: request.operation,
        client: request.client,
        request: request.request,
        request_checksum: request.request_checksum,
        partition_incarnation: request.partition_incarnation,
        op: commit,
        ..Default::default()
    };
    let header =
        ReplyHeader::from_prepare(&prepare, u32::try_from(HEADER_SIZE + body.len()).unwrap());
    let mut frame = bytemuck::bytes_of(&header).to_vec();
    frame.extend_from_slice(&body);
    frame
}
