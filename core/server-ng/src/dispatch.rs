/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Per-shard request dispatch.
//!
//! Client-request queue plumbing, the transport / replica /
//! metadata-submit handler factories, the owner-forwarding helpers that
//! run consensus on shard 0, and the login/register, logout, and
//! non-replicated request handlers.

use crate::auth::{
    complete_login_register, send_login_failure_reply, surface_login_failure,
    verify_login_credentials, verify_pat_credentials,
};
use crate::bootstrap::{ServerNgShard, ServerNgShardHandle};
use crate::login_register::LoginRegisterError;
use crate::pat::maybe_rewrite_pat_request;
use crate::responses::{
    NonReplicatedResponse, build_empty_reply, build_get_me_response, build_non_replicated_response,
    build_raw_pat_reply, connected_client_to_response, current_metadata_commit,
    resolve_partition_request_namespace,
};
use crate::session_manager::SessionManager;
use crate::wire::request_body;
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE, GET_ME_CODE, PING_CODE,
};
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::{GenericHeader, Operation, RequestHeader, WireDecode, WireEncode};
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use message_bus::{IggyMessageBus, MessageBus};
use metadata::impls::metadata::RegisterSubmitError;
use secrecy::ExposeSecret;
use server_common::Message;
use shard::{ConnectedClientInfo, ListClientsHandler};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use tracing::warn;

pub(crate) type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
pub(crate) type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

pub(crate) fn make_client_request_handler(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
) -> RequestHandler {
    let shard = Rc::clone(shard);
    let sessions = Rc::clone(sessions);
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_for_disconnect = Rc::clone(&shard);
    shard
        .bus
        .set_client_connection_lost_fn(Rc::new(move |client_id| {
            if let Some(vsr_client_id) = sessions_for_disconnect
                .borrow_mut()
                .remove_connection(client_id)
            {
                // Local-table reclaim happens here; then spawn a detached
                // task to submit a cluster-replicated `Logout` so peer
                // replicas drop the slot too. Without this, an
                // already-committed `Register` lingers on every backup
                // until they evict the client themselves -- a
                // local-vs-cluster divergence observable across view
                // changes. `submit_logout_in_process` requires shard 0
                // consensus, which this handler always runs on.
                shard_for_disconnect
                    .plane
                    .metadata()
                    .remove_client_session(vsr_client_id);
                let shard = Rc::clone(&shard_for_disconnect);
                compio::runtime::spawn(async move {
                    // request_id 0 + session 0: synthetic disconnect
                    // logout; `submit_logout_in_process` short-circuits to
                    // current commit when no session entry exists, so a
                    // race with the explicit logout path is a no-op.
                    if let Err(error) = shard
                        .plane
                        .metadata()
                        .submit_logout_in_process(vsr_client_id, 0, 0)
                        .await
                    {
                        warn!(
                            vsr_client_id,
                            ?error,
                            "disconnect logout submit failed; peer slots may linger until eviction"
                        );
                    }
                })
                .detach();
            }
        }));
    Rc::new(move |client_id, message| {
        enqueue_client_request(
            Rc::clone(&shard),
            Rc::clone(&sessions),
            Rc::clone(&queues),
            Rc::clone(&active),
            client_id,
            message,
        );
    })
}

/// Build the per-shard [`ListClientsHandler`]: on a `ListClients`
/// broadcast, serialize this shard's locally-homed connected clients from
/// its `SessionManager` and push them back over the reply sender. The
/// aggregation across all shards happens in
/// [`shard::IggyShard::list_all_clients`].
pub(crate) fn make_list_clients_handler(
    sessions: &Rc<RefCell<SessionManager>>,
) -> ListClientsHandler {
    let sessions = Rc::clone(sessions);
    Rc::new(move |reply| {
        let clients: Vec<ConnectedClientInfo> = sessions
            .borrow()
            .iter_clients()
            .map(|record| ConnectedClientInfo {
                client_id: record.client_id,
                user_id: record.user_id,
                transport: record.transport,
                address: record.address,
            })
            .collect();
        // Best-effort: the gather side bounds itself by count + timeout, so
        // a dropped reply (receiver gone) just means this shard is omitted.
        let _ = reply.try_send(clients);
    })
}

pub(crate) fn make_deferred_replica_message_handler(
    shard_handle: &ServerNgShardHandle,
) -> MessageHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

pub(crate) fn make_deferred_client_request_handler(
    bus: &Rc<IggyMessageBus>,
    shard_handle: &ServerNgShardHandle,
    sessions: &Rc<RefCell<SessionManager>>,
) -> RequestHandler {
    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::clone(sessions);
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_handle_for_disconnect = Rc::clone(&shard_handle);
    bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
        if let Some(vsr_client_id) = sessions_for_disconnect
            .borrow_mut()
            .remove_connection(client_id)
            && let Some(shard) = upgrade_shard_handle(&shard_handle_for_disconnect)
        {
            // Local reader-copy reclaim, then route a synthetic Logout to
            // the metadata consensus owner so the replicated session slot
            // is dropped cluster-wide instead of lingering until the peer
            // is evicted. This handler may run on a shard > 0 (no local
            // consensus), so it goes through `submit_logout_on_owner`
            // (forwards to shard 0) -- `submit_logout_in_process` would
            // panic here. request_id 0 + session 0: synthetic disconnect
            // logout; `submit_logout_in_process` short-circuits to the
            // current commit when no session entry exists, so a race with
            // the explicit logout path is a no-op.
            shard.plane.metadata().remove_client_session(vsr_client_id);
            compio::runtime::spawn(async move {
                if let Err(error) = submit_logout_on_owner(&shard, vsr_client_id, 0, 0).await {
                    warn!(
                        vsr_client_id,
                        ?error,
                        "disconnect logout submit failed; peer slots may linger until eviction"
                    );
                }
            })
            .detach();
        }
    }));
    Rc::new(move |client_id, message| {
        let shard_handle = Rc::clone(&shard_handle);
        let sessions = Rc::clone(&sessions);
        let queues = Rc::clone(&queues);
        let active = Rc::clone(&active);
        queues
            .borrow_mut()
            .entry(client_id)
            .or_default()
            .push_back(message);
        if !active.borrow_mut().insert(client_id) {
            return;
        }
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                active.borrow_mut().remove(&client_id);
                return;
            };
            drain_client_requests(shard, sessions, queues, active, client_id).await;
        })
        .detach();
    })
}

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits; replies `None` on transient submit failure so the peer never
/// blocks forever.
pub(crate) fn make_metadata_submit_handler(
    shard_handle: &ServerNgShardHandle,
) -> shard::MetadataSubmitHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |submit| {
        let shard_handle = Rc::clone(&shard_handle);
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                return;
            };
            match submit {
                shard::MetadataSubmit::Register {
                    vsr_client_id,
                    reply,
                } => {
                    let session = shard
                        .plane
                        .metadata()
                        .submit_register_in_process(vsr_client_id)
                        .await
                        .ok();
                    let _ = reply.try_send(session);
                }
                shard::MetadataSubmit::Logout {
                    vsr_client_id,
                    session,
                    request,
                    reply,
                } => {
                    let commit = shard
                        .plane
                        .metadata()
                        .submit_logout_in_process(vsr_client_id, session, request)
                        .await
                        .ok();
                    let _ = reply.try_send(commit);
                }
                shard::MetadataSubmit::ClientRequest { request, reply } => {
                    let committed = match request.try_into_typed::<RequestHeader>() {
                        Ok(typed) => shard
                            .plane
                            .metadata()
                            .submit_request_in_process(typed)
                            .await
                            .ok(),
                        Err(error) => {
                            warn!(?error, "ClientRequest submit: undecodable request header");
                            None
                        }
                    };
                    let _ = reply.try_send(committed);
                }
            }
        })
        .detach();
    })
}

fn enqueue_client_request(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) {
    queues
        .borrow_mut()
        .entry(client_id)
        .or_default()
        .push_back(message);
    if !active.borrow_mut().insert(client_id) {
        return;
    }

    compio::runtime::spawn(async move {
        drain_client_requests(shard, sessions, queues, active, client_id).await;
    })
    .detach();
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
) {
    loop {
        let Some(message) = pop_next_client_request(&queues, &active, client_id) else {
            return;
        };
        handle_client_request(&shard, &sessions, client_id, message).await;
    }
}

fn pop_next_client_request(
    queues: &ClientRequestQueues,
    active: &ActiveClientRequests,
    client_id: u128,
) -> Option<Message<GenericHeader>> {
    let mut queues = queues.borrow_mut();
    let Some(queue) = queues.get_mut(&client_id) else {
        active.borrow_mut().remove(&client_id);
        return None;
    };
    let message = queue.pop_front();
    if queue.is_empty() {
        queues.remove(&client_id);
    }
    if message.is_none() {
        active.borrow_mut().remove(&client_id);
    }
    message
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_client_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
) {
    let request = match message.try_into_typed::<RequestHeader>() {
        Ok(request) => request,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "dropping client request with invalid header"
            );
            return;
        }
    };

    ensure_transport_connection(shard, sessions, transport_client_id);

    let header = *request.header();
    if header.operation == Operation::NonReplicated {
        // Auth bypass guard: only `PING` and `GET_CLUSTER_METADATA` are
        // legitimately pre-auth (liveness probe + connection bootstrap
        // metadata). Every other non-replicated code (`GET_STREAM*`,
        // `GET_TOPIC*`, `GET_STATS`, `POLL_MESSAGES`) reads live state and
        // MUST go through Register first, since server-ng has no
        // per-resource authz layer.
        let nr_code = u32::from_le_bytes(
            request.header().reserved[..4]
                .try_into()
                .unwrap_or_default(),
        );
        let allowed_pre_auth = matches!(nr_code, PING_CODE | GET_CLUSTER_METADATA_CODE);
        if !allowed_pre_auth && sessions.borrow().get_session(transport_client_id).is_none() {
            warn!(
                transport_client_id,
                code = nr_code,
                "dropping pre-auth non-replicated read"
            );
            return;
        }
        handle_non_replicated_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Register && header.session == 0 && header.request == 0 {
        handle_login_register_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Logout {
        handle_logout_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    let bound = sessions.borrow().get_session(transport_client_id);
    if bound.is_none() {
        // Replicated request on an unbound transport. Without this short-
        // circuit, the rewrite below overwrites `header.client` with
        // `transport_client_id` and dispatches; the request_preflight then
        // rejects with `NoSession`/`SessionMismatch` and the failure either
        // disappears silently or emits an Eviction the SDK previously
        // could not decode. Either way the SDK blocked until socket
        // timeout. Emit an empty Reply so the SDK fails fast: the typed
        // decoder downstream rejects the empty body with `InvalidCommand`
        // instead of hanging.
        let commit = current_metadata_commit(shard);
        let reply = build_empty_reply(&header, transport_client_id, 0, commit);
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "failed to surface unbound-session reply"
            );
        } else {
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "dropping replicated request from unbound transport; replied empty"
            );
        }
        return;
    }

    let resolved_namespace = if header.operation.is_partition() {
        match resolve_partition_request_namespace(shard, header.operation, request_body(&request)) {
            Ok(namespace) => Some(namespace),
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "dropping partition request with unresolved namespace"
                );
                return;
            }
        }
    } else {
        None
    };
    let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
        *new_header = header;
        new_header.client = transport_client_id;
        if let Some(namespace) = resolved_namespace {
            new_header.namespace = namespace;
        }
        if let Some((bound_client_id, bound_session)) = bound {
            new_header.client = bound_client_id;
            new_header.session = bound_session;
        }
    });
    let (request, raw_pat_token) =
        match maybe_rewrite_pat_request(sessions, transport_client_id, request) {
            Ok(rewritten) => rewritten,
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "dropping request with invalid PAT replication context"
                );
                return;
            }
        };
    let request_header = *request.header();
    // Replicated request: run consensus on the metadata owner (shard 0) and
    // bring the committed reply back here. This shard owns the connection,
    // so it writes the reply to the socket via the transport client id --
    // shard 0 can't route by the consensus client id (no home-shard bits).
    match submit_client_request_on_owner(shard, request).await {
        Some(reply) => {
            // The raw PAT token never enters consensus (it is non-deterministic
            // and secret), so the committed reply body is empty. Substitute the
            // raw-token response here, on the minting client's home shard, using
            // the confirmed commit position from the committed reply.
            let reply = match build_raw_pat_reply(&request_header, reply, raw_pat_token) {
                Ok(reply) => reply,
                Err(error) => {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "failed to build raw PAT reply"
                    );
                    return;
                }
            };
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "failed to deliver committed reply to client"
                );
            }
        }
        None => {
            // Transient submit failure (not primary / not caught up / dedup
            // absorbed). Stay silent; the SDK read-timeout replays.
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "replicated request not committed (transient); client will replay"
            );
        }
    }
}

#[allow(clippy::future_not_send)]
async fn handle_non_replicated_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    match code {
        PING_CODE => {
            let commit = current_metadata_commit(shard);
            let reply = build_empty_reply(
                request.header(),
                request.header().client,
                request.header().session,
                commit,
            );
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    "failed to send non-replicated ping reply"
                );
            }
        }
        GET_ME_CODE => {
            // `get_me` reports the requesting connection's own identity,
            // sourced from this shard's `SessionManager` (not `IggyMetadata`),
            // so it is built here rather than in
            // `build_non_replicated_response`.
            let response = build_get_me_response(sessions, transport_client_id);
            send_non_replicated_bytes(
                shard,
                &request,
                transport_client_id,
                response.to_bytes(),
                "get_me",
            )
            .await;
        }
        GET_CLIENTS_CODE => {
            // Shared-nothing: each shard knows only its own connections, so
            // gather across all shards (scatter-gather over the mesh).
            let infos = shard.list_all_clients().await;
            let response = GetClientsResponse {
                clients: infos.iter().map(connected_client_to_response).collect(),
            };
            send_non_replicated_bytes(
                shard,
                &request,
                transport_client_id,
                response.to_bytes(),
                "get_clients",
            )
            .await;
        }
        GET_CLIENT_CODE => {
            // No reverse map from the wire u32 id to a u128 transport id /
            // home shard (the u32 is just the seq tail), so gather all and
            // filter -- same fan-out as `get_clients`.
            let target = GetClientRequest::decode_from(request_body(&request))
                .ok()
                .map(|req| req.client_id);
            let infos = shard.list_all_clients().await;
            #[allow(clippy::cast_possible_truncation)]
            let found = target.and_then(|id| infos.iter().find(|info| info.client_id as u32 == id));
            // The SDK decodes an empty body as `None` (client not found).
            let bytes = found.map_or_else(Bytes::new, |info| {
                ClientDetailsResponse {
                    client: connected_client_to_response(info),
                    consumer_groups: Vec::new(),
                }
                .to_bytes()
            });
            send_non_replicated_bytes(shard, &request, transport_client_id, bytes, "get_client")
                .await;
        }
        _ => match build_non_replicated_response(shard, code, request_body(&request)) {
            Ok(response) => {
                let commit = current_metadata_commit(shard);
                let reply = response.into_reply(
                    request.header(),
                    request.header().client,
                    request.header().session,
                    commit,
                );
                if let Err(error) = shard
                    .bus
                    .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                    .await
                {
                    warn!(
                        transport_client_id,
                        code,
                        error = %error,
                        "failed to send non-replicated VSR reply"
                    );
                }
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    code,
                    error = %error,
                    "dropping unsupported non-replicated VSR request"
                );
            }
        },
    }
}

/// Send a non-replicated reply body to a client, stamping the current
/// metadata commit. Shared by the `get_me` / `get_clients` / `get_client`
/// arms.
#[allow(clippy::future_not_send)]
async fn send_non_replicated_bytes(
    shard: &Rc<ServerNgShard>,
    request: &Message<RequestHeader>,
    transport_client_id: u128,
    bytes: Bytes,
    label: &'static str,
) {
    let commit = current_metadata_commit(shard);
    let reply = NonReplicatedResponse::Bytes(bytes).into_reply(
        request.header(),
        request.header().client,
        request.header().session,
        commit,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(transport_client_id, label, error = %error, "failed to send non-replicated reply");
    }
}

/// Run the consensus `Register` proposal on the metadata owner (shard 0)
/// and return the committed session.
///
/// Credential verification and session binding stay on the calling (home)
/// shard -- only this consensus step must execute where the metadata
/// consensus group lives. On shard 0 it calls in-process directly; on a
/// peer it forwards a [`shard::MetadataSubmit`] to shard 0 and awaits the
/// committed op. A dropped reply (shard-0 inbox full / shutdown) maps to a
/// transient `Canceled`, which the caller wraps so the SDK replays.
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_register_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
) -> Result<u64, RegisterSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        reply,
    });
    match rx.recv().await {
        Ok(Some(session)) => Ok(session),
        _ => Err(RegisterSubmitError::Canceled),
    }
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
async fn submit_logout_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, RegisterSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_logout_in_process(vsr_client_id, session, request)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Logout {
        vsr_client_id,
        session,
        request,
        reply,
    });
    match rx.recv().await {
        Ok(Some(commit)) => Ok(commit),
        _ => Err(RegisterSubmitError::Canceled),
    }
}

/// Submit a replicated client request to the metadata owner (shard 0) and
/// return the committed reply.
///
/// The metadata consensus group lives on shard 0, but the connection lives
/// on the home shard (this shard). Run consensus where it belongs and bring
/// the committed reply back here so the caller can write it to the
/// originating socket -- shard 0 cannot route the reply by the consensus
/// `client` id (it's the VSR id, not the transport/home-shard-encoding id).
/// `None` = transient submit failure (SDK read-timeout replays).
#[allow(clippy::future_not_send)]
async fn submit_client_request_on_owner(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Option<Message<GenericHeader>> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_request_in_process(request)
            .await
            .ok();
    }
    let (reply, rx) = shard::channel::<Option<Message<GenericHeader>>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::ClientRequest {
        request: request.into_generic(),
        reply,
    });
    rx.recv().await.ok().flatten()
}

#[allow(clippy::future_not_send)]
async fn handle_logout_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let Some((vsr_client_id, session)) = sessions.borrow().get_session(transport_client_id) else {
        warn!(
            transport_client_id,
            "dropping logout for unbound VSR session"
        );
        return;
    };

    let request_id = request.header().request;
    let commit = match submit_logout_on_owner(shard, vsr_client_id, session, request_id).await {
        Ok(commit) => commit,
        Err(error) => {
            warn!(transport_client_id, error = %error, "logout/unregister failed");
            return;
        }
    };

    sessions.borrow_mut().remove_connection(transport_client_id);

    let reply = build_empty_reply(request.header(), vsr_client_id, session, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send logout reply"
        );
    }
}

fn ensure_transport_connection(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) {
    let Some(meta) = shard.bus.client_meta(transport_client_id) else {
        return;
    };
    sessions
        .borrow_mut()
        .ensure_connection(transport_client_id, meta.peer_addr, meta.transport);
}

#[allow(clippy::future_not_send)]
async fn handle_login_register_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let body = request_body(&request);
    let vsr_client_id = request.header().client;

    if let Ok(wire_request) = LoginRegisterRequest::decode_from(body) {
        match verify_login_credentials(
            shard,
            wire_request.username.as_str(),
            wire_request.password.expose_secret(),
        ) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                )
                .await
                {
                    warn!(transport_client_id, error = %error, "login/register failed");
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(LoginRegisterError::InvalidCredentials) => {
                // Fall through to PAT attempt so a credential payload that
                // collides with a valid PAT payload shape still gets a
                // chance; if PAT also rejects, the final fall-through emits
                // the empty-reply failure path below.
            }
            Err(error) => {
                warn!(transport_client_id, error = %error, "login/register failed");
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    if let Ok(wire_request) = LoginRegisterWithPatRequest::decode_from(body) {
        match verify_pat_credentials(shard, wire_request.token.expose_secret()) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                )
                .await
                {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "login/register with PAT failed"
                    );
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    "login/register with PAT failed"
                );
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    warn!(
        transport_client_id,
        "dropping register request with unsupported payload shape"
    );
    send_login_failure_reply(shard, transport_client_id, request.header()).await;
}

pub(crate) fn upgrade_shard_handle(
    shard_handle: &ServerNgShardHandle,
) -> Option<Rc<ServerNgShard>> {
    shard_handle
        .borrow()
        .as_ref()
        .and_then(std::rc::Weak::upgrade)
}
