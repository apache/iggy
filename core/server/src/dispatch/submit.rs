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

//! The shard-0 metadata-submit RPC, both ends on one page.
//!
//! The metadata consensus group lives on shard 0, but connections live on
//! their home shards. Peer shards send a [`shard::MetadataSubmit`] and await
//! the committed outcome; [`make_metadata_submit_handler`] is what shard 0
//! runs for those frames. The session-lifecycle arms (register / logout and
//! their replica forwards) delegate to `session_ops`, which owns that
//! machinery.

use crate::dispatch::session_ops::{
    answer_forwarded_logout, answer_forwarded_register, submit_logout_local_or_forward,
    submit_register_local_or_forward,
};
use crate::dispatch::upgrade_shard_handle;
use crate::shell::{ShellBus, ShellShard, ShellShardHandle};
use consensus::MetadataHandle;
use iggy_binary_protocol::{GenericHeader, PrepareHeader, RoutedRequestHeader};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use server_common::Message;
use std::rc::Rc;
use tracing::warn;

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits. Submit failures are returned verbatim so the peer can preserve
/// unknown-outcome retry semantics.
pub fn make_metadata_submit_handler<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
) -> shard::MetadataSubmitHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |submit| {
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        let bus = shard.bus.clone();
        bus.spawn(async move {
            match submit {
                shard::MetadataSubmit::Register {
                    vsr_client_id,
                    user_id,
                    reply,
                } => {
                    let bound =
                        submit_register_local_or_forward(&shard, vsr_client_id, user_id).await;
                    let _ = reply.try_send(bound);
                }
                shard::MetadataSubmit::ForwardedRegister {
                    vsr_client_id,
                    user_id,
                    nonce,
                    origin_replica,
                } => {
                    answer_forwarded_register(
                        &shard,
                        vsr_client_id,
                        user_id,
                        nonce,
                        origin_replica,
                    )
                    .await;
                }
                shard::MetadataSubmit::ForwardedLogout {
                    vsr_client_id,
                    session,
                    request,
                    nonce,
                    origin_replica,
                } => {
                    answer_forwarded_logout(
                        &shard,
                        vsr_client_id,
                        session,
                        request,
                        nonce,
                        origin_replica,
                    )
                    .await;
                }
                shard::MetadataSubmit::Logout {
                    vsr_client_id,
                    session,
                    request,
                    reply,
                } => {
                    let outcome =
                        submit_logout_local_or_forward(&shard, vsr_client_id, session, request)
                            .await;
                    let _ = reply.try_send(outcome);
                }
                shard::MetadataSubmit::ClientRequest { request, reply } => {
                    let committed = match request.try_into_typed::<RoutedRequestHeader>() {
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
                shard::MetadataSubmit::CompleteRevocation {
                    stream_id,
                    topic_id,
                    group_id,
                    source_client_id,
                    partition_id,
                    reply,
                } => {
                    let commit = shard
                        .plane
                        .metadata()
                        .submit_complete_revocation_in_process(
                            stream_id,
                            topic_id,
                            group_id,
                            source_client_id,
                            partition_id,
                        )
                        .await
                        .ok();
                    let _ = reply.try_send(commit);
                }
            }
        });
    })
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
pub async fn submit_client_request_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
) -> Option<Message<GenericHeader>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
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
