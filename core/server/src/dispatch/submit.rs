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
use crate::responses::committed_reply_header;
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

/// The commit position a SUCCESSFULLY COMMITTED metadata reply carries, or
/// `None` when the frame promises the caller nothing.
///
/// Only a success promises. Every other frame on this path stamps `commit` with
/// the primary's `commit_max`, an op the caller was never told committed and,
/// on a backup-homed caller, one its own reads would then wait for:
///
/// - an eviction is an `EvictionHeader` whose bytes would cast cleanly as a
///   reply, so the command is checked first (same guard as
///   `build_raw_pat_reply`);
/// - a request-level denial names itself in `ReplyHeader.status`, the channel
///   the SDK peeks before body decode (see `build_deny_reply`);
/// - a transient rejection did not commit and will be replayed;
/// - a TERMINAL pre-consensus rejection (`PreflightOutcome::Reject`, e.g. a
///   fenced session) is a result section carrying a non-transient code, which
///   is byte-identical to a COMMITTED business rejection (duplicate name, bad
///   expiry). Neither is separable here, and neither has to be: a rejection
///   mutated nothing, so the caller has nothing to read back from it, and
///   grading both as no-promise is the only reading that cannot make a read
///   wait for an op that never committed.
///
/// The grading itself is [`committed_reply_header`], shared with the raw-PAT
/// splice, which must admit exactly the same frames.
///
/// Shared with the HTTP write path, which grades the same frames off the same
/// submit entry point ([`submit_client_request_on_owner`]); one classifier is
/// what keeps the two planes' watermarks meaning the same thing.
pub fn committed_reply_commit(reply: &Message<GenericHeader>) -> Option<u64> {
    committed_reply_header(reply).map(|header| header.commit)
}

#[cfg(test)]
mod tests {
    use super::committed_reply_commit;
    use crate::dispatch::test_support::request_message;
    use crate::responses::{build_deny_reply, build_reply_from_bytes};
    use bytes::Bytes;
    use iggy_binary_protocol::Operation;
    use iggy_common::IggyError;

    /// Commit position of the frames below. Above zero on purpose: `0` is the
    /// "promised nothing" answer, so a fixture at zero could not tell a
    /// classified success from a rejected frame.
    const COMMIT: u64 = 9;

    /// A result-framed body: `[count][index][result]`, then the payload.
    fn result_body(code: u32, payload: &[u8]) -> Bytes {
        let mut body = Vec::new();
        let count = u32::from(code != 0);
        body.extend_from_slice(&count.to_le_bytes());
        if count == 1 {
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&code.to_le_bytes());
        }
        body.extend_from_slice(payload);
        Bytes::from(body)
    }

    /// The whole classification in one table: only a successful commit hands
    /// the read gate a floor. Everything else stamps the primary's
    /// `commit_max` into a frame that promised the caller nothing, and a floor
    /// taken from one of those parks the caller's next read on a backup until
    /// the budget expires.
    #[test]
    fn given_a_metadata_reply_when_classified_should_promise_only_a_committed_success() {
        let request = request_message(Operation::CreateStream, 42, 7, 3, &[]);

        let committed =
            build_reply_from_bytes(request.header(), 42, 7, COMMIT, &result_body(0, b"payload"))
                .into_generic();
        assert_eq!(
            committed_reply_commit(&committed),
            Some(COMMIT),
            "a committed success is the one frame that promises the caller its op"
        );

        for code in [
            IggyError::TransientNotCommitted.as_code(),
            IggyError::TransientNotAccepted.as_code(),
            IggyError::UserAlreadyExists.as_code(),
        ] {
            let rejected =
                build_reply_from_bytes(request.header(), 42, 7, COMMIT, &result_body(code, &[]))
                    .into_generic();
            assert_eq!(
                committed_reply_commit(&rejected),
                None,
                "result code {code} mutated nothing, so it promises no read floor"
            );
        }

        let denied = build_deny_reply(
            request.header(),
            42,
            7,
            COMMIT,
            IggyError::Unauthorized.as_code(),
        )
        .into_generic();
        assert_eq!(
            committed_reply_commit(&denied),
            None,
            "a request-level denial names itself in `status` and commits nothing"
        );

        // Any non-`Reply` command stands in for the eviction frame, whose bytes
        // would otherwise cast cleanly as a `ReplyHeader`.
        assert_eq!(
            committed_reply_commit(&request.into_generic()),
            None,
            "only a `Reply` carries a commit position"
        );
    }
}
