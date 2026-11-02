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

use crate::dispatch::authz::authorize_uid;
use crate::shell::{ShellBus, ShellShard};
use bytes::Bytes;
use consensus::MetadataHandle;
use futures::future::{Either, select};
use iggy_binary_protocol::requests::system::GetMetadataChangesRequest;
use iggy_binary_protocol::responses::system::GetMetadataChangesResponse;
use iggy_binary_protocol::{PrepareHeader, WireDecode, WireEncode};
use iggy_common::{IggyError, MetadataChangeStart, MetadataChangesQuery};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::permissioner::Permissioner;
use server_common::Message;
use shard::MetadataSubmit;
use std::pin::pin;
use std::rc::Rc;

/// The binary answer to `metadata.changes`: the request decoded under the
/// rules every transport shares, then one page.
///
/// # Errors
/// As [`metadata_changes_page`], plus `InvalidCommand` for a body that does
/// not decode and `Unauthenticated` without a caller.
#[allow(clippy::future_not_send)]
pub async fn metadata_changes_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
    user_id: Option<u32>,
) -> Result<Bytes, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    let query = GetMetadataChangesRequest::decode_from(body)
        .map_err(|_| IggyError::InvalidCommand)
        .and_then(MetadataChangesQuery::try_from)?;
    metadata_changes_page(shard, query, user_id)
        .await
        .map(|page| page.to_bytes())
}

/// One `metadata.changes` page for an authorized caller.
///
/// A change page answers from this shard's view of the log and is never
/// forwarded. A current state page is assembled on shard 0, which holds the
/// client table and the only write side of the state, and which checks the
/// rule again right before it projects.
///
/// # Errors
/// `InvalidCommand` for a request the rules refuse, `Unauthorized` when the
/// caller lost the grant, `TransientNotAccepted` when no coherent image was
/// captured or shard 0 did not answer within the read budget.
#[allow(clippy::future_not_send)]
pub async fn metadata_changes_page<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    query: MetadataChangesQuery,
    user_id: u32,
) -> Result<GetMetadataChangesResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if !matches!(query.start, MetadataChangeStart::Current(_)) {
        return shard.plane.metadata().metadata_changes(&query);
    }
    if shard.id == 0 {
        return current_state_page(shard, &query, user_id);
    }
    let (reply, receiver) = shard::channel(1);
    shard.forward_metadata_submit(MetadataSubmit::MetadataCurrentState {
        query: Box::new(query),
        user_id,
        reply,
    });
    let outcome = pin!(receiver.recv());
    let deadline = pin!(
        shard
            .bus
            .sleep(shard.plane.metadata().applied_frontier().read_budget())
    );
    match select(outcome, deadline).await {
        Either::Left((result, _)) => result.map_err(|_| IggyError::TransientNotAccepted)?,
        Either::Right(_) => Err(IggyError::TransientNotAccepted),
    }
}

/// A current state page on shard 0: the rule, then the projection, with no
/// await between them.
pub fn current_state_page<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    query: &MetadataChangesQuery,
    user_id: u32,
) -> Result<GetMetadataChangesResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    authorize_uid(shard, Some(user_id), Permissioner::get_metadata_changes)?;
    shard.plane.metadata().metadata_current_state(query)
}
