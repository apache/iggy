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

use crate::reply_frame::current_metadata_commit;
use crate::shell::{ShellBus, ShellShard};
use consensus::MetadataHandle;
use iggy_binary_protocol::{
    Command, ConsensusHeader, HEADER_SIZE, PrepareHeader, SegmentDeleteHeader, SegmentDeleteKind,
};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::{MetadataSubmit, PartitionRead, PartitionReadReply};
use std::rc::Rc;
use std::time::Duration;

const SEGMENT_DELETE_TIMEOUT: Duration = Duration::from_secs(5);

#[allow(clippy::future_not_send)]
pub(super) async fn resolve<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    count: u32,
    created_revision: u64,
    minimum_metadata_op: u64,
) -> Result<u64, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        let (reply, outcome) = shard::channel(1);
        shard.forward_metadata_submit(MetadataSubmit::ResolveSegmentDelete {
            namespace,
            count,
            created_revision,
            minimum_metadata_op,
            reply,
        });
        return shard::bus_timeout(&shard.bus, SEGMENT_DELETE_TIMEOUT, outcome.recv())
            .await
            .and_then(Result::ok)
            .ok_or(IggyError::TransientNotAccepted)?;
    };
    let nonce = shard.next_forward_nonce(consensus.replica());
    let query = SegmentDeleteHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: consensus.cluster(),
        size: u32::try_from(HEADER_SIZE).expect("header size fits u32"),
        view: consensus.view(),
        release: 0,
        command: Command::SegmentDelete,
        replica: consensus.replica(),
        reserved_frame: [0; 66],
        nonce,
        namespace: namespace.inner(),
        created_revision,
        minimum_metadata_op,
        up_to_offset: 0,
        segments_count: count,
        kind: SegmentDeleteKind::Query,
        reserved: [0; 75],
    };
    let result = shard::bus_timeout(&shard.bus, SEGMENT_DELETE_TIMEOUT, async {
        let Some(PartitionReadReply::Primary { primary, .. }) = shard
            .partition_read(namespace, PartitionRead::Primary)
            .await
        else {
            return Err(IggyError::TransientNotAccepted);
        };
        if primary == query.replica {
            return resolve_local(shard, &query).await;
        }
        let (reply, outcome) = shard::channel(1);
        if !shard.park_segment_delete_forward(query, primary, reply) {
            return Err(IggyError::TransientNotAccepted);
        }
        shard
            .bus
            .send_to_replica(primary, message(query).into_generic().into_frozen())
            .await
            .map_err(|_| IggyError::TransientNotAccepted)?;
        let resolved = outcome
            .recv()
            .await
            .map_err(|_| IggyError::TransientNotAccepted)?;
        if resolved.kind != SegmentDeleteKind::Resolved {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(resolved.up_to_offset)
    })
    .await;
    shard.cancel_segment_delete_forward(nonce);
    result.ok_or(IggyError::TransientNotAccepted)?
}

#[allow(clippy::future_not_send)]
pub(super) async fn answer<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    query: SegmentDeleteHeader,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return;
    };
    let mut reply = query;
    reply.replica = consensus.replica();
    match shard::bus_timeout(
        &shard.bus,
        SEGMENT_DELETE_TIMEOUT,
        resolve_local(shard, &query),
    )
    .await
    {
        Some(Ok(offset)) => {
            reply.kind = SegmentDeleteKind::Resolved;
            reply.up_to_offset = offset;
        }
        _ => reply.kind = SegmentDeleteKind::Retry,
    }
    let _ = shard
        .bus
        .send_to_replica(query.replica, message(reply).into_generic().into_frozen())
        .await;
}

#[allow(clippy::future_not_send)]
async fn resolve_local<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    query: &SegmentDeleteHeader,
) -> Result<u64, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let namespace = IggyNamespace::from_raw(query.namespace);
    if current_metadata_commit(shard) < query.minimum_metadata_op {
        return Err(IggyError::TransientNotAccepted);
    }
    let history = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_offset_metadata(namespace, None, 0)
        .ok_or(IggyError::TransientNotAccepted)?;
    if !history.matches_partition(Some(query.created_revision)) {
        return Err(IggyError::TransientNotAccepted);
    }
    // A redirected query never forwards again. A stale primary must reject it.
    match shard
        .partition_read(
            namespace,
            PartitionRead::ResolveSegmentDeleteOffset {
                count: query.segments_count,
            },
        )
        .await
    {
        Some(PartitionReadReply::SegmentDeleteOffset {
            up_to_offset,
            lagging: false,
            created_revision,
        }) if created_revision == query.created_revision => Ok(up_to_offset.unwrap_or(0)),
        _ => Err(IggyError::TransientNotAccepted),
    }
}

fn message(mut header: SegmentDeleteHeader) -> Message<SegmentDeleteHeader> {
    header.seal();
    Message::<SegmentDeleteHeader>::new(HEADER_SIZE).transmute_header(|_, target| *target = header)
}
