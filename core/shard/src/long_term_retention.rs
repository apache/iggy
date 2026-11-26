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

//! Retention of a topic that places its batches.
//!
//! The messages of such a topic are in its long-term segments, so its
//! retention policy is judged over those, by the partition primary alone.
//! What the policy removes is committed as a trim of the partition's
//! long-term log, through the partition-submit path every other request
//! takes. The commit is what makes the manifest leave the segments out and
//! what queues their objects for deletion.
//!
//! The segments on a replica's disk hold placed batches. Every replica
//! removes the ones at or below the committed trim point, on the same pass.
//!
//! The same pass expires the registrations of segments that no publish or
//! replace listed in time, so that their objects are deleted.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel};
use consensus::PartitionsHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{IggyExpiry, IggyTimestamp};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use server_common::Message;
use server_common::sharding::IggyNamespace;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Stage a retention pass for `namespace`, a partition that places its
    /// batches, on this shard's pump. `created_revision` is the incarnation
    /// the policy belongs to. A refused frame is picked up by the cleaner's
    /// next tick.
    pub fn request_retain_long_term(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::RetainLongTerm {
            namespace,
            created_revision,
            now,
            message_expiry,
            max_bytes,
        }));
    }

    /// Run a retention pass over `namespace` on the pump, when its partition
    /// is the incarnation `created_revision`: remove the local segments the
    /// long-term log trimmed, and on the primary send the trim the policy
    /// asks for now and the expiry of registrations that are due at `now`.
    /// Both go out on every pass until they commit, since only then does the
    /// log change.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_retain_long_term(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let (trim, expiry) = {
            // The policy belongs to `created_revision`. Judged against any
            // other incarnation it would trim a topic that never had it.
            let Some(partition) = self
                .plane
                .partitions()
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision)
            else {
                return;
            };
            let removal = partition.remove_trimmed_segments().await;
            if removal.segments > 0 {
                // The offers this shard serves name files that are gone.
                self.drop_partition_transfer_state(namespace, partition);
                tracing::debug!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    segments = removal.segments,
                    messages = removal.messages,
                    "removed the segments below the long-term trim point"
                );
            }
            if removal.budget_spent {
                // A clean pass over this partition only follows the trim
                // point, so the rest goes without a second trim.
                self.request_clean_partition(namespace, now, message_expiry, max_bytes);
            }
            (
                partition.long_term_retention_end(now, message_expiry, max_bytes),
                partition.long_term_registrations_expiry(now),
            )
        };
        if let Some(up_to_offset) = trim {
            self.metrics.record_long_term_trim();
            self.trim_segments(namespace, up_to_offset).await;
        }
        if let Some(before) = expiry {
            self.submit_long_term_command(namespace, |partition, number| {
                partition.expire_registrations_request(before, number)
            })
            .await;
        }
    }

    /// Commit a trim of the long-term log of `namespace` up to
    /// `up_to_offset` the way a client write is committed. Nothing waits for
    /// the reply: a command that is refused or lost leaves the segments
    /// listed, and the next pass decides again.
    #[allow(clippy::future_not_send)]
    async fn trim_segments(&self, namespace: IggyNamespace, up_to_offset: u64)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let number = self.floor_advance_requests.get().wrapping_add(1);
        self.floor_advance_requests.set(number);
        let request = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .and_then(|partition| {
                partition
                    .trim_segments_request(up_to_offset, number)
                    .inspect_err(|error| {
                        tracing::error!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            %error,
                            "a long-term trim cannot be encoded as a command"
                        );
                    })
                    .ok()
            });
        let Some(request) = request else {
            return;
        };
        let (reply, _unread) = channel(1);
        self.dispatch_partition_submit(request, reply, None, None)
            .await;
    }
}
