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

use crate::shard::{
    BROADCAST_TIMEOUT, COMPONENT, IggyShard,
    namespace::IggyNamespace,
    transmission::{
        connector::ShardConnector,
        event::ShardEvent,
        frame::ShardFrame,
        id::ShardId,
        message::{ShardMessage, ShardSendRequestResult},
    },
};
use futures::future::join_all;
use hash32::{Hasher, Murmur3Hasher};
use iggy_common::IggyError;
use std::hash::Hasher as _;
use tracing::{error, info, warn};

impl IggyShard {
    pub async fn send_request_to_shard_or_recoil(
        &self,
        namespace: Option<&IggyNamespace>,
        message: ShardMessage,
    ) -> Result<ShardSendRequestResult, IggyError> {
        if let Some(ns) = namespace {
            if let Some(shard) = self.find_shard(ns) {
                if shard.id == self.id {
                    return Ok(ShardSendRequestResult::Recoil(message));
                }

                let response = match shard.send_request(message).await {
                    Ok(response) => response,
                    Err(err) => {
                        error!(
                            "{COMPONENT} - failed to send request to shard with ID: {}, error: {err}",
                            shard.id
                        );
                        return Err(err);
                    }
                };
                Ok(ShardSendRequestResult::Response(response))
            } else {
                Err(IggyError::ShardNotFound(
                    ns.stream_id(),
                    ns.topic_id(),
                    ns.partition_id(),
                ))
            }
        } else {
            if self.id == 0 {
                return Ok(ShardSendRequestResult::Recoil(message));
            }

            let shard0 = &self.shards[0];
            let response = match shard0.send_request(message).await {
                Ok(response) => response,
                Err(err) => {
                    error!("{COMPONENT} - failed to send admin request to shard0, error: {err}");
                    return Err(err);
                }
            };
            Ok(ShardSendRequestResult::Response(response))
        }
    }

    pub async fn broadcast_event_to_all_shards(&self, event: ShardEvent) -> Result<(), IggyError> {
        if self.is_shutting_down() {
            info!("Skipping broadcast during shutdown for event: {}", event);
            return Ok(());
        }

        let event_type = event.to_string();
        let futures = self
            .shards
            .iter()
            .filter(|s| s.id != self.id)
            .map(|shard| {
                let event = event.clone();
                let conn = shard.clone();
                let shard_id = shard.id;
                let event_type = event_type.clone();

                async move {
                    let (sender, receiver) = async_channel::bounded(1);
                    conn.send(ShardFrame::new(ShardMessage::Event(event), Some(sender)));

                    match compio::time::timeout(BROADCAST_TIMEOUT, receiver.recv()).await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => {
                            warn!(
                                "Broadcast to shard {} failed for event {}: channel error: {}",
                                shard_id, event_type, e
                            );
                            Err(())
                        }
                        Err(e) => {
                            warn!(
                                "Broadcast to shard {} failed for event {}: timeout waiting for response after {:?}, elapsed: {:?}",
                                shard_id, event_type,
                                BROADCAST_TIMEOUT,
                                e
                            );
                            Err(())
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        if futures.is_empty() {
            return Ok(());
        }

        let results = join_all(futures).await;
        let has_failures = results.iter().any(|r| r.is_err());

        if has_failures {
            Err(IggyError::ShardCommunicationError)
        } else {
            Ok(())
        }
    }

    fn find_shard(&self, namespace: &IggyNamespace) -> Option<&ShardConnector<ShardFrame>> {
        self.shards_table.get(namespace).map(|shard_id| {
            self.shards
                .iter()
                .find(|shard| shard.id == shard_id.id())
                .expect("Shard not found in the shards table.")
        })
    }

    pub fn find_shard_table_record(&self, namespace: &IggyNamespace) -> Option<ShardId> {
        self.shards_table.get(namespace).map(|entry| *entry)
    }

    pub fn remove_shard_table_record(&self, namespace: &IggyNamespace) -> ShardId {
        self.shards_table
            .remove(namespace)
            .map(|(_, shard_id)| shard_id)
            .expect("remove_shard_table_record: namespace not found")
    }

    pub fn remove_shard_table_records(
        &self,
        namespaces: &[IggyNamespace],
    ) -> Vec<(IggyNamespace, ShardId)> {
        namespaces
            .iter()
            .map(|ns| {
                let (ns, shard_id) = self.shards_table.remove(ns).unwrap();
                (ns, shard_id)
            })
            .collect()
    }

    pub fn insert_shard_table_record(&self, ns: IggyNamespace, shard_id: ShardId) {
        self.shards_table.insert(ns, shard_id);
    }

    pub fn get_current_shard_namespaces(&self) -> Vec<IggyNamespace> {
        self.shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, shard_id) = entry.pair();
                if shard_id.id() == self.id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect()
    }
}

// Utility function for shard assignment calculation
pub fn calculate_shard_assignment(ns: &IggyNamespace, upperbound: u32) -> u16 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write_u64(ns.inner());
    let hash = hasher.finish32();
    // Murmur3 has problems with weak lower bits for small integer inputs, so we use bits from the middle.
    ((hash >> 16) % upperbound) as u16
}
