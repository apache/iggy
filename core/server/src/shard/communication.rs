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
    transmission::{
        connector::ShardConnector,
        event::ShardEvent,
        frame::{ShardFrame, ShardResponse},
        message::{ShardMessage, ShardRequest},
    },
};
use futures::future::join_all;
use hash32::{Hasher, Murmur3Hasher};
use iggy_common::sharding::{IggyNamespace, PartitionLocation};
use iggy_common::{Identifier, IggyError};
use std::hash::Hasher as _;
use tracing::{error, info, warn};

impl IggyShard {
    /// Sends a control-plane request to shard 0's message pump.
    pub async fn send_to_control_plane(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse, IggyError> {
        let shard0 = &self.shards[0];
        shard0
            .send_request(ShardMessage::Request(request))
            .await
            .map_err(|err| {
                error!(
                    "{COMPONENT} - failed to send control-plane request to shard 0, error: {err}"
                );
                err
            })
    }

    /// Sends a data-plane request to the shard owning the partition.
    pub async fn send_to_data_plane(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse, IggyError> {
        let ns = request
            .routing
            .as_ref()
            .expect("data-plane request requires namespace");
        let shard = self
            .find_shard(ns)
            .ok_or_else(|| self.namespace_not_found_error(ns))?;
        shard
            .send_request(ShardMessage::Request(request))
            .await
            .map_err(|err| {
                error!(
                    "{COMPONENT} - failed to send data-plane request to shard {}, error: {err}",
                    shard.id
                );
                err
            })
    }

    /// Converts a missing namespace in shards_table to the appropriate entity-not-found error.
    fn namespace_not_found_error(&self, ns: &IggyNamespace) -> IggyError {
        let stream_id =
            Identifier::numeric(ns.stream_id() as u32).expect("numeric identifier is always valid");
        let topic_id =
            Identifier::numeric(ns.topic_id() as u32).expect("numeric identifier is always valid");

        if self.metadata.get_stream_id(&stream_id).is_none() {
            return IggyError::StreamIdNotFound(stream_id);
        }

        if self
            .metadata
            .get_topic_id(ns.stream_id(), &topic_id)
            .is_none()
        {
            return IggyError::TopicIdNotFound(stream_id, topic_id);
        }

        IggyError::PartitionNotFound(ns.partition_id(), topic_id, stream_id)
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

    pub fn find_shard(&self, namespace: &IggyNamespace) -> Option<&ShardConnector<ShardFrame>> {
        self.shards_table.get(namespace).map(|location| {
            self.shards
                .iter()
                .find(|shard| shard.id == *location.shard_id)
                .expect("Shard not found in the shards table.")
        })
    }

    pub fn remove_shard_table_record(&self, namespace: &IggyNamespace) -> PartitionLocation {
        self.shards_table
            .remove(namespace)
            .map(|(_, location)| location)
            .expect("remove_shard_table_record: namespace not found")
    }

    pub fn insert_shard_table_record(&self, ns: IggyNamespace, location: PartitionLocation) {
        self.shards_table.insert(ns, location);
    }

    pub fn get_current_shard_namespaces(&self) -> Vec<IggyNamespace> {
        self.shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, location) = entry.pair();
                if *location.shard_id == self.id {
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
