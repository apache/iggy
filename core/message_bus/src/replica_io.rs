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

//! Shard-0 TCP plane bootstrap.
//!
//! Every shard on a node instantiates its own `Rc<IggyMessageBus>` on its
//! own compio runtime, but only shard 0 terminates TCP: it binds the
//! replica listener, binds the client listener, and dials higher-id peers.
//! Each accepted or dialed connection is handed to the delegate callbacks
//! supplied by the caller (typically wrapping `shard::coordinator::ShardZeroCoordinator`),
//! which duplicate the fd and ship it to the owning shard via the
//! inter-shard `ShardFrame` channel.
//!
//! Non-zero shards early-return `Ok(None)`; the launcher calls this helper
//! unconditionally per shard, so non-zero shards just have no listener
//! binding and rely on `send_to_*` slow-path forwarding to reach the
//! owning shard.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use iggy_common::IggyError;

use crate::connector::start as start_connector;
use crate::replica_listener::{bind as bind_replica_listener, run as run_replica_listener};
use crate::{AcceptedClientFn, AcceptedReplicaFn, IggyMessageBus, client_listener};

/// Bound addresses returned to shard 0 after the listeners come up.
#[derive(Debug, Clone, Copy)]
pub struct BoundPlanes {
    pub replica: SocketAddr,
    pub client: SocketAddr,
}

/// Bind the replica + client listeners on shard 0 and start the outbound
/// replica connector. Non-zero shards early-return `Ok(None)`.
///
/// Each accepted / dialed TCP stream is handed to the supplied delegate
/// callback: `on_accepted_replica` for inbound replicas and for freshly
/// dialed higher-id peers; `on_accepted_client` for SDK client accepts.
/// The callbacks are responsible for the dup-fd + inter-shard send.
///
/// # Errors
///
/// Returns `IggyError::CannotBindToSocket` if either listener bind fails.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub async fn start_on_shard_zero(
    bus: &Rc<IggyMessageBus>,
    replica_listen_addr: SocketAddr,
    client_listen_addr: SocketAddr,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_accepted_replica: AcceptedReplicaFn,
    on_accepted_client: AcceptedClientFn,
    reconnect_period: Duration,
) -> Result<Option<BoundPlanes>, IggyError> {
    if bus.shard_id() != 0 {
        return Ok(None);
    }

    let (replica_listener, replica_bound) = bind_replica_listener(replica_listen_addr).await?;
    let (clients_listener, client_bound) = client_listener::bind(client_listen_addr).await?;

    let token_for_replica = bus.token();
    let on_accepted_replica_for_listener = on_accepted_replica.clone();
    let listener_max_message_size = bus.config().max_message_size;
    let replica_handle = compio::runtime::spawn(async move {
        run_replica_listener(
            replica_listener,
            token_for_replica,
            cluster_id,
            self_id,
            replica_count,
            on_accepted_replica_for_listener,
            listener_max_message_size,
        )
        .await;
    });
    bus.track_background(replica_handle);

    let token_for_client = bus.token();
    let client_handle = compio::runtime::spawn(async move {
        client_listener::run(clients_listener, token_for_client, on_accepted_client).await;
    });
    bus.track_background(client_handle);

    start_connector(
        bus,
        cluster_id,
        self_id,
        peers,
        on_accepted_replica,
        reconnect_period,
    )
    .await;

    Ok(Some(BoundPlanes {
        replica: replica_bound,
        client: client_bound,
    }))
}

/// [`start_on_shard_zero`] defaulting `reconnect_period` to the bus's
/// [`crate::MessageBusConfig::reconnect_period`].
///
/// # Errors
///
/// Returns `IggyError::CannotBindToSocket` if either listener bind fails.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub async fn start_on_shard_zero_default(
    bus: &Rc<IggyMessageBus>,
    replica_listen_addr: SocketAddr,
    client_listen_addr: SocketAddr,
    cluster_id: u128,
    self_id: u8,
    replica_count: u8,
    peers: Vec<(u8, SocketAddr)>,
    on_accepted_replica: AcceptedReplicaFn,
    on_accepted_client: AcceptedClientFn,
) -> Result<Option<BoundPlanes>, IggyError> {
    let reconnect_period = bus.config().reconnect_period;
    start_on_shard_zero(
        bus,
        replica_listen_addr,
        client_listen_addr,
        cluster_id,
        self_id,
        replica_count,
        peers,
        on_accepted_replica,
        on_accepted_client,
        reconnect_period,
    )
    .await
}
