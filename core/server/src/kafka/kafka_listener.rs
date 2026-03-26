/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::kafka::kafka_connection_handler;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use compio::net::TcpListener;
use err_trail::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, error, info};

pub async fn start(shard: Rc<IggyShard>, shutdown: ShutdownToken) -> Result<(), IggyError> {
    let addr: SocketAddr = shard
        .config
        .kafka
        .address
        .parse()
        .error(|e: &std::net::AddrParseError| {
            format!(
                "Kafka (error: {e}) - failed to parse address: {}",
                shard.config.kafka.address
            )
        })
        .map_err(|_| IggyError::InvalidConfiguration)?;

    let listener = TcpListener::bind(addr)
        .await
        .error(|e: &std::io::Error| {
            format!("Kafka (error: {e}) - failed to bind to address: {addr}")
        })
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;

    let local_addr = listener.local_addr().unwrap();
    shard.kafka_bound_address.set(Some(local_addr));
    info!("Kafka server has started on: {}", local_addr);

    loop {
        let accept_future = listener.accept();

        futures::select! {
            _ = shutdown.wait().fuse() => {
                debug!("Kafka server received shutdown signal");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, remote_addr)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting Kafka connection from {} during shutdown", remote_addr);
                            continue;
                        }
                        info!("Accepted new Kafka connection from: {}", remote_addr);

                        let shard_clone = shard.clone();
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();

                        registry.spawn_connection(async move {
                            let session = shard_clone.add_client(&remote_addr, TransportProtocol::Tcp);
                            let client_id = session.client_id;
                            let stop_rx = registry_clone.add_connection(client_id);

                            if let Err(e) = kafka_connection_handler::handle_connection(
                                &session,
                                stream,
                                &shard_clone,
                                stop_rx,
                            ).await {
                                debug!("Kafka connection closed for client {client_id}: {e}");
                            }

                            shard_clone.delete_client(client_id).await;
                            registry_clone.remove_connection(&client_id);
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept Kafka connection: {e}");
                    }
                }
            }
        }
    }

    info!("Kafka server listener has stopped");
    Ok(())
}
