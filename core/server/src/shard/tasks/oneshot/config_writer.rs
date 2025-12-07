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

use crate::shard::IggyShard;
use compio::io::AsyncWriteAtExt;
use err_trail::ErrContext;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::info;

pub fn spawn_config_writer_task(shard: &Rc<IggyShard>) {
    let shard_clone = shard.clone();
    shard
        .task_registry
        .oneshot("config_writer")
        .critical(false)
        .run(move |_shutdown_token| async move { write_config(shard_clone).await })
        .spawn();
}

async fn write_config(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let shard_clone = shard.clone();
    let tcp_enabled = shard.config.tcp.enabled;
    let quic_enabled = shard.config.quic.enabled;
    let http_enabled = shard.config.http.enabled;
    let websocket_enabled = shard.config.websocket.enabled;

    let notify_receiver = shard_clone.config_writer_receiver.clone();

    // Wait for notifications until all servers have bound
    loop {
        notify_receiver
            .recv()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)
            .with_error(
                |_| "config_writer: notification channel closed before all servers bound",
            )?;

        let tcp_ready = !tcp_enabled || shard_clone.tcp_bound_address.get().is_some();
        let quic_ready = !quic_enabled || shard_clone.quic_bound_address.get().is_some();
        let http_ready = !http_enabled || shard_clone.http_bound_address.get().is_some();
        let websocket_ready =
            !websocket_enabled || shard_clone.websocket_bound_address.get().is_some();

        if tcp_ready && quic_ready && http_ready && websocket_ready {
            break;
        }
    }

    let mut current_config = shard_clone.config.clone();

    let tcp_addr = shard_clone.tcp_bound_address.get();
    let quic_addr = shard_clone.quic_bound_address.get();
    let http_addr = shard_clone.http_bound_address.get();
    let websocket_addr = shard_clone.websocket_bound_address.get();

    info!(
        "Config writer: TCP addr = {:?}, QUIC addr = {:?}, HTTP addr = {:?}, WebSocket addr = {:?}",
        tcp_addr, quic_addr, http_addr, websocket_addr
    );

    if let Some(tcp_addr) = tcp_addr {
        current_config.tcp.address = tcp_addr.to_string();
    }

    if let Some(quic_addr) = quic_addr {
        current_config.quic.address = quic_addr.to_string();
    }

    if let Some(http_addr) = http_addr {
        current_config.http.address = http_addr.to_string();
    }

    if let Some(websocket_addr) = websocket_addr {
        current_config.websocket.address = websocket_addr.to_string();
    }

    let runtime_path = current_config.system.get_runtime_path();
    let config_path = format!("{runtime_path}/current_config.toml");
    let content = toml::to_string(&current_config)
        .map_err(|_| IggyError::CannotWriteToFile)
        .with_error(|_| "config_writer: cannot serialize current_config")?;

    let mut file = compio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&config_path)
        .await
        .map_err(|_| IggyError::CannotWriteToFile)
        .with_error(|_| format!("config_writer: failed to open current config at {config_path}"))?;

    file.write_all_at(content.into_bytes(), 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)
        .with_error(|_| {
            format!("config_writer: failed to write current config to {config_path}")
        })?;

    file.sync_all()
        .await
        .map_err(|_| IggyError::CannotWriteToFile)
        .with_error(|_| {
            format!("config_writer: failed to fsync current config to {config_path}")
        })?;

    info!("Current config written and synced to: {config_path} with all bound addresses",);

    Ok(())
}
