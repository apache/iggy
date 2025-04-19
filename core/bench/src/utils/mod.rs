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

use iggy::{
    client::{Client, SystemClient, UserClient},
    clients::builder::IggyClientBuilder,
    error::IggyError,
    models::stats::Stats,
    snapshot::{SnapshotCompression, SystemSnapshotType},
    users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME},
};
use iggy_bench_report::transport::BenchmarkTransport;
use std::{fs, path::Path};
use tracing::error;

pub mod client_factory;
pub mod cpu_name;
pub mod server_starter;

pub async fn get_server_stats(
    transport: &BenchmarkTransport,
    server_address: &str,
) -> Result<Stats, IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    client.get_stats().await
}

pub async fn collect_server_logs_and_save_to_file(
    transport: &BenchmarkTransport,
    server_address: &str,
    output_dir: &Path,
) -> Result<(), IggyError> {
    let client = IggyClientBuilder::new();

    let client = match transport {
        BenchmarkTransport::Tcp => client
            .with_tcp()
            .with_server_address(server_address.to_string())
            .build()?,
        BenchmarkTransport::Http => client
            .with_http()
            .with_api_url(format!("http://{}", server_address))
            .build()?,
        BenchmarkTransport::Quic => client
            .with_quic()
            .with_server_address(server_address.to_string())
            .build()?,
    };

    client.connect().await?;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    let snapshot = client
        .snapshot(
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::ServerLogs],
        )
        .await?
        .0;

    fs::write(output_dir.join("server_logs.zip"), snapshot).map_err(|e| {
        error!("Failed to write server logs to file: {:?}", e);
        IggyError::CannotWriteToFile
    })
}
