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

//! Stats against the server (vsr): `clients_count` must report the cross-shard
//! connected-client total gathered by the `ListClients` broadcast, not the
//! hardcoded 0 the sync single-shard read used to answer. `open_files_count`
//! must not depend on the sysinfo printer.

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness(
    test_client_transport = [Tcp]
)]
async fn given_connected_clients_when_getting_stats_should_count_clients(harness: &TestHarness) {
    let clients = harness.tcp_root_clients(2).await.expect("tcp root clients");

    let stats = clients[0].get_stats().await.expect("get stats");

    assert_eq!(
        stats.clients_count, 2,
        "stats must count both connected clients, got {}",
        stats.clients_count
    );
}

#[iggy_harness(
    test_client_transport = [Tcp],
    server(logging.sysinfo_print_interval = "0 s")
)]
async fn given_disabled_sysinfo_printer_when_getting_stats_should_count_open_files(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");

    let stats = client.get_stats().await.expect("get stats");

    assert!(
        stats.open_files_count > 0,
        "stats must count open descriptors without the sysinfo printer, got 0"
    );
    assert!(
        stats.open_files_count <= stats.open_files_limit,
        "open files {} must fit the limit {}",
        stats.open_files_count,
        stats.open_files_limit
    );
}
