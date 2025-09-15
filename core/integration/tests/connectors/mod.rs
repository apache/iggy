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

use integration::{
    test_connectors_runtime::TestConnectorsRuntime,
    test_server::{IpAddrKind, TestServer},
};
use serial_test::parallel;
use std::collections::HashMap;
use tokio::time::timeout;

#[tokio::test]
#[parallel]
async fn connectors_runtime_should_start() {
    let _ = timeout(std::time::Duration::from_secs(3), setup()).await;
}

async fn setup() -> ConnectorsInfra {
    let mut iggy_envs = HashMap::new();
    iggy_envs.insert("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned());
    let mut test_server = TestServer::new(Some(iggy_envs), true, None, IpAddrKind::V4);
    test_server.start();
    let iggy_server_address = test_server
        .get_raw_tcp_addr()
        .expect("Failed to get Iggy TCP address");

    let mut connectors_runtime = TestConnectorsRuntime::with_iggy_address(&iggy_server_address);
    connectors_runtime.start();
    connectors_runtime.ensure_started().await;

    ConnectorsInfra {
        _iggy_server: test_server,
        _connectors_runtime: connectors_runtime,
    }
}

#[derive(Debug)]
struct ConnectorsInfra {
    _iggy_server: TestServer,
    _connectors_runtime: TestConnectorsRuntime,
}
