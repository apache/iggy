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

use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient};
use iggy_binary_protocol::UserClient;
use integration::{
    tcp_client::TcpClientFactory,
    test_connectors_runtime::TestConnectorsRuntime,
    test_server::{ClientFactory, IpAddrKind, TestServer},
};
use serial_test::parallel;
use std::collections::HashMap;

mod sinks;
mod sources;

#[tokio::test]
#[parallel]
async fn connectors_runtime_should_start() {
    let mut infra = setup();
    infra.start_connectors_runtime(None, None).await;
}

fn setup() -> ConnectorsInfra {
    let mut iggy_envs = HashMap::new();
    iggy_envs.insert("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned());
    let mut test_server = TestServer::new(Some(iggy_envs), true, None, IpAddrKind::V4);
    test_server.start();
    ConnectorsInfra {
        iggy_server: test_server,
        connectors_runtim: None,
    }
}

#[derive(Debug)]
struct ConnectorsInfra {
    iggy_server: TestServer,
    connectors_runtim: Option<TestConnectorsRuntime>,
}

impl ConnectorsInfra {
    pub async fn start_connectors_runtime(
        &mut self,
        config_path: Option<&str>,
        envs: Option<HashMap<String, String>>,
    ) {
        if self.connectors_runtim.is_some() {
            return;
        }

        let mut all_envs = None;
        if let Some(config_path) = config_path {
            let config_path = format!("tests/connectors/{config_path}");
            let mut map = HashMap::new();
            map.insert(
                "IGGY_CONNECTORS_CONFIG_PATH".to_owned(),
                config_path.to_owned(),
            );
            all_envs = Some(map);
        }

        if let Some(envs) = envs {
            for (k, v) in envs {
                all_envs
                    .get_or_insert_with(HashMap::new)
                    .insert(k.to_owned(), v.to_owned());
            }
        }

        let iggy_server_address = self
            .iggy_server
            .get_raw_tcp_addr()
            .expect("Failed to get Iggy TCP address");
        let mut connectors_runtime =
            TestConnectorsRuntime::with_iggy_address(&iggy_server_address, all_envs);
        connectors_runtime.start();
        connectors_runtime.ensure_started().await;
        self.connectors_runtim = Some(connectors_runtime);
    }

    pub async fn create_client(&self) -> IggyClient {
        let server_addr = self
            .iggy_server
            .get_raw_tcp_addr()
            .expect("Failed to get Iggy TCP address");
        let client = TcpClientFactory {
            server_addr,
            ..Default::default()
        }
        .create_client()
        .await;
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .expect("Failed to login as root user");
        IggyClient::create(client, None, None)
    }
}
