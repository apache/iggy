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

//! The SASL credential check against a real, spawned `iggy-server`.

use secrecy::SecretString;
use serial_test::serial;
use tokio::sync::Semaphore;

use iggy_gateway_kafka::auth::{IggyAuthenticator, SaslAuthenticator};
use iggy_gateway_kafka::protocol::sasl::PlainCredentials;

#[path = "common/iggy_server.rs"]
mod iggy_server;

use iggy_server::TestServer;

/// Iggy keeps a session that did not sign out until its lease runs out, 36 s later, and a full
/// clients table refuses every new login.
#[tokio::test]
#[serial]
async fn given_more_logins_than_the_clients_table_holds_should_admit_each() {
    const CLIENTS_TABLE_MAX: usize = 4;

    let data_dir = tempfile::tempdir().expect("tempdir");
    let table_max = CLIENTS_TABLE_MAX.to_string();
    // Iggy frees the slot of a session that signed out at its next liveness pass, one per
    // heartbeat interval, 5 s by default. A login waits for a slot only 10 s.
    let server = TestServer::spawn_with_env(
        data_dir.path(),
        &[
            ("IGGY_METADATA_CLIENTS_TABLE_MAX", &table_max),
            ("IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL", "1s"),
        ],
    )
    .await;
    let authenticator = IggyAuthenticator::new(server.test_config().address);
    let credentials = PlainCredentials {
        username: "iggy".to_string(),
        password: SecretString::from("iggy"),
    };
    let slots = Semaphore::new(1);

    for login in 0..2 * CLIENTS_TABLE_MAX {
        let principal = authenticator
            .authenticate(
                &credentials,
                slots.acquire().await.expect("an open semaphore"),
            )
            .await;
        assert!(principal.is_ok(), "login {login}: {principal:?}");
    }
}
