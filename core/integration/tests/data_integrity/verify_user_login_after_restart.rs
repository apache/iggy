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

use iggy::prelude::*;
use integration::harness::{USER_PASSWORD, create_user, login_user};
use integration::iggy_harness;

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn should_login_non_root_user_after_restart(harness: &mut TestHarness) {
    let root_client = harness.root_client().await.unwrap();
    create_user(&root_client, "testuser").await;

    let client = harness.new_client().await.unwrap();
    login_user(&client, "testuser").await;
    drop(client);
    drop(root_client);

    harness.restart_server().await.unwrap();

    let root_client = harness.root_client().await.unwrap();
    let users = root_client.get_users().await.unwrap();
    assert_eq!(users.len(), 2, "Expected root + testuser after restart");
    drop(root_client);

    let client = harness.new_client().await.unwrap();
    login_user(&client, "testuser").await;
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn should_login_after_password_change_and_restart(harness: &mut TestHarness) {
    let root_client = harness.root_client().await.unwrap();
    create_user(&root_client, "testuser").await;

    let new_password = "new_secret_password";
    let user_id = Identifier::named("testuser").unwrap();
    root_client
        .change_password(&user_id, USER_PASSWORD, new_password)
        .await
        .unwrap();

    let client = harness.new_client().await.unwrap();
    client.login_user("testuser", new_password).await.unwrap();
    drop(client);
    drop(root_client);

    harness.restart_server().await.unwrap();

    let client = harness.new_client().await.unwrap();
    client.login_user("testuser", new_password).await.unwrap();
}
