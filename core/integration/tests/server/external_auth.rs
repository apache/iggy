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

//! Single-node external auth integration tests.

use iggy::prelude::*;
use integration::harness::TestHarnessBuilder;
use integration::harness::config::TestServerConfig;
use integration::harness::ext_auth_server::{EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, ExtAuthServer};
use std::collections::HashMap;

fn ext_auth_config(url: &str) -> TestServerConfig {
    TestServerConfig::builder()
        .extra_envs(HashMap::from([
            ("IGGY_EXTERNAL_AUTH_ENABLED".to_string(), "true".to_string()),
            ("IGGY_EXTERNAL_AUTH_URL".to_string(), url.to_string()),
            (
                "IGGY_EXTERNAL_AUTH_FORWARD_CREDENTIALS".to_string(),
                "true".to_string(),
            ),
            ("IGGY_EXTERNAL_AUTH_TIMEOUT".to_string(), "5 s".to_string()),
        ]))
        .build()
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_login_should_authorize_reads() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_login_should_authorize_reads",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-test-stream").await.unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-test-stream"),
        "ext-auth user should see the stream"
    );
}

#[tokio::test]
async fn given_ext_auth_when_inline_grant_login_should_deny_manage_ops() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name(
            "server__ext_auth__given_ext_auth_when_inline_grant_login_should_deny_manage_ops",
        )
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let ext = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    let denied = ext.create_stream("should-fail").await;
    assert!(denied.is_err(), "ext-auth user must not create streams");
}

#[tokio::test]
async fn given_ext_auth_when_unknown_user_should_deny() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_unknown_user_should_deny")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login("mallory", "wrong")
        .connect()
        .await;

    assert!(result.is_err(), "unknown user must be denied");
}

#[tokio::test]
async fn given_ext_auth_when_wrong_password_should_deny() {
    let auth = ExtAuthServer::start().await;
    let mut harness = TestHarnessBuilder::default()
        .test_name("server__ext_auth__given_ext_auth_when_wrong_password_should_deny")
        .server(ext_auth_config(&auth.url()))
        .build()
        .unwrap();
    harness.start().await.unwrap();

    let result = harness
        .server()
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, "wrong-password")
        .connect()
        .await;

    assert!(result.is_err(), "wrong password must be denied");
}
