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

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness]
async fn hello_world(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.ping().await.unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn hello_world(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.ping().await.unwrap();
    client.logout_user().await.unwrap();
}

// The matrix varies the inner transport of ClientWrapper::Iggy.
#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn nested_client_wrapper_forwards_results_and_errors(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    let name = "nested-wrapper";
    let options = StreamCreateOptions::default();
    let stream = client.create_stream(name, &options).await.unwrap();
    let expected_error = client.create_stream(name, &options).await.unwrap_err();
    let client = ClientWrapper::Iggy(client);
    let stream_id = Identifier::numeric(stream.id).unwrap();

    assert_eq!(
        client.get_stream(&stream_id).await.unwrap().unwrap().name,
        name
    );
    assert_eq!(
        client
            .create_stream(name, &options)
            .await
            .unwrap_err()
            .to_string(),
        expected_error.to_string()
    );

    client.delete_stream(&stream_id).await.unwrap();
    assert!(client.get_stream(&stream_id).await.unwrap().is_none());
    client.shutdown().await.unwrap();
}
