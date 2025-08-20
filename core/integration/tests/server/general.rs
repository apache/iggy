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

use crate::server::{
    ScenarioFn, bench_scenario, create_message_payload_scenario, message_headers_scenario,
    run_scenario, stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::test_server::Transport;
use serial_test::parallel;
use test_case::test_matrix;

#[test_matrix(
    [Transport::Tcp, Transport::Quic, Transport::Http],
    [
        system_scenario(),
        user_scenario(),
        message_headers_scenario(),
        create_message_payload_scenario(),
        stream_size_validation_scenario(),
        bench_scenario(),
    ]
)]
#[tokio::test]
#[parallel]
async fn matrix(transport: Transport, scenario: ScenarioFn) {
    run_scenario(transport, scenario).await;
}
