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
use iggy_common::TransportProtocol;
use integration::bench_utils::run_bench_and_wait_for_finish;
use integration::harness::TestHarness;

pub async fn run(harness: &TestHarness) {
    let transport = harness.transport().expect("Transport not set");
    let server = harness.server();
    let server_addr = match transport {
        TransportProtocol::Tcp => server.raw_tcp_addr().expect("TCP address not available"),
        TransportProtocol::Http => server
            .http_addr()
            .expect("HTTP address not available")
            .to_string(),
        TransportProtocol::Quic => server
            .quic_addr()
            .expect("QUIC address not available")
            .to_string(),
        TransportProtocol::WebSocket => server
            .websocket_addr()
            .expect("WebSocket address not available")
            .to_string(),
    };
    let data_size = IggyByteSize::from(8 * 1024 * 1024);

    run_bench_and_wait_for_finish(&server_addr, &transport, "pinned-producer", data_size);
    run_bench_and_wait_for_finish(&server_addr, &transport, "pinned-consumer", data_size);
}
