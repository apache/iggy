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
use integration::iggy_harness;

#[iggy_harness(cluster_nodes = [3, 5, 7],
    test_client_transport = [ Tcp, Http, WebSocket, Quic, TcpTlsSelfSigned, TcpTlsGenerated, WebSocketTlsSelfSigned, WebSocketTlsGenerated],
    server(segment.size = ["1MiB", "2MiB"],
           segment.cache_indexes = ["open_segment", "all"]))]
#[ignore]
async fn should_ping_all_cluster_nodes(harness: TestHarness) {
    for i in 0..harness.cluster_size() {
        let client = harness
            .node(i)
            .test_client()
            .unwrap()
            .with_root_login()
            .connect()
            .await
            .unwrap();
        client.ping().await.unwrap();
    }
}
