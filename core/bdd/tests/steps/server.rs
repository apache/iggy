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

use crate::common::global_context::GlobalContext;
use cucumber::given;
use integration::test_server::TestServer;

#[given("I have a running Iggy server")]
pub async fn given_running_server(world: &mut GlobalContext) {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    world.server_addr = Some(server_addr);
    world.server = Some(test_server);
}
