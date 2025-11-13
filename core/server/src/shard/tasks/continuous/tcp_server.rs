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

use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::tcp::tcp_server;
use iggy_common::IggyError;
use std::rc::Rc;

pub fn spawn_tcp_server(shard: Rc<IggyShard>) {
    let shard_clone = shard.clone();
    shard
        .task_registry
        .continuous("tcp_server")
        .critical(true)
        .run(move |shutdown| tcp_server_task(shard_clone, shutdown))
        .spawn();
}

async fn tcp_server_task(shard: Rc<IggyShard>, shutdown: ShutdownToken) -> Result<(), IggyError> {
    tcp_server::spawn_tcp_server(shard, shutdown).await
}
