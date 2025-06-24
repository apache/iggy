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
use crate::tcp::tcp_listener;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::info;

/// Starts the TCP server.
/// Returns the address the server is listening on.
pub async fn start(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let server_name = if shard.config.tcp.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    info!("Initializing {server_name} server...");
    let addr = match shard.config.tcp.tls.enabled {
        true => unimplemented!("TLS support is not implemented yet"),
        false => tcp_listener::start(server_name, shard).await,
    };
    info!("{server_name} server has started on: {:?}", addr);
    Ok(())
}
