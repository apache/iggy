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

use tokio::signal;
use tokio::sync::broadcast;

use iggy_gateway_kafka::server::init_tracing;
use iggy_gateway_kafka::{KafkaServer, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = ServerConfig::default();
    let server = KafkaServer::new(config);

    let (tx, rx) = broadcast::channel(1);
    let mut server_task = tokio::spawn(async move { server.run(rx).await });

    tokio::select! {
        result = &mut server_task => {
            return Ok(result??);
        }
        _ = signal::ctrl_c() => {
            let _ = tx.send(());
        }
    }

    server_task.await??;
    Ok(())
}
