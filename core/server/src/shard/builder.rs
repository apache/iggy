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

use crate::{configs::server::ServerConfig, shard::Shard};

use super::{connector::ShardConnector, frame::ShardFrame, IggyShard};

#[derive(Default)]
pub struct IggyShardBuilder {
    id: Option<u16>,
    connections: Option<Vec<ShardConnector<ShardFrame>>>,
    config: Option<ServerConfig>,
}

impl IggyShardBuilder {
    pub fn id(mut self, id: u16) -> Self {
        self.id = Some(id);
        self
    }

    pub fn connections(mut self, connections: Vec<ShardConnector<ShardFrame>>) -> Self {
        self.connections = Some(connections);
        self
    }

    pub fn server_config(mut self, config: ServerConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build_and_init(self) -> IggyShard {
        let id = self.id.unwrap();
        let config = self.config.unwrap();
        let connections = self.connections.unwrap();
        let (stop_sender, stop_receiver, receiver) = connections
            .iter()
            .filter(|c| c.id == id)
            .map(|c| {
                (
                    c.stop_sender.clone(),
                    c.stop_receiver.clone(),
                    c.receiver.clone(),
                )
            })
            .next()
            .expect("Failed to find connection with the specified ID");
        let shards = connections.into_iter().map(Shard::new).collect();

        IggyShard::new(
            id,
            shards,
            config,
            stop_receiver,
            stop_sender,
            receiver,
        )
    }
}
