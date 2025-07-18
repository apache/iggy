use std::rc::Rc;

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
use super::server_command::BackgroundServerCommand;
use crate::{configs::server::ServerConfig, shard::IggyShard};

pub struct BackgroundServerCommandHandler<'a> {
    shard: Rc<IggyShard>,
    config: &'a ServerConfig,
}

impl<'a> BackgroundServerCommandHandler<'a> {
    pub fn new(shard: Rc<IggyShard>, config: &'a ServerConfig) -> Self {
        Self { shard, config }
    }

    pub fn install_handler<C, E>(&mut self, mut executor: E) -> Self
    where
        E: BackgroundServerCommand<C> + Send + Sync + 'static,
    {
        let (sender, receiver) = flume::unbounded();
        let shard = self.shard.clone();
        executor.start_command_sender(shard.clone(), self.config, sender);
        executor.start_command_consumer(shard.clone(), self.config, receiver);
        Self {
            shard,
            config: self.config,
        }
    }
}
