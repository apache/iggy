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

//TODO: Fixme
/*
use crate::configs::server::ServerConfig;
use crate::streaming::systems::system::SharedSystem;
use flume::{Receiver, Sender};
use std::future::Future;

pub trait BackgroundServerCommand<C> {
    fn execute(&mut self, system: &SharedSystem, command: C) -> impl Future<Output = ()>;

    fn start_command_sender(
        &mut self,
        system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<C>,
    );

    fn start_command_consumer(
        self,
        system: SharedSystem,
        config: &ServerConfig,
        receiver: Receiver<C>,
    );
}

*/
