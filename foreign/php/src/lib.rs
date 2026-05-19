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

pub mod client;
pub mod consumer;
pub mod error;
pub mod identifier;
pub mod receive_message;
pub mod runtime;
pub mod send_message;
pub mod stream;
pub mod topic;

use ext_php_rs::prelude::*;

use crate::client::IggyClient;
use crate::consumer::{AutoCommit, AutoCommitAfter, AutoCommitWhen, IggyConsumer};
use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .class::<IggyClient>()
        .class::<IggyConsumer>()
        .class::<AutoCommit>()
        .class::<AutoCommitWhen>()
        .class::<AutoCommitAfter>()
        .class::<PollingStrategy>()
        .class::<ReceiveMessage>()
        .class::<SendMessage>()
        .class::<StreamDetails>()
        .class::<TopicDetails>()
}
