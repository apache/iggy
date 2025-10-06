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

use crate::state::command::EntryCommand;
use crate::state::entry::StateEntry;
use iggy_common::IggyError;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;
use std::future::Future;

pub mod command;
pub mod entry;
pub mod file;
pub mod models;
pub mod system;

pub const COMPONENT: &str = "STATE";

// TODO(hubcio): I don't like this approach, we should avoid mocking and simplify it all.

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum StateKind {
    File(file::FileState),
    #[cfg(test)]
    Mock(MockState),
}

#[cfg_attr(test, automock)]
pub trait State {
    fn init(&self) -> impl Future<Output = Result<Vec<StateEntry>, IggyError>>;
    fn load_entries(&self) -> impl Future<Output = Result<Vec<StateEntry>, IggyError>>;
    fn apply(
        &self,
        user_id: u32,
        command: &EntryCommand,
    ) -> impl Future<Output = Result<(), IggyError>>;
}

impl StateKind {
    pub async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.init().await,
            #[cfg(test)]
            Self::Mock(s) => s.init().await,
        }
    }

    pub async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.load_entries().await,
            #[cfg(test)]
            Self::Mock(s) => s.load_entries().await,
        }
    }

    pub async fn apply(&self, user_id: u32, command: &EntryCommand) -> Result<(), IggyError> {
        match self {
            Self::File(s) => s.apply(user_id, command).await,
            #[cfg(test)]
            Self::Mock(s) => s.apply(user_id, command).await,
        }
    }
}
