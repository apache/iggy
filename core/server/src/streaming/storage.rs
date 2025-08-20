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

use super::persistence::persister::PersisterKind;
use crate::configs::system::SystemConfig;
use crate::shard::system::info::SystemInfo;
use crate::shard::system::storage::FileSystemInfoStorage;
use iggy_common::IggyError;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

macro_rules! forward_async_methods {
    (
        $(
            async fn $method_name:ident(
                &self $(, $arg:ident : $arg_ty:ty )*
            ) -> $ret:ty ;
        )*
    ) => {
        $(
            pub async fn $method_name(&self, $( $arg: $arg_ty ),* ) -> $ret {
                match self {
                    Self::File(d) => d.$method_name($( $arg ),*).await,
                    #[cfg(test)]
                    Self::Mock(s) => s.$method_name($( $arg ),*).await,
                }
            }
        )*
    }
}

#[derive(Debug)]
pub enum SystemInfoStorageKind {
    File(FileSystemInfoStorage),
    #[cfg(test)]
    Mock(MockSystemInfoStorage),
}

#[cfg_attr(test, automock)]
pub trait SystemInfoStorage {
    fn load(&self) -> impl Future<Output = Result<SystemInfo, IggyError>>;
    fn save(&self, system_info: &SystemInfo) -> impl Future<Output = Result<(), IggyError>>;
}

#[derive(Debug, Clone)]
pub struct SystemStorage {
    pub info: Arc<SystemInfoStorageKind>,
    pub persister: Arc<PersisterKind>,
}

impl SystemStorage {
    pub fn new(config: Arc<SystemConfig>, persister: Arc<PersisterKind>) -> Self {
        Self {
            info: Arc::new(SystemInfoStorageKind::File(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            ))),
            persister,
        }
    }
}

impl SystemInfoStorageKind {
    forward_async_methods! {
        async fn load(&self) -> Result<SystemInfo, IggyError>;
        async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError>;
    }
}
