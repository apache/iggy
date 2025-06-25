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
use crate::state::system::{PartitionState, StreamState, TopicState};
use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::storage::FilePartitionStorage;
use crate::streaming::streams::storage::FileStreamStorage;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::storage::FileTopicStorage;
use crate::streaming::topics::topic::Topic;
use iggy_common::ConsumerKind;
use iggy_common::IggyError;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;
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

#[derive(Debug)]
pub enum StreamStorageKind {
    File(FileStreamStorage),
    #[cfg(test)]
    Mock(MockStreamStorage),
}

#[derive(Debug)]
pub enum TopicStorageKind {
    File(FileTopicStorage),
    #[cfg(test)]
    Mock(MockTopicStorage),
}

#[derive(Debug)]
pub enum PartitionStorageKind {
    File(FilePartitionStorage),
    #[cfg(test)]
    Mock(MockPartitionStorage),
}

#[cfg_attr(test, automock)]
pub trait SystemInfoStorage {
    fn load(&self) -> impl Future<Output = Result<SystemInfo, IggyError>>;
    fn save(&self, system_info: &SystemInfo) -> impl Future<Output = Result<(), IggyError>>; 
}

#[cfg_attr(test, automock)]
pub trait StreamStorage {
    fn load(
        &self,
        stream: &mut Stream,
        state: StreamState,
    ) -> impl Future<Output = Result<(), IggyError>>; 
    fn save(&self, stream: &Stream) -> impl Future<Output = Result<(), IggyError>>; 
    fn delete(&self, stream: &Stream) -> impl Future<Output = Result<(), IggyError>>;
}

#[cfg_attr(test, automock)]
pub trait TopicStorage {
    fn load(
        &self,
        topic: &mut Topic,
        state: TopicState,
    ) -> impl Future<Output = Result<(), IggyError>>; 
    fn save(&self, topic: &Topic) -> impl Future<Output = Result<(), IggyError>>; 
    fn delete(&self, topic: &Topic) -> impl Future<Output = Result<(), IggyError>>; 
}

#[cfg_attr(test, automock)]
pub trait PartitionStorage {
    fn load(
        &self,
        partition: &mut Partition,
        state: PartitionState,
    ) -> impl Future<Output = Result<(), IggyError>>;
    fn save(&self, partition: &mut Partition)
    -> impl Future<Output = Result<(), IggyError>>;
    fn delete(&self, partition: &Partition) -> impl Future<Output = Result<(), IggyError>>; 
    fn save_consumer_offset(
        &self,
        offset: u64,
        path: &str,
    ) -> impl Future<Output = Result<(), IggyError>>;
    fn load_consumer_offsets(
        &self,
        kind: ConsumerKind,
        path: &str,
    ) -> impl Future<Output = Result<Vec<ConsumerOffset>, IggyError>>; 
    fn delete_consumer_offsets(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(), IggyError>>; 
    fn delete_consumer_offset(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(), IggyError>>;
}

#[derive(Debug)]
pub struct SystemStorage {
    pub info: Rc<SystemInfoStorageKind>,
    pub stream: Arc<StreamStorageKind>,
    pub topic: Arc<TopicStorageKind>,
    pub partition: Arc<PartitionStorageKind>,
    pub persister: Arc<PersisterKind>,
}

impl SystemStorage {
    pub fn new(config: Rc<SystemConfig>, persister: Arc<PersisterKind>) -> Self {
        Self {
            info: Rc::new(SystemInfoStorageKind::File(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            ))),
            stream: Arc::new(StreamStorageKind::File(FileStreamStorage)),
            topic: Arc::new(TopicStorageKind::File(FileTopicStorage)),
            partition: Arc::new(PartitionStorageKind::File(FilePartitionStorage::new(
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

impl StreamStorageKind {
    forward_async_methods! {
        async fn load(&self, stream: &mut Stream, state: StreamState) -> Result<(), IggyError>;
        async fn save(&self, stream: &Stream) -> Result<(), IggyError>;
        async fn delete(&self, stream: &Stream) -> Result<(), IggyError>;
    }
}

impl TopicStorageKind {
    forward_async_methods! {
        async fn load(&self, topic: &mut Topic, state: TopicState) -> Result<(), IggyError>;
        async fn save(&self, topic: &Topic) -> Result<(), IggyError>;
        async fn delete(&self, topic: &Topic) -> Result<(), IggyError>;
    }
}

impl PartitionStorageKind {
    forward_async_methods! {
        async fn load(&self, partition: &mut Partition, state: PartitionState)
            -> Result<(), IggyError>;
        async fn save(&self, partition: &mut Partition) -> Result<(), IggyError>;
        async fn delete(&self, partition: &Partition) -> Result<(), IggyError>;
        async fn save_consumer_offset(&self, offset: u64, path: &str) -> Result<(), IggyError>;
        async fn load_consumer_offsets(
            &self,
            kind: ConsumerKind,
            path: &str
        ) -> Result<Vec<ConsumerOffset>, IggyError>;
        async fn delete_consumer_offsets(&self, path: &str) -> Result<(), IggyError>;
        async fn delete_consumer_offset(&self, path: &str) -> Result<(), IggyError>;
    }
}
