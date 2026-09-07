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

pub mod client;
mod config;
mod consumer;
mod duration;
mod identifier;
mod options;
mod permissions;
mod receive_message;
mod send_message;
mod stats;
mod stream;
mod topic;
mod user;
mod user_headers;

use client::IggyClient;
use config::{AutoLogin, TcpConfig, TcpReconnectionConfig};
use consumer::{
    AutoCommit, AutoCommitAfter, AutoCommitWhen, Consumer, ConsumerGroup, ConsumerGroupDetails,
    ConsumerGroupMember, IggyConsumer, ReceiveMessageIterator,
};
use options::OptionSpec;
use permissions::{GlobalPermissions, Permissions, StreamPermissions, TopicPermissions};
use pyo3::prelude::*;
use receive_message::{PollingStrategy, ReceiveMessage};
use send_message::{SendMessage, SendMessagesConfirmation, SendMessagesResponse};
use stats::{CacheMetrics, CacheMetricsKey, Stats};
use stream::StreamDetails;
use topic::{IggyExpiry, MaxTopicSize, Partition, Topic, TopicDetails};
use user::{UserInfo, UserInfoDetails, UserStatus};
use user_headers::{HeaderKey, HeaderValue, UserHeaders};

/// Python client for Apache Iggy, the persistent message streaming platform.
#[pymodule]
fn apache_iggy(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SendMessage>()?;
    m.add_class::<SendMessagesResponse>()?;
    m.add_class::<SendMessagesConfirmation>()?;
    m.add_class::<ReceiveMessage>()?;
    m.add_class::<IggyClient>()?;
    m.add_class::<AutoLogin>()?;
    m.add_class::<TcpConfig>()?;
    m.add_class::<TcpReconnectionConfig>()?;
    m.add_class::<StreamDetails>()?;
    m.add_class::<Stats>()?;
    m.add_class::<CacheMetrics>()?;
    m.add_class::<CacheMetricsKey>()?;
    m.add_class::<Topic>()?;
    m.add_class::<TopicDetails>()?;
    m.add_class::<IggyExpiry>()?;
    m.add_class::<MaxTopicSize>()?;
    m.add_class::<OptionSpec>()?;
    m.add_class::<Partition>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<ConsumerGroup>()?;
    m.add_class::<ConsumerGroupDetails>()?;
    m.add_class::<ConsumerGroupMember>()?;
    m.add_class::<PollingStrategy>()?;
    m.add_class::<IggyConsumer>()?;
    m.add_class::<AutoCommit>()?;
    m.add_class::<AutoCommitAfter>()?;
    m.add_class::<AutoCommitWhen>()?;
    m.add_class::<ReceiveMessageIterator>()?;
    m.add_class::<UserStatus>()?;
    m.add_class::<UserInfo>()?;
    m.add_class::<UserInfoDetails>()?;
    m.add_class::<UserHeaders>()?;
    m.add_class::<HeaderKey>()?;
    m.add_class::<HeaderValue>()?;
    m.add_class::<Permissions>()?;
    m.add_class::<GlobalPermissions>()?;
    m.add_class::<StreamPermissions>()?;
    m.add_class::<TopicPermissions>()?;
    Ok(())
}
