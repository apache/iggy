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
mod consumer;
mod identifier;
mod receive_message;
mod send_message;
mod stream;
mod topic;

use client::IggyClient;
use consumer::{AutoCommit, AutoCommitAfter, AutoCommitWhen, IggyConsumer};
use pyo3::prelude::*;
use receive_message::{PollingStrategy, ReceiveMessage};
use send_message::SendMessage;
use stream::StreamDetails;
use topic::TopicDetails;

/// A Python module implemented in Rust.
#[pymodule]
fn apache_iggy(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SendMessage>()?;
    m.add_class::<ReceiveMessage>()?;
    m.add_class::<IggyClient>()?;
    m.add_class::<StreamDetails>()?;
    m.add_class::<TopicDetails>()?;
    m.add_class::<PollingStrategy>()?;
    m.add_class::<IggyConsumer>()?;
    m.add_class::<AutoCommit>()?;
    m.add_class::<AutoCommitAfter>()?;
    m.add_class::<AutoCommitWhen>()?;
    Ok(())
}
