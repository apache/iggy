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

use std::time::Duration;

use iggy::prelude::IggyError;

pub mod consumer;
pub mod producer;
pub mod producing_consumer;

#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub messages: u32,
    pub user_data_bytes: u64,
    pub total_bytes: u64,
    pub latency: Duration,
}

pub trait BenchmarkInit: Send + Sync {
    async fn setup(&mut self) -> Result<(), IggyError>;
}

pub trait ApiLabel {
    const API_LABEL: &'static str;
}
