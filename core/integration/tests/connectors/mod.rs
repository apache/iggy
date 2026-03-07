/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

mod api;
mod elasticsearch;
mod fixtures;
mod http_config_provider;
mod iceberg;
mod postgres;
mod quickwit;
mod random;
mod stdout;

use iggy_common::IggyTimestamp;
use serde::{Deserialize, Serialize};

const ONE_DAY_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestMessage {
    pub id: u64,
    pub name: String,
    pub count: u32,
    pub amount: f64,
    pub active: bool,
    pub timestamp: i64,
}

pub fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = IggyTimestamp::now().as_micros();
    (1..=count)
        .map(|i| TestMessage {
            id: i as u64,
            name: format!("user_{}", i - 1),
            count: ((i - 1) * 10) as u32,
            amount: (i - 1) as f64 * 100.0,
            active: (i - 1) % 2 == 0,
            timestamp: (base_timestamp + (i - 1) as u64 * ONE_DAY_MICROS) as i64,
        })
        .collect()
}
