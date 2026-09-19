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

use cucumber::World;
use iggy::clients::client::IggyClient;
use iggy::prelude::{IggyError, PolledMessages};
use std::collections::HashMap;

#[derive(Debug, World, Default)]
pub struct PurgeContext {
    pub client: Option<IggyClient>,
    pub server_addr: Option<String>,
    pub stream_ids: HashMap<String, u32>,
    pub topic_ids: HashMap<String, u32>,
    pub last_polled_messages: Option<PolledMessages>,
    pub last_error: Option<IggyError>,
}
