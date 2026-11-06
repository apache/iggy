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

//! The cross-SDK conformance runner of Apache Iggy.
//!
//! One orchestrator loads the YAML corpus, judges every result and owns the
//! fault proxy. Each SDK supplies a thin agent that turns JSON commands into
//! SDK calls. See `conformance/README.md` for the format and the protocol.

pub mod agent;
pub mod agent_host;
pub mod api;
pub mod canonical;
pub mod format;
pub mod iggy_errors;
pub mod matcher;
pub mod protocol;
pub mod report;
pub mod runner;
pub mod skips;
