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

#![allow(clippy::future_not_send)]

use iggy_common::SemanticVersion;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SEMANTIC_VERSION: SemanticVersion = SemanticVersion::parse_const(VERSION);

pub mod auth;
pub mod bootstrap;
pub(crate) mod cluster_meta;
pub mod config_writer;
pub mod consumer_group;
pub mod dispatch;
pub(crate) mod http;
pub mod login_register;
pub(crate) mod offset_recovery;
pub mod partition_helpers;
pub mod partition_reconciler;
pub mod pat;
pub(crate) mod personal_access_token_cleaner;
pub mod responses;
pub(crate) mod segment_cleaner;
pub(crate) mod segment_recovery;
pub mod server_error;
pub mod session_manager;
pub(crate) mod snapshot;
pub mod users;
pub mod wire;
