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
#[cfg(all(feature = "mimalloc", not(feature = "disable-mimalloc")))]
use mimalloc::MiMalloc;

// Both features are checked because `--all-features` turns on `mimalloc` and
// `disable-mimalloc` at once, while `--no-default-features` drops the crate.
#[cfg(all(feature = "mimalloc", not(feature = "disable-mimalloc")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SEMANTIC_VERSION: SemanticVersion = SemanticVersion::parse_const(VERSION);

// Visibility rule: `pub` = named external consumer. main.rs consumes `boot`
// (including `boot::systemd`) and `server_error`; the simulator consumes
// `shell`, `boot::wire_shell_handlers`, and (through `ShellHandlers.sessions`)
// `session_manager`. Everything else is crate-internal.

// boot: process entry, shard threads, recovery orchestration.
pub mod boot;
pub(crate) mod config_writer;
pub(crate) mod shard_allocator;

// spine: the request path - shell vocabulary, dispatch funnel, per-domain ops.
pub(crate) mod consumer_group;
pub(crate) mod dispatch;
pub mod external_auth;
pub(crate) mod pat;
pub(crate) mod responses;
pub mod session_manager;
pub mod shell;
pub(crate) mod users;
pub(crate) mod wire;

// http: the REST spine (role-leaf tree).
pub(crate) mod http;

// background: per-shard maintenance loops.
pub(crate) mod partition_reconciler;
pub(crate) mod personal_access_token_cleaner;
pub(crate) mod segment_cleaner;
pub(crate) mod snapshot;

// support: shared plumbing and the crash-recovery readers
// (the readers move beside their writers in core/partitions later).
pub(crate) mod cluster_meta;
pub(crate) mod offset_recovery;
pub(crate) mod partition_helpers;
pub(crate) mod segment_recovery;
pub mod server_error;
