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

#[cfg(not(feature = "disable-mimalloc"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "disable-mimalloc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(windows)]
compile_error!("iggy-server doesn't support windows.");

pub mod args;
pub mod binary;
pub mod bootstrap;
pub(crate) mod compat;
pub mod configs;
pub mod http;
pub mod io;
pub mod log;
pub mod quic;
pub mod server_error;
pub mod shard;
pub mod slab;
pub mod state;
pub mod streaming;
pub mod tcp;
pub mod versioning;
pub mod websocket;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
pub const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";
