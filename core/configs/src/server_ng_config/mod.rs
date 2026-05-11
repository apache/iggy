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

//! On-disk schema for the `server-ng` binary.
//!
//! Mirrors the legacy server-config section surface verbatim
//! (operator-facing schema is unchanged) and adds a `message_bus` section
//! for inter-shard / inter-replica bus tunables previously hardcoded in
//! the `core/message_bus` runtime crate.
//!
//! Scaffolding-only at the time of introduction: the type is defined and
//! loadable but no binary calls [`server_ng::ServerNgConfig::load`]; the
//! wiring PR for `core/server-ng`'s bootstrap and the message_bus crate's
//! runtime type is a separate change.

pub mod defaults;
pub mod displays;
pub mod message_bus;
pub mod quic;
pub mod server_ng;
pub mod tcp;
pub mod validators;
pub mod websocket;

/// Component tag used in error messages for the server-ng config surface.
/// Mirrors [`crate::COMPONENT`] (`"CONFIG"`).
pub const COMPONENT_NG: &str = "CONFIG_NG";
