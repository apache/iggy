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

//! On-disk config schema for the `iggy-server` binary.
//!
//! Composes the shared section vocabulary from [`crate::common`] with the
//! transport, cluster, metadata and bus sections this server owns.
//! [`server::ServerConfig`] is the root type the bootstrap loads.

pub mod cluster;
pub mod defaults;
pub mod displays;
pub mod external_auth;
pub mod message_bus;
pub mod metadata;
pub mod node;
pub mod partition;
pub mod quic;
pub mod server;
pub mod sharding;
pub mod tcp;
pub mod validators;
pub mod websocket;

pub use crate::common::COMPONENT;
