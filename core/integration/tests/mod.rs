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

mod bench;
// Drives the `iggy` CLI binary against a running server. Single-node and the
// default 3-node cluster both pass.
mod cli;
// Raw-wire spec tests for VSR session continuity across a node restart
// (IGGY-137).
mod cluster;
mod config_env_listing;
mod config_provider;
mod connectors_harness;
mod data_integrity;
mod mcp;
mod object_store;
mod sdk;
mod server;

integration::install_log_capture!();
