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

pub mod args;
pub mod binary;
pub mod bytes_serializable;
#[cfg(feature = "iggy-cli")]
pub mod cli;
pub mod cli_command;
#[allow(deprecated)]
pub mod client;
pub mod client_error;
#[allow(deprecated)]
pub mod client_provider;
#[allow(deprecated)]
pub mod clients;
pub mod command;
pub mod compression;
pub mod confirmation;
pub mod consumer;
pub mod consumer_ext;
pub mod consumer_groups;
pub mod consumer_offsets;
pub mod diagnostic;
pub mod error;
pub mod http;
pub mod identifier;
pub mod locking;
pub mod messages;
pub mod models;
pub mod partitioner;
pub mod partitions;
pub mod personal_access_tokens;
pub mod prelude;
pub mod quic;
pub mod segments;
pub mod snapshot;
pub mod stream_builder;
pub mod streams;
pub mod system;
pub mod tcp;
pub mod topics;
pub mod users;
pub mod utils;
pub mod validatable;
