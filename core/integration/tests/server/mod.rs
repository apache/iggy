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

// a2a_jwt is HTTP-only (JWT against the HTTP transport); vsr has no HTTP.
#[cfg(not(feature = "vsr"))]
mod a2a_jwt;
// Polling-based consumer-group scenarios are not implemented under vsr yet.
#[cfg(not(feature = "vsr"))]
mod cg;
// The ported round-robin membership join scenario runs against server-ng.
#[cfg(feature = "vsr")]
mod cg_vsr;
// Raw-HTTP data-plane contract against server-ng's shard-0 listener.
#[cfg(feature = "vsr")]
mod http_vsr;
// 80-case race matrix with hardcoded HTTP variants (test_matrix bypasses
// the harness transport filter); revisit under vsr once basics are green.
#[cfg(not(feature = "vsr"))]
mod concurrent_addition;
mod general;
// The per-shard segment cleaner deletes expired / oversize segments from disk
// under both the legacy server and server-ng.
mod message_cleanup;
mod message_retrieval;
// Server restarts, consumer-group barriers, and DeleteSegments maintenance.
// `should_delete_segments_without_consumers` is framing-agnostic + async-aware
// and runs under server-ng; the consumer-group / restart variants stay
// legacy-shaped (cg polling has its own vsr gaps) and are filtered out there.
mod purge_delete;
mod scenarios;
mod specific;
