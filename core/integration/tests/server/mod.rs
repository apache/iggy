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

// a2a_jwt exercises trusted-issuer (JWKS) tokens; both the legacy verifier and
// server-ng's ported trusted-issuer path verify them.
mod a2a_jwt;
// Polling-based consumer-group scenarios are not implemented under vsr yet.
#[cfg(not(feature = "vsr"))]
mod cg;
// The ported round-robin membership join scenario runs against server-ng.
#[cfg(feature = "vsr")]
mod cg_vsr;
// Flush (FLUSH_UNSAVED_BUFFER) has no server-ng primitive; it must deny typed.
#[cfg(feature = "vsr")]
mod flush_vsr;
// Legacy login codes (LOGIN_USER / LOGIN_WITH_PAT) have no server-ng handler;
// they must evict typed (MalformedLogin), not stall or reply empty-ok.
#[cfg(feature = "vsr")]
mod legacy_login_vsr;
// Shared HTTP transport plumbing (session + verb helpers) for the raw-HTTP
// server-ng suites below.
#[cfg(feature = "vsr")]
mod http_client;
// Raw-HTTP data-plane contract against server-ng's shard-0 listener.
#[cfg(feature = "vsr")]
mod http_vsr;
// Raw-HTTP wire-contract residue against server-ng (status codes + typed error
// bodies); the RBAC matrix lives in permissions_scenario.
#[cfg(feature = "vsr")]
mod http_rbac;
// Binary GetClusterMetadata must serve the real roster from a VSR cluster.
#[cfg(feature = "vsr")]
mod cluster_metadata_vsr;
// 80-case race matrix with hardcoded HTTP variants (test_matrix bypasses
// the harness transport filter).
mod concurrent_addition;
mod general;
// The per-shard segment cleaner deletes expired / oversize segments from disk
// under both the legacy server and server-ng.
mod message_cleanup;
mod message_retrieval;
// Server restarts, consumer-group barriers, and DeleteSegments maintenance.
// `should_delete_segments_without_consumers` is framing-agnostic + async-aware
// and runs fully (both restart variants) under server-ng; the consumer-group
// variants stay legacy-shaped or vsr-gated (cg polling has its own vsr gaps).
mod purge_delete;
mod scenarios;
mod specific;
