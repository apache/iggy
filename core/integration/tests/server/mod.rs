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
// the server's ported trusted-issuer path verify them.
mod a2a_jwt;
mod cg;
// Flush (FLUSH_UNSAVED_BUFFER) has no the server primitive; it must deny typed.
mod flush_vsr;
// Legacy login codes (LOGIN_USER / LOGIN_WITH_PAT) have no the server handler;
// they must evict typed (MalformedLogin), not stall or reply empty-ok.
mod legacy_login_vsr;
// A failed credential login must report the credential failure, not the
// payload shape it fell through to.
mod login_credentials_vsr;
// Poll addressing + timestamp semantics: typed PartitionNotFound on a bad
// partition id, at-or-after timestamp polls.
mod poll_semantics_vsr;
// Create-topic static bounds deny typed before consensus.
mod topic_admission_vsr;
// Stats aggregates the cross-shard connected-client count, not a hardcoded 0.
mod stats_vsr;
// Purge durability: applied generation survives restart; journal-resident
// purged batches stay fenced behind the purge floor.
mod purge_vsr;
// Shared HTTP transport plumbing (session + verb helpers) for the raw-HTTP
// server suites below.
pub(crate) mod http_client;
// Raw-HTTP data-plane contract against the server's shard-0 listener.
mod http_vsr;
// Raw-HTTP wire-contract residue against the server (status codes + typed error
// bodies); the RBAC matrix lives in permissions_scenario.
mod http_rbac;
// End-to-end HTTPS: the server serves the REST listener over TLS and negotiates
// HTTP/2 via ALPN.
mod http_tls;
// The iggy-view response header: on authenticated success and redirect
// responses only, never on errors or /ping, relayed from the primary.
mod http_view_header;
// Binary GetClusterMetadata must serve the real roster from a VSR cluster.
mod cluster_metadata_vsr;
// A declared node.advertised_address outranks the bind address a
// cluster-disabled server would otherwise publish.
mod cluster_metadata_advertised;
// Listeners bound to :0 must publish the OS-chosen ports through the runtime
// config dump and both cluster-metadata spines.
mod port_discovery;
// A metadata view change must persist the advanced view and recover it from disk
// across a replica restart.
mod cluster_view_durability_vsr;
// A partition view change must persist the advanced view in that group's own
// superblock and recover it from disk across a replica restart.
mod partition_view_durability_vsr;
// 80-case race matrix with hardcoded HTTP variants (test_matrix bypasses
// the harness transport filter).
mod concurrent_addition;
mod general;
// The per-shard segment cleaner deletes expired / oversize segments from disk.
mod message_cleanup;
mod message_retrieval;
// Server restarts, consumer-group barriers, and DeleteSegments maintenance.
// The full restart matrix (consumer variants included) runs under the server:
// a restarted replica rejoins via the view probe + journal repair.
mod purge_delete;
mod scenarios;
mod specific;
