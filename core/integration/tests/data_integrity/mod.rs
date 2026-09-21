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

// Partially vsr-gated inside the module: the remaining gates cover
// `flush_unsaved_buffer`, which the server answers `FeatureUnavailable` and
// which the eager-flush server envs replace under vsr. The bench-fill test
// itself runs under vsr since PARTITION-plane state transfer landed, but the
// harness spawns `iggy-bench` off disk with no cargo build-graph edge, so the
// binary must be freshly built or a stale build hangs on login.
mod verify_after_server_restart;
mod verify_user_login_after_restart;

// Not restart-based: it creates a user + PAT, stops the server, and greps the
// data dir for plaintext. No replica catch-up needed, and the server hashes the
// password / PAT before either reaches the WAL, so it runs under vsr too.
mod verify_no_plaintext_credentials_on_disk;

// The cooperative-rebalance matrix exercises the server's consumer-group
// rebalancing (a VSR capability). Green at 95/95.
mod verify_consumer_group_partition_assignment;

// Cross-replica on-disk data identity is VSR-only.
mod verify_cluster_replica_data_identical;

// Auto-commit offset replication is inherently a multi-node (VSR) property: the
// backup only holds the offset if the poll's auto-commit rode consensus.
mod verify_auto_commit_offset_replicates;

// On-disk format compatibility across a binary swap. `#[ignore]`d: it needs a
// baseline `iggy-server` built from the merge base, which only
// `scripts/ci/storage-compat.sh` provides.
mod storage_compat;
