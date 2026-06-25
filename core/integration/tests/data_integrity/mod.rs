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

// Restart-based verifiers stay `not(vsr)`: they restart cluster nodes and
// assert on-disk state afterward, which VSR can't satisfy until replica state
// transfer lands (a restarted replica has no way to catch up).
#[cfg(not(feature = "vsr"))]
mod verify_after_server_restart;
#[cfg(not(feature = "vsr"))]
mod verify_no_plaintext_credentials_on_disk;
#[cfg(not(feature = "vsr"))]
mod verify_user_login_after_restart;

// The cooperative-rebalance matrix runs under vsr too: it exercises server-ng's
// consumer-group rebalancing (a VSR feature) and never restarts the cluster, so
// the no-state-transfer limitation above does not apply. Green at 95/95.
mod verify_consumer_group_partition_assignment;

// Cross-replica on-disk data identity is VSR-only.
#[cfg(feature = "vsr")]
mod verify_cluster_replica_data_identical;
