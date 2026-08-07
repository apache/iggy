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

//! server-ng purge durability: the applied purge generation survives a
//! restart (`purge.gen`), and purged journal-resident batches stay fenced
//! behind the purge floor instead of resurfacing through the shutdown flush.

use crate::server::scenarios::purge_delete_scenario;
use integration::iggy_harness;

// Single node: the tests reason about ONE replica's on-disk state across a
// restart (`purge.gen` hydration, shutdown-flush fencing); a cluster would
// route polls to whichever node leads after the restart. Default fsync
// config on purpose: the restart is graceful, so segment bytes survive
// without fsync, and `purge.gen` is unconditionally synced by the purge.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.messages_required_to_save = "1")
)]
async fn given_post_purge_messages_when_server_restarts_should_retain_them(
    harness: &mut TestHarness,
) {
    purge_delete_scenario::run_purge_survives_restart(harness).await;
}

// The huge threshold keeps every batch journal-resident: nothing is ever
// flushed before the purge, so the purged bytes exist ONLY as consensus
// history the shutdown flush re-walks.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.messages_required_to_save = "10000")
)]
async fn given_journal_resident_messages_when_purged_should_not_resurface(
    harness: &mut TestHarness,
) {
    purge_delete_scenario::run_resident_purge_no_resurface(harness).await;
}
