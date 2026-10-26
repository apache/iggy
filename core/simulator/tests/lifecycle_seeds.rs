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

//! Fixed seeds of `workload-fuzz` on the lifecycle and consumer planes through
//! the shell.
//!
//! Every run drives a consumer group through its whole life: the partition
//! refuses a replayed owner token and retires the owner of each deleted group.
//! The fuzzer exits 3 when a run stored no offset as the owner, saw no refusal,
//! committed no retirement or compared none, so only exit 0 passes. A consumer
//! run also polls as consumers and as the group, and exits 3 when no served poll
//! was compared with a committed prefix.

use std::path::PathBuf;
use std::process::Command;

/// The flags of the lifecycle `--shell` review runs, with crashes, restarts and
/// swarm faults before the quiesce phase.
const LIFECYCLE_ARGS: &[&str] = &[
    "--plane",
    "lifecycle",
    "--shell",
    "--clients",
    "3",
    "--crash-prob",
    "0.003",
    "--restart-prob",
    "0.02",
    "--ticks",
    "40000",
    "--heal-before-quiesce",
    "--faults",
    "swarm",
    "--crash-primary",
];

/// [`LIFECYCLE_ARGS`] on the consumer plane, which polls and joins and leaves
/// the fixture group but replaces no fixture topic.
const CONSUMER_ARGS: &[&str] = &[
    "--plane",
    "consumer",
    "--shell",
    "--clients",
    "3",
    "--crash-prob",
    "0.003",
    "--restart-prob",
    "0.02",
    "--ticks",
    "40000",
    "--heal-before-quiesce",
    "--faults",
    "swarm",
    "--crash-primary",
];

macro_rules! fuzz_seeds {
    ($args:expr, $($name:ident => $seed:literal,)*) => {
        $(
            #[test]
            fn $name() {
                run_seed($seed, $args);
            }
        )*
    };
}

fuzz_seeds! {
    LIFECYCLE_ARGS,
    given_seed_1_when_lifecycle_runs_should_pass_every_floor => 1,
    given_seed_2_when_lifecycle_runs_should_pass_every_floor => 2,
    given_seed_3_when_lifecycle_runs_should_pass_every_floor => 3,
    given_seed_4_when_lifecycle_runs_should_pass_every_floor => 4,
    given_seed_5_when_lifecycle_runs_should_pass_every_floor => 5,
    given_seed_6_when_lifecycle_runs_should_pass_every_floor => 6,
    given_seed_7_when_lifecycle_runs_should_pass_every_floor => 7,
    given_seed_8_when_lifecycle_runs_should_pass_every_floor => 8,
    given_seed_18_when_lifecycle_runs_should_pass_every_floor => 18,
    given_seed_21_when_lifecycle_runs_should_pass_every_floor => 21,
    given_seed_23_when_lifecycle_runs_should_pass_every_floor => 23,
}

fuzz_seeds! {
    CONSUMER_ARGS,
    given_seed_1_when_consumers_poll_should_pass_every_floor => 1,
    given_seed_3_when_consumers_poll_should_pass_every_floor => 3,
    given_seed_4_when_consumers_poll_should_pass_every_floor => 4,
    given_seed_7_when_consumers_poll_should_pass_every_floor => 7,
    // Polls below the prefix that its partition flushed out of the journal.
    given_seed_17_when_consumers_poll_should_pass_every_floor => 17,
}

fn run_seed(seed: u64, args: &[&str]) {
    // A reused nextest build moves the binary from where it was compiled.
    let fuzzer = std::env::var_os("NEXTEST_BIN_EXE_workload-fuzz").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_workload-fuzz")),
        PathBuf::from,
    );
    let output = Command::new(&fuzzer)
        .arg("--seed")
        .arg(seed.to_string())
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("cannot start {}: {error}", fuzzer.display()));
    assert!(
        output.status.success(),
        "workload-fuzz --seed {seed} exited with {} (3: vacuous run, 101: failed check)\n\
         stdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
