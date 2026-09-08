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

const EXAMPLES: &str = r"EXAMPLES:

Start iggy-server separately. The benchmark connects to a running server.
Global options precede the kind, kind options precede the transport, and
transport options precede the optional output subcommand.

Default producer and consumer counts are six. Pinned workloads default to six streams.

1) All benchmark kinds and aliases:

    Pinned producer (pp), consumer (pc), and producer/consumer (ppc):

    $ cargo r -r --bin iggy-bench -- pinned-producer --streams 10 --producers 10 tcp
    $ cargo r -r --bin iggy-bench -- pinned-consumer --streams 10 --consumers 10 tcp
    $ cargo r -r --bin iggy-bench -- pinned-producer-and-consumer --streams 10 --producers 10 --consumers 10 tcp

    Balanced producer (bp), consumer group (bcg), and producer/consumer group (bpcg):

    $ cargo r -r --bin iggy-bench -- balanced-producer --partitions 24 --producers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-consumer-group --consumers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-producer-and-consumer-group --partitions 24 --producers 6 --consumers 6 tcp
    $ cargo r -r --bin iggy-bench -- --total-data 10GiB bpcg tcp

    End-to-end producing consumer (e2e) and producing consumer group (e2ecg):

    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --producing-consumers 12 --streams 12 tcp
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer-group --partitions 24 --producers 6 --consumers 6 tcp

2) All transports:

    $ cargo r -r --bin iggy-bench -- pinned-producer tcp --server-address 127.0.0.1:8090
    $ cargo r -r --bin iggy-bench -- pinned-producer quic --server-address 127.0.0.1:8080
    $ cargo r -r --bin iggy-bench -- pinned-producer http --server-address 127.0.0.1:3000
    $ cargo r -r --bin iggy-bench -- pinned-producer websocket --server-address 127.0.0.1:8092

3) Topic durability:

    --durability controls message completion.
    --consumer-offset-durability controls explicit offset-store/delete completion.
    Both independently default to replicated. Neither inherits the other.
    Both policies normally write to disk. Persisted additionally waits for
    recoverable stable storage on the required VSR quorum, or the single replica.

    Both replicated (the default):
    $ cargo r -r --bin iggy-bench -- balanced-producer-and-consumer-group tcp

    Persisted messages, replicated offsets:
    $ cargo r -r --bin iggy-bench -- --durability persisted balanced-producer-and-consumer-group tcp

    Replicated messages, persisted offsets:
    $ cargo r -r --bin iggy-bench -- --consumer-offset-durability persisted balanced-producer-and-consumer-group tcp

    Both persisted:
    $ cargo r -r --bin iggy-bench -- --durability persisted --consumer-offset-durability persisted balanced-producer-and-consumer-group tcp

    These options apply when topics are created. They do not modify topics
    with --reuse-streams. Consumer polling with auto-commit remains asynchronous.
    Its poll latency is not an acknowledged offset-store latency measurement.
    In replicated groups, either persisted policy enables a WAL that also retains
    message predecessors, even when message durability is replicated.

4) Topic flush cadence and retention:

    --messages-required-to-save is a global create-time topic option.
    It controls segment flush cadence, not the acknowledgment guarantee.
    --max-topic-size and --message-expiry are kind-specific topic options.
    These flags do not change existing topics with --reuse-streams.

    Persisted messages without forcing a segment flush for every message:
    $ cargo r -r --bin iggy-bench -- --durability persisted --messages-required-to-save 1024 balanced-producer --partitions 1 --producers 8 tcp

    Replicated acknowledgments with eager segment flushing:
    $ cargo r -r --bin iggy-bench -- --messages-required-to-save 1 balanced-producer tcp

    Retention can delete data during a long run. Use matching policies when comparing:
    $ cargo r -r --bin iggy-bench -- balanced-producer --max-topic-size 10GiB --message-expiry 1h tcp

5) Workload configuration:

    --messages-per-batch (-P): Messages per batch, or a range such as 100..1000.
    --message-batches (-b): Batches per actor, mutually exclusive with --total-data.
    --total-data (-T): Total message bytes across actors, such as 10GiB.
    --message-size (-m): Message bytes, or a range such as 100..1000.
    --rate-limit (-r): Aggregate throughput limit across actors, such as 100MB.
    --warmup-time (-w): Warmup duration, such as 10s.
    --sampling-time (-t): Metrics sampling interval.
    --moving-average-window (-W): Moving-average window size.

    Fixed message and batch sizes:
    $ cargo r -r --bin iggy-bench -- --message-size 1000 --messages-per-batch 100 --message-batches 1000 --rate-limit 100MB balanced-producer --streams 5 --producers 5 tcp

    Random message sizes:
    $ cargo r -r --bin iggy-bench -- --message-size 100..1000 --messages-per-batch 100 --total-data 1GiB balanced-producer --streams 5 --producers 5 tcp

    Random batch sizes:
    $ cargo r -r --bin iggy-bench -- --message-size 1000 --messages-per-batch 10..100 --total-data 500MiB balanced-producer tcp

    Random message and batch sizes with a warmup and aggregate rate limit:
    $ cargo r -r --bin iggy-bench -- --message-size 500..2000 --messages-per-batch 50..200 --total-data 2GiB --warmup-time 10s --rate-limit 50MB balanced-producer tcp

6) Remote server and output:

    $ cargo r -r --bin iggy-bench -- pinned-producer --streams 5 --producers 5 tcp --server-address localhost:8090
    $ cargo r -r --bin iggy-bench -- --username admin --password secret pinned-producer tcp --server-address 192.168.1.100:8090
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp output
    $ cargo r -r --bin iggy-bench -- --durability persisted balanced-producer tcp output --identifier dedicated-host --remark persisted-messages --gitref abc123
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer tcp output --open-charts

    Output options include --output-dir (-o), --identifier, --remark, --gitref,
    --gitref-date, --extra-info, and --open-charts (-c).
    Record both durability policies, CPU allocation, host tuning, and storage.
    See core/bench/README.md and https://iggy.apache.org/docs/server/linux-tuning.

7) Help:

    $ cargo r -r --bin iggy-bench -- --help
    $ cargo r -r --bin iggy-bench -- pinned-producer --help
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp --help
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp output --help
";

pub fn print_examples() {
    println!("{EXAMPLES}");
}

#[cfg(test)]
mod tests {
    use super::EXAMPLES;
    use crate::args::common::IggyBenchArgs;
    use clap::{CommandFactory, Parser, error::ErrorKind};
    use iggy::prelude::Durability;
    use std::collections::BTreeSet;

    #[test]
    fn published_examples_parse_and_cover_every_kind_and_transport() {
        let mut kinds = BTreeSet::new();
        let mut transports = BTreeSet::new();
        for command in EXAMPLES.lines().filter_map(|line| {
            line.trim()
                .strip_prefix("$ cargo r -r --bin iggy-bench -- ")
        }) {
            let arguments = std::iter::once("iggy-bench").chain(command.split_ascii_whitespace());
            match IggyBenchArgs::command().try_get_matches_from(arguments) {
                Ok(matches) => {
                    let (kind, options) = matches.subcommand().unwrap();
                    kinds.insert(kind.to_owned());
                    transports.insert(options.subcommand_name().unwrap().to_owned());
                }
                Err(error) => {
                    assert_eq!(error.kind(), ErrorKind::DisplayHelp, "{command}: {error}");
                }
            }
        }
        let command = IggyBenchArgs::command();
        for kind in command
            .get_subcommands()
            .filter(|kind| kind.get_name() != "examples")
        {
            assert!(
                kinds.contains(kind.get_name()),
                "missing kind {}",
                kind.get_name()
            );
        }
        let producer = command.find_subcommand("pinned-producer").unwrap();
        for transport in producer.get_subcommands() {
            assert!(
                transports.contains(transport.get_name()),
                "missing transport {}",
                transport.get_name()
            );
        }
    }

    #[test]
    fn websocket_and_its_alias_use_the_same_canonical_name() {
        for name in ["websocket", "ws"] {
            let parsed =
                IggyBenchArgs::try_parse_from(["iggy-bench", "pinned-producer", name]).unwrap();
            assert_eq!(parsed.transport_command().as_str(), "websocket");
        }
    }

    #[test]
    fn durability_defaults_are_independent() {
        for (flags, messages, offsets) in [
            (vec![], Durability::Replicated, Durability::Replicated),
            (
                vec!["--durability", "persisted"],
                Durability::Persisted,
                Durability::Replicated,
            ),
            (
                vec!["--consumer-offset-durability", "persisted"],
                Durability::Replicated,
                Durability::Persisted,
            ),
            (
                vec![
                    "--durability",
                    "persisted",
                    "--consumer-offset-durability",
                    "persisted",
                ],
                Durability::Persisted,
                Durability::Persisted,
            ),
        ] {
            let arguments = std::iter::once("iggy-bench")
                .chain(flags)
                .chain(["balanced-producer-and-consumer-group", "tcp"]);
            let parsed = IggyBenchArgs::try_parse_from(arguments).unwrap();
            assert_eq!(parsed.durability, messages);
            assert_eq!(parsed.consumer_offset_durability, offsets);
        }
    }
}
