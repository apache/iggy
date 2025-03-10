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

const EXAMPLES: &str = r#"EXAMPLES:

1) Pinned Mode Benchmarking:

    Run benchmarks with pinned producers and consumers. This mode pins specific producers
    and consumers to specific streams and partitions (one to one):

    $ cargo r -r --bin iggy-bench -- pinned-producer --streams 10 --producers 10 tcp
    $ cargo r -r --bin iggy-bench -- pinned-consumer --streams 10 --consumers 10 tcp
    $ cargo r -r --bin iggy-bench -- pinned-producer-and-consumer --streams 10 --producers 10 --consumers 10 tcp

2) Balanced Mode Benchmarking:

    Run benchmarks with balanced distribution of producers and consumers. This mode
    automatically balances the load across streams and consumer groups:

    $ cargo r -r --bin iggy-bench -- balanced-producer --partitions 24 --producers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-consumer-group --consumers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-producer-and-consumer-group --partitions 24 --producers 6 --consumers 6 tcp

3) End-to-End Benchmarking:

    Run end-to-end benchmarks that measure performance for a producer that is also a consumer:

    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --producers 12 --streams 12 tcp
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer-group --partitions 24 --producers 6 tcp

4) Advanced Configuration:

    You can customize various parameters for any benchmark mode:

    Global options (before the benchmark command):
    --messages-per-batch (-p): Number of messages per batch [default: 1000]
    --message-batches (-b): Total number of batches [default: 1000]
    --message-size (-m): Message size in bytes [default: 1000]
    --start-stream-id (-S): Start stream ID [default: 1]
    --rate-limit (-r): Optional throughput limit per producer (e.g., "50KB/s", "10MB/s")
    --warmup-time (-w): Warmup duration [default: 0s]
    --sampling-time (-t): Metrics sampling interval [default: 10ms]
    --moving-average-window (-W): Window size for moving average [default: 20]
    --cleanup: Remove server data after benchmark
    --verbose (-v): Show server output (only applicable for local server)

    Benchmark-specific options (after the benchmark command):
    --streams (-s): Number of streams
    --partitions (-a): Number of partitions
    --producers (-c): Number of producers
    --consumers (-c): Number of consumers
    --max-topic-size (-t): Max topic size (e.g., "1GiB")

    Example with detailed configuration:
    $ cargo r --bin iggy-bench \
        --message-size 1000 \
        --messages-per-batch 100 \
        --message-batches 1000 \
        --rate-limit "100MB/s" \
        --warmup-time "10s" \
        --sampling-time "1s" \
        balanced-producer \
        --streams 5 \
        --partitions 2 \
        --producers 5 \
        --max-topic-size "1GiB" \
        tcp

5) Remote Server Benchmarking:

    To benchmark a remote server, specify the server address in the transport subcommand:

    $ cargo r -r --bin iggy-bench -- pinned-producer \
        --streams 5 --producers 5 \
        tcp --server-address 192.168.1.100:8090

6) Output Data and Results:

    The benchmark tool can store detailed results for analysis and comparison:

    # Basic result storage:
    $ cargo r -r --bin iggy-bench -- pinned-producer --streams 10 --producers 10 tcp \
        output --output-dir ./results

    # Organized benchmarking with metadata:
    $ cargo r -r --bin iggy-bench -- balanced-producer --partitions 24 --producers 6 tcp \
        output \
        --output-dir performance_results \
        --identifier "prod-test-$(date +%Y%m%d)" \
        --remark "production-config" \
        --gitref "$(git rev-parse --short HEAD)" \
        --gitref-date "$(git show -s --format=%cI HEAD)"

    # Quick result visualization:
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --producers 12 --streams 12 tcp \
        output \
        --output-dir performance_results \
        --open-charts

    Output configuration options:
    --output-dir (-o)  : Directory for storing results (required for output)
    --identifier       : Benchmark run ID (defaults to hostname)
    --remark           : Additional context (e.g., "production-config")
    --extra-info       : Custom metadata for future analysis
    --gitref           : Git reference for version tracking
    --gitref-date      : Git reference date (merge/commit date)
    --open-charts      : Auto-open result charts in browser

7) Help and Documentation:

    For more details on available options:

    # General help
    $ cargo r -r --bin iggy-bench -- --help

    # Help for specific benchmark mode
    $ cargo r -r --bin iggy-bench -- pinned-producer --help
    $ cargo r -r --bin iggy-bench -- balanced-consumer-group --help
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --help

    # Help for transport options
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp --help
    $ cargo r -r --bin iggy-bench -- balanced-producer http --help
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer quic --help

"#;

pub fn print_examples() {
    print!("{}", EXAMPLES)
}
