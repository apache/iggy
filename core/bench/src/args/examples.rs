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
    $ cargo r -r --bin iggy-bench -- -T 10GB pp --producers 5 tcp

2) Balanced Mode Benchmarking:

    Run benchmarks with balanced distribution of producers and consumers. This mode
    automatically balances the load across streams and consumer groups:

    $ cargo r -r --bin iggy-bench -- balanced-producer --partitions 24 --producers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-consumer-group --consumers 6 tcp
    $ cargo r -r --bin iggy-bench -- balanced-producer-and-consumer-group --partitions 24 --producers 6 --consumers 6 tcp
    $ cargo r -r --bin iggy-bench -- -T 10GB bpc tcp

3) End-to-End Benchmarking:

    Run end-to-end benchmarks that measure performance for a producer that is also a consumer:

    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --producers 12 --streams 12 tcp
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer-group --partitions 24 --producers 6 tcp

4) Advanced Configuration:

    You can customize various parameters for any benchmark mode:

    Global options (before the benchmark command):
    --messages-per-batch (-p): Number of messages per batch [default: 1000]
                               For random batch sizes, use range format: "100..1000"
    --message-batches (-b): Total number of batches [default: 1000]
    --total-messages-size (-T): Total size of messages to send (e.g., "1GB", "500MB")
                                Mutually exclusive with --message-batches
    --message-size (-m): Message size in bytes [default: 1000]
                        For random sizes, use range format: "100..1000"
    --start-stream-id (-S): Start stream ID [default: 1]
    --rate-limit (-r): Optional throughput limit per producer (e.g., "50KB", "10MB")
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
    --max-topic-size (-T): Max topic size (e.g., "1GiB")

    Examples with detailed configuration:

    # Fixed message and batch sizes:
    $ cargo r -r --bin iggy-bench -- \
        --message-size 1000 \
        --messages-per-batch 100 \
        --message-batches 1000 \
        --rate-limit "100MB" \
        balanced-producer \
        --streams 5 \
        --producers 5 \
        tcp

    # Random message sizes (100-1000 bytes):
    $ cargo r -r --bin iggy-bench -- \
        --message-size "100..1000" \
        --messages-per-batch 100 \
        --total-messages-size "1GB" \
        balanced-producer \
        --streams 5 \
        --producers 5 \
        tcp

    # Random batch sizes (10-100 messages per batch):
    $ cargo r -r --bin iggy-bench -- \
        --message-size 1000 \
        --messages-per-batch "10..100" \
        --total-messages-size "500MB" \
        balanced-producer \
        --streams 5 \
        --producers 5 \
        tcp

    # Random message and batch sizes with rate limiting:
    $ cargo r -r --bin iggy-bench -- \
        --message-size "500..2000" \
        --messages-per-batch "50..200" \
        --total-messages-size "2GB" \
        --rate-limit "50MB" \
        balanced-producer \
        --streams 5 \
        --producers 5 \
        tcp

5) Remote Server Benchmarking:

    To benchmark a remote server, specify the server address in the transport subcommand:

    $ cargo r -r --bin iggy-bench -- pinned-producer \
        --streams 5 --producers 5 \
        tcp --server-address 192.168.1.100:8090

6) Output Data and Results:

    The benchmark tool can store detailed results for analysis and comparison:

    # Basic result storage (results will be stored in ./performance_results):
    $ cargo r -r --bin iggy-bench -- pinned-producer --streams 10 --producers 10 tcp output


    # Organized benchmarking with metadata:
    $ cargo r -r --bin iggy-bench -- balanced-producer --partitions 24 --producers 6 tcp \
        output \
        --identifier "prod-test-$(date +%Y%m%d)" \
        --remark "production-config" \
        --gitref "$(git rev-parse --short HEAD)" \
        --gitref-date "$(git show -s --format=%cI HEAD)"

    # Quick result visualization:
    $ cargo r -r --bin iggy-bench -- end-to-end-producing-consumer --producers 12 --streams 12 tcp \
        output --open-charts

    Output configuration options:
    --open-charts (-c)  : Open charts after the benchmark
    --output-dir (-o)   : Directory for storing results [default: performance_results]
    --identifier        : Benchmark run ID (if not provided defaults to hostname)
    --remark            : Additional context (e.g., "production-config")
    --extra-info        : Custom metadata for future analysis, currently unused

7) Help and Documentation:

    For more details on available options:

    # General help
    $ cargo r -r --bin iggy-bench -- --help

    # Specific benchmark help
    $ cargo r -r --bin iggy-bench -- pinned-producer --help

    # Protocol help
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp --help

    # Output help
    $ cargo r -r --bin iggy-bench -- pinned-producer tcp output --help
"#;

pub fn print_examples() {
    println!("{}", EXAMPLES);
}
