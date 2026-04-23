/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.iggy.bench.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(
        name = "iggy-bench",
        mixinStandardHelpOptions = true,
        subcommands = {PinnedProducerCommand.class},
        description = "Benchmark CLI for the Apache Iggy Java SDK.")
public final class IggyBenchCommand implements Callable<Integer> {

    @Option(
            names = {"--message-size", "-m"},
            defaultValue = "1000",
            description = "Message size in bytes.")
    public int messageSize = 1000;

    @Option(
            names = {"--messages-per-batch", "-P"},
            defaultValue = "1000",
            description = "Messages per batch.")
    public int messagesPerBatch = 1000;

    @Option(
            names = {"--message-batches", "-b"},
            defaultValue = "1000",
            description = "Number of message batches.")
    public int messageBatches = 1000;

    @Option(
            names = {"--total-data", "-T"},
            defaultValue = "0",
            description = "Total data volume in bytes.")
    public long totalData;

    @Option(
            names = {"--rate-limit", "-r"},
            defaultValue = "0",
            description = "(NOT USED CURRENTLY) Optional total rate limit in bytes per second.")
    public long rateLimit;

    @Option(
            names = {"--warmup-time", "-w"},
            defaultValue = "20000",
            description = "Warmup time in milliseconds.")
    public long warmupTimeMs;

    @Option(
            names = {"--sampling-time", "-t"},
            defaultValue = "10",
            description = "Sampling time in milliseconds.")
    public long samplingTimeMs = 10L;

    @Option(
            names = {"--moving-average-window", "-W"},
            defaultValue = "20",
            description = "Moving average window size.")
    public int movingAverageWindow = 20;

    @Option(
            names = {"--username", "-u"},
            defaultValue = "iggy",
            description = "Server username.")
    public String username = "iggy";

    @Option(
            names = {"--password", "-p"},
            defaultValue = "iggy",
            description = "Server password.")
    public String password = "iggy";

    @Option(names = "--reuse-streams", defaultValue = "false", description = "Reuse existing benchmark streams.")
    public boolean reuseStreams;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() {
        spec.commandLine().usage(spec.commandLine().getOut());
        return ExitCode.USAGE;
    }
}
