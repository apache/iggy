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

package org.apache.iggy.bench.cli;

import picocli.CommandLine.ArgGroup;
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
    private int messageSize = 1000;

    @Option(
            names = {"--messages-per-batch", "-P"},
            defaultValue = "1000",
            description = "Messages per batch.")
    private int messagesPerBatch = 1000;

    @ArgGroup(exclusive = true, multiplicity = "0..1")
    private DataLimitOptions dataLimitOptions;

    @Option(
            names = {"--rate-limit", "-r"},
            defaultValue = "0",
            description = "(NOT USED CURRENTLY) Optional total rate limit in bytes per second.")
    private long rateLimit;

    @Option(
            names = {"--warmup-time", "-w"},
            defaultValue = "20000",
            description = "Warmup time in milliseconds.")
    private long warmupTimeMs;

    @Option(
            names = {"--sampling-time", "-t"},
            defaultValue = "10",
            description = "Sampling time in milliseconds.")
    private long samplingTimeMs = 10L;

    @Option(
            names = {"--moving-average-window", "-W"},
            defaultValue = "20",
            description = "Moving average window size.")
    private int movingAverageWindow = 20;

    @Option(
            names = {"--username", "-u"},
            defaultValue = "iggy",
            description = "Server username.")
    private String username = "iggy";

    @Option(
            names = {"--password", "-p"},
            defaultValue = "iggy",
            description = "Server password.")
    private String password = "iggy";

    @Option(names = "--reuse-streams", defaultValue = "false", description = "Reuse existing benchmark streams.")
    private boolean reuseStreams;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() {
        spec.commandLine().usage(spec.commandLine().getOut());
        return ExitCode.USAGE;
    }

    public int messageSize() {
        return messageSize;
    }

    public int messagesPerBatch() {
        return messagesPerBatch;
    }

    public int messageBatches() {
        if (dataLimitOptions == null || dataLimitOptions.messageBatches == null) {
            return 1000;
        }
        return dataLimitOptions.messageBatches;
    }

    public long totalData() {
        if (dataLimitOptions == null || dataLimitOptions.totalData == null) {
            return 0L;
        }
        return dataLimitOptions.totalData;
    }

    public long rateLimit() {
        return rateLimit;
    }

    public long warmupTimeMs() {
        return warmupTimeMs;
    }

    public long samplingTimeMs() {
        return samplingTimeMs;
    }

    public int movingAverageWindow() {
        return movingAverageWindow;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public boolean reuseStreams() {
        return reuseStreams;
    }

    private static final class DataLimitOptions {

        @Option(
                names = {"--message-batches", "-b"},
                description = "Number of message batches. Defaults to 1000 when --total-data is not specified.")
        private Integer messageBatches;

        @Option(
                names = {"--total-data", "-T"},
                description = "Total data volume in bytes.")
        private Long totalData;
    }
}
