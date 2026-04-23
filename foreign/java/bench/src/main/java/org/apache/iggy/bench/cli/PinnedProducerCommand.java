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

import org.apache.iggy.bench.benchmarks.tcp.async.PinnedProducer;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

@Command(
        name = "pinned-producer",
        aliases = {"pp"},
        mixinStandardHelpOptions = true,
        description = "Pinned producer benchmark.")
public final class PinnedProducerCommand implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PinnedProducerCommand.class);
    private static final long DEFAULT_MAX_TOPIC_SIZE = 0L;
    private static final long DEFAULT_MESSAGE_EXPIRY = 0L;

    @ParentCommand
    IggyBenchCommand rootCommand;

    @Spec
    private CommandSpec spec;

    @Option(
            names = {"--streams", "-s"},
            description = "Number of streams. Defaults to the number of producers.")
    public Integer streams;

    @Option(
            names = {"--producers", "-p"},
            defaultValue = "8",
            description = "Number of producers.")
    public int producers = 8;

    @Option(
            names = {"--max-topic-size", "-T"},
            defaultValue = "0",
            description = "Max topic size in bytes. Use 0 for the server default.")
    public long maxTopicSize = DEFAULT_MAX_TOPIC_SIZE;

    @Option(
            names = {"--message-expiry", "-e"},
            defaultValue = "0",
            description = "Topic message expiry in microseconds. Use 0 to never expire.")
    public long messageExpiry = DEFAULT_MESSAGE_EXPIRY;

    @Override
    public Integer call() {
        try {
            var messageBatches = rootCommand.totalData > 0 ? 0 : rootCommand.messageBatches;
            var globalCliArgs = new GlobalCliArgs(
                    rootCommand.messageSize,
                    rootCommand.messagesPerBatch,
                    messageBatches,
                    rootCommand.totalData,
                    rootCommand.rateLimit,
                    rootCommand.warmupTimeMs,
                    rootCommand.samplingTimeMs,
                    rootCommand.movingAverageWindow,
                    rootCommand.username,
                    rootCommand.password,
                    rootCommand.reuseStreams);

            var pinnedProducerCliArgs = new PinnedProducerCliArgs(
                    streams != null ? streams : producers, producers, maxTopicSize, messageExpiry);

            globalCliArgs.validate();
            pinnedProducerCliArgs.validate();

            LOGGER.info("Starting the Pinned Producer benchmark...");
            var benchmark = new PinnedProducer(globalCliArgs, pinnedProducerCliArgs);
            benchmark.provisionResources();
            benchmark.run();

            return ExitCode.OK;
        } catch (RuntimeException exception) {
            var message = exception.getMessage() != null ? exception.getMessage() : exception.toString();
            spec.commandLine().getErr().println(message);
            return ExitCode.SOFTWARE;
        }
    }
}
