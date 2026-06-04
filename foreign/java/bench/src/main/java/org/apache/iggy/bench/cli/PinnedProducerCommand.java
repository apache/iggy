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

import org.apache.iggy.Iggy;
import org.apache.iggy.IggyVersion;
import org.apache.iggy.bench.benchmarks.runners.tcp.async.TcpAsyncPinnedProducer;
import org.apache.iggy.bench.common.enums.BenchmarkKind;
import org.apache.iggy.bench.common.enums.TransportKind;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.apache.iggy.bench.models.report.context.BenchmarkNumericParameter;
import org.apache.iggy.bench.models.report.context.BenchmarkParams;
import org.apache.iggy.bench.report.FinalReportBuilder;
import org.apache.iggy.bench.report.HardwareInfoCollector;
import org.apache.iggy.bench.report.ServerStatsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(
        name = "pinned-producer",
        aliases = {"pp"},
        mixinStandardHelpOptions = true,
        description = "Pinned producer benchmark.")
public final class PinnedProducerCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(PinnedProducerCommand.class);
    private static final long DEFAULT_MAX_TOPIC_SIZE = 0L;
    private static final long DEFAULT_MESSAGE_EXPIRY = 0L;
    private static final String DEFAULT_SERVER_ADDRESS = "127.0.0.1:8090";

    @ParentCommand
    private IggyBenchCommand rootCommand;

    @Spec
    private CommandSpec spec;

    @Option(
            names = {"--streams", "-s"},
            description = "Number of streams. Defaults to the number of producers.")
    private Integer streams;

    @Option(
            names = {"--producers", "-p"},
            defaultValue = "8",
            description = "Number of producers.")
    private int producers = 8;

    @Option(
            names = {"--max-topic-size", "-T"},
            defaultValue = "0",
            description = "Max topic size in bytes. Use 0 for the server default.")
    private long maxTopicSize = DEFAULT_MAX_TOPIC_SIZE;

    @Option(
            names = {"--message-expiry", "-e"},
            defaultValue = "0",
            description = "Topic message expiry in microseconds. Use 0 to never expire.")
    private long messageExpiry = DEFAULT_MESSAGE_EXPIRY;

    @Override
    public Integer call() {
        try {
            var messageBatches = rootCommand.totalData() > 0 ? 0 : rootCommand.messageBatches();
            var globalCliArgs = new GlobalCliArgs(
                    rootCommand.messageSize(),
                    rootCommand.messagesPerBatch(),
                    messageBatches,
                    rootCommand.totalData(),
                    rootCommand.rateLimit(),
                    rootCommand.warmupTimeMs(),
                    rootCommand.samplingTimeMs(),
                    rootCommand.movingAverageWindow(),
                    rootCommand.username(),
                    rootCommand.password(),
                    rootCommand.reuseStreams());

            var pinnedProducerCliArgs = new PinnedProducerCliArgs(
                    streams != null ? streams : producers, producers, maxTopicSize, messageExpiry);

            globalCliArgs.validate();
            pinnedProducerCliArgs.validate();

            log.info("Starting the Pinned Producer benchmark...");
            var benchmark = new TcpAsyncPinnedProducer(globalCliArgs, pinnedProducerCliArgs);
            benchmark.provisionResources();
            benchmark.run();

            var finalReportBuilder = new FinalReportBuilder(
                    new ServerStatsCollector(globalCliArgs).collect(),
                    new HardwareInfoCollector().collect(),
                    buildPinnedProducerBenchmarkParams(globalCliArgs, pinnedProducerCliArgs, spec),
                    benchmark.groupMetrics(),
                    benchmark.individualMetrics());
            finalReportBuilder.buildReport();

            Path reportPath = finalReportBuilder.writeJson(Path.of(System.getProperty("user.dir")));
            log.info("Wrote benchmark report to {}", reportPath.toAbsolutePath());
            finalReportBuilder.printSummary();

            return ExitCode.OK;
        } catch (RuntimeException exception) {
            var message = exception.getMessage() != null ? exception.getMessage() : exception.toString();
            spec.commandLine().getErr().println(message);
            return ExitCode.SOFTWARE;
        }
    }

    private static BenchmarkParams buildPinnedProducerBenchmarkParams(
            GlobalCliArgs globalCliArgs, PinnedProducerCliArgs pinnedProducerCliArgs, CommandSpec spec) {
        int producers = pinnedProducerCliArgs.producers();
        int streams = pinnedProducerCliArgs.streams();
        IggyVersion versionInfo = Iggy.versionInfo();
        String benchCommand = spec.root().name();
        var originalArgs = spec.root().commandLine().getParseResult().originalArgs();
        if (!originalArgs.isEmpty()) {
            benchCommand += " " + String.join(" ", originalArgs);
        }

        String dataVolumeIdentifier = globalCliArgs.totalData() > 0L
                ? globalCliArgs.totalData() + "B"
                : Integer.toString(globalCliArgs.messageBatches());
        long messageBatches = globalCliArgs.totalData() > 0L
                ? 0L
                : Math.multiplyExact((long) globalCliArgs.messageBatches(), producers);

        return new BenchmarkParams(
                BenchmarkKind.PINNED_PRODUCER,
                TransportKind.TCP,
                DEFAULT_SERVER_ADDRESS,
                Optional.empty(),
                Optional.empty(),
                Optional.of(versionInfo.getVersion()),
                Optional.of(versionInfo.getBuildTime()),
                BenchmarkNumericParameter.ofValue(globalCliArgs.messagesPerBatch()),
                messageBatches,
                BenchmarkNumericParameter.ofValue(globalCliArgs.messageSize()),
                producers,
                0,
                streams,
                1,
                0,
                globalCliArgs.rateLimit() > 0L
                        ? Optional.of(Long.toString(globalCliArgs.rateLimit()))
                        : Optional.empty(),
                producers + " producers, " + globalCliArgs.messageSize() + "B msgs, " + globalCliArgs.messagesPerBatch()
                        + " msgs/batch",
                benchCommand,
                String.join(
                        "_",
                        BenchmarkKind.PINNED_PRODUCER.prettyName(),
                        TransportKind.TCP.name(),
                        "no_remark",
                        Integer.toString(globalCliArgs.messagesPerBatch()),
                        dataVolumeIdentifier,
                        Integer.toString(globalCliArgs.messageSize()),
                        Integer.toString(producers),
                        "0",
                        Integer.toString(streams),
                        "1",
                        "0"));
    }
}
