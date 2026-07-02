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

package org.apache.iggy.bench.report;

import org.apache.iggy.bench.common.enums.BenchmarkKind;
import org.apache.iggy.bench.models.report.FinalReport;
import org.apache.iggy.bench.models.report.context.BenchmarkHardware;
import org.apache.iggy.bench.models.report.context.BenchmarkParams;
import org.apache.iggy.bench.models.report.context.BenchmarkServerStats;
import org.apache.iggy.bench.models.report.metrics.GroupMetrics;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.cfg.EnumFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class FinalReportBuilder {

    private static final String DEFAULT_REPORT_FILE_NAME = "report.json";
    private static final DateTimeFormatter RFC3339_UTC_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter();
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(EnumFeature.WRITE_ENUMS_USING_TO_STRING)
            .enable(EnumFeature.READ_ENUMS_USING_TO_STRING)
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .build();
    private final BenchmarkServerStats serverStats;
    private final BenchmarkHardware hardware;
    private final BenchmarkParams params;
    private final List<GroupMetrics> groupMetrics;
    private final List<IndividualMetrics> individualMetrics;
    private FinalReport report;

    public FinalReportBuilder(
            BenchmarkServerStats serverStats,
            BenchmarkHardware hardware,
            BenchmarkParams params,
            List<GroupMetrics> groupMetrics,
            List<IndividualMetrics> individualMetrics) {
        this.serverStats = Objects.requireNonNull(serverStats, "serverStats");
        this.hardware = Objects.requireNonNull(hardware, "hardware");
        this.params = Objects.requireNonNull(params, "params");
        this.groupMetrics = List.copyOf(Objects.requireNonNull(groupMetrics, "groupMetrics"));
        this.individualMetrics = List.copyOf(Objects.requireNonNull(individualMetrics, "individualMetrics"));
    }

    public FinalReport buildReport() {
        String timestamp = OffsetDateTime.now(ZoneOffset.UTC).format(RFC3339_UTC_FORMATTER);
        BenchmarkParams reportParams = params.gitrefDate().isPresent()
                ? params
                : new BenchmarkParams(
                        params.benchmarkKind(),
                        params.transport(),
                        params.serverAddress(),
                        params.remark(),
                        params.extraInfo(),
                        params.gitref(),
                        Optional.of(timestamp),
                        params.messagesPerBatch(),
                        params.messageBatches(),
                        params.messageSize(),
                        params.producers(),
                        params.consumers(),
                        params.streams(),
                        params.partitions(),
                        params.consumerGroups(),
                        params.rateLimit(),
                        params.prettyName(),
                        params.benchCommand(),
                        params.paramsIdentifier());
        report = new FinalReport(
                UUID.randomUUID(), timestamp, serverStats, hardware, reportParams, groupMetrics, individualMetrics);
        return report;
    }

    public FinalReport report() {
        return report;
    }

    public String toJson() {
        return MAPPER.writeValueAsString(report);
    }

    public void printSummary() {
        if (report == null) {
            throw new IllegalStateException("Benchmark report must be built before printing.");
        }

        BenchmarkParams reportParams = report.params();
        long totalMessages = report.individualMetrics().stream()
                .map(IndividualMetrics::summary)
                .mapToLong(summary -> summary.totalMessages())
                .sum();
        long totalBytes = report.individualMetrics().stream()
                .map(IndividualMetrics::summary)
                .mapToLong(summary -> summary.totalUserDataBytes())
                .sum();
        long totalMessageBatches = report.individualMetrics().stream()
                .map(IndividualMetrics::summary)
                .mapToLong(summary -> summary.totalMessageBatches())
                .sum();
        if (totalMessageBatches == 0L) {
            totalMessageBatches = reportParams.messageBatches();
        }

        printBenchmarkSummary(reportParams, totalMessages, totalBytes, totalMessageBatches);

        for (GroupMetrics metrics : report.groupMetrics()) {
            printGroupSummary(metrics);
        }
    }

    private static void printBenchmarkSummary(
            BenchmarkParams reportParams, long totalMessages, long totalBytes, long totalMessageBatches) {
        String totalDataText = totalBytes + " bytes";
        boolean producingConsumers = reportParams.benchmarkKind() == BenchmarkKind.END_TO_END_PRODUCING_CONSUMER
                || reportParams.benchmarkKind() == BenchmarkKind.END_TO_END_PRODUCING_CONSUMER_GROUP;
        String producersText = "";
        if (reportParams.producers() > 0) {
            producersText = reportParams.producers() + (producingConsumers ? " producing consumers, " : " producers, ");
        }
        String consumersText = reportParams.consumers() > 0 ? reportParams.consumers() + " consumers, " : "";
        String partitionsText =
                reportParams.partitions() > 0 ? reportParams.partitions() + " partitions per topic, " : "";
        String consumerGroupsText =
                reportParams.consumerGroups() > 0 ? reportParams.consumerGroups() + " consumer groups, " : "";
        String messagesPerBatchText = Integer.toString(reportParams.messagesPerBatch());
        String messageSizeText = Integer.toString(reportParams.messageSize());

        LoggerFactory.getLogger("bench_report::prints")
                .info(
                        "Benchmark: {}, {}{}{} streams, 1 topic per stream, {}{}{} messages, {} messages per batch, {} message batches, {} bytes per message, {} of data processed{}",
                        reportParams.benchmarkKind().prettyName(),
                        producersText,
                        consumersText,
                        reportParams.streams(),
                        partitionsText,
                        consumerGroupsText,
                        totalMessages,
                        messagesPerBatchText,
                        totalMessageBatches,
                        messageSizeText,
                        totalDataText,
                        System.lineSeparator());
    }

    private static void printGroupSummary(GroupMetrics metrics) {
        String prefix = groupPrefix(metrics);
        String actor = groupActor(metrics);

        double totalTimeSeconds = metrics.avgThroughputMbTs().points().isEmpty()
                ? 0.0
                : metrics.avgThroughputMbTs()
                        .points()
                        .get(metrics.avgThroughputMbTs().points().size() - 1)
                        .timeS();

        System.out.println();
        System.out.println(String.format(
                "%s: Total throughput: %.2f MB/s, %.0f messages/s, average throughput per %s: %.2f MB/s, "
                        + "p50 latency: %.2f ms, p90 latency: %.2f ms, p95 latency: %.2f ms, "
                        + "p99 latency: %.2f ms, p999 latency: %.2f ms, p9999 latency: %.2f ms, average latency: %.2f ms, "
                        + "median latency: %.2f ms, min: %.2f ms, max: %.2f ms, std dev: %.2f ms, total time: %.2f s",
                prefix,
                metrics.summary().totalThroughputMegabytesPerSecond(),
                metrics.summary().totalThroughputMessagesPerSecond(),
                actor,
                metrics.summary().averageThroughputMegabytesPerSecond(),
                metrics.summary().averageP50LatencyMs(),
                metrics.summary().averageP90LatencyMs(),
                metrics.summary().averageP95LatencyMs(),
                metrics.summary().averageP99LatencyMs(),
                metrics.summary().averageP999LatencyMs(),
                metrics.summary().averageP9999LatencyMs(),
                metrics.summary().averageLatencyMs(),
                metrics.summary().averageMedianLatencyMs(),
                metrics.summary().minLatencyMs(),
                metrics.summary().maxLatencyMs(),
                metrics.summary().stdDevLatencyMs(),
                totalTimeSeconds));
    }

    private static String groupPrefix(GroupMetrics metrics) {
        return switch (metrics.summary().kind()) {
            case PRODUCERS -> "Producers Results";
            case CONSUMERS -> "Consumers Results";
            case PRODUCERS_AND_CONSUMERS -> "Aggregate Results";
            case PRODUCING_CONSUMERS -> "Producing Consumer Results";
            default ->
                throw new IllegalStateException(
                        "Unsupported group kind: " + metrics.summary().kind());
        };
    }

    private static String groupActor(GroupMetrics metrics) {
        return switch (metrics.summary().kind()) {
            case PRODUCERS -> "Producer";
            case CONSUMERS -> "Consumer";
            case PRODUCERS_AND_CONSUMERS -> "Actor";
            case PRODUCING_CONSUMERS -> "Producing Consumer";
            default ->
                throw new IllegalStateException(
                        "Unsupported group kind: " + metrics.summary().kind());
        };
    }

    public Path writeJson(Path outputDirectory) {
        return writeJson(outputDirectory, DEFAULT_REPORT_FILE_NAME);
    }

    public Path writeJson(Path outputDirectory, String fileName) {
        try {
            Files.createDirectories(outputDirectory);
            Path reportPath = outputDirectory.resolve(fileName);
            MAPPER.writeValue(reportPath.toFile(), report);
            return reportPath;
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to write final report JSON.", exception);
        }
    }
}
