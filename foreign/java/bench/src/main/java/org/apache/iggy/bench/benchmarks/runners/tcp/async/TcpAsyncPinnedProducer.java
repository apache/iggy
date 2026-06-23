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

package org.apache.iggy.bench.benchmarks.runners.tcp.async;

import org.apache.iggy.bench.benchmarks.actors.tcp.async.TcpAsyncPinnedProducerActor;
import org.apache.iggy.bench.common.exception.BenchmarkException;
import org.apache.iggy.bench.common.generator.BenchmarkBatchGenerator;
import org.apache.iggy.bench.common.provision.ResourceProvisioner;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.apache.iggy.bench.models.common.generator.DataBatch;
import org.apache.iggy.bench.models.common.provision.ProvisionedResources;
import org.apache.iggy.bench.models.report.metrics.GroupMetrics;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;
import org.apache.iggy.bench.report.GroupMetricsCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class TcpAsyncPinnedProducer {

    private final GlobalCliArgs globalCliArgs;
    private final PinnedProducerCliArgs pinnedProducerCliArgs;
    private final ResourceProvisioner resourceProvisioner;
    private ProvisionedResources provisionedResources;
    private List<GroupMetrics> groupMetrics = List.of();
    private List<IndividualMetrics> individualMetrics = List.of();

    TcpAsyncPinnedProducer(
            GlobalCliArgs globalCliArgs,
            PinnedProducerCliArgs pinnedProducerCliArgs,
            ResourceProvisioner resourceProvisioner) {
        this.globalCliArgs = globalCliArgs;
        this.pinnedProducerCliArgs = pinnedProducerCliArgs;
        this.resourceProvisioner = resourceProvisioner;
    }

    public TcpAsyncPinnedProducer(GlobalCliArgs globalCliArgs, PinnedProducerCliArgs pinnedProducerCliArgs) {
        this(globalCliArgs, pinnedProducerCliArgs, new ResourceProvisioner());
    }

    public void provisionResources() {
        this.provisionedResources = resourceProvisioner.provisionResources(globalCliArgs, pinnedProducerCliArgs);
    }

    public void run() {
        individualMetrics = runBenchmark();
        groupMetrics = new GroupMetricsCalculator(individualMetrics, globalCliArgs).calculate();
    }

    public List<IndividualMetrics> individualMetrics() {
        return List.copyOf(individualMetrics);
    }

    public List<GroupMetrics> groupMetrics() {
        return List.copyOf(groupMetrics);
    }

    private List<IndividualMetrics> runBenchmark() {
        if (provisionedResources == null) {
            throw new BenchmarkException("Benchmark resources must be provisioned before running.");
        }

        String topicName = provisionedResources.topicNames().get(0);
        var batchGenerator = new BenchmarkBatchGenerator(globalCliArgs.messageSize(), globalCliArgs.messagesPerBatch());
        DataBatch fullBatch = batchGenerator.generateBatch();
        long targetMessageBatches = globalCliArgs.totalData() > 0L ? 0L : globalCliArgs.messageBatches();
        long targetDataBytes =
                globalCliArgs.totalData() > 0L ? globalCliArgs.totalData() / pinnedProducerCliArgs.producers() : 0L;
        var actorRuns = new ArrayList<CompletableFuture<IndividualMetrics>>(pinnedProducerCliArgs.producers());

        for (int index = 0; index < pinnedProducerCliArgs.producers(); index++) {
            String streamName = provisionedResources.streamNames().get(index);
            var actor = new TcpAsyncPinnedProducerActor(
                    globalCliArgs, index + 1, streamName, topicName, fullBatch, targetMessageBatches, targetDataBytes);
            actorRuns.add(actor.run());
        }

        CompletableFuture.allOf(actorRuns.toArray(CompletableFuture[]::new)).join();

        var results = new ArrayList<IndividualMetrics>(actorRuns.size());
        for (CompletableFuture<IndividualMetrics> actorRun : actorRuns) {
            results.add(actorRun.join());
        }
        return results;
    }
}
