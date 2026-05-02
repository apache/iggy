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
import org.apache.iggy.bench.common.generator.BenchmarkBatchGenerator;
import org.apache.iggy.bench.common.provision.ResourceProvisioner;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.apache.iggy.bench.models.common.generator.DataBatch;
import org.apache.iggy.bench.models.common.provision.ProvisionedResources;

import java.util.concurrent.CompletableFuture;

public final class TcpAsyncPinnedProducer {

    private final GlobalCliArgs globalCliArgs;
    private final PinnedProducerCliArgs pinnedProducerCliArgs;
    private final ResourceProvisioner resourceProvisioner;
    private ProvisionedResources provisionedResources;

    TcpAsyncPinnedProducer(
            GlobalCliArgs globalCliArgs,
            PinnedProducerCliArgs pinnedProducerCliArgs,
            ResourceProvisioner resourceProvisioner) {
        this.globalCliArgs = globalCliArgs;
        this.pinnedProducerCliArgs = pinnedProducerCliArgs;
        this.resourceProvisioner = resourceProvisioner;

        provisionResources();
        runBenchmark().join();
    }

    public TcpAsyncPinnedProducer(GlobalCliArgs globalCliArgs, PinnedProducerCliArgs pinnedProducerCliArgs) {
        this(globalCliArgs, pinnedProducerCliArgs, new ResourceProvisioner());
    }

    private void provisionResources() {
        this.provisionedResources = resourceProvisioner.provisionResources(globalCliArgs, pinnedProducerCliArgs);
    }

    private CompletableFuture<Void> runBenchmark() {
        try {
            String topicName = provisionedResources.topicNames().get(0);
            var batchGenerator =
                    new BenchmarkBatchGenerator(globalCliArgs.messageSize(), globalCliArgs.messagesPerBatch());
            DataBatch fullBatch = batchGenerator.generateBatch();
            long targetMessageBatches = globalCliArgs.totalData() > 0L ? 0L : globalCliArgs.messageBatches();
            long targetDataBytes =
                    globalCliArgs.totalData() > 0L ? globalCliArgs.totalData() / pinnedProducerCliArgs.producers() : 0L;
            var actorRuns = new CompletableFuture<?>[pinnedProducerCliArgs.producers()];

            for (int index = 0; index < pinnedProducerCliArgs.producers(); index++) {
                String streamName = provisionedResources.streamNames().get(index);
                var actor = new TcpAsyncPinnedProducerActor(
                        globalCliArgs,
                        index + 1,
                        streamName,
                        topicName,
                        fullBatch,
                        targetMessageBatches,
                        targetDataBytes);
                actorRuns[index] = actor.run();
            }

            return CompletableFuture.allOf(actorRuns);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}
