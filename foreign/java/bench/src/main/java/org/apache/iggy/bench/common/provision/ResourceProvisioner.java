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

package org.apache.iggy.bench.common.provision;

import org.apache.iggy.bench.common.exception.BenchmarkException;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.apache.iggy.bench.models.common.provision.ProvisionedResources;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class ResourceProvisioner {

    private static final Logger log = LoggerFactory.getLogger(ResourceProvisioner.class);

    public ProvisionedResources provisionResources(
            GlobalCliArgs globalCliArgs, PinnedProducerCliArgs pinnedProducerCliArgs) {
        var streamNames = new ArrayList<String>();
        var topicNames = List.of("topic-1");

        try {
            try (var client = IggyTcpClient.builder()
                    .credentials(globalCliArgs.username(), globalCliArgs.password())
                    .buildAndLogin()) {
                var existingStreamNames = client.streams().getStreams().stream()
                        .map(stream -> stream.name())
                        .toList();

                for (var i = 1; i <= pinnedProducerCliArgs.producers(); i++) {
                    var streamName = "bench-stream-" + i;
                    streamNames.add(streamName);

                    if (existingStreamNames.contains(streamName) && !globalCliArgs.reuseStreams()) {
                        log.info("Deleting pre-existing stream '{}'", streamName);
                        client.streams().deleteStream(StreamId.of(streamName));
                    }

                    if (existingStreamNames.contains(streamName) && globalCliArgs.reuseStreams()) {
                        log.info("Appending to existing stream '{}'", streamName);
                    } else {
                        log.info("Creating the test stream '{}'", streamName);

                        client.streams().createStream(streamName);

                        var maxTopicSize = pinnedProducerCliArgs.maxTopicSize() == 0L
                                ? "server default"
                                : Long.toString(pinnedProducerCliArgs.maxTopicSize());
                        var messageExpiry = pinnedProducerCliArgs.messageExpiry() == 0L
                                ? "never"
                                : Long.toString(pinnedProducerCliArgs.messageExpiry());

                        log.info(
                                "Creating the test topic '{}' for stream '{}' with max topic size: {}, message expiry: {}",
                                topicNames.get(0),
                                streamName,
                                maxTopicSize,
                                messageExpiry);

                        client.topics()
                                .createTopic(
                                        StreamId.of(streamName),
                                        1L,
                                        CompressionAlgorithm.None,
                                        BigInteger.valueOf(pinnedProducerCliArgs.messageExpiry()),
                                        BigInteger.valueOf(pinnedProducerCliArgs.maxTopicSize()),
                                        Optional.empty(),
                                        topicNames.get(0));
                    }
                }
            }
        } catch (RuntimeException exception) {
            throw new BenchmarkException("Failed to provision benchmark resources.", exception);
        }

        return new ProvisionedResources(streamNames, topicNames);
    }
}
