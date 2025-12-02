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

package org.apache.iggy.benchmark.poll;

import org.apache.iggy.benchmark.util.IggyTestContainer;
import org.apache.iggy.benchmark.util.MessageFixtures;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class BasePollBenchmark {

    protected static final int MESSAGE_PAYLOAD_BYTES = 1000;

    protected static IggyTestContainer iggyContainer;

    private static final String STREAM_NAME = "bench-stream-1";
    private static final String TOPIC_NAME = "topic-1";
    private static final long PARTITION_COUNT = 1;

    @Param({"1", "10", "100", "1000"})
    public long messagesPerPoll;

    protected StreamId streamId;
    protected TopicId topicId;
    protected Consumer consumer;
    protected BigInteger currentOffset = BigInteger.ZERO;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        startContainer();

        consumer = Consumer.of(1L);
        streamId = StreamId.of(STREAM_NAME);
        topicId = TopicId.of(TOPIC_NAME);

        setupClient();
        initializeInfrastructure();
        populateTestData();
    }

    protected void startContainer() {
        if (iggyContainer == null || !iggyContainer.isRunning()) {
            iggyContainer = IggyTestContainer.start();
        }
    }

    @Setup(Level.Iteration)
    public void resetOffset() {
        currentOffset = BigInteger.ZERO;
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        cleanupInfrastructure();
        teardownClient();
        stopContainer();
    }

    protected void stopContainer() {
        if (iggyContainer != null) {
            iggyContainer.stop();
        }
    }

    protected void initializeInfrastructure() {
        IggyBaseClient mgmtClient = getManagementClient();
        assert mgmtClient != null;

        mgmtClient.streams().createStream(STREAM_NAME);
        mgmtClient
                .topics()
                .createTopic(
                        StreamId.of(STREAM_NAME),
                        PARTITION_COUNT,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        TOPIC_NAME);
    }

    protected void populateTestData() {
        IggyBaseClient mgmtClient = getManagementClient();
        assert mgmtClient != null;

        List<Message> messages = MessageFixtures.generateMessages((int) messagesPerPoll, MESSAGE_PAYLOAD_BYTES);
        mgmtClient.messages().sendMessages(streamId, topicId, Partitioning.partitionId(0L), messages);
    }

    protected void cleanupInfrastructure() {
        IggyBaseClient mgmtClient = getManagementClient();
        assert mgmtClient != null;

        mgmtClient.streams().deleteStream(StreamId.of(STREAM_NAME));
    }

    protected abstract void setupClient() throws Exception;

    protected abstract void teardownClient() throws Exception;

    protected abstract IggyBaseClient getManagementClient();
}
