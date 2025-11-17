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

package org.apache.iggy.connector.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.config.OffsetConfig;
import org.apache.iggy.consumergroup.Consumer;

import java.io.Serializable;

/**
 * Flink Source implementation for reading from Iggy streams.
 * Implements the Flink Source API for integration with DataStream API.
 *
 * <p>Example usage:
 * <pre>{@code
 * DataStream<Event> events = env.fromSource(
 *     IggySource.<Event>builder()
 *         .setConnectionConfig(connectionConfig)
 *         .setStreamId("my-stream")
 *         .setTopicId("my-topic")
 *         .setConsumerGroup("flink-job-1")
 *         .setDeserializer(new JsonDeserializationSchema<>(Event.class))
 *         .build(),
 *     WatermarkStrategy.noWatermarks(),
 *     "Iggy Source"
 * );
 * }</pre>
 *
 * @param <T> the type of records produced by this source
 */
public class IggySource<T> implements Source<T, IggySourceSplit, IggySourceEnumeratorState>, Serializable {

    private static final long serialVersionUID = 1L;

    private final IggyConnectionConfig connectionConfig;
    private final String streamId;
    private final String topicId;
    private final String consumerGroupName; // Serializable representation
    private final org.apache.iggy.connector.serialization.DeserializationSchema<T> deserializer;
    private final OffsetConfig offsetConfig;
    private final long pollBatchSize;

    /**
     * Creates a new Iggy source.
     * Use {@link #builder()} to construct instances.
     *
     * @param connectionConfig the connection configuration
     * @param streamId the stream identifier
     * @param topicId the topic identifier
     * @param consumerGroupName the consumer group name
     * @param deserializer the deserialization schema
     * @param offsetConfig the offset configuration
     * @param pollBatchSize the number of messages to fetch per poll
     */
    public IggySource(
            IggyConnectionConfig connectionConfig,
            String streamId,
            String topicId,
            String consumerGroupName,
            org.apache.iggy.connector.serialization.DeserializationSchema<T> deserializer,
            OffsetConfig offsetConfig,
            long pollBatchSize) {

        this.connectionConfig = connectionConfig;
        this.streamId = streamId;
        this.topicId = topicId;
        this.consumerGroupName = consumerGroupName;
        this.deserializer = deserializer;
        this.offsetConfig = offsetConfig;
        this.pollBatchSize = pollBatchSize;
    }

    /**
     * Creates a new builder for configuring the source.
     *
     * @param <T> the type of records produced by the source
     * @return a new builder instance
     */
    public static <T> IggySourceBuilder<T> builder() {
        return new IggySourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, IggySourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        AsyncIggyTcpClient asyncClient = createAsyncIggyClient();
        Consumer consumer = createConsumer();
        return new IggySourceReader<>(readerContext, asyncClient, deserializer, consumer, pollBatchSize);
    }

    @Override
    public SplitEnumerator<IggySourceSplit, IggySourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<IggySourceSplit> enumContext) throws Exception {

        AsyncIggyTcpClient asyncClient = createAsyncIggyClient();
        return new IggySourceSplitEnumerator(
                enumContext, asyncClient, streamId, topicId, consumerGroupName, offsetConfig, null);
    }

    @Override
    public SplitEnumerator<IggySourceSplit, IggySourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<IggySourceSplit> enumContext, IggySourceEnumeratorState checkpoint)
            throws Exception {

        AsyncIggyTcpClient asyncClient = createAsyncIggyClient();
        return new IggySourceSplitEnumerator(
                enumContext, asyncClient, streamId, topicId, consumerGroupName, offsetConfig, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<IggySourceSplit> getSplitSerializer() {
        return new IggySourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<IggySourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new IggySourceEnumeratorStateSerializer();
    }

    /**
     * Creates an async Iggy TCP client based on connection configuration.
     *
     * @return configured async Iggy client
     */
    private AsyncIggyTcpClient createAsyncIggyClient() {
        try {
            // Parse host and port from server address
            String serverAddress = connectionConfig.getServerAddress();
            String host;
            int port = 8090; // Default TCP port

            if (serverAddress.contains(":")) {
                String[] parts = serverAddress.split(":");
                host = parts[0];
                port = Integer.parseInt(parts[1]);
            } else {
                host = serverAddress;
            }

            // Create async TCP client using builder pattern
            AsyncIggyTcpClient asyncClient = AsyncIggyTcpClient.builder()
                    .host(host)
                    .port(port)
                    .credentials(connectionConfig.getUsername(), connectionConfig.getPassword())
                    .connectionPoolSize(4)
                    .build();

            // Connect and block (auto-login happens during connect)
            asyncClient.connect().join();

            return asyncClient;

        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create async Iggy client", e);
        }
    }

    /**
     * Creates a Consumer object from the stored consumer group name.
     *
     * @return configured Consumer
     */
    private Consumer createConsumer() {
        return Consumer.group(org.apache.iggy.identifier.ConsumerId.of(consumerGroupName));
    }

    /**
     * Gets the connection configuration.
     *
     * @return connection configuration
     */
    public IggyConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * Gets the stream identifier.
     *
     * @return stream ID
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Gets the topic identifier.
     *
     * @return topic ID
     */
    public String getTopicId() {
        return topicId;
    }

    /**
     * Gets the consumer group name.
     *
     * @return consumer group name
     */
    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    /**
     * Gets the deserialization schema.
     *
     * @return deserializer
     */
    public org.apache.iggy.connector.serialization.DeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /**
     * Gets the offset configuration.
     *
     * @return offset config
     */
    public OffsetConfig getOffsetConfig() {
        return offsetConfig;
    }

    /**
     * Gets the poll batch size.
     *
     * @return batch size
     */
    public long getPollBatchSize() {
        return pollBatchSize;
    }
}
