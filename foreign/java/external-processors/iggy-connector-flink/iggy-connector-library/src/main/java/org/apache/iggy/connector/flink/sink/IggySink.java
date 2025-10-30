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

package org.apache.iggy.connector.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.serialization.SerializationSchema;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

/**
 * Flink Sink implementation for writing to Iggy streams.
 * Implements the Flink Sink V2 API for integration with DataStream API.
 *
 * <p>Example usage:
 * <pre>{@code
 * events.sinkTo(
 *     IggySink.<Event>builder()
 *         .setConnectionConfig(connectionConfig)
 *         .setStreamId("my-stream")
 *         .setTopicId("my-topic")
 *         .setSerializer(new JsonSerializationSchema<>())
 *         .setBatchSize(100)
 *         .setFlushInterval(Duration.ofSeconds(5))
 *         .withBalancedPartitioning()
 *         .build()
 * ).name("Iggy Sink");
 * }</pre>
 *
 * @param <T> the type of records to write
 */
public class IggySink<T> implements Sink<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private final IggyConnectionConfig connectionConfig;
    private final String streamId;
    private final String topicId;
    private final SerializationSchema<T> serializer;
    private final int batchSize;
    private final Duration flushInterval;
    private final IggySinkWriter.PartitioningStrategy partitioningStrategy;

    /**
     * Creates a new Iggy sink.
     * Use {@link #builder()} to construct instances.
     *
     * @param connectionConfig the connection configuration
     * @param streamId the stream identifier
     * @param topicId the topic identifier
     * @param serializer the serialization schema
     * @param batchSize the batch size for buffering
     * @param flushInterval the maximum flush interval
     * @param partitioningStrategy the partitioning strategy
     */
    public IggySink(
            IggyConnectionConfig connectionConfig,
            String streamId,
            String topicId,
            SerializationSchema<T> serializer,
            int batchSize,
            Duration flushInterval,
            IggySinkWriter.PartitioningStrategy partitioningStrategy) {

        this.connectionConfig = connectionConfig;
        this.streamId = streamId;
        this.topicId = topicId;
        this.serializer = serializer;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.partitioningStrategy = partitioningStrategy;
    }

    /**
     * Creates a new builder for configuring the sink.
     *
     * @param <T> the type of records to write
     * @return a new builder instance
     */
    public static <T> IggySinkBuilder<T> builder() {
        return new IggySinkBuilder<>();
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        IggyHttpClient httpClient = createHttpClient();
        return new IggySinkWriter<>(
                httpClient,
                streamId,
                topicId,
                serializer,
                batchSize,
                flushInterval,
                partitioningStrategy);
    }

    /**
     * Creates an HTTP Iggy client based on connection configuration.
     *
     * @return configured HTTP Iggy client
     */
    private IggyHttpClient createHttpClient() {
        try {
            // Build HTTP URL from server address
            String serverAddress = connectionConfig.getServerAddress();
            String httpUrl;

            // If serverAddress already has http:// or https://, use it as is
            if (serverAddress.startsWith("http://") || serverAddress.startsWith("https://")) {
                httpUrl = serverAddress;
            } else {
                // Extract host and replace TCP port 8090 with HTTP port 3000
                String host;
                if (serverAddress.contains(":")) {
                    // Extract just the host part (before the port)
                    host = serverAddress.substring(0, serverAddress.indexOf(":"));
                } else {
                    host = serverAddress;
                }
                // HTTP server runs on port 3000
                httpUrl = "http://" + host + ":3000";
            }

            // Create HTTP client
            IggyHttpClient httpClient = new IggyHttpClient(httpUrl);

            // Login
            httpClient.users().login(
                connectionConfig.getUsername(),
                connectionConfig.getPassword()
            );

            return httpClient;

        } catch (Exception e) {
            throw new RuntimeException("Failed to create HTTP Iggy client", e);
        }
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
     * Gets the serialization schema.
     *
     * @return serializer
     */
    public SerializationSchema<T> getSerializer() {
        return serializer;
    }

    /**
     * Gets the batch size.
     *
     * @return batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Gets the flush interval.
     *
     * @return flush interval
     */
    public Duration getFlushInterval() {
        return flushInterval;
    }

    /**
     * Gets the partitioning strategy.
     *
     * @return partitioning strategy
     */
    public IggySinkWriter.PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }
}
