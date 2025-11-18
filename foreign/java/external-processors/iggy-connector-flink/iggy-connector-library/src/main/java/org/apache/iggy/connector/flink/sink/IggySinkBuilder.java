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

import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.serialization.SerializationSchema;

import java.io.Serializable;
import java.time.Duration;

/**
 * Builder for creating IggySink instances with fluent API.
 *
 * @param <T> the type of records to write
 */
public class IggySinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private IggyConnectionConfig connectionConfig;
    private String streamId;
    private String topicId;
    private SerializationSchema<T> serializer;
    private int batchSize = 100;
    private Duration flushInterval = Duration.ofSeconds(5);
    private IggySinkWriter.PartitioningStrategy partitioningStrategy = IggySinkWriter.PartitioningStrategy.BALANCED;

    /**
     * Sets the connection configuration.
     *
     * @param connectionConfig the connection configuration
     * @return this builder
     */
    public IggySinkBuilder<T> setConnectionConfig(IggyConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    /**
     * Sets the stream identifier.
     *
     * @param streamId the stream ID (numeric or string)
     * @return this builder
     */
    public IggySinkBuilder<T> setStreamId(String streamId) {
        this.streamId = streamId;
        return this;
    }

    /**
     * Sets the stream identifier (numeric).
     *
     * @param streamId the stream ID
     * @return this builder
     */
    public IggySinkBuilder<T> setStreamId(long streamId) {
        this.streamId = String.valueOf(streamId);
        return this;
    }

    /**
     * Sets the topic identifier.
     *
     * @param topicId the topic ID (numeric or string)
     * @return this builder
     */
    public IggySinkBuilder<T> setTopicId(String topicId) {
        this.topicId = topicId;
        return this;
    }

    /**
     * Sets the topic identifier (numeric).
     *
     * @param topicId the topic ID
     * @return this builder
     */
    public IggySinkBuilder<T> setTopicId(long topicId) {
        this.topicId = String.valueOf(topicId);
        return this;
    }

    /**
     * Sets the serialization schema.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public IggySinkBuilder<T> setSerializer(SerializationSchema<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    /**
     * Sets the batch size (number of records to buffer before flushing).
     *
     * @param batchSize the batch size
     * @return this builder
     */
    public IggySinkBuilder<T> setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Sets the flush interval (maximum time to wait before flushing).
     *
     * @param flushInterval the flush interval
     * @return this builder
     */
    public IggySinkBuilder<T> setFlushInterval(Duration flushInterval) {
        if (flushInterval == null || flushInterval.isNegative()) {
            throw new IllegalArgumentException("flushInterval must be positive");
        }
        this.flushInterval = flushInterval;
        return this;
    }

    /**
     * Sets the partitioning strategy.
     *
     * @param partitioningStrategy the partitioning strategy
     * @return this builder
     */
    public IggySinkBuilder<T> setPartitioningStrategy(IggySinkWriter.PartitioningStrategy partitioningStrategy) {
        this.partitioningStrategy = partitioningStrategy;
        return this;
    }

    /**
     * Enables balanced partitioning (round-robin across partitions).
     *
     * @return this builder
     */
    public IggySinkBuilder<T> withBalancedPartitioning() {
        this.partitioningStrategy = IggySinkWriter.PartitioningStrategy.BALANCED;
        return this;
    }

    /**
     * Enables partition ID based partitioning (uses partition key from records).
     *
     * @return this builder
     */
    public IggySinkBuilder<T> withPartitionIdPartitioning() {
        this.partitioningStrategy = IggySinkWriter.PartitioningStrategy.PARTITION_ID;
        return this;
    }

    /**
     * Enables message key based partitioning (uses partition key as message key).
     *
     * @return this builder
     */
    public IggySinkBuilder<T> withMessageKeyPartitioning() {
        this.partitioningStrategy = IggySinkWriter.PartitioningStrategy.MESSAGE_KEY;
        return this;
    }

    /**
     * Builds the IggySink instance.
     *
     * @return the configured IggySink
     * @throws IllegalStateException if required configuration is missing
     */
    public IggySink<T> build() {
        validate();

        return new IggySink<>(
                connectionConfig, streamId, topicId, serializer, batchSize, flushInterval, partitioningStrategy);
    }

    /**
     * Validates the builder configuration.
     *
     * @throws IllegalStateException if required configuration is missing
     */
    private void validate() {
        if (connectionConfig == null) {
            throw new IllegalStateException("connectionConfig is required");
        }
        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalStateException("streamId is required");
        }
        if (topicId == null || topicId.isEmpty()) {
            throw new IllegalStateException("topicId is required");
        }
        if (serializer == null) {
            throw new IllegalStateException("serializer is required");
        }
    }
}
