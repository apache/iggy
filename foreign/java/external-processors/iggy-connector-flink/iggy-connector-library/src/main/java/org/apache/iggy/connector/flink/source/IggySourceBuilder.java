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

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.config.OffsetConfig;
import org.apache.iggy.connector.serialization.DeserializationSchema;

import java.io.Serializable;

/**
 * Builder for creating IggySource instances with fluent API.
 *
 * @param <T> the type of records produced by the source
 */
public class IggySourceBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private IggyConnectionConfig connectionConfig;
    private String streamId;
    private String topicId;
    private String consumerGroupName; // Store as String for serialization
    private DeserializationSchema<T> deserializer;
    private OffsetConfig offsetConfig = OffsetConfig.builder().build();
    private long pollBatchSize = 100L;

    /**
     * Sets the connection configuration.
     *
     * @param connectionConfig the connection configuration
     * @return this builder
     */
    public IggySourceBuilder<T> setConnectionConfig(IggyConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    /**
     * Sets the stream identifier.
     *
     * @param streamId the stream ID (numeric or string)
     * @return this builder
     */
    public IggySourceBuilder<T> setStreamId(String streamId) {
        this.streamId = streamId;
        return this;
    }

    /**
     * Sets the stream identifier (numeric).
     *
     * @param streamId the stream ID
     * @return this builder
     */
    public IggySourceBuilder<T> setStreamId(long streamId) {
        this.streamId = String.valueOf(streamId);
        return this;
    }

    /**
     * Sets the topic identifier.
     *
     * @param topicId the topic ID (numeric or string)
     * @return this builder
     */
    public IggySourceBuilder<T> setTopicId(String topicId) {
        this.topicId = topicId;
        return this;
    }

    /**
     * Sets the topic identifier (numeric).
     *
     * @param topicId the topic ID
     * @return this builder
     */
    public IggySourceBuilder<T> setTopicId(long topicId) {
        this.topicId = String.valueOf(topicId);
        return this;
    }

    /**
     * Sets the consumer group name.
     *
     * @param consumerGroup the consumer group name
     * @return this builder
     */
    public IggySourceBuilder<T> setConsumerGroup(String consumerGroup) {
        this.consumerGroupName = consumerGroup;
        return this;
    }

    /**
     * Sets the deserialization schema.
     *
     * @param deserializer the deserialization schema
     * @return this builder
     */
    public IggySourceBuilder<T> setDeserializer(DeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Sets the offset configuration.
     *
     * @param offsetConfig the offset configuration
     * @return this builder
     */
    public IggySourceBuilder<T> setOffsetConfig(OffsetConfig offsetConfig) {
        this.offsetConfig = offsetConfig;
        return this;
    }

    /**
     * Sets the number of messages to fetch per poll.
     *
     * @param pollBatchSize the batch size
     * @return this builder
     */
    public IggySourceBuilder<T> setPollBatchSize(long pollBatchSize) {
        if (pollBatchSize <= 0) {
            throw new IllegalArgumentException("pollBatchSize must be > 0");
        }
        this.pollBatchSize = pollBatchSize;
        return this;
    }

    /**
     * Builds the IggySource instance.
     *
     * @return the configured IggySource
     * @throws IllegalStateException if required configuration is missing
     */
    public IggySource<T> build() {
        validate();

        return new IggySource<>(
                connectionConfig, streamId, topicId, consumerGroupName, deserializer, offsetConfig, pollBatchSize);
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
        if (StringUtils.isBlank(streamId)) {
            throw new IllegalStateException("streamId is required");
        }
        if (StringUtils.isBlank(topicId)) {
            throw new IllegalStateException("topicId is required");
        }
        if (StringUtils.isBlank(consumerGroupName)) {
            throw new IllegalStateException("consumerGroup is required (use setConsumerGroup)");
        }
        if (deserializer == null) {
            throw new IllegalStateException("deserializer is required");
        }
        if (offsetConfig == null) {
            throw new IllegalStateException("offsetConfig cannot be null");
        }
    }
}
