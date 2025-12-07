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

package org.apache.iggy.connector.pinot.config;

import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;

import java.util.Map;

/**
 * Configuration class for Iggy stream ingestion in Pinot.
 * Extracts and validates Iggy-specific properties from Pinot's streamConfigs.
 *
 * <p>Configuration properties (with prefix "stream.iggy."):
 * <ul>
 *   <li>host - Iggy server hostname (required)</li>
 *   <li>port - Iggy server TCP port (default: 8090)</li>
 *   <li>username - Authentication username (default: "iggy")</li>
 *   <li>password - Authentication password (default: "iggy")</li>
 *   <li>stream.id - Iggy stream identifier (required)</li>
 *   <li>topic.id - Iggy topic identifier (required)</li>
 *   <li>consumer.group - Consumer group name (required)</li>
 *   <li>connection.pool.size - TCP connection pool size (default: 4)</li>
 *   <li>poll.batch.size - Number of messages to fetch per poll (default: 100)</li>
 *   <li>enable.tls - Enable TLS encryption (default: false)</li>
 * </ul>
 */
public class IggyStreamConfig {

    private static final String IGGY_PREFIX = "stream.iggy.";

    // Connection properties
    private static final String HOST_KEY = IGGY_PREFIX + "host";
    private static final String PORT_KEY = IGGY_PREFIX + "port";
    private static final String USERNAME_KEY = IGGY_PREFIX + "username";
    private static final String PASSWORD_KEY = IGGY_PREFIX + "password";
    private static final String ENABLE_TLS_KEY = IGGY_PREFIX + "enable.tls";
    private static final String CONNECTION_POOL_SIZE_KEY = IGGY_PREFIX + "connection.pool.size";

    // Stream/Topic properties
    private static final String STREAM_ID_KEY = IGGY_PREFIX + "stream.id";
    private static final String TOPIC_ID_KEY = IGGY_PREFIX + "topic.id";

    // Consumer properties
    private static final String CONSUMER_GROUP_KEY = IGGY_PREFIX + "consumer.group";
    private static final String POLL_BATCH_SIZE_KEY = IGGY_PREFIX + "poll.batch.size";

    // Default values
    private static final int DEFAULT_PORT = 8090;
    private static final String DEFAULT_USERNAME = "iggy";
    private static final String DEFAULT_PASSWORD = "iggy";
    private static final boolean DEFAULT_ENABLE_TLS = false;
    private static final int DEFAULT_CONNECTION_POOL_SIZE = 4;
    private static final int DEFAULT_POLL_BATCH_SIZE = 100;

    private final StreamConfig streamConfig;
    private final Map<String, String> props;

    /**
     * Creates a new Iggy stream configuration from Pinot's StreamConfig.
     *
     * @param streamConfig Pinot stream configuration
     */
    public IggyStreamConfig(StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
        this.props = streamConfig.getStreamConfigsMap();
        validate();
    }

    /**
     * Validates required configuration properties.
     *
     * @throws IllegalArgumentException if required properties are missing
     */
    private void validate() {
        requireProperty(HOST_KEY, "Iggy server host is required");
        requireProperty(STREAM_ID_KEY, "Iggy stream ID is required");
        requireProperty(TOPIC_ID_KEY, "Iggy topic ID is required");
        requireProperty(CONSUMER_GROUP_KEY, "Iggy consumer group is required");
    }

    private void requireProperty(String key, String errorMessage) {
        if (!props.containsKey(key)
                || props.get(key) == null
                || props.get(key).trim().isEmpty()) {
            throw new IllegalArgumentException(errorMessage + " (property: " + key + ")");
        }
    }

    public String getHost() {
        return props.get(HOST_KEY);
    }

    public int getPort() {
        String portStr = props.get(PORT_KEY);
        return portStr != null ? Integer.parseInt(portStr) : DEFAULT_PORT;
    }

    public String getUsername() {
        return props.getOrDefault(USERNAME_KEY, DEFAULT_USERNAME);
    }

    public String getPassword() {
        return props.getOrDefault(PASSWORD_KEY, DEFAULT_PASSWORD);
    }

    public boolean isEnableTls() {
        String tlsStr = props.get(ENABLE_TLS_KEY);
        return tlsStr != null ? Boolean.parseBoolean(tlsStr) : DEFAULT_ENABLE_TLS;
    }

    public int getConnectionPoolSize() {
        String poolSizeStr = props.get(CONNECTION_POOL_SIZE_KEY);
        return poolSizeStr != null ? Integer.parseInt(poolSizeStr) : DEFAULT_CONNECTION_POOL_SIZE;
    }

    public String getStreamId() {
        return props.get(STREAM_ID_KEY);
    }

    public String getTopicId() {
        return props.get(TOPIC_ID_KEY);
    }

    public String getConsumerGroup() {
        return props.get(CONSUMER_GROUP_KEY);
    }

    public int getPollBatchSize() {
        String batchSizeStr = props.get(POLL_BATCH_SIZE_KEY);
        return batchSizeStr != null ? Integer.parseInt(batchSizeStr) : DEFAULT_POLL_BATCH_SIZE;
    }

    /**
     * Gets the offset specification from Pinot's consumer config.
     *
     * @return offset criteria
     */
    public OffsetCriteria getOffsetCriteria() {
        return streamConfig.getOffsetCriteria();
    }

    /**
     * Gets the Pinot table name for this stream.
     *
     * @return table name with type suffix
     */
    public String getTableNameWithType() {
        return streamConfig.getTableNameWithType();
    }

    /**
     * Creates server address in format "host:port".
     *
     * @return server address string
     */
    public String getServerAddress() {
        return getHost() + ":" + getPort();
    }

    @Override
    public String toString() {
        return "IggyStreamConfig{"
                + "host='"
                + getHost()
                + '\''
                + ", port="
                + getPort()
                + ", username='"
                + getUsername()
                + '\''
                + ", streamId='"
                + getStreamId()
                + '\''
                + ", topicId='"
                + getTopicId()
                + '\''
                + ", consumerGroup='"
                + getConsumerGroup()
                + '\''
                + ", enableTls="
                + isEnableTls()
                + ", connectionPoolSize="
                + getConnectionPoolSize()
                + ", pollBatchSize="
                + getPollBatchSize()
                + '}';
    }
}
