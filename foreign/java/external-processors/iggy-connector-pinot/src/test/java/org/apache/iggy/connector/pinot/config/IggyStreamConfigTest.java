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

import org.apache.pinot.spi.stream.StreamConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IggyStreamConfigTest {

    @Test
    void testValidConfiguration() {
        Map<String, String> props = createValidConfig();
        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        assertThat(config.getHost()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(8090);
        assertThat(config.getUsername()).isEqualTo("iggy");
        assertThat(config.getPassword()).isEqualTo("iggy");
        assertThat(config.getStreamId()).isEqualTo("analytics");
        assertThat(config.getTopicId()).isEqualTo("events");
        assertThat(config.getConsumerGroup()).isEqualTo("test-consumer-group");
        assertThat(config.getPollBatchSize()).isEqualTo(100);
        assertThat(config.getConnectionPoolSize()).isEqualTo(4);
        assertThat(config.isEnableTls()).isFalse();
    }

    @Test
    void testCustomConfiguration() {
        Map<String, String> props = createValidConfig();
        props.put("stream.iggy.port", "9090");
        props.put("stream.iggy.username", "custom-user");
        props.put("stream.iggy.password", "custom-pass");
        props.put("stream.iggy.poll.batch.size", "500");
        props.put("stream.iggy.connection.pool.size", "8");
        props.put("stream.iggy.enable.tls", "true");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        assertThat(config.getPort()).isEqualTo(9090);
        assertThat(config.getUsername()).isEqualTo("custom-user");
        assertThat(config.getPassword()).isEqualTo("custom-pass");
        assertThat(config.getPollBatchSize()).isEqualTo(500);
        assertThat(config.getConnectionPoolSize()).isEqualTo(8);
        assertThat(config.isEnableTls()).isTrue();
    }

    @Test
    void testMissingHostThrowsException() {
        Map<String, String> props = createValidConfig();
        props.remove("stream.iggy.host");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);

        assertThatThrownBy(() -> new IggyStreamConfig(streamConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host");
    }

    @Test
    void testMissingStreamIdThrowsException() {
        Map<String, String> props = createValidConfig();
        props.remove("stream.iggy.stream.id");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);

        assertThatThrownBy(() -> new IggyStreamConfig(streamConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stream ID");
    }

    @Test
    void testMissingTopicIdThrowsException() {
        Map<String, String> props = createValidConfig();
        props.remove("stream.iggy.topic.id");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);

        assertThatThrownBy(() -> new IggyStreamConfig(streamConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic ID");
    }

    @Test
    void testMissingConsumerGroupThrowsException() {
        Map<String, String> props = createValidConfig();
        props.remove("stream.iggy.consumer.group");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);

        assertThatThrownBy(() -> new IggyStreamConfig(streamConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumer group");
    }

    @Test
    void testServerAddress() {
        Map<String, String> props = createValidConfig();
        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        assertThat(config.getServerAddress()).isEqualTo("localhost:8090");
    }

    @Test
    void testTableNameWithType() {
        Map<String, String> props = createValidConfig();
        StreamConfig streamConfig = new StreamConfig("events_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        assertThat(config.getTableNameWithType()).isEqualTo("events_REALTIME");
    }

    @Test
    void testToString() {
        Map<String, String> props = createValidConfig();
        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        String str = config.toString();
        assertThat(str)
                .contains("localhost")
                .contains("8090")
                .contains("analytics")
                .contains("events")
                .contains("test-consumer-group");
    }

    @Test
    void testNumericStreamAndTopicIds() {
        Map<String, String> props = createValidConfig();
        props.put("stream.iggy.stream.id", "123");
        props.put("stream.iggy.topic.id", "456");

        StreamConfig streamConfig = new StreamConfig("test_table_REALTIME", props);
        IggyStreamConfig config = new IggyStreamConfig(streamConfig);

        assertThat(config.getStreamId()).isEqualTo("123");
        assertThat(config.getTopicId()).isEqualTo("456");
    }

    private Map<String, String> createValidConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("streamType", "iggy"); // Required by Pinot StreamConfig
        props.put("stream.iggy.topic.name", "events"); // Required by Pinot StreamConfig
        props.put("stream.iggy.consumer.type", "lowlevel"); // Required by Pinot
        props.put(
                "stream.iggy.consumer.factory.class.name",
                "org.apache.iggy.connector.pinot.consumer.IggyConsumerFactory");
        props.put("stream.iggy.decoder.class.name", "org.apache.iggy.connector.pinot.decoder.IggyJsonMessageDecoder");

        props.put("stream.iggy.host", "localhost");
        props.put("stream.iggy.port", "8090");
        props.put("stream.iggy.username", "iggy");
        props.put("stream.iggy.password", "iggy");
        props.put("stream.iggy.stream.id", "analytics");
        props.put("stream.iggy.topic.id", "events");
        props.put("stream.iggy.consumer.group", "test-consumer-group");
        props.put("stream.iggy.poll.batch.size", "100");
        props.put("stream.iggy.connection.pool.size", "4");
        props.put("stream.iggy.enable.tls", "false");
        return props;
    }
}
