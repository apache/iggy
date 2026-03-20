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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.system.CacheMetricsKey;
import org.apache.iggy.system.Stats;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectMapperTest {

    private final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();

    @Nested
    class Deserialization {

        @Nested
        @DisplayName("Stats")
        class StatsDeserialization {

            @Test
            void shouldDeserializeAllStatsFields() {
                // given
                String json = """
                        {
                          "process_id": 620474,
                          "cpu_usage": 0.46409693,
                          "total_cpu_usage": 11.344592,
                          "memory_usage": "147412 KiB",
                          "total_memory": "65766576 KiB",
                          "available_memory": "28197044 KiB",
                          "run_time": 300000000,
                          "start_time": 1773843141000000,
                          "read_bytes": "255156 KiB",
                          "written_bytes": "1027432 KiB",
                          "messages_size_bytes": "69.092 KB",
                          "streams_count": 1,
                          "topics_count": 2,
                          "partitions_count": 2,
                          "segments_count": 2,
                          "messages_count": 521,
                          "clients_count": 2,
                          "consumer_groups_count": 0,
                          "hostname": "localhost",
                          "os_name": "Ubuntu",
                          "os_version": "Linux (Ubuntu 24.04)",
                          "kernel_version": "6.18.7-76061807-generic",
                          "iggy_server_version": "0.7.2-edge.1",
                          "iggy_server_semver": 7002,
                          "cache_metrics": {
                            "1-1-1": {"hits": 100, "misses": 10, "hit_ratio": 0.91}
                          },
                          "threads_count": 13,
                          "free_disk_space": "150073704 KiB",
                          "total_disk_space": "959392552 KiB"
                        }
                        """;

                // when
                var stats = objectMapper.readValue(json, Stats.class);

                // then
                assertThat(stats.processId()).isEqualTo(620474L);
                assertThat(stats.cpuUsage()).isCloseTo(0.46409693f, Offset.offset(0.0001f));
                assertThat(stats.totalCpuUsage()).isCloseTo(11.344592f, Offset.offset(0.0001f));
                assertThat(stats.memoryUsage()).isEqualTo("147412 KiB");
                assertThat(stats.streamsCount()).isEqualTo(1L);
                assertThat(stats.hostname()).isEqualTo("localhost");
                assertThat(stats.osName()).isEqualTo("Ubuntu");
                assertThat(stats.iggyServerVersion()).isEqualTo("0.7.2-edge.1");
                assertThat(stats.iggyServerSemver()).isPresent().hasValue(7002L);
                var key = new CacheMetricsKey(1L, 1L, 1L);
                assertThat(stats.cacheMetrics()).hasSize(1);
                assertThat(stats.cacheMetrics()).containsKey(key);
                assertThat(stats.cacheMetrics().get(key).hits()).isEqualTo(BigInteger.valueOf(100));
                assertThat(stats.cacheMetrics().get(key).misses()).isEqualTo(BigInteger.valueOf(10));
                assertThat(stats.cacheMetrics().get(key).hitRatio()).isCloseTo(0.91f, Offset.offset(0.01f));
                assertThat(stats.threadsCount()).isEqualTo(13L);
                assertThat(stats.freeDiskSpace()).isEqualTo("150073704 KiB");
                assertThat(stats.totalDiskSpace()).isEqualTo("959392552 KiB");
            }

            @Test
            void shouldDeserializeStatsWithNullSemverAndEmptyCacheMetrics() {
                // given
                String json = """
                        {
                          "process_id": 1234,
                          "cpu_usage": 12.5,
                          "total_cpu_usage": 50.0,
                          "memory_usage": "1000000",
                          "total_memory": "8000000",
                          "available_memory": "7000000",
                          "run_time": 3600,
                          "start_time": 1000000,
                          "read_bytes": "500",
                          "written_bytes": "600",
                          "messages_size_bytes": "1000",
                          "streams_count": 5,
                          "topics_count": 10,
                          "partitions_count": 20,
                          "segments_count": 100,
                          "messages_count": 5000,
                          "clients_count": 3,
                          "consumer_groups_count": 2,
                          "hostname": "localhost",
                          "os_name": "Linux",
                          "os_version": "5.4.0",
                          "kernel_version": "5.4.0-1",
                          "iggy_server_version": "0.6.1",
                          "iggy_server_semver": null,
                          "cache_metrics": {},
                          "threads_count": 8,
                          "free_disk_space": "250000000000",
                          "total_disk_space": "500000000000"
                        }
                        """;

                // when
                var stats = objectMapper.readValue(json, Stats.class);

                // then
                assertThat(stats.iggyServerVersion()).isEqualTo("0.6.1");
                assertThat(stats.iggyServerSemver()).isEmpty();
                assertThat(stats.cacheMetrics()).isEmpty();
                assertThat(stats.threadsCount()).isEqualTo(8L);
                assertThat(stats.freeDiskSpace()).isEqualTo("250000000000");
                assertThat(stats.totalDiskSpace()).isEqualTo("500000000000");
            }
        }

        @Nested
        @DisplayName("PolledMessages")
        class PolledMessagesDeserialization {

            @Test
            void shouldDeserializePolledMessagesWithEmptyUserHeaders() {
                // given
                String json = """
                        {
                          "partition_id": 1,
                          "current_offset": 10,
                          "count": 1,
                          "messages": [
                            {
                              "header": {
                                "checksum": 0,
                                "id": 42,
                                "offset": 0,
                                "timestamp": 0,
                                "origin_timestamp": 1000,
                                "user_headers_length": 0,
                                "payload_length": 4,
                                "reserved": 0
                              },
                              "payload": "dGVzdA==",
                              "user_headers": []
                            }
                          ]
                        }
                        """;

                // when
                var polledMessages = objectMapper.readValue(json, PolledMessages.class);

                // then
                assertThat(polledMessages).isNotNull();
                assertThat(polledMessages.messages()).hasSize(1);
                assertThat(polledMessages.messages().get(0).userHeaders()).isEmpty();
            }

            @Test
            void shouldDeserializePolledMessagesWithUserHeaders() {
                // given
                String json = """
                        {
                          "partition_id": 1,
                          "current_offset": 10,
                          "count": 1,
                          "messages": [
                            {
                              "header": {
                                "checksum": 0,
                                "id": 42,
                                "offset": 0,
                                "timestamp": 0,
                                "origin_timestamp": 1000,
                                "user_headers_length": 62,
                                "payload_length": 4,
                                "reserved": 0
                              },
                              "payload": "dGVzdA==",
                              "user_headers": [
                                {
                                  "key": {"kind": "string", "value": "Y29udGVudC10eXBl"},
                                  "value": {"kind": "string", "value": "dGV4dC9wbGFpbg=="}
                                }
                              ]
                            }
                          ]
                        }
                        """;

                // when
                var polledMessages = objectMapper.readValue(json, PolledMessages.class);

                // then
                assertThat(polledMessages).isNotNull();
                assertThat(polledMessages.messages()).hasSize(1);
                var headers = polledMessages.messages().get(0).userHeaders();
                assertThat(headers).hasSize(1);
                var header = headers.entrySet().iterator().next();
                assertThat(header.getKey().toString()).isEqualTo("content-type");
                assertThat(header.getValue().toString()).isEqualTo("text/plain");
            }
        }

        @Nested
        @DisplayName("Payload")
        class PayloadDeserialization {

            @Test
            void shouldDeserializeBase64EncodedPayloadToBytes() {
                // given
                String expectedPayload = "test";
                String base64Payload =
                        Base64.getEncoder().encodeToString(expectedPayload.getBytes(StandardCharsets.UTF_8));
                String json = createMessageJson(base64Payload);

                // when
                var polledMessages = objectMapper.readValue(json, PolledMessages.class);

                // then
                byte[] actualPayload = polledMessages.messages().get(0).payload();
                assertThat(actualPayload).isEqualTo(expectedPayload.getBytes(StandardCharsets.UTF_8));
            }

            @Test
            void shouldDeserializeEmptyPayload() {
                // given
                String base64Payload = Base64.getEncoder().encodeToString(new byte[0]);
                String json = createMessageJson(base64Payload);

                // when
                var polledMessages = objectMapper.readValue(json, PolledMessages.class);

                // then
                byte[] actualPayload = polledMessages.messages().get(0).payload();
                assertThat(actualPayload).isEmpty();
            }

            @Test
            void shouldDeserializeBinaryPayload() {
                // given
                byte[] binaryData = new byte[] {0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};
                String base64Payload = Base64.getEncoder().encodeToString(binaryData);
                String json = createMessageJson(base64Payload);

                // when
                var polledMessages = objectMapper.readValue(json, PolledMessages.class);

                // then
                byte[] actualPayload = polledMessages.messages().get(0).payload();
                assertThat(actualPayload).isEqualTo(binaryData);
            }

            private String createMessageJson(String base64Payload) {
                return """
                        {
                          "partition_id": 1,
                          "current_offset": 0,
                          "count": 1,
                          "messages": [
                            {
                              "header": {
                                "checksum": 0,
                                "id": 1,
                                "offset": 0,
                                "timestamp": 0,
                                "origin_timestamp": 0,
                                "user_headers_length": 0,
                                "payload_length": 4,
                                "reserved": 0
                              },
                              "payload": "%s",
                              "user_headers": []
                            }
                          ]
                        }
                        """.formatted(base64Payload);
            }
        }
    }

    @Nested
    class Serialization {

        @Nested
        @DisplayName("Payload")
        class PayloadSerialization {

            @Test
            void shouldSerializePayloadToBase64() {
                // given
                String payloadContent = "test";
                Message message = Message.of(payloadContent);

                // when
                String json = objectMapper.writeValueAsString(message);

                // then
                String expectedBase64 =
                        Base64.getEncoder().encodeToString(payloadContent.getBytes(StandardCharsets.UTF_8));
                assertThat(json).contains("\"payload\":\"" + expectedBase64 + "\"");
            }

            @Test
            void shouldSerializeEmptyPayloadToBase64() {
                // given
                String payloadContent = "";
                Message message = Message.of(payloadContent);

                // when
                String json = objectMapper.writeValueAsString(message);

                // then
                String expectedBase64 = Base64.getEncoder().encodeToString(new byte[0]);
                assertThat(json).contains("\"payload\":\"" + expectedBase64 + "\"");
            }

            @Test
            void shouldSerializeBinaryPayloadToBase64() {
                // given
                byte[] binaryData = new byte[] {0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};
                Message message = Message.of("placeholder");
                Message binaryMessage = new Message(message.header(), binaryData, message.userHeaders());

                // when
                String json = objectMapper.writeValueAsString(binaryMessage);

                // then
                String expectedBase64 = Base64.getEncoder().encodeToString(binaryData);
                assertThat(json).contains("\"payload\":\"" + expectedBase64 + "\"");
            }
        }
    }

    @Nested
    class Roundtrip {

        @Test
        void shouldRoundtripTextPayload() {
            // given
            String payloadContent = "Hello, World!";
            Message originalMessage = Message.of(payloadContent);

            // when
            String json = objectMapper.writeValueAsString(originalMessage);
            Message deserializedMessage = objectMapper.readValue(json, Message.class);

            // then
            assertThat(deserializedMessage.payload()).isEqualTo(originalMessage.payload());
        }

        @Test
        void shouldRoundtripBinaryPayload() {
            // given
            byte[] binaryData = new byte[] {0x00, 0x01, 0x02, (byte) 0x80, (byte) 0xFF};
            Message originalMessage = Message.of("placeholder");
            Message binaryMessage = new Message(originalMessage.header(), binaryData, originalMessage.userHeaders());

            // when
            String json = objectMapper.writeValueAsString(binaryMessage);
            Message deserializedMessage = objectMapper.readValue(json, Message.class);

            // then
            assertThat(deserializedMessage.payload()).isEqualTo(binaryData);
        }
    }
}
