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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CreateTopic wire format:
 * [stream_id:Identifier][partitions_count:u32_le][name_len:u8][name:N][options TLV to end]
 */
class TopicsTcpClientPayloadTest {

    @Test
    void shouldWritePartitionsCountAsFixedFieldBeforeName() {
        ByteBuf payload = TopicsTcpClient.createTopicPayload(
                StreamId.of(1L), 3L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, "orders", Map.of());

        assertThat(payload.readUnsignedByte()).isEqualTo((short) 1); // numeric identifier kind
        assertThat(payload.readUnsignedByte()).isEqualTo((short) 4); // identifier length
        assertThat(payload.readUnsignedIntLE()).isEqualTo(1L);
        assertThat(payload.readUnsignedIntLE()).isEqualTo(3L);
        assertThat(readString(payload)).isEqualTo("orders");
        assertThat(payload.isReadable()).isFalse();
    }

    @Test
    void shouldOmitServerDefaultOptions() {
        ByteBuf payload = TopicsTcpClient.createTopicPayload(
                StreamId.of("my-stream"),
                1L,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                "events",
                Map.of());

        payload.skipBytes(2 + "my-stream".length());
        assertThat(payload.readUnsignedIntLE()).isEqualTo(1L);
        assertThat(readString(payload)).isEqualTo("events");
        assertThat(payload.isReadable()).isFalse();
    }

    @Test
    void shouldEncodeExplicitOptionsWithoutPartitionsCount() {
        ByteBuf payload = TopicsTcpClient.createTopicPayload(
                StreamId.of(1L),
                2L,
                CompressionAlgorithm.Gzip,
                BigInteger.valueOf(60_000_000L),
                BigInteger.valueOf(1024L),
                "orders",
                Map.of());

        payload.skipBytes(6);
        assertThat(payload.readUnsignedIntLE()).isEqualTo(2L);
        readString(payload);

        Map<String, TypedValue> options = readOptions(payload);
        assertThat(options).containsOnlyKeys("compression_algorithm", "message_expiry", "max_topic_size");
        assertThat(options.get("compression_algorithm").kind()).isEqualTo(HeaderKind.String);
        assertThat(new String(options.get("compression_algorithm").value(), StandardCharsets.UTF_8))
                .isEqualTo("gzip");
        assertThat(options.get("message_expiry").kind()).isEqualTo(HeaderKind.Uint64);
        assertThat(options.get("max_topic_size").kind()).isEqualTo(HeaderKind.Uint64);
    }

    private static String readString(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readUnsignedByte()];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Map<String, TypedValue> readOptions(ByteBuf buffer) {
        Map<String, TypedValue> options = new LinkedHashMap<>();
        while (buffer.isReadable()) {
            buffer.readUnsignedByte(); // key kind
            byte[] key = new byte[Math.toIntExact(buffer.readUnsignedIntLE())];
            buffer.readBytes(key);
            HeaderKind valueKind = HeaderKind.fromCode(buffer.readUnsignedByte());
            byte[] value = new byte[Math.toIntExact(buffer.readUnsignedIntLE())];
            buffer.readBytes(value);
            options.put(new String(key, StandardCharsets.UTF_8), new TypedValue(valueKind, value));
        }
        return options;
    }

    private record TypedValue(HeaderKind kind, byte[] value) {}
}
