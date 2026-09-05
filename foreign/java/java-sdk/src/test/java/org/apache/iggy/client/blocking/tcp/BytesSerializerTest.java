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

package org.apache.iggy.client.blocking.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BytesSerializerTest {

    @Nested
    class U64 {

        @Test
        void shouldSerializeMaxValue() {
            // given
            long maxLong = 0xFFFF_FFFF_FFFF_FFFFL;
            var maxU64 = Long.toUnsignedString(maxLong);
            var value = new BigInteger(maxU64);

            // when
            ByteBuf bytesAsU64 = BytesSerializer.toBytesAsU64(value);

            // then
            assertThat(bytesAsU64).isEqualByComparingTo(Unpooled.copyLong(maxLong));
        }

        @Test
        void shouldSerializeZero() {
            // given
            var value = BigInteger.ZERO;
            var zeroAsU64 = Unpooled.buffer(8);
            zeroAsU64.writeZero(8);

            // when
            ByteBuf bytesAsU64 = BytesSerializer.toBytesAsU64(value);

            // then
            assertThat(bytesAsU64).isEqualByComparingTo(zeroAsU64);
        }

        @Test
        void shouldFailForValueBelowZero() {
            // given
            var value = BigInteger.valueOf(-1);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU64(value))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldFailForValueLargerThanU64() {
            // given
            long maxLong = 0xFFFF_FFFF_FFFF_FFFFL;
            var maxU64 = new BigInteger(Long.toUnsignedString(maxLong));
            var value = maxU64.add(BigInteger.ONE);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU64(value))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class U128 {

        @Test
        void shouldSerializeMaxValue() {
            // given
            byte[] maxU128 = new byte[17];
            Arrays.fill(maxU128, 1, 17, (byte) 0xFF);

            var value = new BigInteger(maxU128);

            // when
            ByteBuf bytesAsU128 = BytesSerializer.toBytesAsU128(value);

            // then
            assertThat(bytesAsU128).isEqualByComparingTo(Unpooled.wrappedBuffer(maxU128, 1, 16));
        }

        @Test
        void shouldSerializeZero() {
            // given
            var value = BigInteger.ZERO;
            var zeroAsU128 = Unpooled.buffer(8);
            zeroAsU128.writeZero(16);

            // when
            ByteBuf bytesAsU128 = BytesSerializer.toBytesAsU128(value);

            // then
            assertThat(bytesAsU128).isEqualByComparingTo(zeroAsU128);
        }

        @Test
        void shouldFailForValueBelowZero() {
            // given
            var value = BigInteger.valueOf(-1);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU128(value))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldFailForValueLargerThanU128() {
            // given
            byte[] maxU128 = new byte[17];
            Arrays.fill(maxU128, 1, 17, (byte) 0xFF);
            var maxU128Value = new BigInteger(maxU128);
            var value = maxU128Value.add(BigInteger.ONE);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU128(value))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class StringSerialization {

        @Test
        void shouldSerializeSimpleString() {
            // given
            String input = "test";

            // when
            ByteBuf result = BytesSerializer.toBytes(input);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 4); // length
            byte[] stringBytes = new byte[4];
            result.readBytes(stringBytes);
            assertThat(new String(stringBytes, StandardCharsets.UTF_8)).isEqualTo("test");
        }

        @Test
        void shouldSerializeEmptyString() {
            // given
            String input = "";

            // when
            ByteBuf result = BytesSerializer.toBytes(input);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 0); // length = 0
            assertThat(result.readableBytes()).isEqualTo(0);
        }

        @Test
        void shouldSerializeUtf8Characters() {
            // given
            String input = "Hello世界";

            // when
            ByteBuf result = BytesSerializer.toBytes(input);

            // then
            byte[] expectedBytes = input.getBytes(StandardCharsets.UTF_8);
            assertThat(result.readByte()).isEqualTo((byte) expectedBytes.length);
            byte[] stringBytes = new byte[expectedBytes.length];
            result.readBytes(stringBytes);
            assertThat(stringBytes).isEqualTo(expectedBytes);
        }

        @Test
        void shouldSerializeStringOfExactly255EncodedBytes() {
            // given
            String input = "世".repeat(85);

            // when
            ByteBuf result = BytesSerializer.toBytes(input);

            // then
            assertThat(result.readUnsignedByte()).isEqualTo((short) 255);
            assertThat(result.readableBytes()).isEqualTo(255);
        }

        @Test
        void shouldRejectStringLongerThan255EncodedBytesEvenIfUnder255Chars() {
            // given
            String input = "あ".repeat(86);
            assertThat(input.length()).isLessThan(255);

            // when / then
            assertThatThrownBy(() -> BytesSerializer.toBytes(input))
                    .isInstanceOf(IggyInvalidArgumentException.class)
                    .hasMessageContaining("258");
        }
    }

    @Nested
    class IdentifierSerialization {

        @Test
        void shouldSerializeNumericIdentifier() {
            // given
            var identifier = StreamId.of(123L);

            // when
            ByteBuf result = BytesSerializer.toBytes(identifier);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // kind = 1 (numeric)
            assertThat(result.readByte()).isEqualTo((byte) 4); // length = 4
            assertThat(result.readIntLE()).isEqualTo(123); // id value
        }

        @Test
        void shouldSerializeStringIdentifier() {
            // given
            var identifier = StreamId.of("test-stream");

            // when
            ByteBuf result = BytesSerializer.toBytes(identifier);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 2); // kind = 2 (string)
            assertThat(result.readByte()).isEqualTo((byte) 11); // length = "test-stream".length()
            byte[] nameBytes = new byte[11];
            result.readBytes(nameBytes);
            assertThat(new String(nameBytes)).isEqualTo("test-stream");
        }

        @Test
        void shouldSerializeUtf8StringIdentifierWithByteLength() {
            // given
            String name = "strumień-世界";
            byte[] expectedBytes = name.getBytes(StandardCharsets.UTF_8);
            var identifier = StreamId.of(name);

            // when
            ByteBuf result = BytesSerializer.toBytes(identifier);

            // then
            assertThat(expectedBytes.length).isGreaterThan(name.length());
            assertThat(result.readableBytes()).isEqualTo(identifier.getSize());
            assertThat(result.readByte()).isEqualTo((byte) 2); // kind = 2 (string)
            assertThat(result.readByte()).isEqualTo((byte) expectedBytes.length);
            byte[] nameBytes = new byte[expectedBytes.length];
            result.readBytes(nameBytes);
            assertThat(nameBytes).isEqualTo(expectedBytes);
            assertThat(result.readableBytes()).isEqualTo(0);
        }

        @ParameterizedTest
        @CsvSource({
            "café,       02 05 63 61 66 C3 A9",
            "naïve-café, 02 0C 6E 61 C3 AF 76 65 2D 63 61 66 C3 A9",
            "日本語,     02 09 E6 97 A5 E6 9C AC E8 AA 9E",
        })
        void shouldMatchServerWireFormatForNonAsciiNames(String name, String expectedHex) {
            // given
            var identifier = StreamId.of(name);

            // when
            ByteBuf result = BytesSerializer.toBytes(identifier);

            // then
            assertThat(ByteBufUtil.hexDump(result).toUpperCase()).isEqualTo(expectedHex.replace(" ", ""));
        }
    }

    @Nested
    class ConsumerSerialization {

        @Test
        void shouldSerializeConsumerWithNumericId() {
            // given
            var consumer = Consumer.of(42L);

            // when
            ByteBuf result = BytesSerializer.toBytes(consumer);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // Consumer kind
            assertThat(result.readByte()).isEqualTo((byte) 1); // Identifier kind (numeric)
            assertThat(result.readByte()).isEqualTo((byte) 4); // Identifier length
            assertThat(result.readIntLE()).isEqualTo(42); // ID value
        }

        @Test
        void shouldSerializeConsumerGroupWithNumericId() {
            // given
            var consumer = Consumer.group(99L);

            // when
            ByteBuf result = BytesSerializer.toBytes(consumer);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 2); // ConsumerGroup kind
            assertThat(result.readByte()).isEqualTo((byte) 1); // Identifier kind (numeric)
            assertThat(result.readByte()).isEqualTo((byte) 4); // Identifier length
            assertThat(result.readIntLE()).isEqualTo(99); // ID value
        }

        @Test
        void shouldSerializeConsumerWithStringId() {
            // given
            var consumer = Consumer.of(ConsumerId.of("my-consumer"));

            // when
            ByteBuf result = BytesSerializer.toBytes(consumer);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // Consumer kind
            assertThat(result.readByte()).isEqualTo((byte) 2); // Identifier kind (string)
            assertThat(result.readByte()).isEqualTo((byte) 11); // "my-consumer".length()
        }
    }

    @Nested
    class PartitioningSerialization {

        @Test
        void shouldSerializeBalancedPartitioning() {
            // given
            var partitioning = Partitioning.balanced();

            // when
            ByteBuf result = BytesSerializer.toBytes(partitioning);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // Balanced kind
            assertThat(result.readByte()).isEqualTo((byte) 0); // Empty value
        }

        @Test
        void shouldSerializePartitionIdPartitioning() {
            // given
            var partitioning = Partitioning.partitionId(5L);

            // when
            ByteBuf result = BytesSerializer.toBytes(partitioning);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 2); // PartitionId kind
            assertThat(result.readByte()).isEqualTo((byte) 4); // 4 bytes for int
            assertThat(result.readIntLE()).isEqualTo(5); // partition ID
        }

        @Test
        void shouldSerializeMessagesKeyPartitioning() {
            // given
            var partitioning = Partitioning.messagesKey("user-123");

            // when
            ByteBuf result = BytesSerializer.toBytes(partitioning);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 3); // MessagesKey kind
            assertThat(result.readByte()).isEqualTo((byte) 8); // "user-123".length()
            byte[] keyBytes = new byte[8];
            result.readBytes(keyBytes);
            assertThat(new String(keyBytes)).isEqualTo("user-123");
        }
    }

    @Nested
    class PollingStrategySerialization {

        @Test
        void shouldSerializeFirstStrategy() {
            // given
            var strategy = PollingStrategy.first();

            // when
            ByteBuf result = BytesSerializer.toBytes(strategy);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 3); // First kind = 3
            // Followed by 8 bytes of U64 (zero value)
            assertThat(result.readableBytes()).isEqualTo(8);
        }

        @Test
        void shouldSerializeLastStrategy() {
            // given
            var strategy = PollingStrategy.last();

            // when
            ByteBuf result = BytesSerializer.toBytes(strategy);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 4); // Last kind = 4
            assertThat(result.readableBytes()).isEqualTo(8);
        }

        @Test
        void shouldSerializeOffsetStrategy() {
            // given
            var strategy = PollingStrategy.offset(BigInteger.valueOf(100));

            // when
            ByteBuf result = BytesSerializer.toBytes(strategy);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // Offset kind = 1
            assertThat(result.readableBytes()).isEqualTo(8);
        }

        @Test
        void shouldSerializeNextStrategy() {
            // given
            var strategy = PollingStrategy.next();

            // when
            ByteBuf result = BytesSerializer.toBytes(strategy);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 5); // Next kind = 5
            assertThat(result.readableBytes()).isEqualTo(8);
        }

        @Test
        void shouldSerializeTimestampStrategy() {
            // given
            var strategy = PollingStrategy.timestamp(BigInteger.valueOf(1234567890));

            // when
            ByteBuf result = BytesSerializer.toBytes(strategy);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 2); // Timestamp kind = 2
            assertThat(result.readableBytes()).isEqualTo(8);
        }
    }

    @Nested
    class OptionalValueSerialization {

        @Test
        void shouldSerializePresentOptional() {
            // given
            Optional<Long> optional = Optional.of(42L);

            // when
            ByteBuf result = BytesSerializer.toBytes(optional);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 1); // present flag
            assertThat(result.readIntLE()).isEqualTo(42); // value
        }

        @Test
        void shouldSerializeEmptyOptional() {
            // given
            Optional<Long> optional = Optional.empty();

            // when
            ByteBuf result = BytesSerializer.toBytes(optional);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 0); // not present flag
            assertThat(result.readIntLE()).isEqualTo(0); // zero value
        }
    }

    @Nested
    class HeadersSerialization {

        @Test
        void shouldSerializeEmptyHeaders() {
            // given
            Map<HeaderKey, HeaderValue> headers = new HashMap<>();

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then
            assertThat(result.readableBytes()).isEqualTo(0); // Empty buffer
        }

        @Test
        void shouldSerializeSingleHeader() {
            // given
            Map<HeaderKey, HeaderValue> headers = new HashMap<>();
            headers.put(HeaderKey.fromString("key1"), HeaderValue.fromRaw("value1".getBytes()));

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then
            assertThat(result.readByte()).isEqualTo((byte) 2); // String kind
            assertThat(result.readIntLE()).isEqualTo(4); // "key1".length()
            byte[] keyBytes = new byte[4];
            result.readBytes(keyBytes);
            assertThat(new String(keyBytes)).isEqualTo("key1");
            assertThat(result.readByte()).isEqualTo((byte) 1); // Raw kind
            assertThat(result.readIntLE()).isEqualTo(6); // "value1".length()
            byte[] valueBytes = new byte[6];
            result.readBytes(valueBytes);
            assertThat(new String(valueBytes)).isEqualTo("value1");
        }

        @Test
        void shouldSerializeMultipleHeaders() {
            // given
            Map<HeaderKey, HeaderValue> headers = new HashMap<>();
            headers.put(HeaderKey.fromString("k1"), HeaderValue.fromRaw("v1".getBytes()));
            headers.put(HeaderKey.fromString("k2"), HeaderValue.fromString("v2"));

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then - verify buffer contains data for both headers
            assertThat(result.readableBytes()).isEqualTo(28);
        }
    }

    @Nested
    class PermissionsSerialization {

        @Test
        void shouldSerializeGlobalPermissions() {
            // given
            var permissions = new GlobalPermissions(true, false, true, false, true, false, true, false, true, false);

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            assertThat(result.readableBytes()).isEqualTo(10); // 10 booleans
            assertThat(result.readBoolean()).isTrue(); // manageServers
            assertThat(result.readBoolean()).isFalse(); // readServers
            assertThat(result.readBoolean()).isTrue(); // manageUsers
            assertThat(result.readBoolean()).isFalse(); // readUsers
            assertThat(result.readBoolean()).isTrue(); // manageStreams
            assertThat(result.readBoolean()).isFalse(); // readStreams
            assertThat(result.readBoolean()).isTrue(); // manageTopics
            assertThat(result.readBoolean()).isFalse(); // readTopics
            assertThat(result.readBoolean()).isTrue(); // pollMessages
            assertThat(result.readBoolean()).isFalse(); // sendMessages
        }

        @Test
        void shouldSerializeTopicPermissions() {
            // given
            var permissions = new TopicPermissions(true, false, true, false);

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            assertThat(result.readableBytes()).isEqualTo(4); // 4 booleans
            assertThat(result.readBoolean()).isTrue(); // manageTopic
            assertThat(result.readBoolean()).isFalse(); // readTopic
            assertThat(result.readBoolean()).isTrue(); // pollMessages
            assertThat(result.readBoolean()).isFalse(); // sendMessages
        }

        @Test
        void shouldSerializeStreamPermissionsWithoutTopics() {
            // given
            var permissions = new StreamPermissions(true, false, true, false, true, false, new HashMap<>());

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            assertThat(result.readBoolean()).isTrue(); // manageStream
            assertThat(result.readBoolean()).isFalse(); // readStream
            assertThat(result.readBoolean()).isTrue(); // manageTopics
            assertThat(result.readBoolean()).isFalse(); // readTopics
            assertThat(result.readBoolean()).isTrue(); // pollMessages
            assertThat(result.readBoolean()).isFalse(); // sendMessages
            assertThat(result.readByte()).isEqualTo((byte) 0); // no topics marker
        }

        @Test
        void shouldSerializeStreamPermissionsWithTopics() {
            // given
            Map<Long, TopicPermissions> topicPerms = new HashMap<>();
            topicPerms.put(1L, new TopicPermissions(true, true, true, true));
            var permissions = new StreamPermissions(true, true, true, true, true, true, topicPerms);

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            result.skipBytes(6); // Skip 6 stream-level booleans
            assertThat(result.readByte()).isEqualTo((byte) 1); // has topic marker
            assertThat(result.readIntLE()).isEqualTo(1); // topic ID
            result.skipBytes(4); // Skip topic permissions
            assertThat(result.readByte()).isEqualTo((byte) 0); // end marker
        }

        @Test
        void shouldSerializeFullPermissionsWithoutStreams() {
            // given
            var globalPerms = new GlobalPermissions(true, true, true, true, true, true, true, true, true, true);
            var permissions = new Permissions(globalPerms, new HashMap<>());

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            result.skipBytes(10); // Skip global permissions
            assertThat(result.readByte()).isEqualTo((byte) 0); // no streams marker
        }

        @Test
        void shouldSerializeFullPermissionsWithStreams() {
            // given
            var globalPerms =
                    new GlobalPermissions(false, false, false, false, false, false, false, false, false, false);
            Map<Long, StreamPermissions> streamPerms = new HashMap<>();
            streamPerms.put(1L, new StreamPermissions(true, true, true, true, true, true, new HashMap<>()));
            var permissions = new Permissions(globalPerms, streamPerms);

            // when
            ByteBuf result = BytesSerializer.toBytes(permissions);

            // then
            result.skipBytes(10); // Skip global permissions
            assertThat(result.readByte()).isEqualTo((byte) 1); // has stream marker
            assertThat(result.readIntLE()).isEqualTo(1); // stream ID
            result.skipBytes(6); // Skip stream permissions
            assertThat(result.readByte()).isEqualTo((byte) 0); // no topics in stream
            assertThat(result.readByte()).isEqualTo((byte) 0); // end of streams marker
        }
    }
}
