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
import io.netty.buffer.Unpooled;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.BytesMessageId;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU64(value)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldFailForValueLargerThanU64() {
            // given
            long maxLong = 0xFFFF_FFFF_FFFF_FFFFL;
            var maxU64 = new BigInteger(Long.toUnsignedString(maxLong));
            var value = maxU64.add(BigInteger.ONE);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU64(value)).isInstanceOf(IllegalArgumentException.class);
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
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU128(value)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldFailForValueLargerThanU128() {
            // given
            byte[] maxU128 = new byte[17];
            Arrays.fill(maxU128, 1, 17, (byte) 0xFF);
            var maxU128Value = new BigInteger(maxU128);
            var value = maxU128Value.add(BigInteger.ONE);

            // when & then
            assertThatThrownBy(() -> BytesSerializer.toBytesAsU128(value)).isInstanceOf(IllegalArgumentException.class);
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
            Map<String, HeaderValue> headers = new HashMap<>();

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then
            assertThat(result.readableBytes()).isEqualTo(0); // Empty buffer
        }

        @Test
        void shouldSerializeSingleHeader() {
            // given
            Map<String, HeaderValue> headers = new HashMap<>();
            headers.put("key1", new HeaderValue(HeaderKind.Raw, "value1"));

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then
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
            Map<String, HeaderValue> headers = new HashMap<>();
            headers.put("k1", new HeaderValue(HeaderKind.Raw, "v1")); // 13 bytes
            headers.put("k2", new HeaderValue(HeaderKind.String, "v2")); // 13 bytes

            // when
            ByteBuf result = BytesSerializer.toBytes(headers);

            // then - verify buffer contains data for both headers
            assertThat(result.readableBytes()).isEqualTo(26);
        }
    }

    @Nested
    class MessageSerialization {

        @Test
        void shouldSerializeMessageWithoutUserHeaders() {
            // given
            var messageId = new BytesMessageId(new byte[16]);
            var header = new MessageHeader(
                    BigInteger.valueOf(123), // checksum
                    messageId,
                    BigInteger.valueOf(0), // offset
                    BigInteger.valueOf(1000), // timestamp
                    BigInteger.valueOf(1000), // originTimestamp
                    0L, // userHeadersLength
                    5L // payloadLength
                    );
            byte[] payload = "hello".getBytes();
            var message = new Message(header, payload, new HashMap<>());

            // when
            ByteBuf result = BytesSerializer.toBytes(message);

            // then
            assertThat(result.readableBytes()).isEqualTo(MessageHeader.SIZE + 5); // header + payload, no user headers
        }

        @Test
        void shouldSerializeMessageWithUserHeaders() {
            // given
            var messageId = new BytesMessageId(new byte[16]);
            Map<String, HeaderValue> userHeaders = new HashMap<>();
            userHeaders.put("key", new HeaderValue(HeaderKind.Raw, "val"));

            // Calculate user headers size
            ByteBuf headersBuf = BytesSerializer.toBytes(userHeaders);
            int userHeadersLength = headersBuf.readableBytes();

            var header = new MessageHeader(
                    BigInteger.ZERO,
                    messageId,
                    BigInteger.ZERO,
                    BigInteger.valueOf(1000),
                    BigInteger.valueOf(1000),
                    (long) userHeadersLength,
                    3L // "abc".length()
                    );
            byte[] payload = "abc".getBytes();
            var message = new Message(header, payload, userHeaders);

            // when
            ByteBuf result = BytesSerializer.toBytes(message);

            // then
            assertThat(result.readableBytes()).isEqualTo(MessageHeader.SIZE + 3 + userHeadersLength);
        }
    }

    @Nested
    class MessageHeaderSerialization {

        @Test
        void shouldSerializeMessageHeader() {
            // given
            var messageId = new BytesMessageId(new byte[16]);
            var header = new MessageHeader(
                    BigInteger.valueOf(999), // checksum
                    messageId,
                    BigInteger.valueOf(42), // offset
                    BigInteger.valueOf(2000), // timestamp
                    BigInteger.valueOf(1999), // originTimestamp
                    10L, // userHeadersLength
                    100L // payloadLength
                    );

            // when
            ByteBuf result = BytesSerializer.toBytes(header);

            // then
            assertThat(result.readableBytes()).isEqualTo(MessageHeader.SIZE);
            // Read checksum (8 bytes)
            result.skipBytes(8);
            // Read message ID (16 bytes)
            result.skipBytes(16);
            // Read offset (8 bytes)
            result.skipBytes(8);
            // Read timestamp (8 bytes)
            result.skipBytes(8);
            // Read origin timestamp (8 bytes)
            result.skipBytes(8);
            // Read user headers length (4 bytes)
            assertThat(result.readIntLE()).isEqualTo(10);
            // Read payload length (4 bytes)
            assertThat(result.readIntLE()).isEqualTo(100);
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
