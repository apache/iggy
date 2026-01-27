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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.user.UserStatus;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.apache.iggy.serde.BytesDeserializer.readClientInfo;
import static org.apache.iggy.serde.BytesDeserializer.readConsumerGroup;
import static org.apache.iggy.serde.BytesDeserializer.readConsumerGroupDetails;
import static org.apache.iggy.serde.BytesDeserializer.readConsumerGroupInfo;
import static org.apache.iggy.serde.BytesDeserializer.readConsumerGroupMember;
import static org.apache.iggy.serde.BytesDeserializer.readConsumerOffsetInfo;
import static org.apache.iggy.serde.BytesDeserializer.readGlobalPermissions;
import static org.apache.iggy.serde.BytesDeserializer.readPartition;
import static org.apache.iggy.serde.BytesDeserializer.readPermissions;
import static org.apache.iggy.serde.BytesDeserializer.readPersonalAccessTokenInfo;
import static org.apache.iggy.serde.BytesDeserializer.readPolledMessage;
import static org.apache.iggy.serde.BytesDeserializer.readPolledMessages;
import static org.apache.iggy.serde.BytesDeserializer.readRawPersonalAccessToken;
import static org.apache.iggy.serde.BytesDeserializer.readStats;
import static org.apache.iggy.serde.BytesDeserializer.readStreamBase;
import static org.apache.iggy.serde.BytesDeserializer.readStreamDetails;
import static org.apache.iggy.serde.BytesDeserializer.readStreamPermissions;
import static org.apache.iggy.serde.BytesDeserializer.readTopic;
import static org.apache.iggy.serde.BytesDeserializer.readTopicDetails;
import static org.apache.iggy.serde.BytesDeserializer.readTopicPermissions;
import static org.apache.iggy.serde.BytesDeserializer.readU64AsBigInteger;
import static org.apache.iggy.serde.BytesDeserializer.readUserInfo;
import static org.apache.iggy.serde.BytesDeserializer.readUserInfoDetails;
import static org.assertj.core.api.Assertions.assertThat;

class BytesDeserializerTest {

    // Helper methods for writing test data
    private static void writeU64(ByteBuf buffer, BigInteger value) {
        byte[] bytes = value.toByteArray();
        ArrayUtils.reverse(bytes);
        buffer.writeBytes(bytes, 0, Math.min(8, bytes.length));
        if (bytes.length < 8) {
            buffer.writeZero(8 - bytes.length);
        }
    }

    private static void writeTopicData(ByteBuf buffer) {
        buffer.writeIntLE(10); // topic ID
        writeU64(buffer, BigInteger.valueOf(1000)); // created at
        buffer.writeIntLE(4); // partitions count
        writeU64(buffer, BigInteger.ZERO); // message expiry
        buffer.writeByte(CompressionAlgorithm.None.asCode()); // compression
        writeU64(buffer, BigInteger.valueOf(10000)); // max topic size
        buffer.writeByte(1); // replication factor
        writeU64(buffer, BigInteger.valueOf(500)); // size
        writeU64(buffer, BigInteger.valueOf(50)); // messages count
        buffer.writeByte(4); // name length
        buffer.writeBytes("test".getBytes());
    }

    private static void writePartitionData(ByteBuf buffer) {
        buffer.writeIntLE(1); // partition ID
        writeU64(buffer, BigInteger.valueOf(1000)); // created at
        buffer.writeIntLE(5); // segments count
        writeU64(buffer, BigInteger.valueOf(99)); // current offset
        writeU64(buffer, BigInteger.valueOf(200)); // size
        writeU64(buffer, BigInteger.valueOf(20)); // messages count
    }

    @Nested
    class U64 {

        @Test
        void shouldDeserializeMaxValue() {
            // given
            long maxLong = 0xFFFF_FFFF_FFFF_FFFFL;
            ByteBuf buffer = Unpooled.copyLong(maxLong);
            var expectedMaxU64 = new BigInteger(Long.toUnsignedString(maxLong));

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(expectedMaxU64);
        }

        @Test
        void shouldDeserializeZero() {
            // given
            ByteBuf buffer = Unpooled.buffer(8);
            buffer.writeZero(8);

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(BigInteger.ZERO);
        }

        @Test
        void shouldDeserializeArbitraryValue() {
            // given
            byte[] bytes = HexFormat.of().parseHex("8000000000000000");
            var expected = new BigInteger(1, bytes);
            ArrayUtils.reverse(bytes);
            ByteBuf buffer = Unpooled.wrappedBuffer(bytes);

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(expected);
        }
    }

    @Nested
    class StreamDeserialization {

        @Test
        void shouldDeserializeStreamBase() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1); // stream ID
            writeU64(buffer, BigInteger.valueOf(1000)); // createdAt
            buffer.writeIntLE(2); // topics count
            writeU64(buffer, BigInteger.valueOf(5000)); // size
            writeU64(buffer, BigInteger.valueOf(100)); // messages count
            buffer.writeByte(11); // name length
            buffer.writeBytes("test-stream".getBytes(StandardCharsets.UTF_8));

            // when
            var stream = readStreamBase(buffer);

            // then
            assertThat(stream.id()).isEqualTo(1L);
            assertThat(stream.createdAt()).isEqualTo(BigInteger.valueOf(1000));
            assertThat(stream.topicsCount()).isEqualTo(2L);
            assertThat(stream.name()).isEqualTo("test-stream");
        }

        @Test
        void shouldDeserializeStreamDetails() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            // Write stream base
            buffer.writeIntLE(1); // stream ID
            writeU64(buffer, BigInteger.valueOf(1000));
            buffer.writeIntLE(1); // topics count
            writeU64(buffer, BigInteger.valueOf(5000));
            writeU64(buffer, BigInteger.valueOf(100));
            buffer.writeByte(6);
            buffer.writeBytes("stream".getBytes());
            // Write one topic
            writeTopicData(buffer);

            // when
            var streamDetails = readStreamDetails(buffer);

            // then
            assertThat(streamDetails.id()).isEqualTo(1L);
            assertThat(streamDetails.topics()).hasSize(1);
        }
    }

    @Nested
    class TopicDeserialization {

        @Test
        void shouldDeserializeTopic() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            writeTopicData(buffer);

            // when
            var topic = readTopic(buffer);

            // then
            assertThat(topic.id()).isEqualTo(10L);
            assertThat(topic.name()).isEqualTo("test");
            assertThat(topic.partitionsCount()).isEqualTo(4L);
        }

        @Test
        void shouldDeserializeTopicDetails() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            writeTopicData(buffer);
            // Write one partition
            writePartitionData(buffer);

            // when
            var topicDetails = readTopicDetails(buffer);

            // then
            assertThat(topicDetails.id()).isEqualTo(10L);
            assertThat(topicDetails.partitions()).hasSize(1);
        }
    }

    @Nested
    class PartitionDeserialization {

        @Test
        void shouldDeserializePartition() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            writePartitionData(buffer);

            // when
            var partition = readPartition(buffer);

            // then
            assertThat(partition.id()).isEqualTo(1L);
            assertThat(partition.segmentsCount()).isEqualTo(5L);
            assertThat(partition.currentOffset()).isEqualTo(BigInteger.valueOf(99));
        }
    }

    @Nested
    class ConsumerGroupDeserialization {

        @Test
        void shouldDeserializeConsumerGroup() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1); // group ID
            buffer.writeIntLE(3); // partitions count
            buffer.writeIntLE(2); // members count
            buffer.writeByte(5); // name length
            buffer.writeBytes("group".getBytes());

            // when
            var group = readConsumerGroup(buffer);

            // then
            assertThat(group.id()).isEqualTo(1L);
            assertThat(group.name()).isEqualTo("group");
            assertThat(group.partitionsCount()).isEqualTo(3L);
            assertThat(group.membersCount()).isEqualTo(2L);
        }

        @Test
        void shouldDeserializeConsumerGroupMember() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(42); // member ID
            buffer.writeIntLE(2); // partitions count
            buffer.writeIntLE(1); // partition ID 1
            buffer.writeIntLE(2); // partition ID 2

            // when
            var member = readConsumerGroupMember(buffer);

            // then
            assertThat(member.id()).isEqualTo(42L);
            assertThat(member.partitionsCount()).isEqualTo(2L);
            assertThat(member.partitions()).containsExactly(1L, 2L);
        }

        @Test
        void shouldDeserializeConsumerGroupDetails() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            // Write consumer group
            buffer.writeIntLE(1);
            buffer.writeIntLE(1);
            buffer.writeIntLE(1);
            buffer.writeByte(2);
            buffer.writeBytes("cg".getBytes());
            // Write one member
            buffer.writeIntLE(10);
            buffer.writeIntLE(0); // no partitions

            // when
            var details = readConsumerGroupDetails(buffer);

            // then
            assertThat(details.id()).isEqualTo(1L);
            assertThat(details.members()).hasSize(1);
        }
    }

    @Nested
    class ConsumerOffsetDeserialization {

        @Test
        void shouldDeserializeConsumerOffsetInfo() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(5); // partition ID
            writeU64(buffer, BigInteger.valueOf(100)); // current offset
            writeU64(buffer, BigInteger.valueOf(95)); // stored offset

            // when
            var offsetInfo = readConsumerOffsetInfo(buffer);

            // then
            assertThat(offsetInfo.partitionId()).isEqualTo(5L);
            assertThat(offsetInfo.currentOffset()).isEqualTo(BigInteger.valueOf(100));
            assertThat(offsetInfo.storedOffset()).isEqualTo(BigInteger.valueOf(95));
        }
    }

    @Nested
    class MessageDeserialization {

        @Test
        void shouldDeserializePolledMessageWithoutUserHeaders() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            writeU64(buffer, BigInteger.valueOf(123)); // checksum
            buffer.writeBytes(new byte[16]); // message ID
            writeU64(buffer, BigInteger.ZERO); // offset
            writeU64(buffer, BigInteger.valueOf(1000)); // timestamp
            writeU64(buffer, BigInteger.valueOf(1000)); // origin timestamp
            buffer.writeIntLE(0); // user headers length
            buffer.writeIntLE(5); // payload length
            buffer.writeBytes("hello".getBytes()); // payload

            // when
            var message = readPolledMessage(buffer);

            // then
            assertThat(message.header().checksum()).isEqualTo(BigInteger.valueOf(123));
            assertThat(message.header().payloadLength()).isEqualTo(5L);
            assertThat(message.payload()).isEqualTo("hello".getBytes());
            assertThat(message.userHeaders()).isEmpty();
        }

        @Test
        void shouldDeserializePolledMessageWithUserHeaders() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            writeU64(buffer, BigInteger.ZERO);
            buffer.writeBytes(new byte[16]);
            writeU64(buffer, BigInteger.ZERO);
            writeU64(buffer, BigInteger.valueOf(1000));
            writeU64(buffer, BigInteger.valueOf(1000));

            // Calculate and write user headers
            ByteBuf headersBuffer = Unpooled.buffer();
            headersBuffer.writeIntLE(3); // key length
            headersBuffer.writeBytes("key".getBytes());
            headersBuffer.writeByte(HeaderKind.Raw.asCode());
            headersBuffer.writeIntLE(3); // value length
            headersBuffer.writeBytes("val".getBytes());

            buffer.writeIntLE(headersBuffer.readableBytes()); // user headers length
            buffer.writeIntLE(3); // payload length
            buffer.writeBytes("abc".getBytes()); // payload
            buffer.writeBytes(headersBuffer); // user headers

            // when
            var message = readPolledMessage(buffer);

            // then
            assertThat(message.userHeaders()).hasSize(1);
            assertThat(message.userHeaders().get("key").value()).isEqualTo("val");
        }

        @Test
        void shouldDeserializePolledMessages() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1); // partition ID
            writeU64(buffer, BigInteger.valueOf(10)); // current offset
            buffer.writeIntLE(1); // messages count
            // Write one message
            writeU64(buffer, BigInteger.ZERO);
            buffer.writeBytes(new byte[16]);
            writeU64(buffer, BigInteger.ZERO);
            writeU64(buffer, BigInteger.valueOf(1000));
            writeU64(buffer, BigInteger.valueOf(1000));
            buffer.writeIntLE(0);
            buffer.writeIntLE(2);
            buffer.writeBytes("hi".getBytes());

            // when
            var polledMessages = readPolledMessages(buffer);

            // then
            assertThat(polledMessages.partitionId()).isEqualTo(1L);
            assertThat(polledMessages.currentOffset()).isEqualTo(BigInteger.valueOf(10));
            assertThat(polledMessages.count()).isEqualTo(1L);
            assertThat(polledMessages.messages()).hasSize(1);
        }
    }

    @Nested
    class StatsDeserialization {

        @Test
        void shouldDeserializeStats() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1234); // process ID
            buffer.writeFloatLE(12.5f); // CPU usage
            buffer.writeFloatLE(50.0f); // total CPU usage
            writeU64(buffer, BigInteger.valueOf(1000000)); // memory usage
            writeU64(buffer, BigInteger.valueOf(8000000)); // total memory
            writeU64(buffer, BigInteger.valueOf(7000000)); // available memory
            writeU64(buffer, BigInteger.valueOf(3600)); // run time
            writeU64(buffer, BigInteger.valueOf(1000000)); // start time
            writeU64(buffer, BigInteger.valueOf(500)); // read bytes
            writeU64(buffer, BigInteger.valueOf(600)); // written bytes
            writeU64(buffer, BigInteger.valueOf(1000)); // messages size bytes
            buffer.writeIntLE(5); // streams count
            buffer.writeIntLE(10); // topics count
            buffer.writeIntLE(20); // partitions count
            buffer.writeIntLE(100); // segments count
            writeU64(buffer, BigInteger.valueOf(5000)); // messages count
            buffer.writeIntLE(3); // clients count
            buffer.writeIntLE(2); // consumer groups count
            buffer.writeIntLE(9); // hostname length
            buffer.writeBytes("localhost".getBytes());
            buffer.writeIntLE(5); // OS name length
            buffer.writeBytes("Linux".getBytes());
            buffer.writeIntLE(5); // OS version length
            buffer.writeBytes("5.4.0".getBytes());
            buffer.writeIntLE(7); // kernel version length
            buffer.writeBytes("5.4.0-1".getBytes());

            // when
            var stats = readStats(buffer);

            // then
            assertThat(stats.processId()).isEqualTo(1234L);
            assertThat(stats.cpuUsage()).isEqualTo(12.5f);
            assertThat(stats.streamsCount()).isEqualTo(5L);
            assertThat(stats.hostname()).isEqualTo("localhost");
            assertThat(stats.osName()).isEqualTo("Linux");
        }
    }

    @Nested
    class ClientInfoDeserialization {

        @Test
        void shouldDeserializeClientInfo() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(100); // client ID
            buffer.writeIntLE(5); // user ID
            buffer.writeByte(1); // transport (TCP)
            buffer.writeIntLE(9); // address length
            buffer.writeBytes("127.0.0.1".getBytes());
            buffer.writeIntLE(0); // consumer groups count

            // when
            var clientInfo = readClientInfo(buffer);

            // then
            assertThat(clientInfo.clientId()).isEqualTo(100L);
            assertThat(clientInfo.userId()).isPresent().hasValue(5L);
            assertThat(clientInfo.address()).isEqualTo("127.0.0.1");
            assertThat(clientInfo.transport()).isEqualTo("Tcp");
        }

        @Test
        void shouldDeserializeConsumerGroupInfo() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1); // stream ID
            buffer.writeIntLE(2); // topic ID
            buffer.writeIntLE(3); // group ID

            // when
            var groupInfo = readConsumerGroupInfo(buffer);

            // then
            assertThat(groupInfo.streamId()).isEqualTo(1L);
            assertThat(groupInfo.topicId()).isEqualTo(2L);
            assertThat(groupInfo.consumerGroupId()).isEqualTo(3L);
        }
    }

    @Nested
    class UserInfoDeserialization {

        @Test
        void shouldDeserializeUserInfo() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(42); // user ID
            writeU64(buffer, BigInteger.valueOf(2000)); // created at
            buffer.writeByte(UserStatus.Active.asCode()); // status
            buffer.writeByte(4); // username length
            buffer.writeBytes("user".getBytes());

            // when
            var userInfo = readUserInfo(buffer);

            // then
            assertThat(userInfo.id()).isEqualTo(42L);
            assertThat(userInfo.createdAt()).isEqualTo(BigInteger.valueOf(2000));
            assertThat(userInfo.status()).isEqualTo(UserStatus.Active);
            assertThat(userInfo.username()).isEqualTo("user");
        }

        @Test
        void shouldDeserializeUserInfoDetailsWithoutPermissions() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1);
            writeU64(buffer, BigInteger.valueOf(1000));
            buffer.writeByte(UserStatus.Active.asCode());
            buffer.writeByte(5);
            buffer.writeBytes("admin".getBytes());
            buffer.writeBoolean(false); // no permissions

            // when
            var userInfoDetails = readUserInfoDetails(buffer);

            // then
            assertThat(userInfoDetails.id()).isEqualTo(1L);
            assertThat(userInfoDetails.permissions()).isEmpty();
        }

        @Test
        void shouldDeserializeUserInfoDetailsWithPermissions() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(1);
            writeU64(buffer, BigInteger.valueOf(1000));
            buffer.writeByte(UserStatus.Active.asCode());
            buffer.writeByte(5);
            buffer.writeBytes("admin".getBytes());
            buffer.writeBoolean(true); // has permissions
            buffer.writeIntLE(10); // permissions length (ignored but required)
            // Write global permissions (10 booleans)
            for (int i = 0; i < 10; i++) {
                buffer.writeBoolean(true);
            }
            buffer.writeBoolean(false); // no stream permissions

            // when
            var userInfoDetails = readUserInfoDetails(buffer);

            // then
            assertThat(userInfoDetails.permissions()).isPresent();
        }
    }

    @Nested
    class PermissionsDeserialization {

        @Test
        void shouldDeserializeGlobalPermissions() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBoolean(true); // manageServers
            buffer.writeBoolean(false); // readServers
            buffer.writeBoolean(true); // manageUsers
            buffer.writeBoolean(false); // readUsers
            buffer.writeBoolean(true); // manageStreams
            buffer.writeBoolean(false); // readStreams
            buffer.writeBoolean(true); // manageTopics
            buffer.writeBoolean(false); // readTopics
            buffer.writeBoolean(true); // pollMessages
            buffer.writeBoolean(false); // sendMessages

            // when
            var permissions = readGlobalPermissions(buffer);

            // then
            assertThat(permissions.manageServers()).isTrue();
            assertThat(permissions.readServers()).isFalse();
            assertThat(permissions.pollMessages()).isTrue();
        }

        @Test
        void shouldDeserializeTopicPermissions() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBoolean(true);
            buffer.writeBoolean(false);
            buffer.writeBoolean(true);
            buffer.writeBoolean(false);

            // when
            var permissions = readTopicPermissions(buffer);

            // then
            assertThat(permissions.manageTopic()).isTrue();
            assertThat(permissions.readTopic()).isFalse();
            assertThat(permissions.pollMessages()).isTrue();
            assertThat(permissions.sendMessages()).isFalse();
        }

        @Test
        void shouldDeserializeStreamPermissionsWithoutTopics() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(false); // no topics

            // when
            var permissions = readStreamPermissions(buffer);

            // then
            assertThat(permissions.manageStream()).isTrue();
            assertThat(permissions.topics()).isEmpty();
        }

        @Test
        void shouldDeserializeStreamPermissionsWithTopics() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true); // has topic
            buffer.writeIntLE(1); // topic ID
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(true);
            buffer.writeBoolean(false); // end of topics

            // when
            var permissions = readStreamPermissions(buffer);

            // then
            assertThat(permissions.topics()).hasSize(1);
            assertThat(permissions.topics()).containsKey(1L);
        }

        @Test
        void shouldDeserializeFullPermissionsWithoutStreams() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(10); // permissions length
            for (int i = 0; i < 10; i++) {
                buffer.writeBoolean(false);
            }
            buffer.writeBoolean(false); // no streams

            // when
            var permissions = readPermissions(buffer);

            // then
            assertThat(permissions.streams()).isEmpty();
        }

        @Test
        void shouldDeserializeFullPermissionsWithStreams() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeIntLE(10);
            for (int i = 0; i < 10; i++) {
                buffer.writeBoolean(false);
            }
            buffer.writeBoolean(true); // has stream
            buffer.writeIntLE(1); // stream ID
            for (int i = 0; i < 6; i++) {
                buffer.writeBoolean(true);
            }
            buffer.writeBoolean(false); // no topics in stream
            buffer.writeBoolean(false); // end of streams

            // when
            var permissions = readPermissions(buffer);

            // then
            assertThat(permissions.streams()).hasSize(1);
            assertThat(permissions.streams()).containsKey(1L);
        }
    }

    @Nested
    class PersonalAccessTokenDeserialization {

        @Test
        void shouldDeserializeRawPersonalAccessToken() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(10); // token length
            buffer.writeBytes("token12345".getBytes());

            // when
            var token = readRawPersonalAccessToken(buffer);

            // then
            assertThat(token.token()).isEqualTo("token12345");
        }

        @Test
        void shouldDeserializePersonalAccessTokenInfoWithExpiry() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(7); // name length
            buffer.writeBytes("mytoken".getBytes());
            writeU64(buffer, BigInteger.valueOf(3000)); // expiry

            // when
            var tokenInfo = readPersonalAccessTokenInfo(buffer);

            // then
            assertThat(tokenInfo.name()).isEqualTo("mytoken");
            assertThat(tokenInfo.expiryAt()).isPresent().hasValue(BigInteger.valueOf(3000));
        }

        @Test
        void shouldDeserializePersonalAccessTokenInfoWithoutExpiry() {
            // given
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(7);
            buffer.writeBytes("mytoken".getBytes());
            writeU64(buffer, BigInteger.ZERO); // no expiry

            // when
            var tokenInfo = readPersonalAccessTokenInfo(buffer);

            // then
            assertThat(tokenInfo.name()).isEqualTo("mytoken");
            assertThat(tokenInfo.expiryAt()).isEmpty();
        }
    }
}
