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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

public final class BytesSerializer {

    private BytesSerializer() {}

    static ByteBuf toBytes(Consumer consumer) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(consumer.kind().asCode());
        buffer.writeBytes(toBytes(consumer.id()));
        return buffer;
    }

    static ByteBuf toBytes(Identifier identifier) {
        if (identifier.getKind() == 1) {
            ByteBuf buffer = Unpooled.buffer(6);
            buffer.writeByte(1);
            buffer.writeByte(4);
            buffer.writeIntLE(identifier.getId().intValue());
            return buffer;
        } else if (identifier.getKind() == 2) {
            ByteBuf buffer = Unpooled.buffer(2 + identifier.getName().length());
            buffer.writeByte(2);
            buffer.writeByte(identifier.getName().length());
            buffer.writeBytes(identifier.getName().getBytes());
            return buffer;
        } else {
            throw new IllegalArgumentException("Unknown identifier kind: " + identifier.getKind());
        }
    }

    static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    static ByteBuf toBytes(Message message) {
        var buffer = Unpooled.buffer(MessageHeader.SIZE + message.payload().length);
        buffer.writeBytes(toBytes(message.header()));
        buffer.writeBytes(message.payload());
        return buffer;
    }

    static ByteBuf toBytes(MessageHeader header) {
        var buffer = Unpooled.buffer(MessageHeader.SIZE);
        buffer.writeBytes(toBytesAsU64(header.checksum()));
        buffer.writeBytes(header.id().toBytes());
        buffer.writeBytes(toBytesAsU64(header.offset()));
        buffer.writeBytes(toBytesAsU64(header.timestamp()));
        buffer.writeBytes(toBytesAsU64(header.originTimestamp()));
        buffer.writeIntLE(header.userHeadersLength().intValue());
        buffer.writeIntLE(header.payloadLength().intValue());
        return buffer;
    }

    static ByteBuf toBytes(PollingStrategy strategy) {
        var buffer = Unpooled.buffer(9);
        buffer.writeByte(strategy.kind().asCode());
        buffer.writeBytes(toBytesAsU64(strategy.value()));
        return buffer;
    }

    static ByteBuf toBytes(Optional<Long> optionalLong) {
        var buffer = Unpooled.buffer(5);
        if (optionalLong.isPresent()) {
            buffer.writeByte(1);
            buffer.writeIntLE(optionalLong.get().intValue());
        } else {
            buffer.writeByte(0);
            buffer.writeIntLE(0);
        }
        return buffer;
    }

    static ByteBuf toBytes(Map<String, HeaderValue> headers) {
        if (headers.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }
        var buffer = Unpooled.buffer();
        for (Map.Entry<String, HeaderValue> entry : headers.entrySet()) {
            String key = entry.getKey();
            buffer.writeIntLE(key.length());
            buffer.writeBytes(key.getBytes());

            HeaderValue value = entry.getValue();
            buffer.writeByte(value.kind().asCode());
            buffer.writeIntLE(value.value().length());
            buffer.writeBytes(value.value().getBytes());
        }
        return buffer;
    }

    static ByteBuf toBytes(Permissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBytes(toBytes(permissions.global()));
        if (permissions.streams().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, StreamPermissions> entry :
                    permissions.streams().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    static ByteBuf toBytes(GlobalPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageServers());
        buffer.writeBoolean(permissions.readServers());
        buffer.writeBoolean(permissions.manageUsers());
        buffer.writeBoolean(permissions.readUsers());
        buffer.writeBoolean(permissions.manageStreams());
        buffer.writeBoolean(permissions.readStreams());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    static ByteBuf toBytes(StreamPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageStream());
        buffer.writeBoolean(permissions.readStream());
        buffer.writeBoolean(permissions.manageTopics());
        buffer.writeBoolean(permissions.readTopics());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());

        if (permissions.topics().isEmpty()) {
            buffer.writeByte(0);
        } else {
            for (Map.Entry<Long, TopicPermissions> entry : permissions.topics().entrySet()) {
                buffer.writeByte(1);
                buffer.writeIntLE(entry.getKey().intValue());
                buffer.writeBytes(toBytes(entry.getValue()));
            }
            buffer.writeByte(0);
        }

        return buffer;
    }

    static ByteBuf toBytes(TopicPermissions permissions) {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(permissions.manageTopic());
        buffer.writeBoolean(permissions.readTopic());
        buffer.writeBoolean(permissions.pollMessages());
        buffer.writeBoolean(permissions.sendMessages());
        return buffer;
    }

    static ByteBuf nameToBytes(String name) {
        ByteBuf buffer = Unpooled.buffer(1 + name.length());
        buffer.writeByte(name.length());
        buffer.writeBytes(name.getBytes());
        return buffer;
    }

    static ByteBuf toBytesAsU64(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 64: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(8, 8);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 9) {
            throw new IllegalArgumentException();
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(8, valueAsBytes.length));
        if (valueAsBytes.length < 8) {
            buffer.writeZero(8 - valueAsBytes.length);
        }
        return buffer;
    }

    public static ByteBuf toBytesAsU128(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 128: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(16, 16);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 17) {
            throw new IllegalArgumentException();
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(16, valueAsBytes.length));
        if (valueAsBytes.length < 16) {
            buffer.writeZero(16 - valueAsBytes.length);
        }
        return buffer;
    }
}
