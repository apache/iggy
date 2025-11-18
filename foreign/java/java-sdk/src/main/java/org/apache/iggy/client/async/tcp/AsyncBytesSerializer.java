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
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Async version of BytesSerializer for the async package.
 * Provides the same wire protocol serialization as the blocking version.
 */
public final class AsyncBytesSerializer {

    private AsyncBytesSerializer() {}

    /**
     * Serializes a Consumer to bytes.
     *
     * @param consumer The consumer to serialize (can be null)
     * @return ByteBuf containing the serialized consumer
     */
    public static ByteBuf toBytes(Consumer consumer) {
        ByteBuf buffer = Unpooled.buffer();
        if (consumer == null) {
            // No consumer - use code 0 (1 byte) and empty identifier (4 bytes)
            buffer.writeByte(0);
            buffer.writeIntLE(0);
        } else {
            buffer.writeByte(consumer.kind().asCode());
            buffer.writeBytes(toBytes(consumer.id()));
        }
        return buffer;
    }

    /**
     * Serializes an Identifier to bytes.
     *
     * @param identifier The identifier to serialize
     * @return ByteBuf containing the serialized identifier
     */
    public static ByteBuf toBytes(Identifier identifier) {
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

    /**
     * Serializes a String to bytes with length prefix.
     *
     * @param str The string to serialize
     * @return ByteBuf containing the serialized string
     */
    public static ByteBuf toBytes(String str) {
        ByteBuf buffer = Unpooled.buffer(1 + str.length());
        buffer.writeByte(str.length());
        buffer.writeBytes(str.getBytes());
        return buffer;
    }

    /**
     * Serializes a Partitioning to bytes.
     *
     * @param partitioning The partitioning to serialize
     * @return ByteBuf containing the serialized partitioning
     */
    public static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    /**
     * Serializes a Message to bytes.
     *
     * @param message The message to serialize
     * @return ByteBuf containing the serialized message
     */
    public static ByteBuf toBytes(Message message) {
        var buffer = Unpooled.buffer(MessageHeader.SIZE + message.payload().length);
        buffer.writeBytes(toBytes(message.header()));
        buffer.writeBytes(message.payload());
        return buffer;
    }

    /**
     * Serializes a MessageHeader to bytes.
     *
     * @param header The message header to serialize
     * @return ByteBuf containing the serialized header
     */
    public static ByteBuf toBytes(MessageHeader header) {
        var buffer = Unpooled.buffer(MessageHeader.SIZE);
        buffer.writeBytes(toBytesAsU64(header.checksum()));
        // Convert MessageId to BigInteger and serialize as U128
        buffer.writeBytes(toBytesAsU128(header.id().toBigInteger()));
        buffer.writeBytes(toBytesAsU64(header.offset()));
        buffer.writeBytes(toBytesAsU64(header.timestamp()));
        buffer.writeBytes(toBytesAsU64(header.originTimestamp()));
        buffer.writeIntLE(header.userHeadersLength().intValue());
        buffer.writeIntLE(header.payloadLength().intValue());
        return buffer;
    }

    /**
     * Serializes a PollingStrategy to bytes.
     *
     * @param strategy The polling strategy to serialize
     * @return ByteBuf containing the serialized strategy
     */
    public static ByteBuf toBytes(PollingStrategy strategy) {
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

    /**
     * Converts a BigInteger to bytes as unsigned 64-bit integer.
     *
     * @param value The BigInteger value to convert
     * @return ByteBuf containing the value as 8 bytes in little-endian format
     */
    public static ByteBuf toBytesAsU64(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 64: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(8);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 9) {
            throw new IllegalArgumentException("Value too large for U64: " + value);
        }
        // Handle sign byte if present
        if (valueAsBytes.length == 9 && valueAsBytes[0] == 0) {
            valueAsBytes = ArrayUtils.subarray(valueAsBytes, 1, 9);
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(8, valueAsBytes.length));
        if (valueAsBytes.length < 8) {
            buffer.writeZero(8 - valueAsBytes.length);
        }
        return buffer;
    }

    /**
     * Converts a name string to bytes with length prefix.
     *
     * @param name The name string to convert
     * @return ByteBuf containing the serialized name
     */
    public static ByteBuf nameToBytes(String name) {
        var buffer = Unpooled.buffer(1 + name.length());
        buffer.writeByte(name.length());
        buffer.writeBytes(name.getBytes(StandardCharsets.UTF_8));
        return buffer;
    }

    /**
     * Converts a BigInteger to bytes as unsigned 128-bit integer.
     *
     * @param value The BigInteger value to convert
     * @return ByteBuf containing the value as 16 bytes in little-endian format
     */
    public static ByteBuf toBytesAsU128(BigInteger value) {
        if (value.signum() == -1) {
            throw new IllegalArgumentException("Negative value cannot be serialized to unsigned 128: " + value);
        }
        ByteBuf buffer = Unpooled.buffer(16, 16);
        byte[] valueAsBytes = value.toByteArray();
        if (valueAsBytes.length > 17) {
            throw new IllegalArgumentException("Value too large for U128: " + value);
        }
        // Remove leading zero byte if present (from positive sign bit)
        if (valueAsBytes.length == 17 && valueAsBytes[0] == 0) {
            valueAsBytes = ArrayUtils.subarray(valueAsBytes, 1, 17);
        }
        ArrayUtils.reverse(valueAsBytes);
        buffer.writeBytes(valueAsBytes, 0, Math.min(16, valueAsBytes.length));
        if (valueAsBytes.length < 16) {
            buffer.writeZero(16 - valueAsBytes.length);
        }
        return buffer;
    }
}
