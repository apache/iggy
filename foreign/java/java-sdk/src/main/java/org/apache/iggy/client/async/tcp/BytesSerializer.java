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

/**
 * Utility class for serializing objects to ByteBuf.
 */
public final class BytesSerializer {

    private BytesSerializer() {
    }

    /**
     * Serializes a Consumer to a ByteBuf.
     *
     * @param consumer the consumer
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytes(Consumer consumer) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(consumer.kind().asCode());
        buffer.writeBytes(toBytes(consumer.id()));
        return buffer;
    }

    /**
     * Serializes an Identifier to a ByteBuf.
     *
     * @param identifier the identifier
     * @return the serialized ByteBuf
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
     * Serializes a Partitioning to a ByteBuf.
     *
     * @param partitioning the partitioning
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytes(Partitioning partitioning) {
        ByteBuf buffer = Unpooled.buffer(2 + partitioning.value().length);
        buffer.writeByte(partitioning.kind().asCode());
        buffer.writeByte(partitioning.value().length);
        buffer.writeBytes(partitioning.value());
        return buffer;
    }

    /**
     * Serializes a Message to a ByteBuf.
     *
     * @param message the message
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytes(Message message) {
        var buffer = Unpooled.buffer(MessageHeader.SIZE + message.payload().length);
        buffer.writeBytes(toBytes(message.header()));
        buffer.writeBytes(message.payload());
        return buffer;
    }

    /**
     * Serializes a MessageHeader to a ByteBuf.
     *
     * @param header the message header
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytes(MessageHeader header) {
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

    /**
     * Serializes a PollingStrategy to a ByteBuf.
     *
     * @param strategy the polling strategy
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytes(PollingStrategy strategy) {
        var buffer = Unpooled.buffer(9);
        buffer.writeByte(strategy.kind().asCode());
        buffer.writeBytes(toBytesAsU64(strategy.value()));
        return buffer;
    }

    /**
     * Serializes a BigInteger to a ByteBuf as an unsigned 64-bit integer.
     *
     * @param value the value
     * @return the serialized ByteBuf
     */
    public static ByteBuf toBytesAsU64(BigInteger value) {
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
}