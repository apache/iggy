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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.message.BytesMessageId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.PolledMessages;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Utility class for deserializing ByteBuf to objects.
 */
public final class BytesDeserializer {

    private BytesDeserializer() {
    }

    /**
     * Deserializes a ByteBuf to a PolledMessages object.
     *
     * @param response the ByteBuf to deserialize
     * @return the deserialized PolledMessages
     */
    public static PolledMessages readPolledMessages(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var messagesCount = response.readUnsignedIntLE();
        var messages = new ArrayList<Message>();
        while (response.isReadable()) {
            messages.add(readPolledMessage(response));
        }
        return new PolledMessages(partitionId, currentOffset, messagesCount, messages);
    }

    /**
     * Deserializes a ByteBuf to a Message object.
     *
     * @param response the ByteBuf to deserialize
     * @return the deserialized Message
     */
    public static Message readPolledMessage(ByteBuf response) {
        var checksum = readU64AsBigInteger(response);
        var id = readBytesMessageId(response);
        var offset = readU64AsBigInteger(response);
        var timestamp = readU64AsBigInteger(response);
        var originTimestamp = readU64AsBigInteger(response);
        var userHeadersLength = response.readUnsignedIntLE();
        var payloadLength = response.readUnsignedIntLE();
        var header = new MessageHeader(checksum, id, offset, timestamp, originTimestamp,
                userHeadersLength, payloadLength);
        var payload = newByteArray(payloadLength);
        response.readBytes(payload);
        // TODO: Add support for user headers.
        return new Message(header, payload, Optional.empty());
    }

    /**
     * Reads an unsigned 64-bit integer from a ByteBuf as a BigInteger.
     *
     * @param buffer the ByteBuf to read from
     * @return the BigInteger value
     */
    private static BigInteger readU64AsBigInteger(ByteBuf buffer) {
        var bytesArray = new byte[8];
        buffer.readBytes(bytesArray, 0, 8);
        ArrayUtils.reverse(bytesArray);
        return new BigInteger(bytesArray);
    }

    /**
     * Reads a BytesMessageId from a ByteBuf.
     *
     * @param buffer the ByteBuf to read from
     * @return the BytesMessageId
     */
    private static BytesMessageId readBytesMessageId(ByteBuf buffer) {
        var bytesArray = new byte[16];
        buffer.readBytes(bytesArray);
        ArrayUtils.reverse(bytesArray);
        return new BytesMessageId(bytesArray);
    }

    /**
     * Creates a new byte array of the specified size.
     *
     * @param size the size of the array
     * @return the new byte array
     */
    private static byte[] newByteArray(Long size) {
        return new byte[size.intValue()];
    }
}