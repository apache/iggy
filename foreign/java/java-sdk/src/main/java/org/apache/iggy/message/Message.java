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

package org.apache.iggy.message;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public record Message(MessageHeader header, byte[] payload, Optional<Map<String, HeaderValue>> userHeaders) {

    public static Message of(String payload) {
        return of(payload, Optional.empty());
    }

    public static Message of(String payload, Optional<Map<String, HeaderValue>> userHeaders) {
        final byte[] payloadBytes = payload.getBytes();
        final long userHeadersLength = getUserHeadersSize(userHeaders);
        final MessageHeader msgHeader = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                userHeadersLength,
                (long) payloadBytes.length);
        return new Message(msgHeader, payloadBytes, userHeaders);
    }

    public Message withUserHeaders(Optional<Map<String, HeaderValue>> userHeaders) {
        Optional<Map<String, HeaderValue>> mergedHeaders = mergeUserHeaders(userHeaders);
        long userHeadersLength = getUserHeadersSize(mergedHeaders);
        MessageHeader updatedHeader = new MessageHeader(
                header.checksum(),
                header.id(),
                header.offset(),
                header.timestamp(),
                header.originTimestamp(),
                userHeadersLength,
                (long) payload.length);
        return new Message(updatedHeader, payload, mergedHeaders);
    }

    public int getSize() {
        long userHeadersLength = getUserHeadersSize(userHeaders);
        return Math.toIntExact(MessageHeader.SIZE + payload.length + userHeadersLength);
    }

    private Optional<Map<String, HeaderValue>> mergeUserHeaders(Optional<Map<String, HeaderValue>> userHeaders) {
        if (userHeaders.isEmpty()) {
            return this.userHeaders;
        }

        if (this.userHeaders.isEmpty()) {
            return userHeaders;
        }

        Map<String, HeaderValue> mergedHeaders = new HashMap<>(this.userHeaders.get());
        mergedHeaders.putAll(userHeaders.get());
        return Optional.of(mergedHeaders);
    }

    private static long getUserHeadersSize(Optional<Map<String, HeaderValue>> userHeaders) {
        if (userHeaders.isEmpty()) {
            return 0L;
        }

        long size = 0L;
        for (Map.Entry<String, HeaderValue> entry : userHeaders.get().entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes();
            byte[] valueBytes = entry.getValue().value().getBytes();
            size += 4L + keyBytes.length + 1L + 4L + valueBytes.length;
        }
        return size;
    }
}
