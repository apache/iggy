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

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record Message(MessageHeader header, byte[] payload, Map<HeaderKey, HeaderValue> userHeaders) {

    /**
     * Creates a Message from JSON deserialization. Used by Jackson mixin.
     */
    public static Message of(MessageHeader header, byte[] payload, @Nullable List<HeaderEntry> userHeaders) {
        Map<HeaderKey, HeaderValue> headersMap = new HashMap<>();
        if (userHeaders != null) {
            for (HeaderEntry entry : userHeaders) {
                headersMap.put(entry.key(), entry.value());
            }
        }
        return new Message(header, payload, headersMap);
    }

    public static Message of(String payload) {
        return of(payload, Collections.emptyMap());
    }

    public static Message of(String payload, Map<HeaderKey, HeaderValue> userHeaders) {
        final byte[] payloadBytes = payload.getBytes();
        final long userHeadersLength = getUserHeadersSize(userHeaders);
        final MessageHeader msgHeader = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                userHeadersLength,
                (long) payloadBytes.length,
                BigInteger.ZERO);
        return new Message(msgHeader, payloadBytes, userHeaders);
    }

    public Message withUserHeaders(Map<HeaderKey, HeaderValue> userHeaders) {
        Map<HeaderKey, HeaderValue> mergedHeaders = mergeUserHeaders(userHeaders);
        long userHeadersLength = getUserHeadersSize(mergedHeaders);
        MessageHeader updatedHeader = new MessageHeader(
                header.checksum(),
                header.id(),
                header.offset(),
                header.timestamp(),
                header.originTimestamp(),
                userHeadersLength,
                (long) payload.length,
                header.reserved());
        return new Message(updatedHeader, payload, mergedHeaders);
    }

    public int getSize() {
        long userHeadersLength = getUserHeadersSize(userHeaders);
        return Math.toIntExact(MessageHeader.SIZE + payload.length + userHeadersLength);
    }

    private Map<HeaderKey, HeaderValue> mergeUserHeaders(Map<HeaderKey, HeaderValue> userHeaders) {
        if (userHeaders.isEmpty()) {
            return this.userHeaders;
        }

        if (this.userHeaders.isEmpty()) {
            return userHeaders;
        }

        Map<HeaderKey, HeaderValue> mergedHeaders = new HashMap<>(this.userHeaders);
        mergedHeaders.putAll(userHeaders);
        return mergedHeaders;
    }

    private static long getUserHeadersSize(Map<HeaderKey, HeaderValue> userHeaders) {
        if (userHeaders.isEmpty()) {
            return 0L;
        }

        long size = 0L;
        for (Map.Entry<HeaderKey, HeaderValue> entry : userHeaders.entrySet()) {
            byte[] keyBytes = entry.getKey().value();
            byte[] valueBytes = entry.getValue().value();
            size += 1L + 4L + keyBytes.length + 1L + 4L + valueBytes.length;
        }
        return size;
    }
}
