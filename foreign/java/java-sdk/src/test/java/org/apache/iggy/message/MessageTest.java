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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageTest {
    @Test
    void ofCreatesMessageWhenGivenMessageHeaderPayloadAndUserHeaders() {
        var userHeaders = List.of(new HeaderEntry(HeaderKey.fromString("foo"), HeaderValue.fromString("bar")));
        var payload = "abc".getBytes();
        var header = new MessageHeader(
                BigInteger.ZERO,
                new BytesMessageId(new byte[16]),
                BigInteger.ZERO,
                BigInteger.valueOf(1000),
                BigInteger.valueOf(1000),
                0L,
                (long) payload.length,
                BigInteger.ZERO);

        var message = Message.of(header, payload, userHeaders);

        assertEquals(header, message.header());
        assertEquals(payload, message.payload());
        assertEquals(1, message.userHeaders().size());
        assertTrue(message.userHeaders().containsKey(HeaderKey.fromString("foo")));
        assertEquals(HeaderValue.fromString("bar"), message.userHeaders().get(HeaderKey.fromString("foo")));
    }

    @Test
    void ofCreatesMessageWhenGivenMessageHeaderPayloadAndNullUserHeaders() {
        var payload = "abc".getBytes();
        var header = new MessageHeader(
                BigInteger.ZERO,
                new BytesMessageId(new byte[16]),
                BigInteger.ZERO,
                BigInteger.valueOf(1000),
                BigInteger.valueOf(1000),
                0L,
                (long) payload.length,
                BigInteger.ZERO);

        var message = Message.of(header, payload, null);

        assertEquals(header, message.header());
        assertEquals(payload, message.payload());
        assertEquals(0, message.userHeaders().size());
    }

    @Test
    void ofCreatesExpectedMessageWhenGivenPayloadOnly() {
        var message = Message.of("foo");

        assertArrayEquals(new byte[] {102, 111, 111}, message.payload());
        assertEquals(0, message.userHeaders().size());
        assertDefaultMessageHeaderValues(message.header());
        assertEquals(0L, message.header().userHeadersLength());
        assertEquals(3L, message.header().payloadLength());
    }

    @Test
    void ofCreatesMessageWhenGivenOnlyAPayloadAndUserHeaders() {
        var message = Message.of("foo", Map.of(HeaderKey.fromString("key"), HeaderValue.fromInt32(123)));

        assertArrayEquals(new byte[] {102, 111, 111}, message.payload());
        assertEquals(1, message.userHeaders().size());
        assertDefaultMessageHeaderValues(message.header());
        assertEquals(17L, message.header().userHeadersLength());
        assertEquals(3L, message.header().payloadLength());
    }

    @Test
    void withUserHeadersReturnsNewMessageWithMergedHeaders() {
        var originalMessage = Message.of("foo", Map.of(HeaderKey.fromString("k1"), HeaderValue.fromInt32(123)));

        var newMessage =
                originalMessage.withUserHeaders(Map.of(HeaderKey.fromString("k2"), HeaderValue.fromInt32(456)));

        assertNotSame(originalMessage, newMessage);
        assertArrayEquals(new byte[] {102, 111, 111}, newMessage.payload());
        assertEquals(2, newMessage.userHeaders().size());
        assertEquals(HeaderValue.fromInt32(123), newMessage.userHeaders().get(HeaderKey.fromString("k1")));
        assertEquals(HeaderValue.fromInt32(456), newMessage.userHeaders().get(HeaderKey.fromString("k2")));
        assertTrue(originalMessage.header().userHeadersLength()
                < newMessage.header().userHeadersLength());
    }

    @Test
    void withUserHeadersReturnsNewMessageWithProvidedUserHeadersWhenOriginalMessageHadNoHeaders() {
        var originalMessage = Message.of("foo");

        var newMessage =
                originalMessage.withUserHeaders(Map.of(HeaderKey.fromString("foo"), HeaderValue.fromString("bar")));

        assertNotSame(originalMessage, newMessage);
        assertArrayEquals(new byte[] {102, 111, 111}, newMessage.payload());
        assertEquals(1, newMessage.userHeaders().size());
        assertEquals(HeaderValue.fromString("bar"), newMessage.userHeaders().get(HeaderKey.fromString("foo")));
        assertTrue(originalMessage.header().userHeadersLength()
                < newMessage.header().userHeadersLength());
    }

    @Test
    void withUserHeadersReturnsNewIdenticalMessageProvidedHeadersAreEmpty() {
        var originalMessage = Message.of(
                "foo",
                Map.of(
                        HeaderKey.fromString("k1"),
                        HeaderValue.fromInt32(123),
                        HeaderKey.fromString("k2"),
                        HeaderValue.fromInt32(456)));

        var newMessage = originalMessage.withUserHeaders(Map.of());

        assertNotSame(originalMessage, newMessage);
        assertEquals(originalMessage, newMessage);
    }

    @Test
    void getSizeReturnsExpectedSizeWhenThereAreNoUserHeaders() {
        var message = Message.of("foo");

        assertEquals(67, message.getSize());
    }

    @Test
    void getSizeReturnsExpectedSizeWhenThereAreUserHeaders() {
        var message = Message.of("foo", Map.of(HeaderKey.fromString("k1"), HeaderValue.fromInt32(123)));

        assertEquals(83, message.getSize());
    }

    private void assertDefaultMessageHeaderValues(MessageHeader header) {
        assertEquals(BigInteger.ZERO, header.checksum());
        assertNotNull(header.id());
        assertEquals(BigInteger.ZERO, header.offset());
        assertEquals(BigInteger.ZERO, header.timestamp());
        assertEquals(BigInteger.ZERO, header.originTimestamp());
        assertEquals(BigInteger.ZERO, header.reserved());
    }
}
