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

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(message.header()).isEqualTo(header);
        assertThat(message.payload()).isEqualTo(payload);
        assertThat(message.userHeaders().size()).isEqualTo(1);
        assertThat(message.userHeaders().containsKey(HeaderKey.fromString("foo")))
                .isTrue();
        assertThat(message.userHeaders().get(HeaderKey.fromString("foo"))).isEqualTo(HeaderValue.fromString("bar"));
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

        assertThat(message.header()).isEqualTo(header);
        assertThat(message.payload()).isEqualTo(payload);
        assertThat(message.userHeaders().size()).isEqualTo(0);
    }

    @Test
    void ofCreatesExpectedMessageWhenGivenPayloadOnly() {
        var message = Message.of("foo");

        assertThat(message.payload()).isEqualTo(new byte[] {102, 111, 111});
        assertThat(message.userHeaders().size()).isEqualTo(0);
        assertDefaultMessageHeaderValues(message.header());
        assertThat(message.header().userHeadersLength()).isEqualTo(0L);
        assertThat(message.header().payloadLength()).isEqualTo(3L);
    }

    @Test
    void ofCreatesMessageWhenGivenOnlyAPayloadAndUserHeaders() {
        var message = Message.of("foo", Map.of(HeaderKey.fromString("key"), HeaderValue.fromInt32(123)));

        assertThat(message.payload()).isEqualTo(new byte[] {102, 111, 111});
        assertThat(message.userHeaders().size()).isEqualTo(1);
        assertDefaultMessageHeaderValues(message.header());
        assertThat(message.header().userHeadersLength()).isEqualTo(17L);
        assertThat(message.header().payloadLength()).isEqualTo(3L);
    }

    @Test
    void withUserHeadersReturnsNewMessageWithMergedHeaders() {
        var originalMessage = Message.of("foo", Map.of(HeaderKey.fromString("k1"), HeaderValue.fromInt32(123)));

        var newMessage =
                originalMessage.withUserHeaders(Map.of(HeaderKey.fromString("k2"), HeaderValue.fromInt32(456)));

        assertThat(newMessage).isNotSameAs(originalMessage);
        assertThat(newMessage.payload()).isEqualTo(new byte[] {102, 111, 111});
        assertThat(newMessage.userHeaders().size()).isEqualTo(2);
        assertThat(newMessage.userHeaders().get(HeaderKey.fromString("k1"))).isEqualTo(HeaderValue.fromInt32(123));
        assertThat(newMessage.userHeaders().get(HeaderKey.fromString("k2"))).isEqualTo(HeaderValue.fromInt32(456));
        assertThat(originalMessage.header().userHeadersLength())
                .isLessThan(newMessage.header().userHeadersLength());
    }

    @Test
    void withUserHeadersReturnsNewMessageWithProvidedUserHeadersWhenOriginalMessageHadNoHeaders() {
        var originalMessage = Message.of("foo");

        var newMessage =
                originalMessage.withUserHeaders(Map.of(HeaderKey.fromString("foo"), HeaderValue.fromString("bar")));

        assertThat(newMessage).isNotSameAs(originalMessage);
        assertThat(newMessage.payload()).isEqualTo(new byte[] {102, 111, 111});
        assertThat(newMessage.userHeaders().size()).isEqualTo(1);
        assertThat(newMessage.userHeaders().get(HeaderKey.fromString("foo"))).isEqualTo(HeaderValue.fromString("bar"));
        assertThat(originalMessage.header().userHeadersLength())
                .isLessThan(newMessage.header().userHeadersLength());
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

        assertThat(newMessage).isNotSameAs(originalMessage);
        assertThat(newMessage).isEqualTo(originalMessage);
    }

    @Test
    void getSizeReturnsExpectedSizeWhenThereAreNoUserHeaders() {
        var message = Message.of("foo");

        assertThat(message.getSize()).isEqualTo(67);
    }

    @Test
    void getSizeReturnsExpectedSizeWhenThereAreUserHeaders() {
        var message = Message.of("foo", Map.of(HeaderKey.fromString("k1"), HeaderValue.fromInt32(123)));

        assertThat(message.getSize()).isEqualTo(83);
    }

    private void assertDefaultMessageHeaderValues(MessageHeader header) {
        assertThat(header.checksum()).isEqualTo(BigInteger.ZERO);
        assertThat(header.id()).isNotNull();
        assertThat(header.offset()).isEqualTo(BigInteger.ZERO);
        assertThat(header.timestamp()).isEqualTo(BigInteger.ZERO);
        assertThat(header.originTimestamp()).isEqualTo(BigInteger.ZERO);
        assertThat(header.reserved()).isEqualTo(BigInteger.ZERO);
    }
}
