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

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BytesMessageIdTest {

    @Test
    void constructorThrowsIggyInvalidArgumentExceptionWhenInputNotExactly16Bytes() {
        assertThatThrownBy(() -> new BytesMessageId(new byte[15])).isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> new BytesMessageId(new byte[17])).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void toBigIntegerReturnsBigIntegerRepresentationOfBytes() {
        var bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10};
        var messageId = new BytesMessageId(bytes);

        var result = messageId.toBigInteger();

        assertThat(result).isEqualTo(BigInteger.TEN);
    }

    @Test
    void toBytesReturnsByteBufContainingIdValue() {
        var bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127};
        var messageId = new BytesMessageId(bytes);

        var result = messageId.toBytes();

        assertThat(result.array()).isEqualTo(bytes);
    }

    @Test
    void toStringReturnsStringRepresentationOfByteArray() {
        var bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
        var messageId = new BytesMessageId(bytes);

        var result = messageId.toString();

        assertThat(result).isEqualTo("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]");
    }
}
