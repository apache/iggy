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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.math.BigInteger;
import java.util.Arrays;

public class BytesMessageId implements MessageId {

    private final byte[] value;

    public BytesMessageId(byte[] value) {
        if (value.length != 16) {
            throw new IllegalArgumentException("Message id must have 16 bytes");
        }
        this.value = value;
    }

    @Override
    public BigInteger toBigInteger() {
        return new BigInteger(1, value);
    }

    @Override
    public ByteBuf toBytes() {
        return Unpooled.wrappedBuffer(value);
    }

    @Override
    public String toString() {
        return Arrays.toString(value);
    }
}
