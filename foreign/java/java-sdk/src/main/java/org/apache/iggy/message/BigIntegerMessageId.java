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
import org.apache.iggy.client.blocking.tcp.BytesSerializer;

import java.math.BigInteger;

public class BigIntegerMessageId implements MessageId {

    private static final BigIntegerMessageId DEFAULT_ID = new BigIntegerMessageId(BigInteger.ZERO);
    private final BigInteger value;

    public BigIntegerMessageId(BigInteger value) {
        this.value = value;
    }

    public static BigIntegerMessageId defaultId() {
        return DEFAULT_ID;
    }

    @Override
    public BigInteger toBigInteger() {
        return value;
    }

    public ByteBuf toBytes() {
        return BytesSerializer.toBytesAsU128(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
