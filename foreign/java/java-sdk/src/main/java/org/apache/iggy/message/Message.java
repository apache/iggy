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
import java.util.Map;
import java.util.Optional;

public record Message(MessageHeader header, byte[] payload, Optional<Map<String, HeaderValue>> userHeaders) {

    public static Message of(String payload) {
        final byte[] payloadBytes = payload.getBytes();
        final MessageHeader msgHeader = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                0L,
                (long) payloadBytes.length);
        return new Message(msgHeader, payloadBytes, Optional.empty());
    }

    public int getSize() {
        // userHeaders is empty for now.
        return MessageHeader.SIZE + payload.length;
    }
}
