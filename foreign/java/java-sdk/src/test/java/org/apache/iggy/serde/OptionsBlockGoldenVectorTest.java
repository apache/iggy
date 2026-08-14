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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cross-SDK golden vector for an options block.
 *
 * <p>Rust pins the identical bytes in {@code core/binary_protocol/src/primitives/options.rs}, as do
 * the Node and Go SDKs. Round-tripping a block through this SDK's own decoder proves nothing about
 * interoperability; these bytes are the contract, and a change to the TLV layout has to break every
 * copy of them together.
 *
 * <p>{@code enforce_fsync} (a one-byte {@code Bool}) and {@code segment_size} (an eight-byte
 * {@code Uint64}) cover both value widths, in the sorted key order every encoder has to produce.
 */
class OptionsBlockGoldenVectorTest {

    private static final byte[] GOLDEN_OPTIONS_BLOCK = {
        2, 13, 0, 0, 0, 'e', 'n', 'f', 'o', 'r', 'c', 'e', '_', 'f', 's', 'y', 'n', 'c', 3, 1, 0, 0, 0, 1, 2, 12, 0, 0,
        0, 's', 'e', 'g', 'm', 'e', 'n', 't', '_', 's', 'i', 'z', 'e', 12, 8, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0
    };

    @Test
    void shouldEncodeTheCrossSdkGoldenVector() {
        Map<HeaderKey, HeaderValue> options = new LinkedHashMap<>();
        options.put(HeaderKey.fromString("enforce_fsync"), HeaderValue.fromBool(true));
        options.put(HeaderKey.fromString("segment_size"), HeaderValue.fromUint64(BigInteger.valueOf(1_073_741_824L)));

        ByteBuf encoded = BytesSerializer.toBytes(options);

        byte[] bytes = new byte[encoded.readableBytes()];
        encoded.readBytes(bytes);
        assertThat(bytes).isEqualTo(GOLDEN_OPTIONS_BLOCK);
    }
}
