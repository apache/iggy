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

package org.apache.iggy.client.async.tcp.vsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class VsrLoginCodecTest {

    @Test
    void sdkVersionFieldEncodesVersionAsUtf8() {
        assertThat(VsrLoginCodec.sdkVersionField("0.9.0-SNAPSHOT"))
                .isEqualTo("0.9.0-SNAPSHOT".getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    void sdkVersionFieldFallsBackToUnknownWhenVersionIsMissing(String version) {
        assertThat(VsrLoginCodec.sdkVersionField(version)).isEqualTo("unknown".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sdkVersionFieldTruncatesOnCodePointBoundaryWithinU8Prefix() {
        String version = "é".repeat(300);

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(254);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo("é".repeat(127));
    }

    @Test
    void sdkVersionFieldKeepsSurrogatePairThatEndsExactlyAtU8Prefix() {
        String version = "a".repeat(251) + "\uD83D\uDE00";
        assertThat(version.getBytes(StandardCharsets.UTF_8)).hasSize(255);

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(255);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo(version);
    }

    @Test
    void sdkVersionFieldDropsWholeSurrogatePairThatWouldStraddleU8Prefix() {
        String version = "a".repeat(252) + "\uD83D\uDE00";

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(252);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo("a".repeat(252));
    }
}
