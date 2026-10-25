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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.StreamClientBaseTest;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class StreamHttpClientTest extends StreamClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return HttpClientFactory.create(serverHost(), serverHttpPort());
    }

    @Test
    void givenHttpStreamOptionsWhenListingAndCreatingShouldPreserveBothBlocks() {
        // given
        byte[] key = new byte[EncryptionKeys.KEY_LENGTH];
        Arrays.fill(key, (byte) 3);

        // when
        var created = client.streams()
                .createStream(
                        "http-options-stream",
                        StreamOptions.builder().encryptionKey(key).build());
        trackStream(created.id());
        StreamBase listed = client.streams().getStreams().stream()
                .filter(stream -> stream.id().equals(created.id()))
                .findFirst()
                .orElseThrow();

        // then
        assertThat(created.options()).isEmpty();
        assertThat(listed.options()).isEmpty();
        assertThat(created.derivedOptions()).isEqualTo(listed.derivedOptions());
        assertThat(listed.derivedOptions())
                .containsKeys("encryption_key_source", "encryption_key_fingerprint", "encryption_kek_fingerprint");
        assertThat(listed.derivedOptions().values())
                .allSatisfy(value -> assertThat(value.kind()).isEqualTo(HeaderKind.String));
        assertThat(listed.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
    }
}
