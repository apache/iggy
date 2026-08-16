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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.TopicsClientBaseTest;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicsTcpClientTest extends TopicsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    @Test
    void shouldStoreAnOptionInItsCanonicalKind() {
        // Create admission re-encodes the block from its own parse, so a value
        // arrives back in the key's catalog kind whatever kind it was sent as.
        // Only the binary transport carries kinds: REST renders values as strings.
        var created = topicsClient.createTopic(
                STREAM_NAME,
                1L,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                "canonical-kind-topic",
                Map.of("enforce_fsync", HeaderValue.fromString("true")));

        var topic = topicsClient.getTopic(STREAM_NAME, TopicId.of(created.id())).orElseThrow();
        assertThat(topic.options().get("enforce_fsync").kind()).isEqualTo(HeaderKind.Bool);
    }

    @Test
    void shouldRejectAnOptionKeyOutsideTheCatalog() {
        assertThatThrownBy(() -> topicsClient.createTopic(
                        STREAM_NAME,
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        "bad-options-topic",
                        Map.of("not_a_real_option", HeaderValue.fromString("1"))))
                .isInstanceOf(RuntimeException.class);
    }
}
