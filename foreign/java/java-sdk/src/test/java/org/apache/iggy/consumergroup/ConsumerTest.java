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

package org.apache.iggy.consumergroup;

import org.apache.iggy.consumergroup.Consumer.Kind;
import org.apache.iggy.identifier.ConsumerId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerTest {
    @Test
    void ofWithNumericIdCreatesExpectedConsumerWithConsumerKind() {
        var consumer = Consumer.of(123L);

        assertThat(consumer.id().getId()).isEqualTo(123L);
        assertThat(consumer.kind()).isEqualTo(Kind.Consumer);
    }

    @Test
    void ofWithConsumerIdCreatesExpectedConsumerWithConsumerKind() {
        var consumer = Consumer.of(ConsumerId.of(321L));

        assertThat(consumer.id().getId()).isEqualTo(321L);
        assertThat(consumer.kind()).isEqualTo(Consumer.Kind.Consumer);
    }

    @Test
    void groupWithNumericIdCreatesExpectedConsumerWithConsumerGroupKind() {
        var consumer = Consumer.group(456L);

        assertThat(consumer.id().getId()).isEqualTo(456L);
        assertThat(consumer.kind()).isEqualTo(Consumer.Kind.ConsumerGroup);
    }

    @Test
    void groupWithConsumerIdCreatesExpectedConsumerWithConsumerGroupKind() {
        var consumer = Consumer.group(ConsumerId.of(654L));

        assertThat(consumer.id().getId()).isEqualTo(654L);
        assertThat(consumer.kind()).isEqualTo(Consumer.Kind.ConsumerGroup);
    }

    @Nested
    class ConsumerKindTest {
        @ParameterizedTest
        @CsvSource({"Consumer, 1", "ConsumerGroup, 2"})
        void asCodeReturnsExpectedCode(Consumer.Kind kind, int expectedCode) {
            var code = kind.asCode();

            assertThat(code).isEqualTo(expectedCode);
        }
    }
}
