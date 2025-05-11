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

package org.apache.iggy.client.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class ConsumerOffsetsClientBaseTest extends IntegrationTest {

    ConsumerOffsetsClient consumerOffsetsClient;

    @BeforeEach
    void beforeEachBase() {
        consumerOffsetsClient = client.consumerOffsets();

        login();
        setUpStreamAndTopic();
    }

    @Test
    void shouldGetConsumerOffset() {
        // when
        var consumer = new Consumer(Consumer.Kind.Consumer, ConsumerId.of(1223L));
        consumerOffsetsClient.storeConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.empty(),
                consumer,
                BigInteger.ZERO);
        var consumerOffset = consumerOffsetsClient.getConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.of(1L),
                consumer);

        var nonExistingConsumerOffset = consumerOffsetsClient.getConsumerOffset(StreamId.of(42L),
                TopicId.of(42L),
                Optional.of(2L),
                new Consumer(
                        Consumer.Kind.Consumer, ConsumerId.of(123L)));

        // then
        assertThat(consumerOffset).isPresent();
        assertThat(nonExistingConsumerOffset).isEmpty();
    }

}
