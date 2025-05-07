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

package rs.iggy.clients.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rs.iggy.topic.TopicDetails;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class PartitionsClientBaseTest extends IntegrationTest {

    TopicsClient topicsClient;
    PartitionsClient partitionsClient;

    @BeforeEach
    void beforeEachBase() {
        topicsClient = client.topics();
        partitionsClient = client.partitions();

        login();
        setUpStreamAndTopic();
    }


    @Test
    void shouldCreateAndDeletePartitions() {
        // given
        assert topicsClient.getTopic(42L, 42L).get().partitionsCount() == 1L;

        // when
        partitionsClient.createPartitions(42L, 42L, 10L);

        // then
        TopicDetails topic = topicsClient.getTopic(42L, 42L).get();
        assertThat(topic.partitionsCount()).isEqualTo(11L);

        // when
        partitionsClient.deletePartitions(42L, 42L, 10L);

        // then
        topic = topicsClient.getTopic(42L, 42L).get();
        assertThat(topic.partitionsCount()).isEqualTo(1L);
    }

}
