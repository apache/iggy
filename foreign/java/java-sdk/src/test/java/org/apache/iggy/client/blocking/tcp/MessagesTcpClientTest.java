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
import org.apache.iggy.client.blocking.MessagesClientBaseTest;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class MessagesTcpClientTest extends MessagesClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    /*
     * The TCP client resolves balanced and key-based partitioning to an
     * explicit partition id before encoding the frame (the VSR broker routes
     * explicit partitions only), so messages with the same key must land on
     * the same partition.
     */

    @Test
    void shouldRouteSameMessageKeyToSamePartition() {
        setUpStreamAndTopic();

        var firstResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("first")));
        var secondResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("second")));

        assertThat(firstResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations().get(0).partitionId())
                .isEqualTo(firstResponse.confirmations().get(0).partitionId());
    }
}
