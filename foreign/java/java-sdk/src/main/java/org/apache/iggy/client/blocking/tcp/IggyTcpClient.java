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

import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.client.blocking.ConsumerOffsetsClient;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.PartitionsClient;
import org.apache.iggy.client.blocking.PersonalAccessTokensClient;
import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.UsersClient;

public class IggyTcpClient implements IggyBaseClient {

    private final UsersTcpClient usersClient;
    private final StreamsTcpClient streamsClient;
    private final TopicsTcpClient topicsClient;
    private final PartitionsTcpClient partitionsClient;
    private final ConsumerGroupsTcpClient consumerGroupsClient;
    private final ConsumerOffsetTcpClient consumerOffsetsClient;
    private final MessagesTcpClient messagesClient;
    private final SystemTcpClient systemClient;
    private final PersonalAccessTokensTcpClient personalAccessTokensClient;

    public IggyTcpClient(String host, Integer port) {
        InternalTcpClient tcpClient = new InternalTcpClient(host, port);
        tcpClient.connect();
        usersClient = new UsersTcpClient(tcpClient);
        streamsClient = new StreamsTcpClient(tcpClient);
        topicsClient = new TopicsTcpClient(tcpClient);
        partitionsClient = new PartitionsTcpClient(tcpClient);
        consumerGroupsClient = new ConsumerGroupsTcpClient(tcpClient);
        consumerOffsetsClient = new ConsumerOffsetTcpClient(tcpClient);
        messagesClient = new MessagesTcpClient(tcpClient);
        systemClient = new SystemTcpClient(tcpClient);
        personalAccessTokensClient = new PersonalAccessTokensTcpClient(tcpClient);
    }

    @Override
    public SystemClient system() {
        return systemClient;
    }

    @Override
    public StreamsClient streams() {
        return streamsClient;
    }

    @Override
    public UsersClient users() {
        return usersClient;
    }

    @Override
    public TopicsClient topics() {
        return topicsClient;
    }

    @Override
    public PartitionsClient partitions() {
        return partitionsClient;
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        return consumerGroupsClient;
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        return consumerOffsetsClient;
    }

    @Override
    public MessagesClient messages() {
        return messagesClient;
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        return personalAccessTokensClient;
    }

}
