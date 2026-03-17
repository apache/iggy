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

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
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
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.user.IdentityInfo;

import java.io.Closeable;

public class IggyTcpClient implements IggyBaseClient, Closeable {

    private final AsyncIggyTcpClient asyncClient;

    private UsersClient usersClient;
    private StreamsClient streamsClient;
    private TopicsClient topicsClient;
    private PartitionsClient partitionsClient;
    private ConsumerGroupsClient consumerGroupsClient;
    private ConsumerOffsetsClient consumerOffsetsClient;
    private MessagesClient messagesClient;
    private SystemClient systemClient;
    private PersonalAccessTokensClient personalAccessTokensClient;

    public IggyTcpClient(String host, Integer port) {
        this(new AsyncIggyTcpClient(host, port));
    }

    IggyTcpClient(AsyncIggyTcpClient asyncClient) {
        this.asyncClient = asyncClient;
    }

    /**
     * Connects to the Iggy server.
     */
    public void connect() {
        FutureUtil.resolve(asyncClient.connect());
        usersClient = new UsersTcpClient(asyncClient.users());
        streamsClient = new StreamsTcpClient(asyncClient.streams());
        topicsClient = new TopicsTcpClient(asyncClient.topics());
        partitionsClient = new PartitionsTcpClient(asyncClient.partitions());
        consumerGroupsClient = new ConsumerGroupsTcpClient(asyncClient.consumerGroups());
        consumerOffsetsClient = new ConsumerOffsetsTcpClient(asyncClient.consumerOffsets());
        messagesClient = new MessagesTcpClient(asyncClient.messages());
        systemClient = new SystemTcpClient(asyncClient.system());
        personalAccessTokensClient = new PersonalAccessTokensTcpClient(asyncClient.personalAccessTokens());
    }

    /**
     * Creates a new builder for configuring IggyTcpClient.
     *
     * @return a new Builder instance
     */
    public static IggyTcpClientBuilder builder() {
        return new IggyTcpClientBuilder();
    }

    /**
     * Logs in using the credentials provided during client construction.
     *
     * @throws IggyMissingCredentialsException if no credentials were provided
     * @throws IggyNotConnectedException if client is not connected
     */
    public IdentityInfo login() {
        return FutureUtil.resolve(asyncClient.login());
    }

    @Override
    public void close() {
        FutureUtil.resolve(asyncClient.close());
    }

    @Override
    public SystemClient system() {
        if (systemClient == null) {
            throw new IggyNotConnectedException();
        }
        return systemClient;
    }

    @Override
    public StreamsClient streams() {
        if (streamsClient == null) {
            throw new IggyNotConnectedException();
        }
        return streamsClient;
    }

    @Override
    public UsersClient users() {
        if (usersClient == null) {
            throw new IggyNotConnectedException();
        }
        return usersClient;
    }

    @Override
    public TopicsClient topics() {
        if (topicsClient == null) {
            throw new IggyNotConnectedException();
        }
        return topicsClient;
    }

    @Override
    public PartitionsClient partitions() {
        if (partitionsClient == null) {
            throw new IggyNotConnectedException();
        }
        return partitionsClient;
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        if (consumerGroupsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerGroupsClient;
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        if (consumerOffsetsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerOffsetsClient;
    }

    @Override
    public MessagesClient messages() {
        if (messagesClient == null) {
            throw new IggyNotConnectedException();
        }
        return messagesClient;
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        if (personalAccessTokensClient == null) {
            throw new IggyNotConnectedException();
        }
        return personalAccessTokensClient;
    }
}
