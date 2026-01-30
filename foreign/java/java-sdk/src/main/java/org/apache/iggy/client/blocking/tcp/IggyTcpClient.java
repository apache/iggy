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
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.user.IdentityInfo;

import java.io.File;
import java.time.Duration;
import java.util.Optional;

public class IggyTcpClient implements IggyBaseClient {

    private final String host;
    private final int port;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;
    private final Optional<String> username;
    private final Optional<String> password;

    private UsersTcpClient usersClient;
    private StreamsTcpClient streamsClient;
    private TopicsTcpClient topicsClient;
    private PartitionsTcpClient partitionsClient;
    private ConsumerGroupsTcpClient consumerGroupsClient;
    private ConsumerOffsetTcpClient consumerOffsetsClient;
    private MessagesTcpClient messagesClient;
    private SystemTcpClient systemClient;
    private PersonalAccessTokensTcpClient personalAccessTokensClient;

    public IggyTcpClient(String host, Integer port) {
        this(host, port, null, null, null, null, null, null, false, Optional.empty());
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    IggyTcpClient(
            String host,
            Integer port,
            String username,
            String password,
            Duration connectionTimeout,
            Duration requestTimeout,
            Integer connectionPoolSize,
            RetryPolicy retryPolicy,
            boolean enableTls,
            Optional<File> tlsCertificate) {
        this.host = host;
        this.port = port;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;
    }

    /**
     * Connects to the Iggy server.
     */
    public void connect() {
        InternalTcpClient tcpClient = new InternalTcpClient(host, port, enableTls, tlsCertificate);
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
        if (usersClient == null) {
            throw new IggyNotConnectedException();
        }
        if (username.isEmpty() || password.isEmpty()) {
            throw new IggyMissingCredentialsException();
        }
        return usersClient.login(username.get(), password.get());
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
