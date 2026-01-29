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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

public class IggyHttpClient implements IggyBaseClient, Closeable {

    static final int DEFAULT_HTTP_PORT = 3000;

    private final InternalHttpClient internalHttpClient;
    private final SystemHttpClient systemClient;
    private final StreamsHttpClient streamsClient;
    private final UsersHttpClient usersClient;
    private final TopicsHttpClient topicsClient;
    private final PartitionsHttpClient partitionsClient;
    private final ConsumerGroupsHttpClient consumerGroupsClient;
    private final ConsumerOffsetsHttpClient consumerOffsetsClient;
    private final MessagesHttpClient messagesClient;
    private final PersonalAccessTokensHttpClient personalAccessTokensHttpClient;
    private final Optional<String> username;
    private final Optional<String> password;

    public IggyHttpClient(String url) {
        this(url, null, null, null, null, null);
    }

    IggyHttpClient(
            String url,
            String username,
            String password,
            Duration connectionTimeout,
            Duration requestTimeout,
            File tlsCertificate) {
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
        internalHttpClient = new InternalHttpClient(
                url,
                Optional.ofNullable(connectionTimeout),
                Optional.ofNullable(requestTimeout),
                Optional.ofNullable(tlsCertificate));
        systemClient = new SystemHttpClient(internalHttpClient);
        streamsClient = new StreamsHttpClient(internalHttpClient);
        usersClient = new UsersHttpClient(internalHttpClient);
        topicsClient = new TopicsHttpClient(internalHttpClient);
        partitionsClient = new PartitionsHttpClient(internalHttpClient);
        consumerGroupsClient = new ConsumerGroupsHttpClient(internalHttpClient);
        consumerOffsetsClient = new ConsumerOffsetsHttpClient(internalHttpClient);
        messagesClient = new MessagesHttpClient(internalHttpClient);
        personalAccessTokensHttpClient = new PersonalAccessTokensHttpClient(internalHttpClient);
    }

    /**
     * Creates a new builder for configuring IggyHttpClient.
     *
     * @return a new Builder instance
     */
    public static IggyHttpClientBuilder builder() {
        return new IggyHttpClientBuilder();
    }

    /**
     * Logs in using the credentials provided during client construction.
     *
     * @throws IggyMissingCredentialsException if no credentials were provided
     */
    public void login() {
        if (username.isEmpty() || password.isEmpty()) {
            throw new IggyMissingCredentialsException();
        }
        usersClient.login(username.get(), password.get());
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
        return personalAccessTokensHttpClient;
    }

    @Override
    public void close() throws IOException {
        internalHttpClient.close();
    }
}
