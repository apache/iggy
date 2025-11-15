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

/**
 * Main entry point for synchronous Iggy client operations.
 * This class provides access to the synchronous client functionality.
 */
public class IggyClient {

    private final IggyBaseClient baseClient;

    /**
     * Creates a new IggyClient with the specified base client.
     *
     * @param baseClient the base client
     */
    public IggyClient(IggyBaseClient baseClient) {
        this.baseClient = baseClient;
    }

    /**
     * Gets the base client.
     *
     * @return the base client
     */
    public IggyBaseClient getBaseClient() {
        return baseClient;
    }

    /**
     * Gets the system client.
     *
     * @return the system client
     */
    public SystemClient system() {
        return baseClient.system();
    }

    /**
     * Gets the streams client.
     *
     * @return the streams client
     */
    public StreamsClient streams() {
        return baseClient.streams();
    }

    /**
     * Gets the users client.
     *
     * @return the users client
     */
    public UsersClient users() {
        return baseClient.users();
    }

    /**
     * Gets the topics client.
     *
     * @return the topics client
     */
    public TopicsClient topics() {
        return baseClient.topics();
    }

    /**
     * Gets the partitions client.
     *
     * @return the partitions client
     */
    public PartitionsClient partitions() {
        return baseClient.partitions();
    }

    /**
     * Gets the consumer groups client.
     *
     * @return the consumer groups client
     */
    public ConsumerGroupsClient consumerGroups() {
        return baseClient.consumerGroups();
    }

    /**
     * Gets the consumer offsets client.
     *
     * @return the consumer offsets client
     */
    public ConsumerOffsetsClient consumerOffsets() {
        return baseClient.consumerOffsets();
    }

    /**
     * Gets the messages client.
     *
     * @return the messages client
     */
    public MessagesClient messages() {
        return baseClient.messages();
    }

    /**
     * Gets the personal access tokens client.
     *
     * @return the personal access tokens client
     */
    public PersonalAccessTokensClient personalAccessTokens() {
        return baseClient.personalAccessTokens();
    }
}
