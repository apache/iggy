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
 * Base interface for synchronous Iggy clients.
 * This interface provides access to various synchronous client components.
 */
public interface IggyBaseClient {

    /**
     * Gets the system client.
     *
     * @return the system client
     */
    SystemClient system();

    /**
     * Gets the streams client.
     *
     * @return the streams client
     */
    StreamsClient streams();

    /**
     * Gets the users client.
     *
     * @return the users client
     */
    UsersClient users();

    /**
     * Gets the topics client.
     *
     * @return the topics client
     */
    TopicsClient topics();

    /**
     * Gets the partitions client.
     *
     * @return the partitions client
     */
    PartitionsClient partitions();

    /**
     * Gets the consumer groups client.
     *
     * @return the consumer groups client
     */
    ConsumerGroupsClient consumerGroups();

    /**
     * Gets the consumer offsets client.
     *
     * @return the consumer offsets client
     */
    ConsumerOffsetsClient consumerOffsets();

    /**
     * Gets the messages client.
     *
     * @return the messages client
     */
    MessagesClient messages();

    /**
     * Gets the personal access tokens client.
     *
     * @return the personal access tokens client
     */
    PersonalAccessTokensClient personalAccessTokens();

}
