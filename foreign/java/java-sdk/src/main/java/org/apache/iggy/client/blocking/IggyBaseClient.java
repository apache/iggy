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

public interface IggyBaseClient {

    /**
     * Sends a command code and payload and returns the raw response payload.
     *
     * <p>Session-control codes are rejected with an invalid-command error. HTTP clients report
     * that this operation is unsupported. Over TCP, partition commands carry a partition context
     * the client captures first, so a SendMessages payload must name an explicit partition id.
     *
     * @param code the command code
     * @param payload the command payload
     * @return the raw response payload
     */
    byte[] sendBinaryRequest(int code, byte[] payload);

    /**
     * Sends a command code and payload to the replica with the given id and returns its raw response payload.
     *
     * <p>The replica id is the one that cluster metadata names for the node. Every node answers non-replicated
     * commands such as GetStats from its own state, so this reads the state of the chosen node. Replicated commands,
     * PollMessages, GetConsumerOffset and session-control codes are rejected with an invalid-command error before
     * anything is sent. An id that cluster metadata does not name fails with an invalid-identifier error. A NotPrimary refusal is returned and
     * never sent to another replica. HTTP clients report that this operation is unsupported.
     *
     * @param code the command code
     * @param payload the command payload
     * @param replicaId the replica id that cluster metadata names
     * @return the raw response payload
     */
    byte[] sendBinaryRequestToReplica(int code, byte[] payload, int replicaId);

    SystemClient system();

    StreamsClient streams();

    UsersClient users();

    TopicsClient topics();

    PartitionsClient partitions();

    ConsumerGroupsClient consumerGroups();

    ConsumerOffsetsClient consumerOffsets();

    MessagesClient messages();

    PersonalAccessTokensClient personalAccessTokens();
}
