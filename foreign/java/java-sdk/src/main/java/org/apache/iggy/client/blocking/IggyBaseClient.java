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
     * that this operation is unsupported.
     *
     * @param code the command code
     * @param payload the command payload
     * @return the raw response payload
     */
    byte[] sendRawWithResponse(int code, byte[] payload);

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
