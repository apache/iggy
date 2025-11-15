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

package org.apache.iggy.client.async;

/**
 * Main entry point for asynchronous Iggy client operations.
 * This class provides access to the asynchronous client functionality.
 */
public class IggyAsyncClient {

    private final IggyBaseAsyncClient baseClient;

    /**
     * Creates a new IggyAsyncClient with the specified base client.
     *
     * @param baseClient the base async client
     */
    public IggyAsyncClient(IggyBaseAsyncClient baseClient) {
        this.baseClient = baseClient;
    }

    /**
     * Gets the base async client.
     *
     * @return the base async client
     */
    public IggyBaseAsyncClient getBaseClient() {
        return baseClient;
    }

    /**
     * Gets the asynchronous messages client.
     *
     * @return the messages async client
     */
    public MessagesAsyncClient messages() {
        return baseClient.messages();
    }

    // Additional async client methods will be added here as they are implemented
}
