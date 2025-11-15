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
 * Base interface for asynchronous Iggy clients.
 * This interface provides access to various asynchronous client components.
 */
public interface IggyBaseAsyncClient {

    /**
     * Gets the asynchronous messages client.
     *
     * @return the messages async client
     */
    MessagesAsyncClient messages();

    // Other async client interfaces can be added here as needed
    // For now, we're focusing on the message streaming functionality
}