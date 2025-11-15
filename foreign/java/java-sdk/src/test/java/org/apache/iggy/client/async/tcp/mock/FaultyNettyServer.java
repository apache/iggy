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

package org.apache.iggy.client.async.tcp.mock;

import java.time.Duration;

/**
 * Faulty Netty server for testing specific error scenarios.
 * Extends MockNettyServer with predefined fault injection methods.
 */
public class FaultyNettyServer extends MockNettyServer {

    public FaultyNettyServer(int port) {
        super(port);
    }

    /**
     * Simulates a connection timeout by accepting connections but not responding to requests.
     */
    public void simulateConnectionTimeout() {
        setAcceptButNotRespond();
    }

    /**
     * Simulates a partial frame transmission by sending incomplete responses.
     */
    public void simulatePartialFrame() {
        setDropConnectionAfterBytes(4); // Send only part of the header
    }

    /**
     * Simulates a slow network by adding delay to responses.
     *
     * @param delay the delay duration
     */
    public void simulateSlowNetwork(Duration delay) {
        setResponseDelay(delay);
    }

    /**
     * Simulates a server error response.
     *
     * @param statusCode the error status code
     * @param message the error message
     */
    public void simulateServerError(int statusCode, String message) {
        setResponse(statusCode, message != null ? message.getBytes() : new byte[0]);
    }

    /**
     * Simulates a malformed response.
     */
    public void simulateMalformedResponse() {
        setSendMalformedResponse();
    }
}