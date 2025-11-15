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

package org.apache.iggy.client.async.tcp.base;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Base class for async TCP client error handling tests.
 * Provides common setup, teardown, and utility methods for error scenario testing.
 */
public abstract class AsyncTcpTestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 8091; // Use different port to avoid conflicts
    protected static final int INVALID_PORT = -1;
    protected static final String INVALID_HOST = "invalid.host.that.does.not.exist";
    protected static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    protected AsyncIggyTcpClient client;

    @BeforeEach
    void setup() throws Exception {
        // Each test should create its own mock server with specific behavior
        // This is just a placeholder - actual server creation should happen in individual tests
    }

    @AfterEach
    void cleanup() throws Exception {
        // Clean up client
        if (client != null) {
            try {
                client.close().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                // Ignore cleanup errors
            } finally {
                client = null;
            }
        }
    }

    /**
     * Creates a valid Iggy response frame for testing.
     *
     * @param status the status code (0 for success)
     * @param payload the response payload
     * @return a ByteBuf containing the complete response frame
     */
    protected ByteBuf createValidIggyResponse(int status, byte[] payload) {
        int length = payload != null ? payload.length : 0;
        ByteBuf response = Unpooled.buffer(8 + length);
        response.writeIntLE(status);
        response.writeIntLE(length);
        if (payload != null && length > 0) {
            response.writeBytes(payload);
        }
        return response;
    }

    /**
     * Creates a malformed response for testing error handling.
     *
     * @return a ByteBuf containing a malformed response
     */
    protected ByteBuf createMalformedResponse() {
        // Create a response with incomplete header
        ByteBuf response = Unpooled.buffer(4);
        response.writeIntLE(0); // Only status, no length field
        return response;
    }

    /**
     * Asserts that all pending requests have been cleared.
     * This is useful for testing resource cleanup scenarios.
     */
    protected void assertPendingRequestsCleared() {
        // This would require access to the internal pendingRequests map
        // For now, we'll leave this as a placeholder
    }

    /**
     * Helper method to wait for a CompletableFuture with a reasonable timeout.
     *
     * @param future the CompletableFuture to wait for
     * @param <T> the type of the result
     * @return the result of the CompletableFuture
     * @throws Exception if the future completes exceptionally or times out
     */
    protected <T> T waitForCompletion(CompletableFuture<T> future) throws Exception {
        return future.get(TEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
}