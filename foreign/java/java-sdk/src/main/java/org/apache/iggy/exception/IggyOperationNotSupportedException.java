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

package org.apache.iggy.exception;

/**
 * Exception thrown when an operation is not supported by the current client implementation.
 *
 * <p>For example, certain operations may only be available via TCP and not HTTP.
 */
public class IggyOperationNotSupportedException extends IggyException {

    private final String operation;
    private final String clientType;

    /**
     * Constructs a new IggyOperationNotSupportedException.
     *
     * @param operation the operation that is not supported
     * @param clientType the type of client (e.g., "HTTP", "TCP")
     */
    public IggyOperationNotSupportedException(String operation, String clientType) {
        super("Operation '" + operation + "' is not supported by the " + clientType + " client");
        this.operation = operation;
        this.clientType = clientType;
    }

    /**
     * Constructs a new IggyOperationNotSupportedException with a custom message.
     *
     * @param message the detail message
     */
    public IggyOperationNotSupportedException(String message) {
        super(message);
        this.operation = null;
        this.clientType = null;
    }

    /**
     * Returns the operation that is not supported.
     *
     * @return the operation name, or null if not specified
     */
    public String getOperation() {
        return operation;
    }

    /**
     * Returns the client type that doesn't support the operation.
     *
     * @return the client type, or null if not specified
     */
    public String getClientType() {
        return clientType;
    }
}
