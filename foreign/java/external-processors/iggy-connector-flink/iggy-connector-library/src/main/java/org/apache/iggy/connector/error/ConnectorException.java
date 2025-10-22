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

package org.apache.iggy.connector.error;

/**
 * Base exception for all connector-related errors.
 * This exception hierarchy is framework-agnostic and can be used
 * across different stream processing integrations.
 */
public class ConnectorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final ErrorCode errorCode;
    private final boolean retryable;

    public ConnectorException(String message) {
        this(message, ErrorCode.UNKNOWN, false);
    }

    public ConnectorException(String message, Throwable cause) {
        this(message, cause, ErrorCode.UNKNOWN, false);
    }

    public ConnectorException(String message, ErrorCode errorCode, boolean retryable) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public ConnectorException(String message, Throwable cause, ErrorCode errorCode, boolean retryable) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    /**
     * Returns the error code associated with this exception.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Returns whether this error is retryable.
     * Retryable errors indicate transient failures that may succeed on retry.
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Error codes for connector exceptions.
     */
    public enum ErrorCode {
        /** Unknown or unclassified error. */
        UNKNOWN,
        /** Connection to Iggy server failed. */
        CONNECTION_FAILED,
        /** Authentication failed. */
        AUTHENTICATION_FAILED,
        /** Requested stream or topic not found. */
        RESOURCE_NOT_FOUND,
        /** Serialization or deserialization failed. */
        SERIALIZATION_ERROR,
        /** Offset management error. */
        OFFSET_ERROR,
        /** Partition discovery or assignment error. */
        PARTITION_ERROR,
        /** Configuration error. */
        CONFIGURATION_ERROR,
        /** Timeout waiting for operation. */
        TIMEOUT,
        /** Resource limit exceeded. */
        RESOURCE_EXHAUSTED
    }
}
