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
 * Exception thrown when a TLS/SSL-related error occurs.
 *
 * <p>This includes errors during TLS context setup, certificate validation,
 * and SSL handshake failures.
 */
public class IggyTlsException extends IggyConnectionException {

    /**
     * Constructs a new IggyTlsException with the specified message.
     *
     * @param message the detail message
     */
    public IggyTlsException(String message) {
        super(message);
    }

    /**
     * Constructs a new IggyTlsException with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public IggyTlsException(String message, Throwable cause) {
        super(message, cause);
    }
}
