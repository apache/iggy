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
 * Base exception class for all Iggy SDK exceptions.
 *
 * <p>This is the root of the Iggy exception hierarchy. All exceptions thrown by the SDK
 * extend this class, allowing callers to catch all Iggy-related exceptions with a single
 * catch block if desired.
 */
public abstract class IggyException extends RuntimeException {

    /**
     * Constructs a new IggyException with the specified message.
     *
     * @param message the detail message
     */
    protected IggyException(String message) {
        super(message);
    }

    /**
     * Constructs a new IggyException with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    protected IggyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new IggyException with the specified cause.
     *
     * @param cause the cause of the exception
     */
    protected IggyException(Throwable cause) {
        super(cause);
    }
}
