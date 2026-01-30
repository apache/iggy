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
 * Exception thrown when credentials are required but not provided.
 *
 * <p>This is thrown when attempting to login without providing credentials,
 * either through the builder or the login method.
 */
public class IggyMissingCredentialsException extends IggyClientException {

    private static final String DEFAULT_MESSAGE =
            "No credentials provided. Use login(username, password) or provide credentials when building the client.";

    /**
     * Constructs a new IggyMissingCredentialsException with a default message.
     */
    public IggyMissingCredentialsException() {
        super(DEFAULT_MESSAGE);
    }

    /**
     * Constructs a new IggyMissingCredentialsException with the specified message.
     *
     * @param message the detail message
     */
    public IggyMissingCredentialsException(String message) {
        super(message);
    }
}
