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

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Exception thrown when a resource conflict occurs.
 *
 * <p>This corresponds to 409-type errors, such as trying to create a resource
 * that already exists (stream, topic, user, consumer group, etc.).
 */
public class IggyConflictException extends IggyServerException {

    private static final Set<IggyErrorCode> CODES = EnumSet.of(
            IggyErrorCode.USER_ALREADY_EXISTS,
            IggyErrorCode.CLIENT_ALREADY_EXISTS,
            IggyErrorCode.STREAM_ALREADY_EXISTS,
            IggyErrorCode.TOPIC_ALREADY_EXISTS,
            IggyErrorCode.CONSUMER_GROUP_ALREADY_EXISTS,
            IggyErrorCode.PAT_NAME_ALREADY_EXISTS);

    /**
     * Constructs a new IggyConflictException.
     *
     * @param errorCode the error code enum
     * @param rawErrorCode the raw numeric error code from the server
     * @param reason the error reason/message
     * @param field the optional field related to the error
     * @param errorId the optional error ID for correlation with server logs
     */
    public IggyConflictException(
            IggyErrorCode errorCode,
            int rawErrorCode,
            String reason,
            Optional<String> field,
            Optional<String> errorId) {
        super(errorCode, rawErrorCode, reason, field, errorId);
    }

    /**
     * Returns whether the given error code should map to this exception type.
     *
     * @param code the error code to check
     * @return true if this exception type handles the given error code
     */
    public static boolean matches(IggyErrorCode code) {
        return CODES.contains(code);
    }
}
