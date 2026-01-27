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

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Exception thrown when the server returns an error response.
 *
 * <p>This exception carries the error code, reason, and optional field information
 * from the server response. The factory methods automatically map error codes to
 * more specific exception subclasses where appropriate.
 */
public class IggyServerException extends IggyException {

    private static final Set<IggyErrorCode> NOT_FOUND_CODES = EnumSet.of(
            IggyErrorCode.RESOURCE_NOT_FOUND,
            IggyErrorCode.CANNOT_LOAD_RESOURCE,
            IggyErrorCode.STREAM_ID_NOT_FOUND,
            IggyErrorCode.STREAM_NAME_NOT_FOUND,
            IggyErrorCode.TOPIC_ID_NOT_FOUND,
            IggyErrorCode.TOPIC_NAME_NOT_FOUND,
            IggyErrorCode.PARTITION_NOT_FOUND,
            IggyErrorCode.SEGMENT_NOT_FOUND,
            IggyErrorCode.CLIENT_NOT_FOUND,
            IggyErrorCode.CONSUMER_GROUP_ID_NOT_FOUND,
            IggyErrorCode.CONSUMER_GROUP_NAME_NOT_FOUND,
            IggyErrorCode.CONSUMER_GROUP_NOT_JOINED,
            IggyErrorCode.MESSAGE_NOT_FOUND);

    private static final Set<IggyErrorCode> AUTHENTICATION_CODES = EnumSet.of(
            IggyErrorCode.UNAUTHENTICATED,
            IggyErrorCode.INVALID_CREDENTIALS,
            IggyErrorCode.INVALID_USERNAME,
            IggyErrorCode.INVALID_PASSWORD,
            IggyErrorCode.INVALID_PAT_TOKEN,
            IggyErrorCode.PASSWORD_DOES_NOT_MATCH,
            IggyErrorCode.PASSWORD_HASH_INTERNAL_ERROR);

    private static final Set<IggyErrorCode> AUTHORIZATION_CODES = EnumSet.of(IggyErrorCode.UNAUTHORIZED);

    private static final Set<IggyErrorCode> CONFLICT_CODES = EnumSet.of(
            IggyErrorCode.USER_ALREADY_EXISTS,
            IggyErrorCode.CLIENT_ALREADY_EXISTS,
            IggyErrorCode.STREAM_ALREADY_EXISTS,
            IggyErrorCode.TOPIC_ALREADY_EXISTS,
            IggyErrorCode.CONSUMER_GROUP_ALREADY_EXISTS,
            IggyErrorCode.PAT_NAME_ALREADY_EXISTS);

    private static final Set<IggyErrorCode> VALIDATION_CODES = EnumSet.of(
            IggyErrorCode.INVALID_COMMAND,
            IggyErrorCode.INVALID_FORMAT,
            IggyErrorCode.FEATURE_UNAVAILABLE,
            IggyErrorCode.CANNOT_PARSE_INT,
            IggyErrorCode.CANNOT_PARSE_SLICE,
            IggyErrorCode.CANNOT_PARSE_UTF8,
            IggyErrorCode.INVALID_STREAM_NAME,
            IggyErrorCode.CANNOT_CREATE_STREAM_DIRECTORY,
            IggyErrorCode.INVALID_TOPIC_NAME,
            IggyErrorCode.INVALID_REPLICATION_FACTOR,
            IggyErrorCode.CANNOT_CREATE_TOPIC_DIRECTORY,
            IggyErrorCode.CONSUMER_GROUP_MEMBER_NOT_FOUND,
            IggyErrorCode.INVALID_CONSUMER_GROUP_NAME,
            IggyErrorCode.TOO_MANY_MESSAGES,
            IggyErrorCode.EMPTY_MESSAGES,
            IggyErrorCode.TOO_BIG_MESSAGE,
            IggyErrorCode.INVALID_MESSAGE_CHECKSUM);

    private final IggyErrorCode errorCode;
    private final int rawErrorCode;
    private final String reason;
    private final Optional<String> field;
    private final Optional<String> errorId;

    /**
     * Constructs a new IggyServerException.
     *
     * @param errorCode the error code enum
     * @param rawErrorCode the raw numeric error code from the server
     * @param reason the error reason/message
     * @param field the optional field related to the error
     * @param errorId the optional error ID for correlation with server logs
     */
    public IggyServerException(
            IggyErrorCode errorCode,
            int rawErrorCode,
            String reason,
            Optional<String> field,
            Optional<String> errorId) {
        super(buildMessage(errorCode, rawErrorCode, reason, field, errorId));
        this.errorCode = errorCode;
        this.rawErrorCode = rawErrorCode;
        this.reason = reason;
        this.field = field;
        this.errorId = errorId;
    }

    /**
     * Constructs a new IggyServerException with just a status code.
     *
     * @param rawErrorCode the raw numeric error code from the server
     */
    public IggyServerException(int rawErrorCode) {
        this(IggyErrorCode.fromCode(rawErrorCode), rawErrorCode, "Server error", Optional.empty(), Optional.empty());
    }

    /**
     * Returns the error code enum.
     *
     * @return the error code
     */
    public IggyErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the raw numeric error code from the server.
     *
     * @return the raw error code
     */
    public int getRawErrorCode() {
        return rawErrorCode;
    }

    /**
     * Returns the error reason/message.
     *
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Returns the optional field related to the error.
     *
     * @return the field, if present
     */
    public Optional<String> getField() {
        return field;
    }

    /**
     * Returns the optional error ID for correlation with server logs.
     *
     * <p>This ID is only available for HTTP responses and can be used to find
     * the corresponding error in server logs.
     *
     * @return the error ID, if present
     */
    public Optional<String> getErrorId() {
        return errorId;
    }

    /**
     * Creates an appropriate exception from a TCP response.
     *
     * @param status the status code from the TCP response
     * @param payload the error payload bytes (may contain error message)
     * @return an appropriate IggyServerException subclass
     */
    public static IggyServerException fromTcpResponse(long status, byte[] payload) {
        int errorCode = (int) status;
        String reason =
                payload != null && payload.length > 0 ? new String(payload, StandardCharsets.UTF_8) : "Server error";
        return createFromCode(errorCode, reason, Optional.empty(), Optional.empty());
    }

    /**
     * Creates an appropriate exception from an HTTP response.
     *
     * @param id the error ID for correlation with server logs
     * @param code the error code string
     * @param reason the error reason
     * @param field the optional field related to the error
     * @return an appropriate IggyServerException subclass
     */
    public static IggyServerException fromHttpResponse(String id, String code, String reason, String field) {
        IggyErrorCode errorCode = IggyErrorCode.fromString(code);
        int rawCode = errorCode.getCode();
        if (rawCode == -1) {
            // Try parsing code as integer
            try {
                rawCode = Integer.parseInt(code);
            } catch (NumberFormatException e) {
                rawCode = -1;
            }
        }
        Optional<String> fieldOpt = Optional.ofNullable(StringUtils.stripToNull(field));
        Optional<String> errorIdOpt = Optional.ofNullable(StringUtils.stripToNull(id));
        return createFromCode(rawCode, reason != null ? reason : "Server error", fieldOpt, errorIdOpt);
    }

    private static IggyServerException createFromCode(
            int code, String reason, Optional<String> field, Optional<String> errorId) {
        IggyErrorCode errorCode = IggyErrorCode.fromCode(code);

        if (NOT_FOUND_CODES.contains(errorCode)) {
            return new IggyResourceNotFoundException(errorCode, code, reason, field, errorId);
        }
        if (AUTHENTICATION_CODES.contains(errorCode)) {
            return new IggyAuthenticationException(errorCode, code, reason, field, errorId);
        }
        if (AUTHORIZATION_CODES.contains(errorCode)) {
            return new IggyAuthorizationException(errorCode, code, reason, field, errorId);
        }
        if (CONFLICT_CODES.contains(errorCode)) {
            return new IggyConflictException(errorCode, code, reason, field, errorId);
        }
        if (VALIDATION_CODES.contains(errorCode)) {
            return new IggyValidationException(errorCode, code, reason, field, errorId);
        }

        return new IggyServerException(errorCode, code, reason, field, errorId);
    }

    private static String buildMessage(
            IggyErrorCode errorCode,
            int rawErrorCode,
            String reason,
            Optional<String> field,
            Optional<String> errorId) {
        StringBuilder sb = new StringBuilder();
        sb.append("Server error [code=").append(rawErrorCode);
        if (errorCode != IggyErrorCode.UNKNOWN) {
            sb.append(" (").append(errorCode.name()).append(")");
        }
        sb.append("]: ").append(reason);
        field.ifPresent(f -> sb.append(" (field: ").append(f).append(")"));
        errorId.ifPresent(id -> sb.append(" [errorId: ").append(id).append("]"));
        return sb.toString();
    }
}
