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
import java.util.Optional;

/**
 * Exception thrown when the server returns an error response.
 *
 * <p>This exception carries the error code, reason, and optional field information
 * from the server response. The factory methods automatically map error codes to
 * more specific exception subclasses where appropriate.
 */
public class IggyServerException extends IggyException {

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
        this(IggyErrorCode.fromCode(rawErrorCode), rawErrorCode, "", Optional.empty(), Optional.empty());
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
            rawCode = parseIntOrDefault(code, rawCode);
        }
        Optional<String> fieldOpt = Optional.ofNullable(StringUtils.stripToNull(field));
        Optional<String> errorIdOpt = Optional.ofNullable(StringUtils.stripToNull(id));
        return createFromCode(rawCode, StringUtils.stripToEmpty(reason), fieldOpt, errorIdOpt);
    }

    private static int parseIntOrDefault(String code, int rawCode) {
        try {
            return Integer.parseInt(code);
        } catch (NumberFormatException e) {
            return rawCode;
        }
    }

    private static IggyServerException createFromCode(
            int code, String reason, Optional<String> field, Optional<String> errorId) {
        IggyErrorCode errorCode = IggyErrorCode.fromCode(code);

        if (IggyResourceNotFoundException.matches(errorCode)) {
            return new IggyResourceNotFoundException(errorCode, code, reason, field, errorId);
        }
        if (IggyAuthenticationException.matches(errorCode)) {
            return new IggyAuthenticationException(errorCode, code, reason, field, errorId);
        }
        if (IggyAuthorizationException.matches(errorCode)) {
            return new IggyAuthorizationException(errorCode, code, reason, field, errorId);
        }
        if (IggyConflictException.matches(errorCode)) {
            return new IggyConflictException(errorCode, code, reason, field, errorId);
        }
        if (IggyValidationException.matches(errorCode)) {
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
        sb.append("]");
        if (StringUtils.isNotBlank(reason)) {
            sb.append(": ").append(reason);
        }
        field.ifPresent(f -> sb.append(" (field: ").append(f).append(")"));
        errorId.ifPresent(id -> sb.append(" [errorId: ").append(id).append("]"));
        return sb.toString();
    }
}
