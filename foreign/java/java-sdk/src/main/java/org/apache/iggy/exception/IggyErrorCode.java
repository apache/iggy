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

import java.util.HashMap;
import java.util.Map;

/**
 * Error codes returned by the Iggy server.
 *
 * <p>These codes correspond to the error codes defined in the Iggy server's iggy_error.rs.
 */
public enum IggyErrorCode {
    // General errors
    ERROR(1),
    INVALID_COMMAND(3),
    INVALID_FORMAT(4),
    FEATURE_UNAVAILABLE(6),
    CANNOT_PARSE_INT(7),
    CANNOT_PARSE_SLICE(8),
    CANNOT_PARSE_UTF8(9),

    // Resource errors
    RESOURCE_NOT_FOUND(20),
    CANNOT_LOAD_RESOURCE(100),

    // Authentication/Authorization errors
    UNAUTHENTICATED(40),
    UNAUTHORIZED(41),
    INVALID_CREDENTIALS(42),
    INVALID_USERNAME(43),
    INVALID_PASSWORD(44),
    CLEAR_TEXT_PASSWORD_REQUIRED(45),
    USER_ALREADY_EXISTS(46),
    USER_INACTIVE(47),
    CANNOT_DELETE_USER_WITH_ACTIVE_PAT(48),
    CANNOT_UPDATE_OWN_PERMISSIONS(49),
    CANNOT_DELETE_YOURSELF(50),
    CLIENT_ALREADY_EXISTS(51),
    CLIENT_NOT_FOUND(52),
    INVALID_PAT_TOKEN(53),
    PAT_NAME_ALREADY_EXISTS(54),
    PASSWORD_DOES_NOT_MATCH(77),
    PASSWORD_HASH_INTERNAL_ERROR(78),

    // Stream errors
    STREAM_ID_NOT_FOUND(1009),
    STREAM_NAME_NOT_FOUND(1010),
    STREAM_ALREADY_EXISTS(1012),
    INVALID_STREAM_NAME(1013),
    CANNOT_CREATE_STREAM_DIRECTORY(1014),

    // Topic errors
    TOPIC_ID_NOT_FOUND(2010),
    TOPIC_NAME_NOT_FOUND(2011),
    TOPICS_COUNT_EXCEEDED(2012),
    TOPIC_ALREADY_EXISTS(2013),
    INVALID_TOPIC_NAME(2014),
    INVALID_REPLICATION_FACTOR(2015),
    CANNOT_CREATE_TOPIC_DIRECTORY(2016),

    // Partition errors
    PARTITION_NOT_FOUND(3007),

    // Consumer group errors
    CONSUMER_GROUP_ID_NOT_FOUND(5000),
    CONSUMER_GROUP_MEMBER_NOT_FOUND(5002),
    CONSUMER_GROUP_NAME_NOT_FOUND(5003),
    CONSUMER_GROUP_ALREADY_EXISTS(5004),
    INVALID_CONSUMER_GROUP_NAME(5005),
    CONSUMER_GROUP_NOT_JOINED(5006),

    // Segment errors
    SEGMENT_NOT_FOUND(4000),
    SEGMENT_CLOSED(4001),
    CANNOT_READ_SEGMENT(4002),
    CANNOT_SAVE_SEGMENT(4003),

    // Message errors
    TOO_MANY_MESSAGES(7000),
    EMPTY_MESSAGES(7001),
    TOO_BIG_MESSAGE(7002),
    INVALID_MESSAGE_CHECKSUM(7003),
    MESSAGE_NOT_FOUND(7004),

    // Unknown error code
    UNKNOWN(-1);

    private static final Map<Integer, IggyErrorCode> CODE_MAP = new HashMap<>();

    static {
        for (IggyErrorCode errorCode : values()) {
            CODE_MAP.put(errorCode.code, errorCode);
        }
    }

    private final int code;

    IggyErrorCode(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric error code.
     *
     * @return the error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the IggyErrorCode for the given numeric code.
     *
     * @param code the numeric error code
     * @return the corresponding IggyErrorCode, or UNKNOWN if not found
     */
    public static IggyErrorCode fromCode(int code) {
        return CODE_MAP.getOrDefault(code, UNKNOWN);
    }

    /**
     * Returns the IggyErrorCode for the given string code.
     *
     * @param code the string error code (can be numeric or enum name)
     * @return the corresponding IggyErrorCode, or UNKNOWN if not found
     */
    public static IggyErrorCode fromString(String code) {
        if (StringUtils.isBlank(code)) {
            return UNKNOWN;
        }
        try {
            int numericCode = Integer.parseInt(code);
            return fromCode(numericCode);
        } catch (NumberFormatException e) {
            try {
                return valueOf(code.toUpperCase().replace(".", "_").replace(" ", "_"));
            } catch (IllegalArgumentException ex) {
                return UNKNOWN;
            }
        }
    }
}
