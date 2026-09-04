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
    FEATURE_UNAVAILABLE(5),
    INVALID_IDENTIFIER(6),
    RESOURCE_NOT_FOUND(20),
    STALE_CLIENT(30),

    // Authentication/Authorization errors
    UNAUTHENTICATED(40),
    UNAUTHORIZED(41),
    INVALID_CREDENTIALS(42),
    INVALID_USERNAME(43),
    INVALID_PASSWORD(44),
    INVALID_USER_STATUS(45),
    USER_ALREADY_EXISTS(46),
    USER_INACTIVE(47),
    CANNOT_DELETE_USER(48),
    CANNOT_CHANGE_PERMISSIONS(49),
    INVALID_PERSONAL_ACCESS_TOKEN_NAME(50),
    PERSONAL_ACCESS_TOKEN_ALREADY_EXISTS(51),
    PERSONAL_ACCESS_TOKENS_LIMIT_REACHED(52),
    INVALID_PERSONAL_ACCESS_TOKEN(53),
    PERSONAL_ACCESS_TOKEN_EXPIRED(54),
    TRANSIENT_NOT_COMMITTED(57),
    TRANSIENT_NOT_ACCEPTED(58),
    ACCESS_TOKEN_MISSING(77),
    INVALID_ACCESS_TOKEN(78),

    // Wire decoding errors
    INVALID_SIZE_BYTES(80),
    INVALID_UTF8(81),
    INVALID_NUMBER_ENCODING(82),
    INVALID_BOOLEAN_VALUE(83),
    INVALID_NUMBER_VALUE(84),

    // Client errors
    CLIENT_NOT_FOUND(100),

    // Stream errors
    CANNOT_CREATE_STREAM_DIRECTORY(1001),
    STREAM_ID_NOT_FOUND(1009),
    STREAM_NAME_NOT_FOUND(1010),
    STREAM_NAME_ALREADY_EXISTS(1012),
    INVALID_STREAM_NAME(1013),
    INVALID_STREAM_ID(1014),
    TOO_MANY_STREAMS(1020),

    // Topic errors
    CANNOT_CREATE_TOPIC_DIRECTORY(2001),
    TOPIC_ID_NOT_FOUND(2010),
    TOPIC_NAME_NOT_FOUND(2011),
    TOPIC_NAME_ALREADY_EXISTS(2013),
    INVALID_TOPIC_NAME(2014),
    TOO_MANY_PARTITIONS(2015),
    INVALID_TOPIC_ID(2016),
    INVALID_REPLICATION_FACTOR(2018),
    TOO_MANY_TOPICS(2021),

    // Partition errors
    PARTITION_NOT_FOUND(3007),
    PARTITION_ID_SPACE_EXHAUSTED(3013),
    TOO_MANY_CONSUMER_OFFSETS(3024),

    // Segment errors
    SEGMENT_NOT_FOUND(4000),
    SEGMENT_CLOSED(4001),
    INVALID_SEGMENT_SIZE(4002),
    CANNOT_CREATE_SEGMENT_LOG_FILE(4003),

    // Message errors
    TOO_BIG_MESSAGE_PAYLOAD(4022),
    TOO_MANY_MESSAGES(4023),
    EMPTY_MESSAGE_PAYLOAD(4024),
    INVALID_MESSAGE_CHECKSUM(4027),

    // Consumer group errors
    CONSUMER_GROUP_ID_NOT_FOUND(5000),
    INVALID_CONSUMER_GROUP_ID(5002),
    CONSUMER_GROUP_NAME_NOT_FOUND(5003),
    CONSUMER_GROUP_NAME_ALREADY_EXISTS(5004),
    INVALID_CONSUMER_GROUP_NAME(5005),
    CONSUMER_GROUP_MEMBER_NOT_FOUND(5006),

    // VSR protocol errors
    INCOMPATIBLE_PROTOCOL_VERSION(14003),

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
