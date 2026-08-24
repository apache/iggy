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

package org.apache.iggy.client.async.tcp.vsr;

import java.util.BitSet;
import java.util.Map;

/**
 * VSR {@code Operation} discriminants and the command-code mapping, mirroring
 * {@code core/binary_protocol/src/consensus/operation.rs} and the SDK-side
 * mapping in {@code core/sdk/src/vsr.rs}.
 */
public final class VsrOperation {

    public static final int RESERVED = 0;
    public static final int REGISTER = 1;
    public static final int NON_REPLICATED = 2;
    public static final int LOGOUT = 3;

    public static final int CREATE_TOPIC_WITH_ASSIGNMENTS = 64;
    public static final int CREATE_PARTITIONS_WITH_ASSIGNMENTS = 65;
    public static final int REMOVE_CONSUMER_GROUP_MEMBER = 66;
    public static final int COMPLETE_CONSUMER_GROUP_REVOCATION = 67;
    public static final int TRUNCATE_PARTITION = 68;

    public static final int CREATE_STREAM = 128;
    public static final int UPDATE_STREAM = 129;
    public static final int DELETE_STREAM = 130;
    public static final int PURGE_STREAM = 131;
    public static final int CREATE_TOPIC = 132;
    public static final int UPDATE_TOPIC = 133;
    public static final int DELETE_TOPIC = 134;
    public static final int PURGE_TOPIC = 135;
    public static final int CREATE_PARTITIONS = 136;
    public static final int DELETE_PARTITIONS = 137;
    public static final int DELETE_SEGMENTS = 138;
    public static final int CREATE_CONSUMER_GROUP = 139;
    public static final int DELETE_CONSUMER_GROUP = 140;
    public static final int CREATE_USER = 141;
    public static final int UPDATE_USER = 142;
    public static final int DELETE_USER = 143;
    public static final int CHANGE_PASSWORD = 144;
    public static final int UPDATE_PERMISSIONS = 145;
    public static final int CREATE_PERSONAL_ACCESS_TOKEN = 146;
    public static final int DELETE_PERSONAL_ACCESS_TOKEN = 147;
    public static final int JOIN_CONSUMER_GROUP = 148;
    public static final int LEAVE_CONSUMER_GROUP = 149;

    public static final int SEND_MESSAGES = 160;
    public static final int STORE_CONSUMER_OFFSET = 161;
    public static final int DELETE_CONSUMER_OFFSET = 162;

    private static final int INTERNAL_START = 64;
    private static final int METADATA_START = 128;
    private static final int PARTITION_START = 160;

    /**
     * Replicated command code to operation, from the server's
     * {@code COMMAND_TABLE}. Codes absent here travel as
     * {@link #NON_REPLICATED} with the code stamped into the header's
     * reserved bytes; the server is the authority on unknown codes.
     */
    private static final Map<Integer, Integer> REPLICATED_OPERATIONS = Map.ofEntries(
            Map.entry(33, CREATE_USER),
            Map.entry(34, DELETE_USER),
            Map.entry(35, UPDATE_USER),
            Map.entry(36, UPDATE_PERMISSIONS),
            Map.entry(37, CHANGE_PASSWORD),
            Map.entry(42, CREATE_PERSONAL_ACCESS_TOKEN),
            Map.entry(43, DELETE_PERSONAL_ACCESS_TOKEN),
            Map.entry(101, SEND_MESSAGES),
            Map.entry(121, STORE_CONSUMER_OFFSET),
            Map.entry(122, DELETE_CONSUMER_OFFSET),
            Map.entry(202, CREATE_STREAM),
            Map.entry(203, DELETE_STREAM),
            Map.entry(204, UPDATE_STREAM),
            Map.entry(205, PURGE_STREAM),
            Map.entry(302, CREATE_TOPIC),
            Map.entry(303, DELETE_TOPIC),
            Map.entry(304, UPDATE_TOPIC),
            Map.entry(305, PURGE_TOPIC),
            Map.entry(402, CREATE_PARTITIONS),
            Map.entry(403, DELETE_PARTITIONS),
            Map.entry(503, DELETE_SEGMENTS),
            Map.entry(602, CREATE_CONSUMER_GROUP),
            Map.entry(603, DELETE_CONSUMER_GROUP),
            Map.entry(604, JOIN_CONSUMER_GROUP),
            Map.entry(605, LEAVE_CONSUMER_GROUP));

    private static final int LOGOUT_USER_CODE = 39;

    private static final BitSet KNOWN_OPERATIONS = new BitSet();

    static {
        KNOWN_OPERATIONS.set(RESERVED, LOGOUT + 1);
        KNOWN_OPERATIONS.set(CREATE_TOPIC_WITH_ASSIGNMENTS, TRUNCATE_PARTITION + 1);
        KNOWN_OPERATIONS.set(CREATE_STREAM, LEAVE_CONSUMER_GROUP + 1);
        KNOWN_OPERATIONS.set(SEND_MESSAGES);
        KNOWN_OPERATIONS.set(STORE_CONSUMER_OFFSET);
        KNOWN_OPERATIONS.set(DELETE_CONSUMER_OFFSET);
    }

    private VsrOperation() {}

    /**
     * Maps a command code to its operation. Login codes are handled upstream
     * (they select {@link #REGISTER}); everything unmapped is forwarded as
     * {@link #NON_REPLICATED}.
     */
    static int operationForCode(int commandCode) {
        if (commandCode == LOGOUT_USER_CODE) {
            return LOGOUT;
        }
        return REPLICATED_OPERATIONS.getOrDefault(commandCode, NON_REPLICATED);
    }

    static boolean isMetadata(int operation) {
        if (operation >= INTERNAL_START && operation < METADATA_START) {
            return true;
        }
        // DeleteSegments replicates in its per-partition group, not the
        // metadata group, so it is excluded from the metadata band.
        if (operation == DELETE_SEGMENTS) {
            return false;
        }
        return operation >= METADATA_START && operation <= LEAVE_CONSUMER_GROUP;
    }

    static boolean isPartition(int operation) {
        return operation >= PARTITION_START;
    }

    /**
     * Whether a reply body for this operation starts with a committed result
     * section ({@code [count:u32][{index,result} x count]}).
     */
    static boolean isResultFramed(int operation) {
        return isMetadata(operation) || operation == STORE_CONSUMER_OFFSET || operation == DELETE_CONSUMER_OFFSET;
    }

    /**
     * The server controls the reply's operation byte; an undeclared value must
     * be rejected rather than routed through a predicate that happens to match.
     */
    static boolean isKnown(int operation) {
        return operation >= 0 && KNOWN_OPERATIONS.get(operation);
    }

    /**
     * Maps an internal operation returned by the server to the client
     * operation that initiated it. The server preserves the request id when
     * it enriches these commands before replication.
     */
    static int correlationOperation(int operation) {
        return switch (operation) {
            case CREATE_TOPIC_WITH_ASSIGNMENTS -> CREATE_TOPIC;
            case CREATE_PARTITIONS_WITH_ASSIGNMENTS -> CREATE_PARTITIONS;
            case TRUNCATE_PARTITION -> DELETE_SEGMENTS;
            default -> operation;
        };
    }
}
