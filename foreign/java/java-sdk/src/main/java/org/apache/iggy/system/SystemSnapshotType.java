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

package org.apache.iggy.system;

import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.util.List;

/**
 * A kind of diagnostic data the server can include in a snapshot. Codes match the server's wire values.
 */
public enum SystemSnapshotType {
    FilesystemOverview(1),
    ProcessList(2),
    ResourceUsage(3),
    Test(4),
    ServerLogs(5),
    ServerConfig(6),
    All(100);

    /** The TCP request encodes the number of types in one unsigned byte. */
    private static final int MAX_TYPES = 255;

    private final int code;

    SystemSnapshotType(int code) {
        this.code = code;
    }

    public int asCode() {
        return code;
    }

    /**
     * Copies the snapshot types after checking the count shared by the TCP and HTTP clients.
     *
     * @throws IggyInvalidArgumentException if there are more than 255 snapshot types
     */
    public static List<SystemSnapshotType> validatedCopyOf(List<SystemSnapshotType> types) {
        validateCount(types.size());
        List<SystemSnapshotType> copy = List.copyOf(types);
        validateCount(copy.size());
        return copy;
    }

    private static void validateCount(int count) {
        if (count > MAX_TYPES) {
            throw new IggyInvalidArgumentException("Too many snapshot types: " + count + " (max " + MAX_TYPES + ")");
        }
    }
}
