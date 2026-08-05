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

package org.apache.iggy.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iggy.exception.IggyMalformedResponseException;

/**
 * Role of a node in the cluster.
 */
public enum ClusterNodeRole {
    @JsonProperty("leader")
    Leader(0),
    @JsonProperty("follower")
    Follower(1);

    private final int code;

    ClusterNodeRole(int code) {
        this.code = code;
    }

    public static ClusterNodeRole fromCode(int code) {
        for (ClusterNodeRole role : ClusterNodeRole.values()) {
            if (role.code == code) {
                return role;
            }
        }
        throw new IggyMalformedResponseException("Invalid cluster node role: " + code);
    }

    public int asCode() {
        return code;
    }
}
