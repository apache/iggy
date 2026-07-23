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
 * Status of a node in the cluster.
 */
public enum ClusterNodeStatus {
    @JsonProperty("healthy")
    Healthy(0),
    @JsonProperty("starting")
    Starting(1),
    @JsonProperty("stopping")
    Stopping(2),
    @JsonProperty("unreachable")
    Unreachable(3),
    @JsonProperty("maintenance")
    Maintenance(4);

    private final int code;

    ClusterNodeStatus(int code) {
        this.code = code;
    }

    public static ClusterNodeStatus fromCode(int code) {
        for (ClusterNodeStatus status : ClusterNodeStatus.values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IggyMalformedResponseException("Invalid cluster node status: " + code);
    }

    public int asCode() {
        return code;
    }
}
