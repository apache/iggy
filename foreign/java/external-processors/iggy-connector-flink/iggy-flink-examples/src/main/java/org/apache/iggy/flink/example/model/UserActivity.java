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

package org.apache.iggy.flink.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * User activity event (clicks, views, purchases, etc.).
 */
public class UserActivity implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String userId;
    private final String activityType;
    private final String resourceId;
    private final Instant timestamp;

    @JsonCreator
    public UserActivity(
            @JsonProperty("userId") String userId,
            @JsonProperty("activityType") String activityType,
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("timestamp") Instant timestamp) {
        this.userId = userId;
        this.activityType = activityType;
        this.resourceId = resourceId;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    public String getUserId() {
        return userId;
    }

    public String getActivityType() {
        return activityType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserActivity that = (UserActivity) o;
        return Objects.equals(userId, that.userId)
                && Objects.equals(activityType, that.activityType)
                && Objects.equals(resourceId, that.resourceId)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, activityType, resourceId, timestamp);
    }

    @Override
    public String toString() {
        return "UserActivity{"
                + "userId='" + userId + '\''
                + ", activityType='" + activityType + '\''
                + ", resourceId='" + resourceId + '\''
                + ", timestamp=" + timestamp
                + '}';
    }
}
