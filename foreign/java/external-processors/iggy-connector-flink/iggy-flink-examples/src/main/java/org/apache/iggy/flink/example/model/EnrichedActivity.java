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
 * User activity enriched with profile information.
 */
public class EnrichedActivity implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String userId;
    private final String userName;
    private final String userTier;
    private final String country;
    private final String activityType;
    private final String resourceId;
    private final Instant timestamp;

    @JsonCreator
    public EnrichedActivity(
            @JsonProperty("userId") String userId,
            @JsonProperty("userName") String userName,
            @JsonProperty("userTier") String userTier,
            @JsonProperty("country") String country,
            @JsonProperty("activityType") String activityType,
            @JsonProperty("resourceId") String resourceId,
            @JsonProperty("timestamp") Instant timestamp) {
        this.userId = userId;
        this.userName = userName;
        this.userTier = userTier;
        this.country = country;
        this.activityType = activityType;
        this.resourceId = resourceId;
        this.timestamp = timestamp;
    }

    public static EnrichedActivity from(UserActivity activity, UserProfile profile) {
        return new EnrichedActivity(
                activity.getUserId(),
                profile.getUserName(),
                profile.getUserTier(),
                profile.getCountry(),
                activity.getActivityType(),
                activity.getResourceId(),
                activity.getTimestamp());
    }

    public String getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserTier() {
        return userTier;
    }

    public String getCountry() {
        return country;
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
        EnrichedActivity that = (EnrichedActivity) o;
        return Objects.equals(userId, that.userId)
                && Objects.equals(userName, that.userName)
                && Objects.equals(userTier, that.userTier)
                && Objects.equals(country, that.country)
                && Objects.equals(activityType, that.activityType)
                && Objects.equals(resourceId, that.resourceId)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, userName, userTier, country, activityType, resourceId, timestamp);
    }

    @Override
    public String toString() {
        return "EnrichedActivity{"
                + "userId='" + userId + '\''
                + ", userName='" + userName + '\''
                + ", userTier='" + userTier + '\''
                + ", country='" + country + '\''
                + ", activityType='" + activityType + '\''
                + ", resourceId='" + resourceId + '\''
                + ", timestamp=" + timestamp
                + '}';
    }
}
