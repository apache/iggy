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
