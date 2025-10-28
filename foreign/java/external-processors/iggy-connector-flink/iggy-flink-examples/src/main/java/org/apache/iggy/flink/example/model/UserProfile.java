package org.apache.iggy.flink.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * User profile information.
 */
public class UserProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String userId;
    private final String userName;
    private final String userTier;
    private final String country;

    @JsonCreator
    public UserProfile(
            @JsonProperty("userId") String userId,
            @JsonProperty("userName") String userName,
            @JsonProperty("userTier") String userTier,
            @JsonProperty("country") String country) {
        this.userId = userId;
        this.userName = userName;
        this.userTier = userTier;
        this.country = country;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserProfile that = (UserProfile) o;
        return Objects.equals(userId, that.userId)
                && Objects.equals(userName, that.userName)
                && Objects.equals(userTier, that.userTier)
                && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, userName, userTier, country);
    }

    @Override
    public String toString() {
        return "UserProfile{"
                + "userId='" + userId + '\''
                + ", userName='" + userName + '\''
                + ", userTier='" + userTier + '\''
                + ", country='" + country + '\''
                + '}';
    }
}
