package rs.iggy.user;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum UserStatus {
    @JsonProperty("active")
    Active(1),
    @JsonProperty("inactive")
    Inactive(2);

    private final int code;

    UserStatus(int code) {
        this.code = code;
    }

    public static UserStatus fromCode(int code) {
        for (UserStatus userStatus : UserStatus.values()) {
            if (userStatus.code == code) {
                return userStatus;
            }
        }
        throw new IllegalArgumentException("Invalid user status: " + code);
    }

    public int asCode() {
        return code;
    }
}
