package rs.iggy.user;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.Map;

public record Permissions(
        GlobalPermissions global,
        @JsonSetter(nulls = Nulls.AS_EMPTY)
        Map<Long, StreamPermissions> streams
) {
}
